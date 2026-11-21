from __future__ import annotations

import asyncio
import logging
import math
import uuid
from collections.abc import Callable
from typing import Any

from volume_mm_robot.brokers import Broker, FillCallback, OrderCallback, OrderRejected, PaperBroker
from volume_mm_robot.config import AppConfig
from volume_mm_robot.detector import VolumeDetector
from volume_mm_robot.journal import Journal
from volume_mm_robot.models import (
    Fill,
    FlowSignal,
    MarketRules,
    Order,
    OrderBook,
    OrderPurpose,
    OrderRequest,
    OrderStatus,
    Position,
    RuntimeStats,
    Side,
    Trade,
)

logger = logging.getLogger(__name__)
BrokerFactory = Callable[[FillCallback, OrderCallback, Position], Broker]


class MarketMakingRobot:
    def __init__(
        self,
        config: AppConfig,
        rules: MarketRules,
        journal: Journal,
        broker_factory: BrokerFactory,
        initial_position: Position | None = None,
    ) -> None:
        self.config = config
        self.rules = rules
        self.journal = journal
        self.detector = VolumeDetector(config.strategy)
        self.position = initial_position or Position()
        self.stats = RuntimeStats()
        self.book: OrderBook | None = None
        self.broker = broker_factory(self._on_fill, self._on_order, self.position)
        self.halted = False
        self.cooldown_until_ms = 0
        self._last_timestamp = 0
        self._last_tick = -100
        self._last_reconcile: int | None = None
        self._last_stop_attempt: int | None = None
        self._stopping_position = False
        self._reconciled = True
        self._last_signal: FlowSignal | None = None
        self._seen_round_trips = self.position.round_trips
        self._lock = asyncio.Lock()

    async def handle(self, event: Trade | OrderBook, received_at_ms: int | None = None) -> None:
        async with self._lock:
            if event.symbol != self.config.symbol:
                raise ValueError(f"Получен инструмент {event.symbol}, ожидался {self.config.symbol}")
            timestamp_ms = event.timestamp_ms if received_at_ms is None else received_at_ms
            self._last_timestamp = max(self._last_timestamp, timestamp_ms)
            self.journal.raw(event, received_at_ms=received_at_ms)
            if isinstance(event, Trade):
                if self._book_is_fresh(self._last_timestamp):
                    self.detector.add(event)
                self.broker.on_trade(event)
            elif self.book is None or event.timestamp_ms >= self.book.timestamp_ms:
                self.book = event
            if self._last_timestamp - self._last_tick >= 100:
                await self._tick(self._last_timestamp)

    async def tick(self, timestamp_ms: int) -> None:
        async with self._lock:
            self._last_timestamp = max(self._last_timestamp, timestamp_ms)
            self.journal.tick(timestamp_ms)
            await self._tick(self._last_timestamp)

    def _book_is_fresh(self, timestamp_ms: int) -> bool:
        book = self.book
        return (
            book is not None
            and 0 < book.best_bid < book.best_ask
            and abs(timestamp_ms - book.timestamp_ms) <= self.config.strategy.max_book_age_ms
        )

    async def _tick(self, timestamp_ms: int) -> None:
        self._last_tick = timestamp_ms
        if (
            self._last_reconcile is None
            or timestamp_ms - self._last_reconcile >= self.config.risk.reconcile_ms
        ):
            self._last_reconcile = timestamp_ms
            try:
                await self.broker.sync()
                self._reconciled = True
            except (RuntimeError, TimeoutError) as error:
                self._reconciled = False
                logger.warning("Сверка ордеров: %s", error)
                self.journal.event("error", kind="reconcile", message=str(error))

        book = self.book
        fresh_book = book is not None and self._book_is_fresh(timestamp_ms)
        midpoint = book.midpoint if book is not None and fresh_book else 0.0
        equity_pnl = self.position.realized_pnl + self.position.unrealized_pnl(midpoint)
        if equity_pnl <= -self.config.risk.max_daily_loss_quote:
            self.halted = True
            self.stats.reason("daily_loss")
        if self.position.round_trips >= self.config.risk.max_round_trips:
            self.halted = True
            self.stats.reason("round_trip_limit")

        if book is None or not fresh_book:
            if book is not None:
                self.stats.reason("stale_book")
            self.detector.reset()
            self._last_signal = None
            await self._cancel_all_entries()
            return

        signal = self.detector.evaluate(timestamp_ms)
        self._record_signal(signal)
        if self.position.quantity != 0:
            await self._cancel_all_entries()
            if self.position.quantity != 0:
                await self._manage_position(timestamp_ms, book)
            return

        exits_closed = await self._cancel_purposes((OrderPurpose.EXIT, OrderPurpose.STOP))
        can_quote = (
            exits_closed
            and self._reconciled
            and not self.broker.has_pending
            and self.position.quantity == 0
            and signal.active
            and not self.halted
            and timestamp_ms >= self.cooldown_until_ms
        )
        if not can_quote:
            await self._cancel_all_entries()
            return
        await self._manage_entries(timestamp_ms, book, signal)

    async def _manage_entries(self, timestamp_ms: int, book: OrderBook, signal: FlowSignal) -> None:
        config = self.config.strategy
        midpoint = book.midpoint
        if book.spread_bps <= 0 or book.spread_bps > config.max_spread_bps:
            self.stats.reason("spread_filter")
            await self._cancel_all_entries()
            return
        bid = self.rules.price(
            min(book.best_bid, midpoint * (1 - config.entry_offset_bps / 10_000)), Side.BUY
        )
        ask = self.rules.price(
            max(book.best_ask, midpoint * (1 + config.entry_offset_bps / 10_000)), Side.SELL
        )
        edge_bps = (ask - bid) / midpoint * 10_000
        if edge_bps < 2 * config.maker_fee_bps + config.min_net_edge_bps:
            self.stats.reason("edge_filter")
            await self._cancel_all_entries()
            return
        base_quote = min(config.max_order_quote, signal.matched_quote * config.participation_rate)
        buy_quote = self._entry_quote(Side.BUY, base_quote, bid, book, midpoint)
        sell_quote = self._entry_quote(Side.SELL, base_quote, ask, book, midpoint)
        await self._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, bid, buy_quote, timestamp_ms)
        if self.position.quantity == 0 and not self.broker.has_pending:
            await self._ensure_quote(OrderPurpose.ENTRY_SELL, Side.SELL, ask, sell_quote, timestamp_ms)
        if self.position.quantity != 0 or self.broker.has_pending:
            await self._cancel_all_entries()

    def _entry_quote(
        self, side: Side, base_quote: float, price: float, book: OrderBook, midpoint: float
    ) -> float:
        config = self.config.strategy
        inventory_quote = max(self.position.quantity * side.sign, 0.0) * midpoint
        capacity = max(0.0, config.max_inventory_quote - inventory_quote)
        level_quantity = book.level_quantity(side, price)
        if level_quantity <= 0:
            return min(base_quote, capacity)
        book_cap = level_quantity * price * config.max_book_participation
        return min(base_quote, capacity, book_cap)

    async def _manage_position(self, timestamp_ms: int, book: OrderBook) -> None:
        config = self.config.strategy
        midpoint = book.midpoint
        long = self.position.quantity > 0
        side = Side.SELL if long else Side.BUY
        average_price = self.position.average_price
        adverse_bps = math.inf
        if math.isfinite(average_price) and average_price > 0:
            adverse_bps = (midpoint / average_price - 1) * side.sign * 10_000
        opened_at = self.position.opened_at_ms
        held_ms = timestamp_ms - opened_at if opened_at is not None else 0
        if self.halted or adverse_bps >= config.stop_loss_bps or held_ms >= config.max_hold_sec * 1000:
            self._stopping_position = True
        quantity = abs(self.position.quantity)
        if self._stopping_position:
            self.stats.reason("protective_exit")
            if not await self._cancel_exit_before_replace(OrderPurpose.EXIT):
                return
            price = (
                book.best_bid * (1 - config.stop_slippage_bps / 10_000)
                if long
                else book.best_ask * (1 + config.stop_slippage_bps / 10_000)
            )
            price = self.rules.price(price, side)
            await self._ensure_quote(
                OrderPurpose.STOP,
                side,
                price,
                0.0,
                timestamp_ms,
                post_only=False,
                reduce_only=True,
                reduce_quantity=quantity,
            )
            return
        if not await self._cancel_exit_before_replace(OrderPurpose.STOP):
            return
        target = self.position.average_price * (
            1 + config.exit_offset_bps / 10_000 if long else 1 - config.exit_offset_bps / 10_000
        )
        price = self.rules.price(max(target, book.best_ask) if long else min(target, book.best_bid), side)
        await self._ensure_quote(
            OrderPurpose.EXIT,
            side,
            price,
            0.0,
            timestamp_ms,
            post_only=True,
            reduce_only=True,
            reduce_quantity=quantity,
        )

    async def _cancel_exit_before_replace(self, purpose: OrderPurpose) -> bool:
        quantity = self.position.quantity
        return await self._cancel_purposes((purpose,)) and self.position.quantity == quantity

    async def _ensure_quote(
        self,
        purpose: OrderPurpose,
        side: Side,
        price: float,
        quote: float,
        timestamp_ms: int,
        post_only: bool = True,
        reduce_only: bool = False,
        reduce_quantity: float | None = None,
    ) -> None:
        existing = self._open_order(purpose)
        if not math.isfinite(price) or price <= 0 or price < self.rules.min_price:
            if existing is not None:
                await self._cancel(existing)
            return
        quantity = self.rules.quantity(reduce_quantity if reduce_quantity is not None else quote / price)
        min_notional = (
            0.0 if reduce_only and self.rules.reduce_only_below_min_notional else self.rules.min_notional
        )
        if quantity <= 0 or quantity * price < min_notional:
            if existing is not None:
                await self._cancel(existing)
            if reduce_only:
                self.stats.reason("position_below_order_minimum")
            return
        if existing is not None:
            age = timestamp_ms - existing.created_at_ms
            same_price = abs(existing.request.price - price) < max(self.rules.tick_size, 1e-12) / 2
            same_size = abs(existing.remaining_quantity - quantity) < self.rules.step_size / 2
            if same_price and same_size:
                return
            if age < self.config.strategy.quote_refresh_ms:
                return
            quantity_before_cancel = self.position.quantity
            if not await self._cancel(existing) or self.position.quantity != quantity_before_cancel:
                return
        if self.broker.has_pending or self.book is None:
            return
        if purpose is OrderPurpose.STOP:
            if (
                self._last_stop_attempt is not None
                and timestamp_ms - self._last_stop_attempt < self.config.risk.reconcile_ms
            ):
                return
            self._last_stop_attempt = timestamp_ms
        request = OrderRequest(
            symbol=self.config.symbol,
            client_id=uuid.uuid4().hex,
            side=side,
            purpose=purpose,
            price=price,
            quantity=quantity,
            post_only=post_only,
            reduce_only=reduce_only,
        )
        try:
            await self.broker.place(request, timestamp_ms, self.book)
        except (RuntimeError, TimeoutError) as error:
            if isinstance(error, OrderRejected):
                self.stats.rejected += 1
            self.journal.event(
                "error", kind="place", message=str(error), purpose=purpose.value, client_id=request.client_id
            )
            logger.warning("Размещение %s: %s", purpose.value, error)

    async def _cancel_all_entries(self) -> None:
        await self._cancel_purposes((OrderPurpose.ENTRY_BUY, OrderPurpose.ENTRY_SELL))

    async def _cancel_purposes(self, purposes: tuple[OrderPurpose, ...]) -> bool:
        closed = True
        for order in list(self.broker.orders.values()):
            if order.status is OrderStatus.OPEN and order.request.purpose in purposes:
                closed = await self._cancel(order) and closed
        return closed

    async def _cancel(self, order: Order) -> bool:
        try:
            await self.broker.cancel(order)
        except (RuntimeError, TimeoutError) as error:
            self.journal.event("error", kind="cancel", message=str(error), order_id=order.order_id)
            logger.warning("Отмена %s: %s", order.order_id, error)
            return False
        return order.status is not OrderStatus.OPEN

    def _open_order(self, purpose: OrderPurpose) -> Order | None:
        return next(
            (
                order
                for order in self.broker.orders.values()
                if order.status is OrderStatus.OPEN and order.request.purpose is purpose
            ),
            None,
        )

    def _record_signal(self, signal: FlowSignal) -> None:
        previous = self._last_signal
        changed = previous is None or previous.reason != signal.reason or previous.active != signal.active
        if signal.active and (previous is None or not previous.active):
            self.stats.signals += 1
        self.stats.reason(signal.reason)
        if changed:
            self.journal.event(
                "signal",
                timestamp_ms=signal.timestamp_ms,
                active=signal.active,
                reason=signal.reason,
                turnover_quote=signal.turnover_quote,
                matched_quote=signal.matched_quote,
                baseline_quote=signal.baseline_quote,
                volume_ratio=signal.volume_ratio if math.isfinite(signal.volume_ratio) else None,
                range_bps=signal.range_bps,
                trend_bps=signal.trend_bps,
            )
        self._last_signal = signal

    def _on_fill(self, fill: Fill) -> None:
        timestamp = fill.timestamp_ms or self._last_timestamp
        self.position.apply(
            fill,
            self.config.strategy.maker_fee_bps / 10_000,
            self.config.strategy.taker_fee_bps / 10_000,
            timestamp,
        )
        self.stats.fills += 1
        if self.position.quantity == 0:
            self._stopping_position = False
            self._last_stop_attempt = None
        if self.position.round_trips > self._seen_round_trips:
            self._seen_round_trips = self.position.round_trips
            self.cooldown_until_ms = timestamp + self.config.strategy.cooldown_sec * 1000
        self.journal.event(
            "fill",
            fill=fill,
            position_quantity=self.position.quantity,
            average_price=self.position.average_price,
            realized_pnl=self.position.realized_pnl,
        )

    def _on_order(self, action: str, order: Order) -> None:
        if action == "placed":
            self.stats.orders_placed += 1
        elif action == "rejected":
            self.stats.rejected += 1
        elif action == "canceled":
            self.stats.orders_canceled += 1
        self.journal.event("order", action=action, order=order)

    async def shutdown(self) -> None:
        async with self._lock:
            if self.broker.has_pending:
                try:
                    await self.broker.sync()
                except (RuntimeError, TimeoutError) as error:
                    logger.warning("Сверка перед остановкой: %s", error)
            for order in list(self.broker.orders.values()):
                if order.status is OrderStatus.OPEN:
                    await self._cancel(order)
            if (
                isinstance(self.broker, PaperBroker)
                and self.position.quantity != 0
                and self.book is not None
                and self._book_is_fresh(self._last_timestamp)
            ):
                side = Side.SELL if self.position.quantity > 0 else Side.BUY
                price = self.book.best_bid if side is Side.SELL else self.book.best_ask
                request = OrderRequest(
                    symbol=self.config.symbol,
                    client_id=uuid.uuid4().hex,
                    side=side,
                    purpose=OrderPurpose.STOP,
                    price=price,
                    quantity=abs(self.position.quantity),
                    post_only=False,
                    reduce_only=True,
                )
                await self.broker.place(request, self._last_timestamp, self.book)
            self.journal.event("summary", **self.summary())

    def summary(self) -> dict[str, Any]:
        midpoint = self.book.midpoint if self.book else 0.0
        unrealized = self.position.unrealized_pnl(midpoint)
        return {
            "exchange": self.config.exchange,
            "symbol": self.config.symbol,
            "mode": self.config.mode,
            "signals": self.stats.signals,
            "orders_placed": self.stats.orders_placed,
            "orders_canceled": self.stats.orders_canceled,
            "fills": self.stats.fills,
            "rejected": self.stats.rejected,
            "round_trips": self.position.round_trips,
            "open_orders": len(self.broker.orders),
            "pending_order_confirmation": self.broker.has_pending,
            "position_quantity": self.position.quantity,
            "position_notional_quote": self.position.notional,
            "realized_pnl_quote": self.position.realized_pnl,
            "unrealized_pnl_quote": unrealized,
            "net_pnl_quote": self.position.realized_pnl + unrealized,
            "fees_quote": self.position.fees,
            "halted": self.halted,
            "last_reason": self.stats.last_reason,
            "reason_counts": self.stats.reason_counts,
        }
