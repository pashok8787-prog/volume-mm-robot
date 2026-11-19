from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from collections.abc import Callable
from typing import Any

from volume_mm_robot.brokers import Broker, FillCallback, OrderCallback, PaperBroker
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
BrokerFactory = Callable[[FillCallback, OrderCallback], Broker]


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
        self.broker = broker_factory(self._on_fill, self._on_order)
        self.halted = False
        self.cooldown_until_ms = 0
        self._last_timestamp = int(time.time() * 1000)
        self._last_tick = 0
        self._last_reconcile = 0
        self._last_signal: FlowSignal | None = None
        self._seen_round_trips = self.position.round_trips
        self._lock = asyncio.Lock()

    async def handle(self, event: Trade | OrderBook) -> None:
        async with self._lock:
            self._last_timestamp = event.timestamp_ms
            self.journal.raw(event)
            if isinstance(event, Trade):
                self.detector.add(event)
                self.broker.on_trade(event)
            else:
                self.book = event
            if event.timestamp_ms - self._last_tick >= 100:
                await self._tick(event.timestamp_ms)

    async def tick(self, timestamp_ms: int) -> None:
        async with self._lock:
            self._last_timestamp = timestamp_ms
            await self._tick(timestamp_ms)

    async def _tick(self, timestamp_ms: int) -> None:
        self._last_tick = timestamp_ms
        if self.book is None:
            return
        book = self.book
        if timestamp_ms - book.timestamp_ms > self.config.strategy.max_book_age_ms:
            self.stats.reason("stale_book")
            await self._cancel_purposes(tuple(OrderPurpose))
            return
        if timestamp_ms - self._last_reconcile >= self.config.risk.reconcile_ms:
            self._last_reconcile = timestamp_ms
            try:
                await self.broker.sync()
            except (RuntimeError, TimeoutError) as error:
                logger.warning("Сверка ордеров: %s", error)
                self.journal.event("error", kind="reconcile", message=str(error))
                return

        signal = self.detector.evaluate(timestamp_ms)
        self._record_signal(signal)
        midpoint = book.midpoint
        equity_pnl = self.position.realized_pnl + self.position.unrealized_pnl(midpoint)
        if equity_pnl <= -self.config.risk.max_daily_loss_quote:
            self.halted = True
            self.stats.reason("daily_loss")
        if self.position.round_trips >= self.config.risk.max_round_trips:
            self.halted = True
            self.stats.reason("round_trip_limit")

        if self.position.quantity != 0:
            await self._cancel_all_entries()
            await self._manage_position(timestamp_ms, book)
            return

        await self._cancel_purposes((OrderPurpose.EXIT, OrderPurpose.STOP))
        can_quote = (
            signal.active and not self.halted and timestamp_ms >= self.cooldown_until_ms and midpoint > 0
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
        await self._ensure_quote(OrderPurpose.ENTRY_SELL, Side.SELL, ask, sell_quote, timestamp_ms)

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
        adverse_bps = (
            (self.position.average_price - midpoint) / self.position.average_price * 10_000
            if long
            else (midpoint - self.position.average_price) / self.position.average_price * 10_000
        )
        held_ms = timestamp_ms - (self.position.opened_at_ms or timestamp_ms)
        stop = self.halted or adverse_bps >= config.stop_loss_bps or held_ms >= config.max_hold_sec * 1000
        quantity = self.rules.quantity(abs(self.position.quantity))
        if quantity <= 0:
            await self._cancel_purposes((OrderPurpose.EXIT, OrderPurpose.STOP))
            return
        if stop:
            self.stats.reason("protective_exit")
            await self._cancel_purposes((OrderPurpose.EXIT,))
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
                quantity * price,
                timestamp_ms,
                post_only=False,
                reduce_only=True,
            )
            return
        await self._cancel_purposes((OrderPurpose.STOP,))
        target = self.position.average_price * (
            1 + config.exit_offset_bps / 10_000 if long else 1 - config.exit_offset_bps / 10_000
        )
        price = self.rules.price(max(target, book.best_ask) if long else min(target, book.best_bid), side)
        await self._ensure_quote(
            OrderPurpose.EXIT,
            side,
            price,
            quantity * price,
            timestamp_ms,
            post_only=True,
            reduce_only=True,
        )

    async def _ensure_quote(
        self,
        purpose: OrderPurpose,
        side: Side,
        price: float,
        quote: float,
        timestamp_ms: int,
        post_only: bool = True,
        reduce_only: bool = False,
    ) -> None:
        existing = self._open_order(purpose)
        if quote < self.rules.min_notional or price <= 0:
            if existing is not None:
                await self._cancel(existing)
            return
        quantity = self.rules.quantity(quote / price)
        if quantity <= 0 or quantity * price < self.rules.min_notional:
            if existing is not None:
                await self._cancel(existing)
            return
        if existing is not None:
            age = timestamp_ms - existing.created_at_ms
            same_price = abs(existing.request.price - price) < max(self.rules.tick_size, 1e-12) / 2
            same_size = abs(existing.remaining_quantity - quantity) <= self.rules.step_size
            if same_price and same_size:
                return
            if age < self.config.strategy.quote_refresh_ms:
                return
            await self._cancel(existing)
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
        if self.book is None:
            return
        try:
            await self.broker.place(request, timestamp_ms, self.book)
        except (RuntimeError, TimeoutError) as error:
            self.stats.rejected += 1
            self.journal.event("error", kind="place", message=str(error), purpose=purpose.value)
            logger.warning("Размещение %s: %s", purpose.value, error)

    async def _cancel_all_entries(self) -> None:
        await self._cancel_purposes((OrderPurpose.ENTRY_BUY, OrderPurpose.ENTRY_SELL))

    async def _cancel_purposes(self, purposes: tuple[OrderPurpose, ...]) -> None:
        for order in list(self.broker.orders.values()):
            if order.status is OrderStatus.OPEN and order.request.purpose in purposes:
                await self._cancel(order)

    async def _cancel(self, order: Order) -> None:
        try:
            await self.broker.cancel(order)
            self.stats.orders_canceled += 1
        except (RuntimeError, TimeoutError) as error:
            self.journal.event("error", kind="cancel", message=str(error), order_id=order.order_id)
            logger.warning("Отмена %s: %s", order.order_id, error)

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
        self.journal.event("order", action=action, order=order)

    async def shutdown(self) -> None:
        async with self._lock:
            for order in list(self.broker.orders.values()):
                if order.status is OrderStatus.OPEN:
                    await self._cancel(order)
            if isinstance(self.broker, PaperBroker) and self.position.quantity != 0 and self.book is not None:
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
