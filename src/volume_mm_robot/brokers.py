from __future__ import annotations

import math
from collections.abc import Callable
from decimal import Decimal
from typing import Protocol

from volume_mm_robot.models import (
    Fill,
    Order,
    OrderBook,
    OrderRequest,
    OrderStatus,
    Side,
    Trade,
    VenueOrderState,
)

FillCallback = Callable[[Fill], None]
OrderCallback = Callable[[str, Order], None]


class OrderRejected(RuntimeError):
    """The venue confirmed that it did not accept the order."""


class Broker(Protocol):
    orders: dict[str, Order]

    @property
    def has_pending(self) -> bool: ...

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order: ...

    async def cancel(self, order: Order) -> None: ...

    async def sync(self) -> None: ...

    def on_trade(self, trade: Trade) -> None: ...


class PaperBroker:
    has_pending = False

    def __init__(
        self,
        on_fill: FillCallback,
        on_order: OrderCallback,
        queue_ratio: float,
        position_quantity: Callable[[], float],
    ) -> None:
        self.orders: dict[str, Order] = {}
        self._on_fill = on_fill
        self._on_order = on_order
        self._queue_ratio = queue_ratio
        self._position_quantity = position_quantity
        self._sequence = 0
        self._book_key: tuple[str, int] | None = None
        self._consumed_depth: dict[tuple[Side, float], float] = {}

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order:
        self._sequence += 1
        order = Order(request=request, order_id=f"paper-{self._sequence}", created_at_ms=timestamp_ms)
        if request.post_only and (
            (request.side is Side.BUY and request.price >= book.best_ask)
            or (request.side is Side.SELL and request.price <= book.best_bid)
        ):
            order.status = OrderStatus.REJECTED
            self._on_order("rejected", order)
            return order
        self.orders[order.order_id] = order
        order.queue_ahead = book.level_quantity(request.side, request.price) * self._queue_ratio
        self._on_order("placed", order)
        if not request.post_only:
            self._fill_aggressive(order, book, timestamp_ms)
        return order

    async def cancel(self, order: Order) -> None:
        if order.status is OrderStatus.OPEN:
            self._close(order, OrderStatus.CANCELED)

    async def sync(self) -> None:
        return

    def on_trade(self, trade: Trade) -> None:
        for order in list(self.orders.values()):
            if order.status is not OrderStatus.OPEN or not order.request.post_only:
                continue
            if order.request.side is Side.BUY and trade.side is Side.SELL:
                self._fill_passive(order, trade)
            elif order.request.side is Side.SELL and trade.side is Side.BUY:
                self._fill_passive(order, trade)

    def _fill_passive(self, order: Order, trade: Trade) -> None:
        if trade.symbol != order.request.symbol or trade.timestamp_ms < order.created_at_ms:
            return
        limit = order.request.price
        if order.request.side is Side.BUY and trade.price > limit:
            return
        if order.request.side is Side.SELL and trade.price < limit:
            return
        available = trade.quantity
        if trade.price == limit:
            consumed = min(order.queue_ahead, available)
            order.queue_ahead -= consumed
            available -= consumed
            if available <= 0:
                return
        quantity = min(order.remaining_quantity, available)
        if quantity > 0:
            self._fill(order, limit, quantity, trade.timestamp_ms, maker=True)

    def _fill_aggressive(self, order: Order, book: OrderBook, timestamp_ms: int) -> None:
        book_key = (book.symbol, book.timestamp_ms)
        if book_key != self._book_key:
            self._book_key = book_key
            self._consumed_depth.clear()
        levels = book.asks if order.request.side is Side.BUY else book.bids
        for price, available in levels:
            if order.status is not OrderStatus.OPEN:
                break
            if order.request.side is Side.BUY and price > order.request.price:
                break
            if order.request.side is Side.SELL and price < order.request.price:
                break
            key = (order.request.side, price)
            consumed = self._consumed_depth.get(key, 0.0)
            quantity = min(order.remaining_quantity, available - consumed)
            if quantity > 0:
                filled = self._fill(order, price, quantity, timestamp_ms, maker=False)
                self._consumed_depth[key] = consumed + filled
        if order.status is OrderStatus.OPEN:
            self._close(order, OrderStatus.CANCELED)

    def _fill(self, order: Order, price: float, quantity: float, timestamp_ms: int, maker: bool) -> float:
        if order.request.reduce_only:
            closable = max(0.0, -order.request.side.sign * self._position_quantity())
            quantity = min(quantity, closable)
            if quantity <= 1e-12:
                self._close(order, OrderStatus.CANCELED)
                return 0.0
        previous_notional = order.average_fill_price * order.filled_quantity
        order.filled_quantity = float(Decimal(str(order.filled_quantity)) + Decimal(str(quantity)))
        order.average_fill_price = (previous_notional + price * quantity) / order.filled_quantity
        if order.remaining_quantity <= 1e-12:
            self._close(order, OrderStatus.FILLED)
        fill = Fill(
            symbol=order.request.symbol,
            timestamp_ms=timestamp_ms,
            order_id=order.order_id,
            side=order.request.side,
            purpose=order.request.purpose,
            price=price,
            quantity=quantity,
            maker=maker,
            reduce_only=order.request.reduce_only,
        )
        self._on_fill(fill)
        if order.request.reduce_only and order.status is OrderStatus.OPEN and self._position_quantity() == 0:
            self._close(order, OrderStatus.CANCELED)
        return quantity

    def _close(self, order: Order, status: OrderStatus) -> None:
        order.status = status
        self.orders.pop(order.order_id)
        self._on_order(status.value, order)


class LiveVenue(Protocol):
    async def place_order(self, request: OrderRequest) -> VenueOrderState: ...

    async def cancel_order(self, symbol: str, order_id: str) -> None: ...

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState: ...

    async def fetch_order_by_client_id(self, symbol: str, client_id: str) -> VenueOrderState | None: ...


class LiveBroker:
    def __init__(self, venue: LiveVenue, on_fill: FillCallback, on_order: OrderCallback) -> None:
        self.orders: dict[str, Order] = {}
        self._pending: dict[str, tuple[OrderRequest, int]] = {}
        self._venue = venue
        self._on_fill = on_fill
        self._on_order = on_order

    @property
    def has_pending(self) -> bool:
        return bool(self._pending)

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order:
        if self.has_pending:
            raise RuntimeError("Размещение остановлено до сверки заявки с неизвестным статусом")
        self._pending[request.client_id] = (request, timestamp_ms)
        self._on_order("submitting", Order(request, "", timestamp_ms))
        try:
            state = await self._venue.place_order(request)
        except OrderRejected:
            del self._pending[request.client_id]
            raise
        return self._register(request, state, timestamp_ms)

    def _register(self, request: OrderRequest, state: VenueOrderState, timestamp_ms: int) -> Order:
        order = Order(request=request, order_id=state.order_id, created_at_ms=timestamp_ms)
        self._apply_state(order, state, timestamp_ms, registering=True)
        del self._pending[request.client_id]
        return order

    async def cancel(self, order: Order) -> None:
        if order.status is not OrderStatus.OPEN:
            return
        await self._venue.cancel_order(order.request.symbol, order.order_id)
        state = await self._venue.fetch_order(order.request.symbol, order.order_id)
        self._apply_state(order, state, 0)

    async def sync(self) -> None:
        error: RuntimeError | TimeoutError | None = None
        for order in list(self.orders.values()):
            try:
                state = await self._venue.fetch_order(order.request.symbol, order.order_id)
                self._apply_state(order, state, 0)
            except (RuntimeError, TimeoutError) as exc:
                error = exc
        for request, timestamp_ms in list(self._pending.values()):
            state_or_none = await self._venue.fetch_order_by_client_id(request.symbol, request.client_id)
            if state_or_none is not None:
                self._register(request, state_or_none, timestamp_ms)
        if error is not None:
            raise error

    def on_trade(self, trade: Trade) -> None:
        return

    def _apply_state(
        self, order: Order, state: VenueOrderState, timestamp_ms: int, *, registering: bool = False
    ) -> None:
        if state.filled_quantity + 1e-12 < order.filled_quantity:
            raise RuntimeError(f"Биржа вернула меньший накопленный объём для ордера {order.order_id}")
        delta = float(Decimal(str(state.filled_quantity)) - Decimal(str(order.filled_quantity)))
        price = 0.0
        if delta > 1e-12:
            previous_notional = order.average_fill_price * order.filled_quantity
            state_notional = state.average_fill_price * state.filled_quantity
            price = (state_notional - previous_notional) / delta
            if price <= 0 or not math.isfinite(price):
                raise RuntimeError(f"Биржа не подтвердила цену исполнения ордера {order.order_id}")
        if registering:
            self.orders[order.order_id] = order
            self._on_order("placed", order)
        if delta > 1e-12:
            order.filled_quantity = state.filled_quantity
            order.average_fill_price = state.average_fill_price
            self._on_fill(
                Fill(
                    symbol=order.request.symbol,
                    timestamp_ms=timestamp_ms,
                    order_id=order.order_id,
                    side=order.request.side,
                    purpose=order.request.purpose,
                    price=price,
                    quantity=delta,
                    maker=order.request.post_only,
                    reduce_only=order.request.reduce_only,
                )
            )
        if order.status is not state.status:
            order.status = state.status
            self.orders.pop(order.order_id)
            self._on_order(state.status.value, order)
