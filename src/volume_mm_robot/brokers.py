from __future__ import annotations

from collections.abc import Callable
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


class Broker(Protocol):
    orders: dict[str, Order]

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order: ...

    async def cancel(self, order: Order) -> None: ...

    async def sync(self) -> None: ...

    def on_trade(self, trade: Trade) -> None: ...


class PaperBroker:
    def __init__(
        self,
        on_fill: FillCallback,
        on_order: OrderCallback,
        queue_ratio: float,
    ) -> None:
        self.orders: dict[str, Order] = {}
        self._on_fill = on_fill
        self._on_order = on_order
        self._queue_ratio = queue_ratio
        self._sequence = 0

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order:
        self._sequence += 1
        order = Order(request=request, order_id=f"paper-{self._sequence}", created_at_ms=timestamp_ms)
        self.orders[order.order_id] = order
        if request.post_only and (
            (request.side is Side.BUY and request.price >= book.best_ask)
            or (request.side is Side.SELL and request.price <= book.best_bid)
        ):
            order.status = OrderStatus.REJECTED
            self._on_order("rejected", order)
            return order
        order.queue_ahead = book.level_quantity(request.side, request.price) * self._queue_ratio
        self._on_order("placed", order)
        if not request.post_only:
            self._fill_aggressive(order, book, timestamp_ms)
        return order

    async def cancel(self, order: Order) -> None:
        if order.status is OrderStatus.OPEN:
            order.status = OrderStatus.CANCELED
            self._on_order("canceled", order)

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
        levels = book.asks if order.request.side is Side.BUY else book.bids
        for price, available in levels:
            if order.remaining_quantity <= 0:
                break
            if order.request.side is Side.BUY and price > order.request.price:
                break
            if order.request.side is Side.SELL and price < order.request.price:
                break
            quantity = min(order.remaining_quantity, available)
            if quantity > 0:
                self._fill(order, price, quantity, timestamp_ms, maker=False)
        if order.remaining_quantity <= 1e-12:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.CANCELED
        self._on_order("closed", order)

    def _fill(self, order: Order, price: float, quantity: float, timestamp_ms: int, maker: bool) -> None:
        previous_notional = order.average_fill_price * order.filled_quantity
        order.filled_quantity += quantity
        order.average_fill_price = (previous_notional + price * quantity) / order.filled_quantity
        if order.remaining_quantity <= 1e-12:
            order.status = OrderStatus.FILLED
            self._on_order("filled", order)
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


class LiveVenue(Protocol):
    async def place_order(self, request: OrderRequest) -> VenueOrderState: ...

    async def cancel_order(self, symbol: str, order_id: str) -> None: ...

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState: ...


class LiveBroker:
    def __init__(self, venue: LiveVenue, on_fill: FillCallback, on_order: OrderCallback) -> None:
        self.orders: dict[str, Order] = {}
        self._venue = venue
        self._on_fill = on_fill
        self._on_order = on_order

    async def place(self, request: OrderRequest, timestamp_ms: int, book: OrderBook) -> Order:
        state = await self._venue.place_order(request)
        order = Order(request=request, order_id=state.order_id, created_at_ms=timestamp_ms)
        self.orders[order.order_id] = order
        self._on_order("placed", order)
        self._apply_state(order, state, timestamp_ms)
        return order

    async def cancel(self, order: Order) -> None:
        if order.status is not OrderStatus.OPEN:
            return
        await self._venue.cancel_order(order.request.symbol, order.order_id)
        state = await self._venue.fetch_order(order.request.symbol, order.order_id)
        self._apply_state(order, state, 0)
        if order.status is OrderStatus.OPEN:
            order.status = OrderStatus.CANCELED
            self._on_order("canceled", order)

    async def sync(self) -> None:
        for order in list(self.orders.values()):
            if order.status is not OrderStatus.OPEN:
                continue
            state = await self._venue.fetch_order(order.request.symbol, order.order_id)
            self._apply_state(order, state, 0)

    def on_trade(self, trade: Trade) -> None:
        return

    def _apply_state(self, order: Order, state: VenueOrderState, timestamp_ms: int) -> None:
        delta = max(0.0, state.filled_quantity - order.filled_quantity)
        if delta > 0:
            previous_notional = order.average_fill_price * order.filled_quantity
            state_notional = state.average_fill_price * state.filled_quantity
            price = (state_notional - previous_notional) / delta
            if price <= 0:
                price = order.request.price
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
            self._on_order(state.status.value, order)
