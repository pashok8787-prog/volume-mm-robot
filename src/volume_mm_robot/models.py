from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import StrEnum


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY


class OrderStatus(StrEnum):
    OPEN = "open"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class OrderPurpose(StrEnum):
    ENTRY_BUY = "entry_buy"
    ENTRY_SELL = "entry_sell"
    EXIT = "exit"
    STOP = "stop"


@dataclass(frozen=True, slots=True)
class Trade:
    symbol: str
    timestamp_ms: int
    price: float
    quantity: float
    side: Side

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass(frozen=True, slots=True)
class OrderBook:
    symbol: str
    timestamp_ms: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def midpoint(self) -> float:
        return (self.best_bid + self.best_ask) / 2 if self.bids and self.asks else 0.0

    @property
    def spread_bps(self) -> float:
        midpoint = self.midpoint
        return (self.best_ask - self.best_bid) / midpoint * 10_000 if midpoint > 0 else math.inf

    def level_quantity(self, side: Side, price: float) -> float:
        levels = self.bids if side is Side.BUY else self.asks
        return next((quantity for level_price, quantity in levels if level_price == price), 0.0)


@dataclass(frozen=True, slots=True)
class MarketRules:
    tick_size: float
    step_size: float
    min_notional: float
    max_price_decimals: int | None = None
    significant_figures: int | None = None

    def price(self, value: float, side: Side) -> float:
        rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
        decimal_value = Decimal(str(value))
        if self.significant_figures is not None and value > 0:
            significant_decimals = max(0, self.significant_figures - 1 - math.floor(math.log10(value)))
            decimals = significant_decimals
            if self.max_price_decimals is not None:
                decimals = min(decimals, self.max_price_decimals)
            quantum = Decimal(1).scaleb(-max(0, decimals))
        else:
            quantum = Decimal(str(self.tick_size))
        return float(decimal_value.quantize(quantum, rounding=rounding))

    def quantity(self, value: float) -> float:
        quantum = Decimal(str(self.step_size))
        return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_FLOOR))


@dataclass(frozen=True, slots=True)
class FlowSignal:
    timestamp_ms: int
    active: bool
    reason: str
    turnover_quote: float
    matched_quote: float
    baseline_quote: float
    volume_ratio: float
    buy_ratio: float
    range_bps: float
    trend_bps: float
    trade_count: int


@dataclass(frozen=True, slots=True)
class OrderRequest:
    symbol: str
    client_id: str
    side: Side
    purpose: OrderPurpose
    price: float
    quantity: float
    post_only: bool
    reduce_only: bool


@dataclass(slots=True)
class Order:
    request: OrderRequest
    order_id: str
    created_at_ms: int
    status: OrderStatus = OrderStatus.OPEN
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    queue_ahead: float = 0.0

    @property
    def remaining_quantity(self) -> float:
        return max(0.0, self.request.quantity - self.filled_quantity)


@dataclass(frozen=True, slots=True)
class VenueOrderState:
    order_id: str
    status: OrderStatus
    filled_quantity: float
    average_fill_price: float


@dataclass(frozen=True, slots=True)
class Fill:
    symbol: str
    timestamp_ms: int
    order_id: str
    side: Side
    purpose: OrderPurpose
    price: float
    quantity: float
    maker: bool
    reduce_only: bool

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass(slots=True)
class Position:
    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0
    fees: float = 0.0
    opened_at_ms: int | None = None
    round_trips: int = 0

    def apply(self, fill: Fill, maker_fee_rate: float, taker_fee_rate: float, timestamp_ms: int) -> None:
        old_quantity = self.quantity
        delta = fill.quantity * fill.side.sign
        new_quantity = old_quantity + delta
        fee_rate = maker_fee_rate if fill.maker else taker_fee_rate
        fee = fill.notional * fee_rate
        self.fees += fee
        self.realized_pnl -= fee

        if old_quantity == 0 or old_quantity * delta > 0:
            total = abs(old_quantity) + fill.quantity
            self.average_price = (
                (abs(old_quantity) * self.average_price + fill.quantity * fill.price) / total
                if total > 0
                else 0.0
            )
            if old_quantity == 0:
                self.opened_at_ms = timestamp_ms
        else:
            closing = min(abs(old_quantity), fill.quantity)
            self.realized_pnl += (fill.price - self.average_price) * closing * (1 if old_quantity > 0 else -1)
            if new_quantity == 0:
                self.average_price = 0.0
                self.opened_at_ms = None
                self.round_trips += 1
            elif old_quantity * new_quantity < 0:
                self.average_price = fill.price
                self.opened_at_ms = timestamp_ms

        self.quantity = 0.0 if abs(new_quantity) < 1e-12 else new_quantity

    def unrealized_pnl(self, midpoint: float) -> float:
        if self.quantity == 0 or midpoint <= 0:
            return 0.0
        return (midpoint - self.average_price) * self.quantity

    @property
    def notional(self) -> float:
        return abs(self.quantity) * self.average_price


@dataclass(slots=True)
class RuntimeStats:
    signals: int = 0
    orders_placed: int = 0
    orders_canceled: int = 0
    fills: int = 0
    rejected: int = 0
    last_reason: str = "starting"
    reason_counts: dict[str, int] = field(default_factory=dict)

    def reason(self, value: str) -> None:
        self.last_reason = value
        self.reason_counts[value] = self.reason_counts.get(value, 0) + 1
