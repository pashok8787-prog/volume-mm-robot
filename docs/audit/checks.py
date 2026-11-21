"""Offline audit reproductions with fake venues and synthetic market data."""

from __future__ import annotations

import asyncio
import math
from typing import Any
from unittest.mock import AsyncMock, patch

from volume_mm_robot import cli
from volume_mm_robot.brokers import FillCallback, LiveBroker, OrderCallback, PaperBroker
from volume_mm_robot.config import AppConfig
from volume_mm_robot.detector import VolumeDetector
from volume_mm_robot.engine import MarketMakingRobot
from volume_mm_robot.models import (
    Fill,
    MarketRules,
    OrderBook,
    OrderPurpose,
    OrderRequest,
    OrderStatus,
    Position,
    Side,
    Trade,
    VenueOrderState,
)

BOOK = OrderBook("ASTERUSDT", 10_000, ((99.9, 10.0),), ((100.1, 10.0),))
RULES = MarketRules(0.01, 0.001, 5)


class MemoryJournal:
    def raw(self, event: Trade | OrderBook) -> None:
        pass

    def event(self, event_type: str, **payload: Any) -> None:
        pass


class FakeVenue:
    def __init__(self, cancel_fails: bool = False, first_ack_lost: bool = False) -> None:
        self.cancel_fails = cancel_fails
        self.first_ack_lost = first_ack_lost
        self.submitted: list[OrderRequest] = []
        self.fetch_calls = 0

    async def place_order(self, request: OrderRequest) -> VenueOrderState:
        self.submitted.append(request)
        if self.first_ack_lost and len(self.submitted) == 1:
            raise TimeoutError("simulated accepted order with lost response")
        return VenueOrderState(str(len(self.submitted)), OrderStatus.OPEN, 0, 0)

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        if self.cancel_fails:
            raise RuntimeError("simulated cancel failure")

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState:
        self.fetch_calls += 1
        return VenueOrderState(order_id, OrderStatus.OPEN, 0, 0)


def live_robot(venue: FakeVenue, position: Position | None = None) -> MarketMakingRobot:
    return MarketMakingRobot(
        AppConfig(),
        RULES,
        MemoryJournal(),
        lambda on_fill, on_order: LiveBroker(venue, on_fill, on_order),
        position,
    )


def order_request() -> OrderRequest:
    return OrderRequest(BOOK.symbol, "a" * 32, Side.BUY, OrderPurpose.ENTRY_BUY, 99.9, 0.2, True, False)


async def cli_bypasses_second_gate() -> str:
    args = cli.parser().parse_args(["run", "--mode", "live", "--confirm-live"])
    with (
        patch.object(cli, "load_config", return_value=AppConfig()),
        patch.object(cli, "run", new=AsyncMock()) as run,
    ):
        await cli.dispatch(args)
    config, _, confirmed = run.await_args.args
    assert (config.mode, config.allow_live, confirmed) == ("live", False, True)
    return "dispatcher forwards live, allow_live=False, confirm_live=True"


async def replacement_after_cancel_error() -> str:
    venue = FakeVenue(cancel_fails=True)
    robot = live_robot(venue)
    robot.book = BOOK
    await robot.broker.place(order_request(), BOOK.timestamp_ms, BOOK)
    await robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.8, 20, 13_000)
    open_ids = [order.order_id for order in robot.broker.orders.values() if order.status is OrderStatus.OPEN]
    assert open_ids == ["1", "2"]
    return f"two local open entries on one side: {open_ids}"


async def lost_order_ack() -> str:
    venue = FakeVenue(first_ack_lost=True)
    robot = live_robot(venue)
    robot.book = BOOK
    for timestamp_ms in (10_000, 10_100):
        await robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.9, 20, timestamp_ms)
    assert len(venue.submitted) == 2 and set(robot.broker.orders) == {"2"}
    assert venue.submitted[0].client_id != venue.submitted[1].client_id
    return "two unique requests sent to fake venue, only the second is tracked"


async def open_snapshot_after_cancel() -> str:
    venue = FakeVenue()
    robot = live_robot(venue)
    order = await robot.broker.place(order_request(), BOOK.timestamp_ms, BOOK)
    await robot.broker.cancel(order)
    await robot.broker.sync()
    assert order.status is OrderStatus.CANCELED and venue.fetch_calls == 1
    return "venue returned OPEN, local status is CANCELED and sync skipped it"


async def stale_book_blocks_reconciliation() -> str:
    venue = FakeVenue(cancel_fails=True)
    robot = live_robot(venue)
    robot.book = BOOK
    await robot.broker.place(order_request(), BOOK.timestamp_ms, BOOK)
    await robot.tick(BOOK.timestamp_ms + 5_000)
    assert venue.fetch_calls == 0 and robot.stats.last_reason == "stale_book"
    return "stale book and failed cancel skip account reconciliation"


async def stop_with_small_remainder() -> str:
    venue = FakeVenue()
    robot = live_robot(venue, Position(quantity=0.03, average_price=100.0, opened_at_ms=1000))
    robot.halted = True
    robot.book = BOOK
    await robot._manage_position(BOOK.timestamp_ms, BOOK)
    assert not venue.submitted and robot.position.quantity == 0.03
    return "halted with ~3 quote exposure and no reduce-only request"


async def paper_reduce_only_reverses() -> str:
    position = Position(quantity=0.1, average_price=100.0)
    broker = PaperBroker(lambda fill: position.apply(fill, 0, 0, fill.timestamp_ms), lambda *_: None, 1)
    request = OrderRequest(BOOK.symbol, "b" * 32, Side.SELL, OrderPurpose.STOP, 99.9, 0.2, False, True)
    await broker.place(request, BOOK.timestamp_ms, BOOK)
    assert math.isclose(position.quantity, -0.1)
    return "simulated reduce-only IOC reverses +0.1 into -0.1"


async def repeated_ioc_reuses_depth() -> str:
    book = OrderBook(BOOK.symbol, BOOK.timestamp_ms, ((99.9, 0.1),), BOOK.asks)
    robot = MarketMakingRobot(
        AppConfig(),
        RULES,
        MemoryJournal(),
        paper_broker_factory,
        Position(quantity=0.2, average_price=100.0, opened_at_ms=1000),
    )
    robot.halted = True
    robot.book = book
    await robot._manage_position(book.timestamp_ms, book)
    assert math.isclose(robot.position.quantity, 0.1)
    await robot._manage_position(book.timestamp_ms + 100, book)
    assert robot.position.quantity == 0 and robot.stats.fills == 2
    return "two IOC fills totaling 0.2 against the same snapshot with 0.1 bid depth"


def paper_broker_factory(on_fill: FillCallback, on_order: OrderCallback) -> PaperBroker:
    return PaperBroker(on_fill, on_order, 1)


async def late_trade_fills_existing_order() -> str:
    fills = []
    broker = PaperBroker(fills.append, lambda *_: None, 1)
    await broker.place(order_request(), BOOK.timestamp_ms, BOOK)
    broker.on_trade(Trade(BOOK.symbol, BOOK.timestamp_ms - 100, 99.8, 1, Side.SELL))
    assert len(fills) == 1 and fills[0].timestamp_ms < BOOK.timestamp_ms
    return "paper fill at 9900 ms against order created at 10000 ms"


async def lot_grid_rounding() -> str:
    rules = MarketRules(0.05, 0.05, 5)
    price = rules.price(100.079, Side.BUY)
    quantity = rules.quantity(1.039)
    assert (price, quantity) == (100.07, 1.03)
    return f"step 0.05 yields price={price}, size={quantity}, both off-grid"


async def partial_close_drops_round_trip() -> str:
    position = Position()
    for side, size in ((Side.BUY, 0.3), (Side.SELL, 0.1), (Side.SELL, 0.2)):
        fill = Fill(BOOK.symbol, 1000, "1", side, OrderPurpose.EXIT, 100, size, True, False)
        position.apply(fill, 0, 0, 1000)
    assert position.quantity == 0 and position.round_trips == 0
    return "flat after partial fills but round_trips=0"


async def incomplete_baseline() -> str:
    detector = VolumeDetector(AppConfig().strategy)
    for second in range(30):
        detector.add(Trade(BOOK.symbol, second * 1000, 100, 3, Side.BUY))
    for second in range(30, 40):
        detector.add(Trade(BOOK.symbol, second * 1000, 100, 2, Side.BUY))
        detector.add(Trade(BOOK.symbol, second * 1000 + 200, 100, 4, Side.SELL))
    signal = detector.evaluate(40_000)
    assert signal.active and signal.baseline_quote == 1500 and signal.volume_ratio == 4
    return "30 observed baseline seconds divided by 60, ratio=4 vs 2 over observed seconds"


async def main() -> None:
    checks = (
        cli_bypasses_second_gate,
        replacement_after_cancel_error,
        lost_order_ack,
        open_snapshot_after_cancel,
        stale_book_blocks_reconciliation,
        stop_with_small_remainder,
        paper_reduce_only_reverses,
        repeated_ioc_reuses_depth,
        late_trade_fills_existing_order,
        lot_grid_rounding,
        partial_close_drops_round_trip,
        incomplete_baseline,
    )
    for check in checks:
        print(f"REPRODUCED {check.__name__}: {await check()}")
    print(f"{len(checks)} cases reproduced")


if __name__ == "__main__":
    asyncio.run(main())
