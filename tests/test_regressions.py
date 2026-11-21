from __future__ import annotations

import asyncio
import io
import json
import tempfile
import unittest
from collections.abc import AsyncIterator
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from volume_mm_robot import cli
from volume_mm_robot.brokers import LiveBroker, OrderRejected, PaperBroker
from volume_mm_robot.config import AppConfig
from volume_mm_robot.detector import VolumeDetector
from volume_mm_robot.engine import MarketMakingRobot
from volume_mm_robot.exchanges import AsterVenue, HyperliquidVenue, VenueAPIError
from volume_mm_robot.journal import Journal, read_raw, read_raw_metadata
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
RULES = MarketRules(0.01, 0.001, 5, reduce_only_below_min_notional=True)
OPEN = VenueOrderState("1", OrderStatus.OPEN, 0.0, 0.0)


def entry_request() -> OrderRequest:
    return OrderRequest(BOOK.symbol, "a" * 32, Side.BUY, OrderPurpose.ENTRY_BUY, 99.9, 0.2, True, False)


def read_summary(directory: Path, pattern: str = "*.events.jsonl") -> dict[str, Any]:
    events = next(directory.glob(pattern)).read_text(encoding="utf-8")
    return json.loads(events.splitlines()[-1])


class LiveOrdersTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        journal = Journal(Path(directory.name), "test", record_raw=False)
        self.addCleanup(journal.close)
        self.venue = AsyncMock()
        self.venue.place_order.return_value = OPEN
        self.venue.fetch_order.return_value = OPEN
        self.venue.fetch_order_by_client_id.return_value = None
        self.robot = MarketMakingRobot(
            AppConfig(),
            RULES,
            journal,
            lambda on_fill, on_order, _: LiveBroker(self.venue, on_fill, on_order),
        )
        self.robot.book = BOOK

    async def test_failed_cancel_does_not_replace_order(self) -> None:
        self.venue.cancel_order.side_effect = RuntimeError("cancel failed")
        await self.robot.broker.place(entry_request(), BOOK.timestamp_ms, BOOK)
        await self.robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.8, 20, 13_000)
        self.assertEqual(list(self.robot.broker.orders), ["1"])
        self.venue.place_order.assert_awaited_once()

    async def test_fill_during_cancel_prevents_entry_replacement(self) -> None:
        await self.robot.broker.place(entry_request(), BOOK.timestamp_ms, BOOK)
        self.venue.fetch_order.return_value = VenueOrderState("1", OrderStatus.CANCELED, 0.1, 99.9)
        await self.robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.8, 20, 13_000)
        self.assertEqual(self.robot.position.quantity, 0.1)
        self.venue.place_order.assert_awaited_once()

    async def test_open_snapshot_after_cancel_remains_tracked(self) -> None:
        order = await self.robot.broker.place(entry_request(), BOOK.timestamp_ms, BOOK)
        await self.robot.broker.cancel(order)
        await self.robot.broker.sync()
        self.assertIs(order.status, OrderStatus.OPEN)
        self.assertEqual(self.venue.fetch_order.await_count, 2)
        self.assertEqual(self.robot.stats.orders_canceled, 0)

    async def test_lost_ack_blocks_new_entries_until_client_id_is_resolved(self) -> None:
        self.venue.place_order.side_effect = TimeoutError("response lost")
        await self.robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.9, 20, 10_000)
        request = self.venue.place_order.await_args.args[0]
        await self.robot.broker.sync()
        await self.robot._ensure_quote(OrderPurpose.ENTRY_SELL, Side.SELL, 100.1, 20, 10_100)
        self.assertTrue(self.robot.broker.has_pending)
        self.venue.place_order.assert_awaited_once()
        self.venue.fetch_order_by_client_id.assert_awaited_once_with(BOOK.symbol, request.client_id)
        self.venue.fetch_order_by_client_id.return_value = VenueOrderState("1", OrderStatus.FILLED, 0.2, 99.9)
        await self.robot.broker.sync()
        await self.robot.broker.sync()
        self.assertFalse(self.robot.broker.has_pending)
        self.assertEqual(self.robot.position.quantity, 0.2)
        self.assertEqual(self.robot.stats.fills, 1)
        self.assertFalse(self.robot.broker.orders)

    async def test_confirmed_rejection_does_not_leave_pending_order(self) -> None:
        self.venue.place_order.side_effect = OrderRejected("maker would cross")
        await self.robot._ensure_quote(OrderPurpose.ENTRY_BUY, Side.BUY, 99.9, 20, 10_000)
        self.assertFalse(self.robot.broker.has_pending)
        self.assertEqual(self.robot.stats.rejected, 1)

    async def test_unconfirmed_fill_price_blocks_reconciliation(self) -> None:
        order = await self.robot.broker.place(entry_request(), BOOK.timestamp_ms, BOOK)
        self.venue.fetch_order.return_value = VenueOrderState("1", OrderStatus.FILLED, 0.2, 0.0)
        with self.assertRaises(RuntimeError):
            await self.robot.broker.sync()
        self.assertIs(order.status, OrderStatus.OPEN)
        self.assertEqual(self.robot.position.quantity, 0.0)

    async def test_stale_book_still_reconciles_and_preserves_working_exit(self) -> None:
        exit_request = replace(entry_request(), purpose=OrderPurpose.EXIT, side=Side.SELL, reduce_only=True)
        await self.robot.broker.place(exit_request, BOOK.timestamp_ms, BOOK)
        await self.robot.tick(BOOK.timestamp_ms + 5_000)
        self.venue.fetch_order.assert_awaited_once()
        self.venue.cancel_order.assert_not_awaited()
        self.assertEqual(self.robot.stats.last_reason, "stale_book")

    async def test_small_reduce_only_stop_uses_aster_exception(self) -> None:
        self.robot.position.quantity = 0.03
        self.robot.position.average_price = 100.0
        self.robot.halted = True
        await self.robot._manage_position(BOOK.timestamp_ms, BOOK)
        request = self.venue.place_order.await_args.args[0]
        self.assertEqual(request.quantity, 0.03)
        self.assertLess(request.price * request.quantity, RULES.min_notional)
        self.assertTrue(request.reduce_only)
        self.assertFalse(request.post_only)

    async def test_protective_stop_continues_after_partial_ioc(self) -> None:
        self.robot.position.quantity = 0.2
        self.robot.position.average_price = 102.0
        self.venue.place_order.return_value = VenueOrderState("1", OrderStatus.CANCELED, 0.1, 99.9)
        await self.robot._manage_position(BOOK.timestamp_ms, BOOK)
        recovered = OrderBook(BOOK.symbol, 12_000, ((101.99, 10),), ((102.01, 10),))
        self.robot.book = recovered
        self.venue.place_order.return_value = VenueOrderState("2", OrderStatus.FILLED, 0.1, 101.99)
        await self.robot._manage_position(recovered.timestamp_ms, recovered)
        request = self.venue.place_order.await_args.args[0]
        self.assertIs(request.purpose, OrderPurpose.STOP)
        self.assertEqual(self.robot.position.quantity, 0.0)


class PaperExecutionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.position = Position()
        self.fills: list[Fill] = []
        self.broker = PaperBroker(self.on_fill, lambda *_: None, 1.0, lambda: self.position.quantity)

    def on_fill(self, fill: Fill) -> None:
        self.fills.append(fill)
        self.position.apply(fill, 0, 0, fill.timestamp_ms)

    async def test_reduce_only_ioc_cannot_reverse_position(self) -> None:
        self.position.quantity = 0.1
        self.position.average_price = 100.0
        order = await self.broker.place(
            replace(
                entry_request(), side=Side.SELL, purpose=OrderPurpose.STOP, post_only=False, reduce_only=True
            ),
            BOOK.timestamp_ms,
            BOOK,
        )
        self.assertEqual(order.filled_quantity, 0.1)
        self.assertEqual(self.position.quantity, 0.0)

    async def test_repeat_ioc_waits_for_new_book_depth(self) -> None:
        book = replace(BOOK, bids=((99.9, 0.1),))
        request = replace(entry_request(), side=Side.SELL, post_only=False)
        await self.broker.place(request, book.timestamp_ms, book)
        await self.broker.place(request, book.timestamp_ms + 100, book)
        self.assertEqual([fill.quantity for fill in self.fills], [0.1])
        new_book = replace(book, timestamp_ms=book.timestamp_ms + 200)
        await self.broker.place(request, new_book.timestamp_ms, new_book)
        self.assertEqual([fill.quantity for fill in self.fills], [0.1, 0.1])

    async def test_late_trade_cannot_fill_a_newer_order(self) -> None:
        await self.broker.place(entry_request(), BOOK.timestamp_ms, BOOK)
        self.broker.on_trade(Trade(BOOK.symbol, BOOK.timestamp_ms - 100, 99.8, 1, Side.SELL))
        self.assertFalse(self.fills)
        self.broker.on_trade(Trade(BOOK.symbol, BOOK.timestamp_ms + 100, 99.8, 1, Side.SELL))
        self.assertEqual(len(self.fills), 1)


class StrategyTest(unittest.TestCase):
    def test_grids_respect_step_and_offset(self) -> None:
        rules = MarketRules(0.05, 0.05, 5)
        self.assertEqual(rules.price(100.079, Side.BUY), 100.05)
        self.assertEqual(rules.price(100.079, Side.SELL), 100.1)
        self.assertEqual(rules.quantity(1.039), 1.0)
        offset_rules = replace(rules, min_price=0.02, min_quantity=0.02)
        self.assertEqual(offset_rules.price(100.079, Side.BUY), 100.07)
        self.assertEqual(offset_rules.quantity(1.039), 1.02)
        self.assertEqual(offset_rules.quantity(0.01), 0.0)

    def test_partial_close_keeps_exact_size_and_counts_round_trip(self) -> None:
        position = Position()
        for side, quantity in ((Side.BUY, 0.3), (Side.SELL, 0.1), (Side.SELL, 0.2)):
            position.apply(
                Fill(BOOK.symbol, 1000, "1", side, OrderPurpose.EXIT, 100, quantity, True, False), 0, 0, 1000
            )
            if quantity == 0.1:
                self.assertEqual(RULES.quantity(position.quantity), 0.2)
        self.assertEqual(position.quantity, 0.0)
        self.assertEqual(position.average_price, 0.0)
        self.assertEqual(position.round_trips, 1)

    def test_partial_baseline_is_normalized_by_observed_seconds(self) -> None:
        detector = VolumeDetector(AppConfig().strategy)
        for second in range(30):
            detector.add(Trade(BOOK.symbol, second * 1000, 100, 3, Side.BUY))
        for second in range(30, 40):
            detector.add(Trade(BOOK.symbol, second * 1000, 100, 2, Side.BUY))
            detector.add(Trade(BOOK.symbol, second * 1000 + 200, 100, 4, Side.SELL))
        signal = detector.evaluate(40_000)
        self.assertFalse(signal.active)
        self.assertEqual(signal.volume_ratio, 2.0)
        detector.reset()
        self.assertEqual(detector.evaluate(40_001).reason, "warmup")


class CLITest(unittest.IsolatedAsyncioTestCase):
    async def test_cli_live_override_requires_yaml_permission(self) -> None:
        args = cli.parser().parse_args(["run", "--mode", "live", "--confirm-live"])
        with (
            patch.object(cli, "load_config", return_value=AppConfig()),
            patch.object(cli, "run", new=AsyncMock()) as run,
            self.assertRaisesRegex(ValueError, "allow_live"),
        ):
            await cli.dispatch(args)
        run.assert_not_awaited()

    async def test_live_entry_point_checks_both_permissions_before_network(self) -> None:
        disabled = AppConfig().model_copy(update={"mode": "live"})
        enabled = AppConfig(mode="live", allow_live=True)
        with patch.object(cli.aiohttp, "ClientSession") as session:
            for config, confirm in ((disabled, True), (enabled, False)):
                with self.assertRaisesRegex(ValueError, "allow_live"):
                    await cli.run(config, 1, confirm)
        session.assert_not_called()

    async def test_watchdog_failure_ends_run_and_writes_summary(self) -> None:
        async def blocked_feed(symbol: str) -> AsyncIterator[OrderBook]:
            yield replace(BOOK, symbol=symbol)
            await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as directory:
            config = AppConfig(data_dir=Path(directory))
            venue = MagicMock()
            venue.market_rules = AsyncMock(return_value=RULES)
            venue.events = blocked_feed
            with (
                patch.object(cli, "AsterVenue", return_value=venue),
                patch.object(MarketMakingRobot, "tick", side_effect=RuntimeError("watchdog stopped")),
                redirect_stdout(io.StringIO()),
                self.assertRaisesRegex(RuntimeError, "watchdog stopped"),
            ):
                await asyncio.wait_for(cli.run(config, 0, False), timeout=2)
            summary = await asyncio.to_thread(read_summary, config.data_dir)
            self.assertEqual(summary["type"], "summary")

    async def test_raw_recording_replays_config_and_watchdog_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            config = AppConfig(data_dir=data_dir)
            journal = Journal(data_dir, "recorded", record_raw=True)
            try:
                journal.metadata(config, RULES)
                journal.raw(BOOK, received_at_ms=10_100)
                journal.tick(15_200)
            finally:
                journal.close()
            path = journal.raw_path
            assert path is not None
            self.assertEqual(list(read_raw(path)), [(BOOK, 10_100), (None, 15_200)])
            self.assertEqual(read_raw_metadata(path), (AppConfig(), RULES))
            args = cli.parser().parse_args(["replay", "--input", str(path)])
            with patch.object(cli, "load_config", return_value=config), redirect_stdout(io.StringIO()):
                await cli.dispatch(args)
            summary = await asyncio.to_thread(read_summary, data_dir, "*-replay.events.jsonl")
            self.assertEqual(summary["last_reason"], "stale_book")


class VenueContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_aster_resolves_using_same_client_id_as_placement(self) -> None:
        venue = AsterVenue(MagicMock(), testnet=True)
        state = {"orderId": 1, "status": "NEW", "executedQty": "0", "avgPrice": "0"}
        request = entry_request()
        with patch.object(venue, "_signed_request", new=AsyncMock(return_value=state)) as api:
            await venue.place_order(request)
            await venue.fetch_order_by_client_id(request.symbol, request.client_id)
        place, query = (call.args[2] for call in api.await_args_list)
        self.assertEqual(place["newClientOrderId"], query["origClientOrderId"])

    async def test_aster_distinguishes_timeout_from_confirmed_rejection(self) -> None:
        venue = AsterVenue(MagicMock(), testnet=True)
        with patch.object(venue, "_signed_request", new=AsyncMock()) as api:
            api.side_effect = VenueAPIError(503, -1007, "unknown")
            with self.assertRaises(VenueAPIError):
                await venue.place_order(entry_request())
            api.side_effect = VenueAPIError(400, -2022, "reduce-only reject")
            with self.assertRaises(OrderRejected):
                await venue.place_order(entry_request())

    async def test_hyperliquid_unknown_order_is_uncertain(self) -> None:
        venue = HyperliquidVenue(MagicMock(), testnet=True)
        with (
            patch.object(venue, "_sdk", return_value=MagicMock()),
            patch.object(venue, "_call_sdk", new=AsyncMock(return_value={"status": "unknownOid"})),
            self.assertRaises(RuntimeError),
        ):
            await venue.fetch_order("HYPE", "1")
