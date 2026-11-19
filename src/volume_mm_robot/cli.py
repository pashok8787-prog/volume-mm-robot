from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections.abc import Sequence
from pathlib import Path

import aiohttp

from volume_mm_robot.brokers import LiveBroker, PaperBroker
from volume_mm_robot.config import AppConfig, load_config
from volume_mm_robot.engine import MarketMakingRobot
from volume_mm_robot.exchanges import AsterVenue, HyperliquidVenue
from volume_mm_robot.journal import Journal, read_raw
from volume_mm_robot.models import MarketRules, Position
from volume_mm_robot.synthetic import demo_events

logger = logging.getLogger(__name__)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Market-making робот Aster DEX и Hyperliquid")
    result.add_argument("--config", help="Путь к YAML-конфигурации")
    commands = result.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Paper/testnet/live на публичном потоке")
    run.add_argument("--exchange", choices=("aster", "hyperliquid"))
    run.add_argument("--symbol")
    run.add_argument("--mode", choices=("paper", "testnet", "live"))
    run.add_argument("--duration", type=float, default=60, help="Секунды; 0 — до Ctrl+C")
    run.add_argument("--confirm-live", action="store_true", help="Подтверждение реальных ордеров")

    replay = commands.add_parser("replay", help="Автономный повтор raw.jsonl")
    replay.add_argument("--input", type=Path, required=True)
    replay.add_argument("--tick-size", type=float, required=True)
    replay.add_argument("--step-size", type=float, required=True)
    replay.add_argument("--min-notional", type=float, default=5.0)

    commands.add_parser("demo", help="Синтетический полный цикл без сети")
    return result


async def run(config: AppConfig, duration: float, confirm_live: bool) -> None:
    if config.mode == "live" and not confirm_live:
        raise ValueError("Для реальных ордеров добавьте --confirm-live")
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        venue = (
            AsterVenue(session, testnet=config.mode == "testnet")
            if config.exchange == "aster"
            else HyperliquidVenue(session, testnet=config.mode == "testnet")
        )
        rules = await venue.market_rules(config.symbol)
        initial_position = (
            await venue.prepare_trading(config.symbol) if config.mode != "paper" else Position()
        )
        journal = Journal(
            config.data_dir, f"{config.exchange}-{config.symbol}-{config.mode}", record_raw=True
        )
        robot = MarketMakingRobot(
            config,
            rules,
            journal,
            (
                lambda on_fill, on_order: (
                    PaperBroker(on_fill, on_order, config.strategy.paper_queue_ratio)
                    if config.mode == "paper"
                    else LiveBroker(venue, on_fill, on_order)
                )
            ),
            initial_position,
        )

        async def feed() -> None:
            async for event in venue.events(config.symbol):
                await robot.handle(event)

        async def watchdog() -> None:
            while True:
                await asyncio.sleep(0.25)
                await robot.tick(int(time.time() * 1000))

        feed_task = asyncio.create_task(feed())
        watchdog_task = asyncio.create_task(watchdog())
        print(
            f"{config.exchange} {config.symbol} {config.mode}: tick={rules.tick_size}, "
            f"step={rules.step_size}, min={rules.min_notional}. Ctrl+C для остановки."
        )
        try:
            await asyncio.wait({feed_task}, timeout=duration if duration > 0 else None)
            if feed_task.done():
                await feed_task
        finally:
            feed_task.cancel()
            watchdog_task.cancel()
            await asyncio.gather(feed_task, watchdog_task, return_exceptions=True)
            await robot.shutdown()
            print(json.dumps(robot.summary(), ensure_ascii=False, indent=2))
            print(f"События: {journal.events_path}")
            print(f"Исходные данные: {journal.raw_path}")
            journal.close()


async def replay(config: AppConfig, input_path: Path, rules: MarketRules) -> None:
    paper_config = config.model_copy(update={"mode": "paper", "allow_live": False})
    journal = Journal(config.data_dir, f"{config.exchange}-{config.symbol}-replay", record_raw=False)
    robot = MarketMakingRobot(
        paper_config,
        rules,
        journal,
        lambda on_fill, on_order: PaperBroker(on_fill, on_order, config.strategy.paper_queue_ratio),
    )
    events = read_raw(input_path)
    try:
        for event in events:
            await robot.handle(event)
        if events:
            await robot.tick(events[-1].timestamp_ms)
    finally:
        await robot.shutdown()
        print(json.dumps(robot.summary(), ensure_ascii=False, indent=2))
        print(f"События: {journal.events_path}")
        journal.close()


async def demo(config: AppConfig) -> None:
    strategy = config.strategy.model_copy(
        update={
            "baseline_buckets": 20,
            "min_baseline_buckets": 10,
            "signal_buckets": 5,
            "volume_multiplier": 3.0,
            "min_signal_quote": 1000,
            "min_trades": 8,
            "cooldown_sec": 0,
        }
    )
    demo_config = config.model_copy(
        update={
            "exchange": "aster",
            "symbol": "DEMO",
            "mode": "paper",
            "allow_live": False,
            "strategy": strategy,
        }
    )
    journal = Journal(config.data_dir, "demo", record_raw=True)
    robot = MarketMakingRobot(
        demo_config,
        MarketRules(tick_size=0.01, step_size=0.001, min_notional=5.0),
        journal,
        lambda on_fill, on_order: PaperBroker(on_fill, on_order, config.strategy.paper_queue_ratio),
    )
    try:
        for event in demo_events():
            await robot.handle(event)
    finally:
        await robot.shutdown()
        print(json.dumps(robot.summary(), ensure_ascii=False, indent=2))
        print(f"События: {journal.events_path}")
        print(f"Исходные данные: {journal.raw_path}")
        journal.close()


async def dispatch(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.command == "run":
        updates = {
            key: value
            for key, value in {
                "exchange": args.exchange,
                "symbol": args.symbol,
                "mode": args.mode,
            }.items()
            if value is not None
        }
        await run(config.model_copy(update=updates), args.duration, args.confirm_live)
    elif args.command == "replay":
        rules = MarketRules(args.tick_size, args.step_size, args.min_notional)
        await replay(config, args.input, rules)
    else:
        await demo(config)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parser().parse_args(argv)
    try:
        asyncio.run(dispatch(args))
    except KeyboardInterrupt:
        print("Остановлено пользователем")
    except (ValueError, RuntimeError, OSError, aiohttp.ClientError) as error:
        raise SystemExit(f"Ошибка: {error}") from error


if __name__ == "__main__":
    main()
