from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from volume_mm_robot.config import AppConfig
from volume_mm_robot.models import MarketRules, OrderBook, Side, Trade


def json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Неподдерживаемый тип JSON: {type(value).__name__}")


class Journal:
    def __init__(self, data_dir: Path, prefix: str, record_raw: bool) -> None:
        data_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        name = f"{stamp}-{uuid4().hex[:12]}-{prefix}"
        self.events_path = data_dir / f"{name}.events.jsonl"
        self.raw_path = data_dir / f"{name}.raw.jsonl" if record_raw else None
        self._events = self.events_path.open("x", encoding="utf-8", buffering=1)
        self._raw = self.raw_path.open("x", encoding="utf-8", buffering=1) if self.raw_path else None

    def metadata(self, config: AppConfig, rules: MarketRules) -> None:
        settings = config.model_dump(exclude={"mode", "allow_live", "data_dir"}, mode="json")
        self._write_raw({"type": "metadata", "data": {"config": settings, "rules": asdict(rules)}})

    def raw(self, event: Trade | OrderBook, *, received_at_ms: int | None = None) -> None:
        if self._raw is None:
            return
        kind = "trade" if isinstance(event, Trade) else "book"
        record: dict[str, Any] = {"type": kind, "data": asdict(event)}
        if received_at_ms is not None:
            record["received_at_ms"] = received_at_ms
        self._write_raw(record)

    def tick(self, timestamp_ms: int) -> None:
        self._write_raw({"type": "tick", "data": {"timestamp_ms": timestamp_ms}})

    def _write_raw(self, record: dict[str, Any]) -> None:
        if self._raw is not None:
            self._raw.write(json.dumps(record, default=json_value, allow_nan=False) + "\n")

    def event(self, event_type: str, **payload: Any) -> None:
        record = {"type": event_type, **payload}
        self._events.write(json.dumps(record, default=json_value, allow_nan=False) + "\n")

    def close(self) -> None:
        self._events.close()
        if self._raw is not None:
            self._raw.close()


def read_raw_metadata(path: Path) -> tuple[AppConfig, MarketRules] | None:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record = json.loads(line)
                if record["type"] == "metadata":
                    data = record["data"]
                    return AppConfig.model_validate(data["config"]), MarketRules(**data["rules"])
                break
    return None


def read_raw(path: Path) -> Iterator[tuple[Trade | OrderBook | None, int]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            record: Any = json.loads(line)
            if record["type"] == "metadata":
                continue
            data = record["data"]
            timestamp_ms = record.get("received_at_ms", data["timestamp_ms"])
            if record["type"] == "trade":
                yield Trade(**{**data, "side": Side(data["side"])}), timestamp_ms
            elif record["type"] == "book":
                yield (
                    OrderBook(
                        symbol=data["symbol"],
                        timestamp_ms=data["timestamp_ms"],
                        bids=tuple(tuple(level) for level in data["bids"]),
                        asks=tuple(tuple(level) for level in data["asks"]),
                    ),
                    timestamp_ms,
                )
            elif record["type"] == "tick":
                yield None, timestamp_ms
            else:
                raise ValueError(f"Неизвестное событие в строке {line_number}: {record['type']}")
