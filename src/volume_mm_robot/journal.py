from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from volume_mm_robot.models import OrderBook, Side, Trade


def json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Неподдерживаемый тип JSON: {type(value).__name__}")


class Journal:
    def __init__(self, data_dir: Path, prefix: str, record_raw: bool) -> None:
        data_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.events_path = data_dir / f"{stamp}-{prefix}.events.jsonl"
        self.raw_path = data_dir / f"{stamp}-{prefix}.raw.jsonl" if record_raw else None
        self._events = self.events_path.open("w", encoding="utf-8", buffering=1)
        self._raw = self.raw_path.open("w", encoding="utf-8", buffering=1) if self.raw_path else None

    def raw(self, event: Trade | OrderBook) -> None:
        if self._raw is None:
            return
        kind = "trade" if isinstance(event, Trade) else "book"
        self._raw.write(json.dumps({"type": kind, "data": asdict(event)}, default=json_value) + "\n")

    def event(self, event_type: str, **payload: Any) -> None:
        record = {"type": event_type, **payload}
        self._events.write(json.dumps(record, default=json_value, allow_nan=False) + "\n")

    def close(self) -> None:
        self._events.close()
        if self._raw is not None:
            self._raw.close()


def read_raw(path: Path) -> list[Trade | OrderBook]:
    events: list[Trade | OrderBook] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            record: Any = json.loads(line)
            data = record["data"]
            if record["type"] == "trade":
                events.append(Trade(**{**data, "side": Side(data["side"])}))
            elif record["type"] == "book":
                events.append(
                    OrderBook(
                        symbol=data["symbol"],
                        timestamp_ms=data["timestamp_ms"],
                        bids=tuple(tuple(level) for level in data["bids"]),
                        asks=tuple(tuple(level) for level in data["asks"]),
                    )
                )
            else:
                raise ValueError(f"Неизвестный событие в строке {line_number}: {record['type']}")
    return events
