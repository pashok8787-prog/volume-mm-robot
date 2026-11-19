from __future__ import annotations

from volume_mm_robot.models import OrderBook, Side, Trade


def demo_events() -> list[Trade | OrderBook]:
    start = 1_760_000_000_000
    events: list[Trade | OrderBook] = []

    def book(second: float) -> None:
        timestamp = start + int(second * 1000)
        events.append(
            OrderBook(
                symbol="DEMO",
                timestamp_ms=timestamp,
                bids=((99.95, 10.0), (99.90, 20.0)),
                asks=((100.05, 10.0), (100.10, 20.0)),
            )
        )

    def trade(second: float, side: Side, price: float, quantity: float) -> None:
        events.append(
            Trade(
                symbol="DEMO",
                timestamp_ms=start + int(second * 1000),
                price=price,
                quantity=quantity,
                side=side,
            )
        )

    for second in range(35):
        book(second)
        if second < 30:
            trade(second + 0.2, Side.BUY, 100.0, 0.05)
            trade(second + 0.4, Side.SELL, 100.0, 0.05)
        else:
            for part in range(4):
                side = Side.BUY if part % 2 == 0 else Side.SELL
                trade(second + 0.1 + part * 0.15, side, 100.0, 1.5)
    book(35.0)
    trade(35.1, Side.SELL, 99.91, 5.0)
    book(35.2)
    trade(35.3, Side.BUY, 100.06, 5.0)
    book(35.4)
    return events
