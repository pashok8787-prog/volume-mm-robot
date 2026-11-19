from __future__ import annotations

from dataclasses import dataclass

from volume_mm_robot.config import StrategyConfig
from volume_mm_robot.models import FlowSignal, Side, Trade


@dataclass(slots=True)
class Bucket:
    buy_quote: float = 0.0
    sell_quote: float = 0.0
    trades: int = 0
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0

    def add(self, trade: Trade) -> None:
        if self.trades == 0:
            self.open_price = trade.price
            self.high_price = trade.price
            self.low_price = trade.price
        self.high_price = max(self.high_price, trade.price)
        self.low_price = min(self.low_price, trade.price)
        self.close_price = trade.price
        self.trades += 1
        if trade.side is Side.BUY:
            self.buy_quote += trade.notional
        else:
            self.sell_quote += trade.notional

    @property
    def turnover(self) -> float:
        return self.buy_quote + self.sell_quote


class VolumeDetector:
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self._buckets: dict[int, Bucket] = {}

    def add(self, trade: Trade) -> None:
        bucket_id = trade.timestamp_ms // self.config.bucket_ms
        self._buckets.setdefault(bucket_id, Bucket()).add(trade)
        oldest = bucket_id - self.config.baseline_buckets - self.config.signal_buckets - 5
        for key in [key for key in self._buckets if key < oldest]:
            del self._buckets[key]

    def evaluate(self, timestamp_ms: int) -> FlowSignal:
        last = timestamp_ms // self.config.bucket_ms - 1
        recent_ids = range(last - self.config.signal_buckets + 1, last + 1)
        baseline_ids = range(
            last - self.config.signal_buckets - self.config.baseline_buckets + 1,
            last - self.config.signal_buckets + 1,
        )
        recent = [self._buckets.get(key, Bucket()) for key in recent_ids]
        baseline = [self._buckets.get(key, Bucket()) for key in baseline_ids]
        first_bucket = min(self._buckets, default=last)
        available_baseline = max(0, last - self.config.signal_buckets - first_bucket + 1)
        turnover = sum(bucket.turnover for bucket in recent)
        buy = sum(bucket.buy_quote for bucket in recent)
        sell = sum(bucket.sell_quote for bucket in recent)
        trades = sum(bucket.trades for bucket in recent)
        baseline_turnover = (
            sum(bucket.turnover for bucket in baseline)
            / self.config.baseline_buckets
            * self.config.signal_buckets
        )
        matched = 2 * min(buy, sell)
        side_ratio = min(buy, sell) / turnover if turnover > 0 else 0.0
        nonempty = [bucket for bucket in recent if bucket.trades]
        high = max((bucket.high_price for bucket in nonempty), default=0.0)
        low = min((bucket.low_price for bucket in nonempty), default=0.0)
        midpoint = (high + low) / 2 if high and low else 0.0
        range_bps = (high - low) / midpoint * 10_000 if midpoint else 0.0
        trend_bps = (
            abs(nonempty[-1].close_price - nonempty[0].open_price) / nonempty[0].open_price * 10_000
            if len(nonempty) > 1 and nonempty[0].open_price > 0
            else 0.0
        )
        ratio = turnover / baseline_turnover if baseline_turnover > 0 else (float("inf") if turnover else 0.0)
        reason = "ok"
        if available_baseline < self.config.min_baseline_buckets:
            reason = "warmup"
        elif (
            turnover < self.config.min_signal_quote
            or turnover < baseline_turnover * self.config.volume_multiplier
        ):
            reason = "low_volume"
        elif trades < self.config.min_trades:
            reason = "few_trades"
        elif side_ratio < self.config.min_side_ratio:
            reason = "one_sided"
        elif range_bps > self.config.max_range_bps:
            reason = "wide_range"
        elif trend_bps > self.config.max_trend_bps:
            reason = "directional"
        return FlowSignal(
            timestamp_ms=timestamp_ms,
            active=reason == "ok",
            reason=reason,
            turnover_quote=turnover,
            matched_quote=matched,
            baseline_quote=baseline_turnover,
            volume_ratio=ratio,
            buy_ratio=buy / turnover if turnover else 0.0,
            range_bps=range_bps,
            trend_bps=trend_bps,
            trade_count=trades,
        )
