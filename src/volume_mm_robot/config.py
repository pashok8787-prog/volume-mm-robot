from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StrategyConfig(StrictModel):
    bucket_ms: int = Field(default=1000, ge=100, le=10_000)
    baseline_buckets: int = Field(default=60, ge=10)
    signal_buckets: int = Field(default=10, ge=2)
    min_baseline_buckets: int = Field(default=30, ge=5)
    volume_multiplier: float = Field(default=4.0, gt=1)
    min_signal_quote: float = Field(default=5000, gt=0)
    min_trades: int = Field(default=12, ge=2)
    min_side_ratio: float = Field(default=0.20, gt=0, le=0.5)
    max_range_bps: float = Field(default=120, gt=0)
    max_trend_bps: float = Field(default=45, gt=0)
    max_spread_bps: float = Field(default=250, gt=0)
    entry_offset_bps: float = Field(default=8, ge=0)
    exit_offset_bps: float = Field(default=12, gt=0)
    min_net_edge_bps: float = Field(default=3, ge=0)
    participation_rate: float = Field(default=0.02, gt=0, le=0.25)
    max_book_participation: float = Field(default=0.10, gt=0, le=0.5)
    max_order_quote: float = Field(default=25, gt=0)
    max_inventory_quote: float = Field(default=75, gt=0)
    quote_refresh_ms: int = Field(default=2500, ge=500)
    max_book_age_ms: int = Field(default=4000, gt=0)
    stop_loss_bps: float = Field(default=100, gt=0)
    stop_slippage_bps: float = Field(default=25, gt=0)
    max_hold_sec: int = Field(default=180, gt=0)
    cooldown_sec: int = Field(default=20, ge=0)
    maker_fee_bps: float = Field(default=1.0, ge=0)
    taker_fee_bps: float = Field(default=4.0, ge=0)
    paper_queue_ratio: float = Field(default=1.0, ge=0, le=2)

    @model_validator(mode="after")
    def check_windows(self) -> Self:
        if self.min_baseline_buckets > self.baseline_buckets:
            raise ValueError("min_baseline_buckets не может превышать baseline_buckets")
        if self.max_order_quote > self.max_inventory_quote:
            raise ValueError("max_order_quote не может превышать max_inventory_quote")
        return self


class RiskConfig(StrictModel):
    max_daily_loss_quote: float = Field(default=20, gt=0)
    max_round_trips: int = Field(default=30, ge=1)
    reconcile_ms: int = Field(default=1500, ge=500)


class AppConfig(StrictModel):
    exchange: Literal["aster", "hyperliquid"] = "aster"
    symbol: str = "ASTERUSDT"
    mode: Literal["paper", "testnet", "live"] = "paper"
    allow_live: bool = False
    data_dir: Path = Path("data")
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)

    @model_validator(mode="after")
    def normalize(self) -> Self:
        symbol = self.symbol.upper() if self.exchange == "aster" else self.symbol
        object.__setattr__(self, "symbol", symbol)
        if self.mode == "live" and not self.allow_live:
            raise ValueError("Для mode=live необходимо явно указать allow_live: true")
        return self


def load_config(path: str | Path | None = None) -> AppConfig:
    config_path = Path(path) if path is not None else Path("config.yaml")
    if not config_path.exists():
        if path is not None:
            raise FileNotFoundError(f"Конфигурация не найдена: {config_path}")
        return AppConfig()
    with config_path.open("r", encoding="utf-8") as handle:
        raw: Any = yaml.safe_load(handle)
    if raw is None:
        return AppConfig()
    if not isinstance(raw, dict):
        raise ValueError("Корень YAML должен быть словарём")
    return AppConfig.model_validate(raw)
