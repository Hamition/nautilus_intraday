# src/config.py
from __future__ import annotations

from nautilus_trader.config import StrategyConfig
from typing import List

import msgspec


class ExecutionConfig(msgspec.Struct):
    algo: str = "vwap"
    horizon_minutes: int = 30
    participation_rate: float = 0.1
    min_slice_qty: int = 1


class MomentumConfig(StrategyConfig):
    instrument_ids: List[str]
    venue: str = "XNYS"

    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_trade_weight: float = 0.05
    max_delta: float = 1_000_000.0
    max_factor_exposure: float = 1_000_000.0
    min_trade_qty: float = 1.0

    execution: ExecutionConfig = msgspec.field(
        default_factory=ExecutionConfig
    )
