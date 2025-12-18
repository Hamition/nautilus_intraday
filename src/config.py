# src/config.py
from __future__ import annotations

from typing import List

from nautilus_trader.config import StrategyConfig


class MomentumConfig(StrategyConfig):
    """
    Configuration for the intraday MomentumStrategy.
    """
    instrument_ids: List[str]
    wave_times: List[str] #= ["10:00", "11:00", "14:00"]
    venue: str = "XNYS"
    rebalance_freq: int = 1
    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_volume_ratio: float = 0.01
    portfolio_value: float = 1_000_000.0
