# src/config.py
from __future__ import annotations

from typing import List

from nautilus_trader.config import StrategyConfig


class MomentumConfig(StrategyConfig):
    """
    Configuration for the intraday MomentumStrategy.
    """
    instrument_ids: List[str]
    venue: str = "XNYS"
    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_trade_weight: float = 0.05
    max_delta: float = 1_000_000.0
    max_factor_exposure: float = 1_000_000.0
    min_trade_qty: float = 1.0
