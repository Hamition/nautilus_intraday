# src/engine.py
from __future__ import annotations

from datetime import datetime
from typing import List

from nautilus_trader.backtest.node import BacktestNode, BacktestVenueConfig, BacktestRunConfig
from nautilus_trader.config import BacktestEngineConfig, ImportableStrategyConfig
from nautilus_trader.backtest.config import BacktestDataConfig


def run_backtest(
    strategy_path: str,
    config_path: str,
    strategy_config,
    venue_name: str = "XNYS",
    data_configs: List[BacktestDataConfig] = None,
    start: datetime | None = None,
    end: datetime | None = None,
):
    """Run a high-level backtest using BacktestNode (recommended Nautilus API)."""
    if data_configs is None:
        data_configs = []

    venue_configs = [
        BacktestVenueConfig(
            name=venue_name,
            oms_type="NETTING",
            account_type="MARGIN",
            starting_balances=["1_000_000 USD"],
        )
    ]

    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path=strategy_path,
                config_path=config_path,
                config=strategy_config,
            )
        ]
    )

    run_config = BacktestRunConfig(
        engine=engine_config,
        venues=venue_configs,
        data=data_configs,
        start=start,
        end=end,
    )

    node = BacktestNode(configs=[run_config])
    print("Starting backtest...")
    results = node.run()
    print("Backtest completed.")
    return results
