# src/data.py
from __future__ import annotations

import os
from typing import List

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.model.data import Bar


def get_catalog(path) -> ParquetDataCatalog:
    """Initialize catalog from NAUTILUS_ROOT env var (fallback to current dir)."""
    return ParquetDataCatalog(path=path)


def get_top_liquid_instruments(catalog: ParquetDataCatalog, limit: int = 10) -> List[str]:
    """
    Return top N instrument IDs.

    In production: filter by average daily volume or liquidity.
    """
    instruments = catalog.instruments()
    return [str(inst.id) for inst in instruments][:limit]


def create_data_configs(
    catalog_path: str,
    instrument_ids: List[str],
) -> List[BacktestDataConfig]:
    """Create configs for 1-minute (execution) and 1-day (ADV) bars."""
    configs: List[BacktestDataConfig] = []

    # 1-minute bars for intraday trading
    configs.extend(
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-MINUTE-LAST-EXTERNAL",
        )
        for inst_id in instrument_ids
    )

    # 1-day bars for average daily volume calculations
    configs.extend(
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-DAY-LAST-EXTERNAL",
        )
        for inst_id in instrument_ids
    )

    return configs
