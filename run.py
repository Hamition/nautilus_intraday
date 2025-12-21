# run.py
from __future__ import annotations

from dotenv import load_dotenv
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

import os
import pandas as pd
import hydra

from src.config import MomentumConfig
from src.data import get_catalog, get_top_liquid_instruments, create_data_configs
from src.engine import run_backtest

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    # 1. Prepare Data
    load_dotenv()
    NAUTILUS_ROOT = os.getenv('NAUTILUS_ROOT')
    catalog = get_catalog(NAUTILUS_ROOT)
    instruments = get_top_liquid_instruments(catalog,
                                             limit=cfg.universe.top_n_instruments)
    print(f"Selected Instruments: {instruments}")
    data_configs = create_data_configs(NAUTILUS_ROOT, instruments)

    # 2. Define Parameters

    strategy_path = cfg.backtest.strategy_path
    config_path = cfg.backtest.config_path

    start_date = pd.Timestamp(cfg.backtest.start_date, tz='UTC')
    end_date = pd.Timestamp(cfg.backtest.end_date, tz='UTC')
    venue = cfg.backtest.venue

    starting_balances = list(cfg.backtest.starting_balances)
    cfg.strategy.instrument_ids = instruments

    strategy_config = instantiate(cfg.strategy, _convert_="all")


    print(start_date, end_date, venue, starting_balances)
    print(strategy_config)

    # 3. Run
    results = run_backtest(
        strategy_path=strategy_path,
        config_path=config_path,
        strategy_config=strategy_config,
        venue_name=venue,
        data_configs=data_configs,
        start=start_date,
        end=end_date,
        starting_balances=starting_balances,
    )

    print(results)

if __name__ == '__main__':
    main()
