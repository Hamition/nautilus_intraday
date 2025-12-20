import os
import pandas as pd
import hydra

from dotenv import load_dotenv
from omegaconf import DictConfig

from src.config import MomentumConfig
from src.data import get_catalog, get_top_liquid_instruments, create_data_configs
from src.engine import run_backtest

# Load environment

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
    start_date = pd.Timestamp(cfg.backtest.start_date, tz='UTC')
    end_date = pd.Timestamp(cfg.backtest.end_date, tz='UTC')
    venue = cfg.backtest.venue
    strategy_config = MomentumConfig(
        instrument_ids=instruments,
        venue=venue,
    )

    starting_balances = list(cfg.backtest.starting_balances)
    print(start_date, end_date, venue, starting_balances)
    # 3. Run
    results = run_backtest(
        strategy_path="src.strategy:MomentumStrategy",
        config_path="src.config:MomentumConfig",
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
