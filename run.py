import os
import pandas as pd
from dotenv import load_dotenv

from src.config import MomentumConfig
from src.data import get_catalog, get_top_liquid_instruments, create_data_configs
from src.engine import run_backtest

# Load environment
load_dotenv()
NAUTILUS_ROOT = os.getenv('NAUTILUS_ROOT')

def main():
    # 1. Prepare Data
    catalog = get_catalog(NAUTILUS_ROOT)
    instruments = get_top_liquid_instruments(catalog, limit=10)
    print(f"Selected Instruments: {instruments}")

    data_configs = create_data_configs(NAUTILUS_ROOT, instruments)

    # 2. Define Parameters
    start_date = pd.Timestamp('2024-10-01', tz='UTC')
    end_date = pd.Timestamp('2024-11-01', tz='UTC')
    venue = "XNYS"

    strategy_config = MomentumConfig(
        instrument_ids=instruments,
        venue=venue,
    )

    # 3. Run
    results = run_backtest(
        strategy_path="src.strategy:MomentumStrategy",
        config_path="src.config:MomentumConfig",
        strategy_config=strategy_config,
        venue_name=venue,
        data_configs=data_configs,
        start=start_date,
        end=end_date
    )

    print(results)

if __name__ == '__main__':
    main()
