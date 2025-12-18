import os
import pandas as pd
from dotenv import load_dotenv

from nautilus_trader.backtest.node import BacktestNode, BacktestVenueConfig, BacktestDataConfig, BacktestRunConfig
from nautilus_trader.config import BacktestEngineConfig, ImportableStrategyConfig
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.config import MarginModelConfig
from nautilus_trader.model.data import Bar

from src.config import MomentumConfig

# Load environment variables
load_dotenv()

def main():
    # 1. Setup Data Catalog
    catalog_path = os.path.expanduser(os.getenv('NAUTILUS_ROOT'))
    catalog = ParquetDataCatalog(path=catalog_path)
    
    # Select Instruments (First 10)
    n_symbols = 10
    instruments = catalog.instruments()
    instrument_ids = [str(inst.id) for inst in instruments][:n_symbols]
    print(f"Selected Instruments: {instrument_ids}")

    # 2. Define Simulation Period
    start_date = pd.Timestamp('2024-10-01', tz='UTC')
    end_date = pd.Timestamp('2024-11-01', tz='UTC')

    # 3. Configure Venue (Exchange)
    venue_configs = [
        BacktestVenueConfig(
            name="XNYS",
            oms_type="NETTING",
            account_type="MARGIN",
            starting_balances=["1_000_000 USD"],
            margin_model=MarginModelConfig(model_type="standard")
        )
    ]

    # 4. Configure Data Feeds
    # Loading 1-minute bars for trading
    data_configs = [
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-MINUTE-LAST-EXTERNAL"
        ) for inst_id in instrument_ids
    ]
    
    # (Optional) Add Daily bars if needed for the strategy's ADV calculation:
    data_configs += [
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-DAY-LAST-EXTERNAL"
        ) for inst_id in instrument_ids
    ]

    # 5. Configure Strategy
    strategy_config = MomentumConfig(
        instrument_ids=instrument_ids, 
        wave_times=["10:00", "11:00", "14:00"],
        venue="XNYS"
    )

    # Note: 'strategy_path' must point to the module path relative to where you run python
    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="src.strategy:MomentumStrategy",
                config_path="src.config:MomentumConfig",
                config=strategy_config
            )
        ]
    )

    # 6. Run Backtest
    node = BacktestNode(
        configs=[
            BacktestRunConfig(
                engine=engine_config,
                venues=venue_configs,
                data=data_configs,
                start=start_date,
                end=end_date,
            )
        ]
    )
    
    print("Starting Backtest...")
    results = node.run()
    print("Backtest Complete.")
    print(results)

if __name__ == '__main__':
    main()
