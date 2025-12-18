from nautilus_trader.backtest.node import BacktestNode, BacktestVenueConfig, BacktestRunConfig
from nautilus_trader.config import BacktestEngineConfig, ImportableStrategyConfig
from nautilus_trader.backtest.config import MarginModelConfig

def run_backtest(
    strategy_path: str,
    config_path: str,
    strategy_config,
    venue_name: str,
    data_configs: list,
    start,
    end
):
    """Configures and runs the backtest engine."""
    
    # 1. Venue Config
    venue_configs = [
        BacktestVenueConfig(
            name=venue_name,
            oms_type="NETTING",
            account_type="MARGIN",
            starting_balances=["1_000_000 USD"],
            margin_model=MarginModelConfig(model_type="standard")
        )
    ]

    # 2. Engine Config
    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path=strategy_path,
                config_path=config_path,
                config=strategy_config
            )
        ]
    )

    # 3. Node Config
    node = BacktestNode(
        configs=[
            BacktestRunConfig(
                engine=engine_config,
                venues=venue_configs,
                data=data_configs,
                start=start,
                end=end,
            )
        ]
    )

    print("Starting Backtest...")
    results = node.run()
    print("Backtest Complete.")
    return results
