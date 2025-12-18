from nautilus_trader.trading.strategy import StrategyConfig

class MomentumConfig(StrategyConfig):
    instrument_ids: list[str]
    wave_times: list[str]
    venue: str = "XNYS"
    rebalance_freq: int = 1
    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_volume_ratio: float = 0.01
