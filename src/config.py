from nautilus_trader.trading.strategy import StrategyConfig

class MomentumConfig(StrategyConfig):
    instrument_ids: list[str]
    # Default waves: 10:00 AM, 11:00 AM, 2:00 PM (EST)
    wave_times: list[str] #= ["10:00", "11:00", "14:00"]
    venue: str = "XNYS"
    rebalance_freq: int = 1
    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_volume_ratio: float = 0.01
