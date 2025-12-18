import os
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.model.data import Bar

def get_catalog(root_path: str = None) -> ParquetDataCatalog:
    """Initialize the data catalog."""
    path = root_path or os.path.expanduser(os.getenv('NAUTILUS_ROOT', '.'))
    return ParquetDataCatalog(path=path)

def get_top_liquid_instruments(catalog: ParquetDataCatalog, limit: int = 10) -> list[str]:
    """Filter and return the top N instruments."""
    instruments = catalog.instruments()
    # In a real scenario, you might filter by volume here
    return [str(inst.id) for inst in instruments][:limit]

def create_data_configs(catalog_path: str, instrument_ids: list[str]) -> list[BacktestDataConfig]:
    """Generate data configurations for minute and daily bars."""
    configs = []
    
    # Minute bars for trading
    configs.extend([
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst,
            bar_spec=f"{inst}-1-MINUTE-LAST-EXTERNAL"
        ) for inst in instrument_ids
    ])
    
    # Daily bars for ADV calculations
    configs.extend([
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst,
            bar_spec=f"{inst}-1-DAY-LAST-EXTERNAL"
        ) for inst in instrument_ids
    ])
    
    return configs
