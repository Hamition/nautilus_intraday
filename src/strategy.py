import os
import pandas as pd
import random
from datetime import time

from nautilus_trader.model import Quantity
from nautilus_trader.model.data import Bar, BarType, BarSpecification, BarAggregation
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, Venue
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.core.rust.model import PriceType
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.indicators import VolumeWeightedAveragePrice
from exchange_calendars import get_calendar

from .config import MomentumConfig
from .alpha import calculate_target_weights

class MomentumStrategy(Strategy):
    def __init__(self, config: MomentumConfig):
        super().__init__(config)
        self.instrument_ids = [InstrumentId.from_str(i) for i in config.instrument_ids]
        self.venue = Venue(config.venue)
        self.custom_config = config

        self.wave_times = [time.fromisoformat(t) for t in config.wave_times]
        self.wave = 0
        self.weights = None
        self.day_count = 0
        self.at_wave = False

        # We need the catalog for ADV lookups
        self.catalog = ParquetDataCatalog(path=os.path.expanduser(os.getenv('NAUTILUS_ROOT')))
        self.calendar = get_calendar(config.venue)
        self.vwaps = {inst_id: VolumeWeightedAveragePrice() for inst_id in self.instrument_ids}

    def on_start(self):
        for instrument_id in self.instrument_ids:
            bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            self.subscribe_bars(BarType(instrument_id, bar_spec))

    def on_bar(self, bar: Bar):
        # ... (Same Timezone logic as before) ...
        timestamp = pd.Timestamp(bar.ts_event, unit='ns', tz='UTC').astimezone('America/New_York')
        current_time = timestamp.time()

        if not self.calendar.is_session(str(timestamp.date())):
            return

        self.vwaps[bar.bar_type.instrument_id].update_raw(
            price=float(bar.close), volume=float(bar.volume), timestamp=timestamp
        )

        prev_at_wave = self.at_wave

        # 1. Daily Rebalance (Alpha Calculation)
        if self.day_count % self.custom_config.rebalance_freq == 0 and current_time == self.wave_times[0]:
            if not self.at_wave:
                # Call the external Alpha module
                self.weights = calculate_target_weights(
                    self.instrument_ids,
                    self.custom_config.max_position_weight,
                    self.custom_config.max_leverage
                )
                self.at_wave = True

        # 2. Execution
        if current_time == self.wave_times[self.wave]:
            self.execute_wave(bar.bar_type.instrument_id, bar)
            self.at_wave = True
        else:
            self.at_wave = False

        if prev_at_wave and not self.at_wave:
            self.wave = (self.wave + 1) % len(self.wave_times)
            if self.wave == 0:
                self.day_count += 1

    def execute_wave(self, instrument_id: InstrumentId, bar: Bar):
        if self.weights is None: return

        # ... (Execution logic remains similar) ...
        # (Omitted for brevity: Same as previous step, but uses self.custom_config.*)
        pass
