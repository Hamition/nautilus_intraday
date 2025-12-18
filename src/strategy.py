import cvxpy as cp
import numpy as np
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
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.indicators import VolumeWeightedAveragePrice
from nautilus_trader.core.rust.model import PriceType
from exchange_calendars import get_calendar

from .config import MomentumConfig

class MomentumStrategy(Strategy):
    def __init__(self, config: MomentumConfig):
        super().__init__(config)
        self.instrument_ids = [InstrumentId.from_str(i) for i in config.instrument_ids]
        self.venue = Venue(config.venue)
        self.rebalance_freq = config.rebalance_freq
        self.max_leverage = config.max_leverage
        self.max_position_weight = config.max_position_weight
        self.max_volume_ratio = config.max_volume_ratio
        
        # Parse wave times
        self.wave_times = [time.fromisoformat(t) for t in config.wave_times]
        
        # State variables
        self.wave = 0
        self.weights = None
        self.day_count = 0
        self.at_wave = False
        
        # Infrastructure
        self.catalog = ParquetDataCatalog(path=os.path.expanduser(os.getenv('NAUTILUS_ROOT')))
        self.calendar = get_calendar(config.venue)
        self.vwaps = {inst_id: VolumeWeightedAveragePrice() for inst_id in self.instrument_ids}

    def on_start(self):
        """Subscribe to minute bars for all instruments."""
        for instrument_id in self.instrument_ids:
            bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            bar_type = BarType(instrument_id=instrument_id, bar_spec=bar_spec)
            self.subscribe_bars(bar_type)

    def on_bar(self, bar: Bar):
        """Process incoming bars to trigger rebalancing or wave execution."""
        # Convert timestamp to Strategy Timezone (NY)
        timestamp = pd.Timestamp(bar.ts_event, unit='ns', tz='UTC').astimezone('America/New_York')
        current_time = timestamp.time()
        current_date = timestamp.date()
        
        # Skip non-session times
        if not self.calendar.is_session(str(current_date)):
            return

        # Update Indicators
        self.vwaps[bar.bar_type.instrument_id].update_raw(
            price=float(bar.close),
            volume=float(bar.volume),
            timestamp=timestamp
        )

        prev_at_wave = self.at_wave

        # 1. Daily Rebalance Logic (First wave of the day)
        if self.day_count % self.rebalance_freq == 0 and current_time == self.wave_times[0]:
            if not self.at_wave:
                self.compute_weights()
                self.at_wave = True

        # 2. Intra-day Wave Execution
        if current_time == self.wave_times[self.wave]:
            self.execute_wave(bar.bar_type.instrument_id, bar)
            self.at_wave = True
        else:
            self.at_wave = False

        # Advance wave counter after wave completion
        if prev_at_wave and not self.at_wave:
            self.wave = (self.wave + 1) % len(self.wave_times)
            if self.wave == 0:
                self.day_count += 1

    def compute_weights(self):
        """Calculate target portfolio weights using CVXPY."""
        n = len(self.instrument_ids)
        
        # --- Optimization Logic ---
        # Note: Replace this random signal with actual alpha signal loading if needed
        mu = np.random.normal(size=n) * 0.1 
        Sigma = np.diag(np.ones(n))
        gamma = 1.0

        w = cp.Variable(n)
        ret = mu.T @ w
        risk = cp.quad_form(w, Sigma)
        
        prob = cp.Problem(
            cp.Maximize(ret - gamma * risk),
            [
                cp.sum(w) == 1, 
                w >= 0, 
                w <= self.max_position_weight, 
                cp.norm(w, 1) <= self.max_leverage
            ]
        )

        try:
            prob.solve(solver=cp.SCS, max_iters=10000)
            weights_val = w.value
            if weights_val is None: 
                raise ValueError("Solver returned None")
        except (cp.SolverError, ValueError) as e:
            # Fallback to equal weights on failure
            print(f"Optimization failed: {e}")
            weights_val = np.ones(n) / n

        self.weights = pd.Series(weights_val, index=self.instrument_ids)
        self.wave = 0 # Reset wave counter on rebalance

    def execute_wave(self, instrument_id: InstrumentId, bar: Bar):
        """Execute orders for a specific instrument during a wave."""
        if self.weights is None:
            return

        # Distribute execution across waves
        wave_weight = self.weights / len(self.wave_times)
        weight = wave_weight.get(instrument_id, 0)
        if weight == 0:
            return

        current_price = float(bar.close)
        if current_price == 0:
            return
            
        # Portfolio Sizing
        portfolio_value = 1_000_000  # Could fetch from self.portfolio.cash()
        
        # Calculate Average Daily Volume (ADV) for liquidity limits
        # Note: This catalog query is expensive in a loop; consider caching results in on_start or compute_weights
        ed = pd.Timestamp(self.clock.timestamp_ns(), unit='ns', tz='UTC').date()
        sd = ed - pd.Timedelta(days=21)
        
        # Simplified ADV Calculation
        daily_bars = self.catalog.bars(
            instrument_ids=[instrument_id],
            bar_types=[f"{instrument_id}-1-DAY-LAST-EXTERNAL"],
            start=sd,
            end=ed
        )
        
        if not daily_bars:
            return
            
        avg_daily_volume = np.mean([float(b.volume) * float(b.close) for b in daily_bars])
        max_notional = avg_daily_volume * self.max_volume_ratio

        # Determine Order Size
        target_shares = (weight * portfolio_value) / current_price
        
        # Clip size to max notional allowed
        if weight > 0:
            shares_to_order = min(target_shares, max_notional / current_price)
        else:
            shares_to_order = max(target_shares, -max_notional / current_price)

        if abs(shares_to_order) < 1:
            return

        # Random sampling filter (from original code - remove for production)
        if int(str(instrument_id).split('.')[0][3:]) % 15 != random.randint(0, 14):
            return

        # Construct Order
        timestamp = pd.Timestamp(self.clock.timestamp_ns(), unit='ns', tz='UTC').astimezone('America/New_York')
        client_order_id = f"O-{instrument_id}-{str(timestamp).replace(' ', '-')}"
        
        order = MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.id,
            instrument_id=instrument_id,
            client_order_id=ClientOrderId(client_order_id),
            order_side=OrderSide.BUY if shares_to_order > 0 else OrderSide.SELL,
            quantity=Quantity(abs(shares_to_order), precision=0),
            init_id=UUID4(),
            time_in_force=TimeInForce.DAY,
            ts_init=self.clock.timestamp_ns()
        )

        self.submit_order(order)
