# src/strategy.py
from __future__ import annotations

from datetime import time
from exchange_calendars import get_calendar

from nautilus_trader.core.rust.model import PriceType
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.indicators import VolumeWeightedAveragePrice
from nautilus_trader.model import Quantity
from nautilus_trader.model.data import Bar, BarType, BarSpecification, BarAggregation
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, Venue
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.trading.strategy import Strategy

import numpy as np
import os
import pandas as pd

from .alpha import optimize_target_positions_usd
from .config import MomentumConfig
from .execution.engine import ExecutionEngine

class MomentumStrategy(Strategy):
    def __init__(self, config: MomentumConfig):
        super().__init__(config)

        self.instrument_ids = [InstrumentId.from_str(i) for i in config.instrument_ids]
        self.venue = Venue(config.venue)
        self.custom_config = config

        self.target_positions_usd = None
        self.day_count = 0
        self.at_wave = False
        self.catalog = ParquetDataCatalog(
            path=os.path.expanduser(os.getenv("NAUTILUS_ROOT"))
        )
        self.calendar = get_calendar(config.venue)

        self.vwaps = {
            inst_id: VolumeWeightedAveragePrice()
            for inst_id in self.instrument_ids

        }
        self.execution = ExecutionEngine(
            strategy=self,
            config=self.config.execution,
        )
        self._last_minute: int | None = None

    def _get_prices(self):
        prices = {}
        for inst_id in self.instrument_ids:
            price = self.cache.price(inst_id, PriceType.LAST)
            prices[inst_id] = float(price)

        prices = pd.Series(prices)
        return prices

    def _get_positions(self):
        positions = self.cache.positions()
        #print(positions)

        if len(positions) == 0:
            positions = pd.Series(0.0, index=self.instrument_ids)
        else:
            '''
            import inspect
            all_members = inspect.getmembers(positions[0])
            for name, value in all_members:
                if not name.startswith('__'):
                    print(f"{name}: {value}")
            '''
            positions = pd.Series([x.signed_qty for x in positions], index=[x.symbol for x in positions])
            positions = pd.Series(positions, index=self.instrument_ids).fillna(0.)
        return positions

    def _get_portfilio_value(self):

        account = self.portfolio.account(self.venue)
        '''
        import inspect
        all_members = inspect.getmembers(account)
        for name, value in all_members:
            if not name.startswith('__'):
                print(f"{name}: {value}")

        print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
        '''
        portfolio_value = account.balances_total()
        '''
        all_members = inspect.getmembers(portfolio_value)
        for name, value in all_members:
            if not name.startswith('__'):
                print(f"{name}: {value}")
        '''
        portfolio_value = float(list(portfolio_value.values())[0])
        return portfolio_value

    def on_start(self):
        for instrument_id in self.instrument_ids:
            bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            self.subscribe_bars(BarType(instrument_id, bar_spec))

    def on_bar(self, bar: Bar):
        ts_event = bar.ts_event
        timestamp = (
            pd.Timestamp(ts_event, unit="ns", tz="UTC")
            .astimezone("America/New_York")
        )
        self.execution.on_bar(bar)
        if not self.calendar.is_session(str(timestamp.date())):
            return

        inst_id = bar.bar_type.instrument_id

        # --- Update indicators (per-bar) ---
        self.vwaps[inst_id].update_raw(
            price=float(bar.close),
            volume=float(bar.volume),
            timestamp=timestamp,
        )

        # ------------------------------------------------------------
        # Execute strategy logic ONCE per minute
        # ------------------------------------------------------------
        current_minute = timestamp.minute

        if self._last_minute is None:
            self._last_minute = current_minute

        if current_minute != self._last_minute:
            self._last_minute = current_minute
            self.on_minute(ts_event)

    def on_minute(self, ts_event):
        """
        Called once per minute to:
        - build portfolio state
        - optimize target positions
        - execute trades
        """

        # ------------------------------------------------------------------
        # 1️⃣ Build inputs for optimizer
        # ------------------------------------------------------------------
        prices = self._get_prices()
        positions = self._get_positions()
        portfolio_value = self._get_portfilio_value()

        current_position_usd = positions * prices

        # --- Alpha & model inputs (unchanged placeholders) ---
        alpha = pd.Series(
            np.random.normal(loc=0.0, scale=1.0, size=len(self.instrument_ids)),
            index=self.instrument_ids,
        )

        trading_cost = pd.Series(0.005, index=self.instrument_ids)
        risk_lambda = pd.Series(0.001, index=self.instrument_ids)

        clip_pos_usd = pd.Series(
            self.custom_config.max_position_weight * portfolio_value, index=self.instrument_ids
        )
        clip_trd_usd = pd.Series(
            self.custom_config.max_trade_weight * portfolio_value, index=self.instrument_ids
        )

        factor_loading = pd.Series(
            np.random.normal(loc=0.0, scale=1.0, size=len(self.instrument_ids)),
            index=self.instrument_ids,
        )

        # ------------------------------------------------------------------
        # 2️⃣ Optimize TARGET POSITIONS (USD)
        # ------------------------------------------------------------------
        self.target_positions_usd = optimize_target_positions_usd(
            alpha=alpha,
            current_position_usd=current_position_usd,
            trading_cost=trading_cost,
            risk_lambda=risk_lambda,
            clip_pos_usd=clip_pos_usd,
            clip_trd_usd=clip_trd_usd,
            max_delta=self.custom_config.max_delta,
            factor_loading=factor_loading,
            max_factor_exposure=self.custom_config.max_factor_exposure,
        )

        # ------------------------------------------------------------------
        # 3️⃣ Execute trades
        # ------------------------------------------------------------------
        self.execute_wave(ts_event)

    def execute_wave(self, ts_event):
        if self.target_positions_usd is None:
            return

        prices = self._get_prices()
        positions = self._get_positions()

        target_shares = self.target_positions_usd / prices
        trades = target_shares - positions

        for inst_id, trade_qty in trades.items():
            if abs(trade_qty) < self.custom_config.min_trade_qty:
                continue
            self.execution.submit_target(
                instrument_id=inst_id,
                delta_qty=trade_qty,
                ts_event=ts_event,
            )

    def submit_market_order(self, instrument_id, side, quantity):

        order = self.order_factory.market(
                        instrument_id=instrument_id,
                        order_side=side,
                        quantity=Quantity(abs(quantity), 0),
                        time_in_force=TimeInForce.IOC,
                        client_order_id=ClientOrderId(str(UUID4()))
                    )
        self.submit_order(order)
