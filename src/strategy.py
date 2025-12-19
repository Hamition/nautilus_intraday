# src/strategy.py
from __future__ import annotations

import os
import pandas as pd
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
from .alpha import optimize_target_positions_usd   # 🔴 CHANGED


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

    def _get_prices(self):

        prices = {}
        for inst_id in self.instrument_ids:
            price = self.cache.price(inst_id, PriceType.LAST)
            prices[inst_id] = float(price)

        prices = pd.Series(prices)
        return prices

    def _get_positions(self):
        positions = self.cache.positions()
        print(positions)
        positions = pd.Series(positions)
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
        timestamp = (
            pd.Timestamp(bar.ts_event, unit="ns", tz="UTC")
            .astimezone("America/New_York")
        )

        if not self.calendar.is_session(str(timestamp.date())):
            return

        inst_id = bar.bar_type.instrument_id

        self.vwaps[inst_id].update_raw(
            price=float(bar.close),
            volume=float(bar.volume),
            timestamp=timestamp,
        )

        # ------------------------------------------------------------------
        # 1️⃣ Build inputs for optimizer
        # ------------------------------------------------------------------
        prices = self._get_prices()  # Series indexed by InstrumentId
        positions = self._get_positions()  # shares
        portfolio_value = self._get_portfilio_value()

        current_position_usd = positions * prices

        alpha = self.custom_config.alpha_signal.loc[self.instrument_ids]

        trading_cost = self.custom_config.trading_cost_usd.loc[self.instrument_ids]
        risk_lambda = self.custom_config.risk_lambda.loc[self.instrument_ids]
        clip_pos_usd = self.custom_config.clip_pos_usd.loc[self.instrument_ids]
        clip_trd_usd = self.custom_config.clip_trd_usd.loc[self.instrument_ids]

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
            factor_exposure=self.custom_config.factor_exposure,
            clip_exposure=self.custom_config.clip_exposure,
        )

        # ------------------------------------------------------------------
        # 3️⃣ Execute trades
        # ------------------------------------------------------------------
        self.execute_wave(bar)

    def execute_wave(self, bar: Bar):
        if self.target_positions_usd is None:
            return

        prices = self.cache.prices()
        positions = self.cache.positions()

        target_shares = self.target_positions_usd / prices
        trades = target_shares - positions

        for inst_id, trade_qty in trades.items():
            if abs(trade_qty) < self.custom_config.min_trade_qty:
                continue

            side = OrderSide.BUY if trade_qty > 0 else OrderSide.SELL

            order = MarketOrder(
                instrument_id=inst_id,
                order_side=side,
                quantity=Quantity(abs(trade_qty), 0),
                time_in_force=TimeInForce.IOC,
                client_order_id=ClientOrderId(UUID4()),
            )

            self.submit_order(order)
