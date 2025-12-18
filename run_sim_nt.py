import cvxpy as cp
import numpy as np
import os
import pandas as pd
import pytz
import random
import uuid
#from dataclasses import field
from datetime import time, datetime
from dotenv import load_dotenv
from nautilus_trader.model import Quantity
from nautilus_trader.model.data import Bar, BarType, BarSpecification, BarAggregation
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig

from nautilus_trader.config import ImportableStrategyConfig


from nautilus_trader.model.identifiers import InstrumentId, Venue, Symbol
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.indicators import VolumeWeightedAveragePrice
from exchange_calendars import get_calendar

from nautilus_trader.core.rust.model import PriceType

from nautilus_trader.config import LoggingConfig


load_dotenv()
start_date = pd.Timestamp('2024-10-01', tz='UTC')
end_date = pd.Timestamp('2024-11-01', tz='UTC')  # 9:02 PM JST = ~12:02 PM UTC

n_symbols = 10

waves = ["10:00", "11:00", "14:00"]
class MomentumConfig(StrategyConfig):
    instrument_ids: list[str]
    wave_times: list[str] #= field(default_factory=lambda: ["10:00", "11:00", "14:00"])
    venue: str = "XNYS"
    rebalance_freq: int = 1
    max_leverage: float = 1.5
    max_position_weight: float = 0.05
    max_volume_ratio: float = 0.01

class MomentumStrategy(Strategy):
    def __init__(self, config: MomentumConfig):
        super().__init__(config)
        self.instrument_ids = [InstrumentId.from_str(id) for id in config.instrument_ids][:n_symbols]
        self.venue = Venue(config.venue)
        self.rebalance_freq = config.rebalance_freq
        self.max_leverage = config.max_leverage
        self.max_position_weight = config.max_position_weight
        self.max_volume_ratio = config.max_volume_ratio
        self.wave_times = [time.fromisoformat(t) for t in config.wave_times]
        self.wave = 0
        self.weights = None
        self.day_count = 0
        self.catalog = ParquetDataCatalog(path=os.path.expanduser(os.getenv('NAUTILUS_ROOT')))
        self.calendar = get_calendar("XNYS")
        self.vwaps = {inst_id: VolumeWeightedAveragePrice() for inst_id in self.instrument_ids}
        self.at_wave = False
        #self.risk_engine.set_max_order_submit_rate(1_000_000)

    def on_start(self):
        for instrument_id in self.instrument_ids:
            bar_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            bar_type = BarType(instrument_id=instrument_id, bar_spec=bar_spec)
            self.subscribe_bars(bar_type)

    def on_bar(self, bar: Bar):
        timestamp = pd.Timestamp(bar.ts_event, unit='ns', tz='UTC').astimezone('America/New_York')
        #print ("on_bar", timestamp, bar)
        current_time = timestamp.time()
        current_date = timestamp.date()
        #print(bar.ts_event)
        #print(current_date)
        session = self.calendar.is_session(str(current_date))

        if not session: #or current_time >= time(11, 30) and current_time < time(13, 0):
            return  # Skip

        # Update VWAP
        self.vwaps[bar.bar_type.instrument_id].update_raw(
            price=float(bar.close),
            volume=float(bar.volume),
            timestamp=timestamp
        )

        prev_at_wave = self.at_wave
        # Rebalance every day
        if self.day_count % self.rebalance_freq == 0 and current_time == self.wave_times[0]:
            if not self.at_wave:
                self.compute_weights()
                self.at_wave = True

        # Execute intra-day waves
        #print(current_time, self.wave_times[self.wave])
        #print ("execute_wave", self.wave, current_time)
        if current_time == self.wave_times[self.wave]:
            #print(bar.bar_type.instrument_id, flush=True)
            #print(bar)
            self.execute_wave(bar.bar_type.instrument_id, bar)
            self.at_wave = True
        else:
            self.at_wave = False
        if prev_at_wave and not self.at_wave:
            self.wave = (self.wave + 1) % 3


    def compute_weights(self):
        # Load daily bars for momentum and volume
        '''
        data = []
        #current_date = pd.Timestamp(bar.ts_event / 1e9, unit='ns', tz='UTC').astimezone('America/New_York').date()

        for instrument_id in self.instrument_ids:
            bars = self.catalog.bars(
                instrument_id=instrument_id,
                bar_type=f"{instrument_id}-1-DAY-LAST-EXTERNAL",
                start=start_date,
                end=end_date
            )
            df = pd.DataFrame([(b.ts_event, b.close, b.volume) for b in bars], columns=['ts_event', 'close', 'volume'])
            df['symbol'] = instrument_id.symbol.value
            data.append(df)
        df = pd.concat(data)
        df['date'] = pd.to_datetime(df['ts_event'] / 1e9, unit='ns', tz='UTC')
        df = df.pivot(index='date', columns='symbol', values=['close', 'volume'])

        # Compute momentum and volume
        returns = df['close'].pct_change(periods=21).iloc[-1]
        volume = df['volume'].rolling(21).mean().iloc[-1]
        universe = volume > 1e6  # Filter by dollar volume


        # Load fundamentals
        eps_data = []
        sector_data = []
        for instrument_id in self.instrument_ids:
            eps = self.catalog.generic_data(
                data_type="EPS",
                instrument_id=instrument_id,
                start=start_date,
                end=end_date
            )
            sector = self.catalog.generic_data(
                data_type="Sector",
                instrument_id=instrument_id,
                start=start_date,
                end=end_date
            )
            eps_df = pd.DataFrame([(e.ts_event, e.value) for e in eps], columns=['ts_event', 'eps'])
            eps_df['symbol'] = instrument_id.symbol.value
            sector_value = sector[0].value if sector else 'Unknown'
            sector_df = pd.DataFrame({'ts_event': [start_date.timestamp() * 1e9], 'sector': [sector_value], 'symbol': instrument_id.symbol.value})
            eps_data.append(eps_df)
            sector_data.append(sector_df)
        eps_df = pd.concat(eps_data).pivot(index='ts_event', columns='symbol', values='eps').ffill()
        sector_df = pd.concat(sector_data).pivot(index='ts_event', columns='symbol', values='sector').ffill()

        # Apply fundamental filters
        positive_eps = eps_df.iloc[-1] > 0
        exclude_sectors = ~sector_df.iloc[-1].isin(['Bank', 'Insurance'])
        screen = universe & positive_eps & exclude_sectors
        top_momentum = returns[screen].nlargest(100)
        '''

        # CVXPY optimization
        n = n_symbols
        w = cp.Variable(n)
        #mu = top_momentum.values
        mu = np.random.normal(size=n) * 0.1
        Sigma = np.diag(np.ones(n))
        gamma = 1.0
        ret = mu.T @ w
        risk = cp.quad_form(w, Sigma)
        prob = cp.Problem(
            cp.Maximize(ret - gamma * risk),
            [cp.sum(w) == 1, w >= 0, w <= self.max_position_weight, cp.norm(w, 1) <= self.max_leverage]
        )
        #for instrument_id in self.instrument_ids:
        #    print(instrument_id)

        try:
            prob.solve(solver=cp.SCS, max_iters=10000)
            self.weights = pd.Series(w.value, index=self.instrument_ids)
        except (cp.SolverError, ValueError) as e:
            print(f"CVXPY failed: {e}")
            self.weights = pd.Series(np.ones(n) / n, index=self.instrument_ids)
        self.weights = pd.Series(np.ones(n) / n, index=self.instrument_ids)
        self.wave = 0

    def execute_wave(self, instrument_id, bar):
        if self.weights is None:
            return

        wave_weight = self.weights / len(self.wave_times)
        weight = wave_weight.loc[instrument_id]

        current_price = float(bar.close)
        if current_price == 0:
            return
        portfolio_value = 1e6

        ed = pd.Timestamp(self.clock.timestamp_ns(), unit='ns', tz='UTC').date()
        sd = ed - pd.Timedelta(days=21)
        daily_bars = self.catalog.bars(instrument_ids=[instrument_id],
                                       bar_types=[f"{instrument_id}-1-DAY-LAST-EXTERNAL"],
                                       start=sd,
                                       end=ed)


        avg_daily_volume = np.mean([float(bar.volume) * float(bar.close) for bar in daily_bars])
        #print(daily_bars, avg_daily_volume)
        #print(pd.Timestamp(daily_bars[0].ts_event, unit='ns', tz='UTC').astimezone('America/New_York'),
        #      pd.Timestamp(daily_bars[-1].ts_event, unit='ns', tz='UTC').astimezone('America/New_York'))
        #os.system("sleep 1")
        max_notional = avg_daily_volume * self.max_volume_ratio
        #max_notional = portfolio_value * 0.5
        target_shares = (weight * portfolio_value) / current_price
        shares_to_order = min(target_shares, max_notional / current_price) if weight > 0 else max(target_shares, -max_notional / current_price)
        #print(shares_to_order)
        #shares_to_order = 10
        #print(Quantity(abs(shares_to_order), precision=0))
        #print(self.cache.positions_open())

        timestamp = pd.Timestamp(self.clock.timestamp_ns(), unit='ns', tz='UTC').astimezone('America/New_York')
        client_order_id = "O-" + str(instrument_id) + "-" + str(timestamp).replace(' ', '-')
        #print(client_order_id)
        #os.system("sleep 10")
        if abs(shares_to_order) < 1:
            return
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

        current_vwap = self.vwaps[instrument_id].value
        print(current_vwap, current_price)
        '''
        if current_vwap and current_price > current_vwap * 1.01:  # Avoid buying above VWAP + 1%
            return
        '''

        if int(str(instrument_id).split('.')[0][3:]) % 15 == random.randint(0, 14):
            self.submit_order(order)
        print(self.portfolio.account(self.venue))
        print(self.portfolio.unrealized_pnl(instrument_id))
        print(self.portfolio.realized_pnl(instrument_id))

if __name__ == '__main__':
    from nautilus_trader.backtest.node import BacktestNode, BacktestVenueConfig, BacktestDataConfig, BacktestRunConfig
    from nautilus_trader.config import BacktestEngineConfig
    from nautilus_trader.backtest.config import MarginModelConfig
    catalog_path = os.path.expanduser(os.getenv('NAUTILUS_ROOT'))
    catalog = ParquetDataCatalog(path=catalog_path)
    instruments = catalog.instruments()
    instrument_ids = [str(inst.id) for inst in instruments][:n_symbols]
    print(instrument_ids)
    venue_configs = [BacktestVenueConfig(name="XNYS",
                                         oms_type="NETTING",
                                         account_type="MARGIN",
                                         starting_balances=["1_000_000 USD"],
                                         margin_model=MarginModelConfig(model_type="standard"))]
    data_configs = [
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-MINUTE-LAST-EXTERNAL"
        ) for inst_id in instrument_ids
    ]
    '''
    + [
        BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls=Bar,
            instrument_id=inst_id,
            bar_spec=f"{inst_id}-1-DAY-LAST-EXTERNAL"
        ) for inst_id in instrument_ids
    ]
    '''
    strategy_config = MomentumConfig(instrument_ids=instrument_ids, wave_times=waves)

    engine_config = BacktestEngineConfig(strategies=[ImportableStrategyConfig(strategy_path="__main__:MomentumStrategy",
                                                                              config_path="__main__:MomentumConfig",
                                                                              config=strategy_config)
                                                     ])
    #logging_config = LoggingConfig(log_level="ERROR")
    #logging=logging_config
    node = BacktestNode(configs=[BacktestRunConfig(
        engine=engine_config,
        venues=venue_configs,
        data=data_configs,
        start=start_date,
        end=end_date,
    )])
    results = node.run()
    print(results)
