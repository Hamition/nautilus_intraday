from dotenv import load_dotenv
import os
import pandas as pd

from datetime import datetime
from nautilus_trader.model.data import Bar, BarType, BarSpecification, BarAggregation
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.currencies import USD
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.identifiers import InstrumentId, Venue, Symbol
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.common.config import NautilusConfig

from nautilus_trader.core.rust.model import PriceType


load_dotenv()

# Initialize Tushare

# Initialize Parquet catalog
catalog_path = os.path.expanduser(os.getenv("NAUTILUS_ROOT"))
catalog = ParquetDataCatalog(path=catalog_path)

# Configuration
venue = Venue("XNYS")
#start_date = pd.Timestamp('2020-01-01', tz='UTC')
#end_date = pd.Timestamp('2025-09-11', tz='UTC')  # 9:02 PM JST = ~12:02 PM UTC
#overwrite_start_date = None  # Set to pd.Timestamp('2025-09-01', tz='UTC') for overwriting

def ingest_equities():
    # Load OHLCV data
    df_minute = pd.read_csv(f'{os.getenv("SIMULATE_DATA")}/simulated_intraday_stocks.csv', parse_dates=['date'])
    df_minute['date'] = pd.to_datetime(df_minute['date']).dt.tz_localize('America/New_York').dt.tz_convert('UTC')

    # Aggregate daily data
    df_daily = df_minute.groupby([df_minute['symbol'], df_minute['date'].dt.date]).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    df_daily['date'] = pd.to_datetime(df_daily['date']).dt.tz_localize('UTC')

    '''
    # Load fundamental data
    df_sectors = pro.stock_basic(exchange='SSE', fields='ts_code,symbol,industry')
    df_sectors['symbol'] = df_sectors['ts_code'].str.replace('.SH', '', regex=False)
    df_sectors = df_sectors[['symbol', 'industry']].drop_duplicates()

    df_earnings = pd.DataFrame()
    for symbol in df_sectors['symbol'][:100]:  # Limit to 100 for testing
        df_e = pro.income(ts_code=f"{symbol}.SH", start_date='20200101', end_date='20250911', fields='ts_code,ann_date,end_date,eps')
        df_e['symbol'] = symbol
        df_e['date'] = pd.to_datetime(df_e['ann_date']).dt.tz_localize('UTC')
        df_earnings = pd.concat([df_earnings, df_e[['symbol', 'date', 'eps']]])

    # Filter for append/overwrite
    if overwrite_start_date:
        df_minute = df_minute[df_minute['date'] >= overwrite_start_date]
        df_daily = df_daily[df_daily['date'] >= overwrite_start_date]
        df_earnings = df_earnings[df_earnings['date'] >= overwrite_start_date]
    '''

    # Define instrument
    instruments = []
    for symbol in sorted(df_daily['symbol'].unique()):  # Limit to 100 for testing
        #sector = df_sectors[df_sectors['symbol'] == symbol]['industry'].iloc[0] if symbol in df_sectors['symbol'].values else 'Unknown'
        instrument = Equity(
            instrument_id=InstrumentId(symbol=Symbol(symbol), venue=venue),
            raw_symbol=Symbol(symbol),
            currency=USD,
            price_precision=2,
            price_increment=Price.from_str("0.01"),
            lot_size=Quantity.from_int(100),
            ts_event=int(pd.Timestamp.now(tz='UTC').timestamp() * 1e9),
            ts_init=int(pd.Timestamp.now(tz='UTC').timestamp() * 1e9)
        )
        instruments.append(instrument)
        catalog.write_data([instrument])

    # Ingest minute bars
    minute_spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
    for i, symbol in enumerate(sorted(df_daily['symbol'].unique())):
        minute_type = BarType(instrument_id=instruments[i].id, bar_spec=minute_spec)
        instrument = instruments[i]
        wrangler = BarDataWrangler(bar_type=minute_type, instrument=instrument)
        df = df_minute[df_minute['symbol'] == symbol][['date', 'open', 'high', 'low', 'close', 'volume']].round(2)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df = df.set_index('timestamp')
        bars = wrangler.process(
            data=df,
        )
        catalog.write_data(data=bars)

    # Ingest daily bars
    daily_spec = BarSpecification(1, BarAggregation.DAY, PriceType.LAST)
    for i, symbol in enumerate(sorted(df_daily['symbol'].unique())):
        daily_type = BarType(instrument_id=instruments[i].id, bar_spec=daily_spec)
        instrument = instruments[i]
        wrangler = BarDataWrangler(bar_type=daily_type, instrument=instrument)
        df = df_daily[df_daily['symbol'] == symbol][['date', 'open', 'high', 'low', 'close', 'volume']].round(2)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df = df.set_index('timestamp')
        bars = wrangler.process(
            data=df,
        )
        catalog.write_data(data=bars)

    '''
    # Ingest fundamental data
    for symbol in df_earnings['symbol'].unique():
        instrument_id = InstrumentId(symbol=Symbol(symbol), venue=venue)
        df = df_earnings[df_earnings['symbol'] == symbol][['date', 'eps']]
        df['eps'] = df['eps'].astype(float)
        catalog.write_generic_data(
            data_type="EPS",
            instrument_id=instrument_id,
            data=df,
            ts_column="date",
            value_column="eps"
        )

    # Ingest sector data as metadata
    for instrument in instruments:
        symbol = instrument.id.symbol.value
        sector = df_sectors[df_sectors['symbol'] == symbol]['industry'].iloc[0] if symbol in df_sectors['symbol'].values else 'Unknown'
        catalog.write_generic_data(
            data_type="Sector",
            instrument_id=instrument.id,
            data=pd.DataFrame({
                'ts_event': [int(start_date.timestamp() * 1e9)],
                'sector': [sector]
            })
        )

    # Corporate actions (preprocess data)
    df_splits = pd.DataFrame([
        {'symbol': 'STOCK1', 'date': pd.Timestamp('2021-06-01', tz='UTC'), 'ratio': 2.0}
    ])
    df_mergers = pd.DataFrame([
        {'symbol': 'STOCK1', 'date': pd.Timestamp('2022-01-01', tz='UTC'), 'ratio': 0.5, 'acquirer_symbol': 'STOCK2'}
    ])
    for _, split in df_splits.iterrows():
        symbol = split['symbol']
        date = split['date']
        ratio = split['ratio']
        df_minute.loc[(df_minute['symbol'] == symbol) & (df_minute['date'] < date), ['open', 'high', 'low', 'close']] /= ratio
        df_daily.loc[(df_daily['symbol'] == symbol) & (df_daily['date'] < date), ['open', 'high', 'low', 'close']] /= ratio
        df_minute.loc[(df_minute['symbol'] == symbol) & (df_minute['date'] < date), 'volume'] *= ratio
        df_daily.loc[(df_daily['symbol'] == symbol) & (df_daily['date'] < date), 'volume'] *= ratio
    # Mergers require custom handling (e.g., adjust positions in strategy)
    '''


if __name__ == '__main__':
    ingest_equities()
