import exchange_calendars as xcals
import pandas as pd

def is_trading_time(timestamp: pd.Timestamp, exchange: str = "XNYS") -> bool:
    """
    Returns True if the timestamp is during regular trading hours for the exchange.

    Parameters
    ----------
    timestamp : pd.Timestamp
        The timestamp to check (should be timezone-aware, preferably UTC or exchange local).
    exchange : str
        Exchange code (e.g., "XNYS" for NYSE, "XLON" for London, "XHKG" for Hong Kong).

    Returns
    -------
    bool
        True if the exchange is open at that exact minute.
    """
    cal = xcals.get_calendar(exchange)

    # minute_open_at_time evaluates if the exchange is open as at that specific minute
    return cal.is_open_at_time(timestamp)
