# src/execution/algos/market.py
from __future__ import annotations

from nautilus_trader.model.enums import OrderSide

class MarketExecutionAlgo:
    def __init__(self, config):
        self.cfg = config

    def on_bar(self, bar, schedule, engine):
        # Submit once, then finish

        side = (
            OrderSide.BUY
            if schedule.remaining_qty > 0
            else OrderSide.SELL
        )

        engine.submit_market_order(
            instrument_id=schedule.instrument_id,
            side=side,
            quantity=schedule.remaining_qty,
        )

        engine.finish_schedule(schedule)
