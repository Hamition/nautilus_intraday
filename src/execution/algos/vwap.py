# src/execution/algos/vwap.py
from __future__ import annotations

from nautilus_trader.model.enums import OrderSide

class VWAPExecutionAlgo:
    def __init__(self, config):
        self.cfg = config

    def on_bar(self, bar, schedule, engine):
        if schedule.remaining_qty == 0:
            return

        now = bar.ts_event
        if now >= schedule.end_ts:
            engine.finish_schedule(schedule)
            return

        max_participation = int(
            bar.volume * self.cfg.participation_rate
        )

        slice_qty = min(abs(schedule.remaining_qty), max_participation)
        slice_qty = max(slice_qty, self.cfg.min_slice_qty)

        if slice_qty <= 0:
            return

        side = (
            OrderSide.BUY
            if schedule.remaining_qty > 0
            else OrderSide.SELL
        )

        engine.submit_market_order(
            instrument_id=schedule.instrument_id,
            side=side,
            quantity=slice_qty,
        )

        schedule.remaining_qty -= (
            slice_qty if schedule.remaining_qty > 0 else -slice_qty
        )
