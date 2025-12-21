# src/execution/algos/twap.py
from __future__ import annotations

from nautilus_trader.model.enums import OrderSide

class TWAPExecutionAlgo:
    def __init__(self, config):
        self.cfg = config

    def on_bar(self, bar, schedule, engine):
        now = bar.ts_event

        if now >= schedule.end_ts or schedule.remaining_qty == 0:
            engine.finish_schedule(schedule)
            return

        remaining_minutes = max(
            1,
            int((schedule.end_ts - now) / 60_000_000_000),
        )

        slice_qty = max(
            abs(schedule.remaining_qty) // remaining_minutes,
            self.cfg.min_slice_qty,
        )

        slice_qty = min(abs(schedule.remaining_qty), slice_qty)

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
