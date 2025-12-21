# src/execution/algos/pov.py
from nautilus_trader.model.enums import OrderSide

class POVExecutionAlgo:
    def __init__(self, config):
        self.cfg = config

    def on_bar(self, bar, schedule, engine):
        if schedule.remaining_qty == 0:
            engine.finish_schedule(schedule)
            return

        bar_volume = getattr(bar, "volume", 0)
        if bar_volume <= 0:
            return

        slice_qty = int(bar_volume * self.cfg.participation_rate)
        slice_qty = max(slice_qty, self.cfg.min_slice_qty)
        slice_qty = min(abs(schedule.remaining_qty), slice_qty)

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
