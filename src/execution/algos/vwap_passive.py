from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.orders import LimitOrder

class PassiveVWAPExecutionAlgo:
    def __init__(self, config):
        self.cfg = config

    def on_bar(self, bar, schedule, engine):
        now = bar.ts_event

        if schedule.remaining_qty == 0:
            engine.finish_schedule(schedule)
            return

        # --- urgency logic ---
        remaining_minutes = max(
            1,
            int((schedule.end_ts - now) / 60_000_000_000),
        )

        aggressive = (
            remaining_minutes <= self.cfg.max_cross_spread_minutes
        )

        # --- volume-based slice ---
        bar_volume = getattr(bar, "volume", 0)
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

        if aggressive:
            engine.submit_market_order(
                instrument_id=schedule.instrument_id,
                side=side,
                quantity=slice_qty,
            )
        else:
            price = engine.compute_passive_price(
                bar,
                side,
                offset_ticks=self.cfg.price_offset_ticks,
            )

            engine.submit_limit_order(
                instrument_id=schedule.instrument_id,
                side=side,
                quantity=slice_qty,
                price=price,
            )

        schedule.remaining_qty -= (
            slice_qty if schedule.remaining_qty > 0 else -slice_qty
        )
