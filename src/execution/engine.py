# src/execution/engine.py
from __future__ import annotations

from collections import defaultdict
from nautilus_trader.model.enums import OrderSide

from .algos.market import MarketExecutionAlgo
from .algos.pov import POVExecutionAlgo
from .algos.twap import TWAPExecutionAlgo
from .algos.vwap import VWAPExecutionAlgo
from .algos.vwap_passive import PassiveVWAPExecutionAlgo
from .state import ExecutionSchedule

class ExecutionEngine:
    def __init__(self, strategy, config):
        self.strategy = strategy
        self.cfg = config
        self._schedules = {}

        if config.algo == "vwap" and config.passive:
            self.algo = PassiveVWAPExecutionAlgo(config)
        elif config.algo == "vwap":
            self.algo = VWAPExecutionAlgo(config)
        elif config.algo == "twap":
            self.algo = TWAPExecutionAlgo(config)
        elif config.algo == "pov":
            self.algo = POVExecutionAlgo(config)
        elif config.algo == "market":
            self.algo = MarketExecutionAlgo(config)
        else:
            raise ValueError(f"Unknown execution algo: {config.algo}")


    # -----------------------------
    # Public API (used by strategy)
    # -----------------------------
    def submit_target(self, instrument_id, delta_qty, ts_event):
        if delta_qty == 0:
            return

        end_ts = (
            ts_event
            if self.cfg.algo == "market"
            else ts_event + self.cfg.horizon_minutes * 60_000_000_000
        )

        self._schedules.setdefault(instrument_id, []).append(
            ExecutionSchedule(
                instrument_id=instrument_id,
                remaining_qty=delta_qty,
                start_ts=ts_event,
                end_ts=end_ts,
            )
        )

    # -----------------------------
    # Called every bar
    # -----------------------------

    def on_bar(self, bar):
        instrument_id = bar.bar_type.instrument_id

        schedules = self._schedules.get(instrument_id)
        if not schedules:
            return

        for schedule in list(schedules):
            self.algo.on_bar(bar, schedule, self)

            if schedule.remaining_qty == 0:
                schedules.remove(schedule)

        if not schedules:
            self._schedules.pop(instrument_id, None)

    # -----------------------------
    # Order submission
    # -----------------------------

    def submit_market_order(self, instrument_id, side, quantity):
        self.strategy.submit_market_order(instrument_id, side, quantity)

    def submit_limit_order(self, instrument_id, side, quantity, price):
        self.strategy.submit_limit_order(instrument_id, side, quantity, price)

    def compute_passive_price(self, bar, side, offset_ticks=0):
        tick_size = self.strategy.cache.instrument(bar.bar_type.instrument_id).price_increment
        if side == OrderSide.BUY:
            return bar.close - tick_size * offset_ticks
        else:
            return bar.close + tick_size * offset_ticks

    def finish_schedule(self, schedule):
        self._schedules[schedule.instrument_id].remove(schedule)
