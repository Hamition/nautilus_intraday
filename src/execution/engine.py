from collections import defaultdict

from .state import ExecutionSchedule
from .algos.vwap import VWAPExecutionAlgo


class ExecutionEngine:
    def __init__(self, strategy, config):
        self.strategy = strategy
        self.cfg = config

        self._schedules = defaultdict(list)

        if config.algo == "vwap":
            self.algo = VWAPExecutionAlgo(config)
        else:
            raise ValueError(f"Unknown execution algo: {config.algo}")

    # -----------------------------
    # Public API (used by strategy)
    # -----------------------------

    def submit_target(self, instrument_id, delta_qty, ts_event):
        if delta_qty == 0:
            return

        end_ts = ts_event + self.cfg.horizon_minutes * 60 * 1_000_000_000

        schedule = ExecutionSchedule(
            instrument_id=instrument_id,
            remaining_qty=delta_qty,
            end_ts=end_ts,
        )

        self._schedules[instrument_id].append(schedule)

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

    def finish_schedule(self, schedule):
        self._schedules[schedule.instrument_id].remove(schedule)
