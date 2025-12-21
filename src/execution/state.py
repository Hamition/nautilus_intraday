from dataclasses import dataclass

@dataclass
class ExecutionSchedule:
    instrument_id: object
    remaining_qty: int
    end_ts: int
