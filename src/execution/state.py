# src/execution/state.py
from __future__ import annotations

from dataclasses import dataclass

@dataclass
class ExecutionSchedule:
    instrument_id: object
    remaining_qty: int
    start_ts: int
    end_ts: int
