"""Fire dispatch - turns a tick decision into a brain.FireSchedule call.

Submodule:

- :mod:`~z4j_scheduler.dispatch.fire` - the dispatcher itself

Responsibilities (per ``docs/SCHEDULER.md §7.2``):

- Generate idempotency keys (deterministic from
  ``schedule_id + scheduled_for_timestamp``)
- Apply per-call deadline (60s default)
- Retry with jitter on transient failures (up to 3 attempts)
- Push acknowledgements back via the storage module
- Handle "no agent online" responses per the schedule's miss policy

Per-fire metric emissions:

- ``z4j_scheduler_fires_total{project,engine,status}`` increments
- ``z4j_scheduler_fire_latency_seconds`` observation
- Audit row attribution via brain (the FireSchedule call writes
  the audit row brain-side)
"""
