"""Scheduler-side gRPC server for the ``TriggerSchedule`` RPC.

This is the reverse-direction half of the SchedulerService contract.
Brain hosts the bulk of the service (List/Watch/Fire/Ack/Ping) and
the scheduler is the gRPC client; for ``TriggerSchedule`` the roles
flip - the dashboard's "fire now" button issues a brain-side request
that becomes a gRPC call INTO the scheduler.

Why the inversion: triggering a fire from the dashboard needs the
scheduler's local cache state to update (so the next tick doesn't
double-fire) and needs to flow through the same FireDispatcher
retry + audit pipeline as a tick-driven fire. Putting the entry
point on the scheduler side keeps that path single.

Submodules:

- :mod:`~z4j_scheduler.trigger_grpc.handlers` - the
  ``TriggerScheduleServicer`` implementation
- :mod:`~z4j_scheduler.trigger_grpc.server` - grpc.aio lifecycle
- :mod:`~z4j_scheduler.trigger_grpc.auth` - mTLS interceptor +
  CN allow-list (mirrors the brain-side pattern in
  ``z4j_brain.scheduler_grpc.auth``)

Off by default - opt in via ``Z4J_SCHEDULER_TRIGGER_GRPC_ENABLED``
once the operator deploys both halves with matching mTLS material.
"""

from __future__ import annotations
