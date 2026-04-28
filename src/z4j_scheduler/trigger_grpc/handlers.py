"""``TriggerScheduleServicer`` - the operator-trigger handler.

One RPC: brain calls ``TriggerSchedule(schedule_id, user_id,
idempotency_key)``, the scheduler resolves the schedule from its
local cache, fires it through the existing :class:`FireDispatcher`,
and returns the brain-side ``command_id``.

Why route through the scheduler at all (vs. brain dispatching
directly): so the scheduler's local cache last_fire_at gets the
update (preventing the next tick from double-firing), and so the
operator-trigger flows through the same retry + audit pipeline as
a tick-driven fire. Single code path = single place to debug.

The ``user_id`` and ``idempotency_key`` from the request are
threaded into the audit/log breadcrumbs so the operator who clicked
is preserved on both sides.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

import grpc

from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.proto import scheduler_pb2_grpc as pb_grpc

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.dispatch.fire import FireDispatcher
    from z4j_scheduler.storage.cache import ScheduleCache

logger = logging.getLogger("z4j.scheduler.trigger_grpc.handlers")


class TriggerScheduleServicer(pb_grpc.SchedulerServiceServicer):
    """Implements only the ``TriggerSchedule`` RPC.

    The brain side of the proto declares all six RPCs on a single
    ``SchedulerService``; the scheduler implements only the one
    that flows brain → scheduler. Other RPCs return UNIMPLEMENTED
    via the default servicer behaviour.

    Audit fix C-2 (Apr 2026 follow-up): the servicer now checks the
    leader gate before dispatching. In an HA deployment with
    multiple scheduler instances behind a load balancer, brain may
    route ``TriggerSchedule`` to any instance. Without a leader-gate
    check, every standby that has the schedule in its cache would
    dispatch the fire - one operator click became N parallel fires
    (the brain-side idempotency-key constraint does NOT save us
    because ``trigger_now`` mints a fresh ``uuid4`` per call). The
    fix returns ``not_leader`` so brain can retry against the actual
    leader, mirroring the pattern brain already implements for
    tick-driven fires.
    """

    def __init__(
        self,
        *,
        cache: ScheduleCache,
        dispatcher: FireDispatcher,
        leader_gate: object,
    ) -> None:
        self._cache = cache
        self._dispatcher = dispatcher
        self._leader_gate = leader_gate

    async def TriggerSchedule(  # noqa: N802 - gRPC-generated name
        self,
        request: pb.TriggerScheduleRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.TriggerScheduleResponse:
        try:
            schedule_id = UUID(request.schedule_id)
        except ValueError as exc:
            return pb.TriggerScheduleResponse(
                error_code="invalid_request",
                error_message=str(exc),
            )

        # Lookup in the local cache. Fast: the cache is hot via the
        # WatchSchedules stream so a freshly-created schedule
        # appears within ~3s of the brain commit.
        entry = await self._cache.get(schedule_id)
        if entry is None:
            return pb.TriggerScheduleResponse(
                error_code="not_in_cache",
                error_message=(
                    f"schedule {schedule_id} not in this scheduler's cache "
                    "(may belong to a different cluster, or watch stream "
                    "has not seen it yet)"
                ),
            )
        if not entry.is_enabled:
            return pb.TriggerScheduleResponse(
                error_code="schedule_disabled",
                error_message="schedule is disabled",
            )

        # Audit fix C-2: leader gate. Standbys reject the trigger so
        # brain can retry against the leader. Single-instance
        # deployments use ``SingleInstanceLeaderGate`` whose
        # ``is_leader`` is unconditionally True - no behavior change.
        is_leader_fn = getattr(self._leader_gate, "is_leader", None)
        if is_leader_fn is None or not bool(
            is_leader_fn(entry.project_id),
        ):
            logger.info(
                "z4j.scheduler.trigger_grpc: not leader for project=%s "
                "schedule_id=%s; rejecting TriggerSchedule",
                entry.project_id, schedule_id,
            )
            return pb.TriggerScheduleResponse(
                error_code="not_leader",
                error_message=(
                    "this scheduler instance is not leader for the "
                    "schedule's project; brain should retry against the "
                    "leader"
                ),
            )

        logger.info(
            "z4j.scheduler.trigger_grpc: TriggerSchedule schedule_id=%s "
            "user_id=%s idempotency_key=%s",
            schedule_id,
            request.user_id or "(unknown)",
            request.idempotency_key or "(none)",
        )

        result = await self._dispatcher.trigger_now(
            schedule_id=schedule_id,
        )
        if result.success:
            return pb.TriggerScheduleResponse(
                command_id=str(result.command_id),
            )
        return pb.TriggerScheduleResponse(
            error_code=result.error_code or "unknown",
            error_message=result.error_message or "",
        )


__all__ = ["TriggerScheduleServicer"]
