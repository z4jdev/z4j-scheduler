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
is preserved on both sides. The ``idempotency_key`` is also
honored for dedup (audit fix S002, 1.4.0): brain's
``TriggerScheduleClient`` retries exactly once on UNAVAILABLE /
DEADLINE_EXCEEDED, which is precisely the case where the first
call may have succeeded but the response was lost. Without a
dedup cache, that retry caused a duplicate fire because
``trigger_now`` mints a fresh ``uuid4`` per call.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

import grpc

from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.proto import scheduler_pb2_grpc as pb_grpc

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.dispatch.fire import FireDispatcher
    from z4j_scheduler.storage.cache import ScheduleCache

logger = logging.getLogger("z4j.scheduler.trigger_grpc.handlers")


# TTL for the idempotency cache. Audit fix S002 (1.4.0): brain's
# TriggerScheduleClient uses a 10s per-call deadline and retries
# at most once on UNAVAILABLE / DEADLINE_EXCEEDED, so the
# worst-case duplicate-fire window is ~20s. 60s gives 3x margin
# without growing the in-memory cache unbounded.
_IDEM_TTL_SECONDS = 60.0

# Sweep budget: cap the per-call cleanup work so a slow operator
# session doesn't accumulate millions of expired keys before the
# next request arrives. The cache is per-process and per-scheduler-
# instance so realistic working set is small (one entry per
# operator click in the last minute), but we cap the pass anyway.
_IDEM_SWEEP_BUDGET = 64


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

    Audit fix S002 (1.4.0): the servicer now honors the
    ``idempotency_key`` from the request for dedup. A repeated
    call within ``_IDEM_TTL_SECONDS`` (60s) returns the cached
    ``command_id`` from the first call instead of issuing a fresh
    fire. Empty ``idempotency_key`` (legacy callers) bypasses the
    cache entirely, preserving the previous behavior.
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
        # Idempotency cache: (schedule_id_str, idempotency_key) ->
        # (cached_at_monotonic, command_id_str). Per-process; not
        # shared across scheduler instances because brain pins each
        # trigger retry to the same scheduler instance via the
        # leader gate (a retry that hits a different instance gets
        # ``not_leader``, not a fresh fire, so no cache miss leaks).
        self._idem_cache: dict[
            tuple[str, str],
            tuple[float, str],
        ] = {}

    async def TriggerSchedule(  # noqa: N802, PLR0911  gRPC-generated name, status dispatch
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

        # Leader gate. Standbys reject the trigger so
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
                entry.project_id,
                schedule_id,
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

        # Idempotency cache check. Only
        # honored when brain supplies a non-empty key; legacy
        # callers (empty key) bypass the cache and always fire.
        idem_key = request.idempotency_key or ""
        cache_key = (request.schedule_id, idem_key) if idem_key else None
        if cache_key is not None:
            self._sweep_expired_idem_entries()
            cached = self._idem_cache.get(cache_key)
            if cached is not None:
                cached_at, cached_command_id = cached
                if (time.monotonic() - cached_at) < _IDEM_TTL_SECONDS:
                    logger.info(
                        "z4j.scheduler.trigger_grpc: idempotency hit "
                        "schedule_id=%s key=%s command_id=%s "
                        "(skipping duplicate fire)",
                        schedule_id,
                        idem_key,
                        cached_command_id,
                    )
                    return pb.TriggerScheduleResponse(
                        command_id=cached_command_id,
                    )

        result = await self._dispatcher.trigger_now(
            schedule_id=schedule_id,
            triggered_by_user_id=request.user_id or "",
        )
        if result.success:
            command_id_str = str(result.command_id)
            if cache_key is not None:
                self._idem_cache[cache_key] = (
                    time.monotonic(),
                    command_id_str,
                )
            return pb.TriggerScheduleResponse(command_id=command_id_str)
        return pb.TriggerScheduleResponse(
            error_code=result.error_code or "unknown",
            error_message=result.error_message or "",
        )

    def _sweep_expired_idem_entries(self) -> None:
        """Drop expired idempotency-cache entries opportunistically.

        Bounded by ``_IDEM_SWEEP_BUDGET`` so a single call's
        cleanup work is constant-time even if the cache somehow
        accumulated thousands of expired entries (it shouldn't,
        given the realistic operator-click rate; this is purely
        defensive against a pathological state).
        """
        if not self._idem_cache:
            return
        now = time.monotonic()
        # Iterate snapshot so we can mutate the dict.
        expired: list[tuple[str, str]] = []
        for key, (cached_at, _command_id) in self._idem_cache.items():
            if (now - cached_at) >= _IDEM_TTL_SECONDS:
                expired.append(key)
                if len(expired) >= _IDEM_SWEEP_BUDGET:
                    break
        for key in expired:
            self._idem_cache.pop(key, None)


__all__ = ["TriggerScheduleServicer"]
