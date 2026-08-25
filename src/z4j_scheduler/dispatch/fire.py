"""Fire dispatcher - the hot path between tick engine and brain.

Implements the :class:`Dispatcher` Protocol expected by
:class:`~z4j_scheduler.tick.engine.TickEngine`.

Per-fire responsibilities:

1. **Idempotency** - the ``fire_id`` is deterministic from
   ``(schedule_id, scheduled_for)`` via uuid5. A retry of the same
   logical fire reuses the same id, so brain's
   ``commands.idempotency_key`` constraint deduplicates server-side.

2. **Retry with backoff** - on transient gRPC failures (UNAVAILABLE,
   DEADLINE_EXCEEDED), retry up to ``settings.fire_retry_max`` times
   with capped exponential backoff. Permanent failures
   (PERMISSION_DENIED, FAILED_PRECONDITION) raise immediately.

3. **Acknowledge** - on terminal result (success OR final failure),
   call ``brain.acknowledge_result`` so the schedule's last_run_at
   updates in brain. The ack is fire-and-forget from the engine's
   perspective - if the ack fails, we log + drop and move on.

4. **Metrics** - increment fires_total per fire, observe
   fire_latency_seconds histogram. Driven by
   :mod:`~z4j_scheduler.observability.metrics`.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import NAMESPACE_DNS, UUID, uuid5

import grpc

from z4j_scheduler.observability import metrics as m

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage._models import CursorTransitionResult, FireResult
    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.tick._entry import ScheduleEntry
    from z4j_scheduler.tick._prepared import PreparedFire

logger = logging.getLogger("z4j.scheduler.dispatch")

#: Namespace for deterministic fire_id derivation. Pinned so the
#: same (schedule_id, scheduled_for_iso) pair always produces the
#: same fire_id across scheduler restarts and across HA instances.
_FIRE_ID_NAMESPACE = uuid5(NAMESPACE_DNS, "z4j-scheduler.fire-id.v1")

#: The one answer an operator gets when their trigger cannot ride the
#: FireSchedule wire, whether the scheduler works that out from the schedule in
#: front of it or hears it from the Brain. Two spellings of the same refusal
#: would read as two different problems, and only one of them has a remedy.
_MANUAL_TRIGGER_REFUSED_CODE = "manual_trigger_not_accepted"
_MANUAL_TRIGGER_REFUSED_MESSAGE = (
    "this Brain does not accept operator triggers through the scheduler; "
    "it fires them itself, so unset scheduler_trigger_url and trigger from "
    "the Brain"
)

#: gRPC status codes we treat as transient and retry. Anything else
#: is treated as permanent and surfaced to the engine immediately.
_RETRYABLE_STATUS_CODES: frozenset[grpc.StatusCode] = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
    },
)


def derive_fire_id(schedule_id: UUID, scheduled_for: datetime) -> UUID:
    """Compute the deterministic fire_id for a (schedule, slot) pair.

    Used as the brain-side ``Command.idempotency_key`` so a retried
    FireSchedule call (e.g. due to network blip + retry) cannot
    create two Commands for the same logical fire.

    The scheduled_for is normalised to UTC ISO-8601 before hashing
    so callers in any timezone produce the same id.

    The scheduled_for is also truncated to whole seconds before
    hashing. Cron returns microsecond-zero datetimes but interval
    truncates via ``int(timestamp())`` and one-shot comes from
    ``datetime.fromisoformat`` with full microseconds; after a
    scheduler restart, a slot reloaded from Postgres can have
    full microsecond precision while the in-memory recompute has
    zero microseconds. Without the second-truncation the two ISO
    strings would differ → deterministic ``fire_id`` no longer
    matches → brain's idempotency_key dedup misses → same logical
    fire creates two Commands.
    """
    if scheduled_for.tzinfo is None:
        raise ValueError("derive_fire_id requires tz-aware scheduled_for")
    truncated = scheduled_for.astimezone(UTC).replace(microsecond=0)
    canonical = f"{schedule_id}:{truncated.isoformat()}"
    return uuid5(_FIRE_ID_NAMESPACE, canonical)


def _manual_trigger_refused() -> FireResult:
    """The refusal the scheduler reaches on its own, before any wire call.

    ``disposition`` is deliberately left unset. The dispositions are the
    Brain's vocabulary for what it did with a fire, and here the Brain was
    never asked, so claiming one would be putting words in its mouth. Unset
    also keeps ``FireResult.success`` false, which is the property the trigger
    handler reports to the operator.
    """

    from z4j_scheduler.storage._models import FireResult as _FireResult

    return _FireResult(
        command_id=None,
        error_code=_MANUAL_TRIGGER_REFUSED_CODE,
        error_message=_MANUAL_TRIGGER_REFUSED_MESSAGE,
        buffered=False,
    )


class FireDispatcher:
    """Production :class:`Dispatcher` - calls brain over gRPC.

    Tests inject a fake brain client so this class itself can be
    exercised without a network. The retry + backoff loop is the
    core complexity; the gRPC call itself is one line.
    """

    def __init__(
        self,
        *,
        client: BrainClient,
        settings: Settings,
    ) -> None:
        self._client = client
        self._settings = settings

    async def trigger_now(
        self,
        *,
        schedule_id: UUID,
        schedule_entry: ScheduleEntry,
        triggered_by_user_id: str = "",
    ) -> FireResult:
        """Fire a schedule on operator demand. Returns the FireResult.

        Reached from the scheduler-side TriggerSchedule handler, which a Brain
        calls only when an operator has wired ``scheduler_trigger_url``. A
        Brain that has activated durable schedule control does not: it performs
        an operator trigger itself, because it is the authority on whether the
        schedule may run and the only side that can see a hold. This path
        remains for a Brain that still calls.

        Differs from :meth:`dispatch` in two ways:

        - ``fire_id`` is a fresh :func:`uuid4` instead of being
          derived from ``scheduled_for`` (operator triggers are not
          tied to a scheduled time slot - they are extra fires on
          top of the normal cadence).
        - Returns the :class:`FireResult` so the handler can pass the
          ``command_id`` back to brain. The normal :meth:`dispatch`
          path is void because the tick engine doesn't need it.

        ``triggered_by_user_id`` (from the TriggerSchedule request) is
        forwarded to the brain's FireSchedule so the fire-history row is
        attributed to the operator who clicked; empty for the cadence
        :meth:`dispatch` path.

        ``schedule_entry`` decides which of those two worlds this is, and it is
        required for that reason. A schedule under durable control carries a
        control token, and the wire that governs it accepts cadence
        acceptances only: a slot-less operator fire is not one, no version of
        this scheduler can express it, and the Brain would refuse it. Asking
        anyway spent a round trip to be told something the schedule in hand
        already said, and left an acknowledgement addressed to a fire the Brain
        never recorded. So the refusal is decided here, from the same field
        :meth:`dispatch` reads to choose its protocol.

        :meth:`_manual_fire_refusal_result` stays as the answer for the race
        this cannot see: a Brain that activates control between the snapshot
        this entry came from and the operator's click. Both routes end in the
        same refusal, because they are the same refusal.

        Same retry + ack semantics as :meth:`dispatch` so a flaky
        brain doesn't make the operator's click look like a no-op.
        """
        if schedule_entry.id != schedule_id:
            raise ValueError("schedule entry does not match schedule_id")
        if schedule_entry.control_token is not None:
            m.fires_total.labels(status="trigger_failed").inc()
            logger.info(
                "z4j.scheduler.dispatch: refusing operator trigger for "
                "schedule_id=%s; this Brain fires operator triggers itself",
                schedule_id,
            )
            return _manual_trigger_refused()

        from uuid import uuid4 as _uuid4

        fire_id = _uuid4()
        scheduled_for = datetime.now(UTC)
        fired_at = scheduled_for
        start = time.monotonic()

        result = await self._fire_with_retry(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
            triggered_by_user_id=triggered_by_user_id,
        )
        result = self._manual_fire_refusal_result(result)
        elapsed = time.monotonic() - start
        m.fire_latency_seconds.observe(elapsed)

        if result.success:
            status_label = "buffered" if result.buffered else "delivered"
            m.fires_total.labels(status=f"trigger_{status_label}").inc()
            await self._best_effort_ack(
                fire_id=fire_id,
                command_id=result.command_id,
                status="success",
            )
            return result

        m.fires_total.labels(status="trigger_failed").inc()
        if result.error_code == _MANUAL_TRIGGER_REFUSED_CODE:
            # A Brain that refused this fire for its shape recorded nothing to
            # acknowledge, and answers an acknowledgement for an unknown fire
            # with FAILED_PRECONDITION. Sending it anyway put a stack trace in
            # the log of the operator who is at that moment reading the log to
            # find out why their click did nothing.
            return result
        await self._best_effort_ack(
            fire_id=fire_id,
            command_id=None,
            status="failed",
            error=result.error_message or result.error_code or "unknown",
        )
        return result

    @staticmethod
    def _manual_fire_refusal_result(result: FireResult) -> FireResult:
        """Say what a refused operator trigger actually means.

        The Brain answers an attributed fire with the legacy-upgrade
        disposition, whose code reads as "upgrade the scheduler". On a cadence
        fire that is exactly right. On an operator trigger it is the opposite
        of the truth: the scheduler is current, and no version of it can put an
        operator's extra fire on a wire that carries cadence acceptances. An
        operator handed that code re-deploys a component that was never the
        problem, so the code and the message are replaced with the one action
        that resolves it.

        The Brain's disposition is preserved: it is what the Brain actually
        said, ``success`` is derived from it, and a refusal reported as a fired
        schedule would be far worse than a confusing code.

        Only this disposition is rewritten. Every other refusal (paused,
        disabled, quarantined, no agent) already says what it means.
        """
        if result.disposition != "legacy_upgrade_required":
            return result
        return replace(
            result,
            error_code=_MANUAL_TRIGGER_REFUSED_CODE,
            error_message=_MANUAL_TRIGGER_REFUSED_MESSAGE,
        )

    async def dispatch(  # noqa: PLR0912, PLR0915 - explicit protocol state machine
        self,
        *,
        schedule_id: UUID,
        scheduled_for: datetime,
        schedule_name: str = "",
        engine: str = "",
        project_id: UUID | None = None,
        project_schedule_count: int | None = None,
        prepared_fire: PreparedFire | None = None,
        schedule_entry: ScheduleEntry | None = None,
    ) -> FireResult | None:
        """Fire a schedule and return typed current-protocol outcomes.

        Raises :class:`Exception` if every retry attempt fails - the
        tick engine treats this as "do not advance, retry next tick"
        per its contract. The engine clamps the next-tick interval
        so this does not become a tight loop.

        The explicit legacy path returns ``None`` after preserving its
        historical success/error behavior. A current schedule returns the
        Brain's typed result without flattening non-accepted dispositions into
        one generic exception.

        ``schedule_name`` is forwarded into per-schedule Prometheus
        labels (Phase 4). Optional - older callers that don't pass
        it get the empty-string label which Prometheus aggregates
        as the "unknown name" series.

        ``engine`` / ``project_id`` / ``project_schedule_count`` (A3)
        label the fire-variance histogram; the per-schedule label is
        emitted only while the project has fewer than
        ``FIRE_VARIANCE_SCHEDULE_ID_MAX`` schedules so cardinality stays
        bounded on large tenants (``None`` = unknown -> no per-schedule
        label).
        """
        if prepared_fire is not None and prepared_fire.scheduled_for != scheduled_for:
            raise ValueError("prepared fire slot does not match scheduled_for")
        if schedule_entry is not None and schedule_entry.id != schedule_id:
            raise ValueError("schedule entry does not match schedule_id")
        current_entry = (
            schedule_entry
            if schedule_entry is not None and schedule_entry.control_token is not None
            else None
        )
        if current_entry is not None and prepared_fire is None:
            raise ValueError("current fire requires a prepared cadence transition")
        fire_id = derive_fire_id(schedule_id, scheduled_for)
        fired_at = datetime.now(UTC)
        start = time.monotonic()

        scheduler_protocol_epoch = 0
        if current_entry is not None:
            from z4j_scheduler.storage._protocol import CURRENT_PROTOCOL_EPOCH

            scheduler_protocol_epoch = CURRENT_PROTOCOL_EPOCH
        result = await self._fire_with_retry(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
            schedule_entry=current_entry,
            prepared_fire=prepared_fire if current_entry is not None else None,
            scheduler_protocol_epoch=scheduler_protocol_epoch,
        )

        elapsed = time.monotonic() - start
        m.fire_latency_seconds.observe(elapsed)
        # A3: fire-time variance (fired_at - next_fire_at) sliced by
        # engine + project (+ per-schedule label only for small
        # projects). Clamp tiny negative skew to 0. Metrics must never
        # break a fire; swallow emission errors.
        try:
            emit_schedule_id = (
                project_schedule_count is not None
                and project_schedule_count < m.FIRE_VARIANCE_SCHEDULE_ID_MAX
            )
            variance = max(0.0, (fired_at - scheduled_for).total_seconds())
            m.fire_variance_seconds.labels(
                schedule_id=str(schedule_id) if emit_schedule_id else "",
                engine=engine,
                project=str(project_id) if project_id is not None else "",
            ).observe(variance)
        except Exception:
            logger.debug("fire variance metric emission failed", exc_info=True)
        # Phase 4: per-schedule latency slice. Same value, additional
        # label dimension. Cardinality bounded by schedule count.
        try:
            m.per_schedule_fire_latency_seconds.labels(
                schedule_id=str(schedule_id),
                schedule_name=schedule_name,
            ).observe(elapsed)
        except Exception:
            # Metrics emission must never break a fire path. Swallow
            # silently - missing one data point is preferable to
            # missing the fire.
            logger.debug("per-schedule metric emission failed", exc_info=True)

        current_response = current_entry is not None
        if current_response and result.disposition is None:
            m.fires_total.labels(status="failed").inc()
            logger.warning(
                "z4j.scheduler.dispatch: current fire returned an untyped "
                "response schedule_id=%s fire_id=%s; treating as ambiguous",
                schedule_id,
                fire_id,
            )
            return result

        if result.success:
            status_label = "buffered" if result.buffered else "delivered"
            m.fires_total.labels(status=status_label).inc()
            try:
                m.per_schedule_fires_total.labels(
                    schedule_id=str(schedule_id),
                    schedule_name=schedule_name,
                    status=status_label,
                ).inc()
            except Exception:
                logger.debug("per-schedule counter inc failed", exc_info=True)
            logger.info(
                "z4j.scheduler.dispatch: fire %s schedule_id=%s fire_id=%s "
                "command_id=%s elapsed=%.3fs",
                status_label,
                schedule_id,
                fire_id,
                result.command_id,
                elapsed,
            )
            # The receipt reports this FireSchedule round trip, not the task's
            # execution: the agent reports that separately, and the scheduler
            # never learns it. So it is sent here, at handoff, where the fact
            # it records is the fact that just happened. A buffered fire has no
            # command to receipt against and is left unacknowledged until one
            # exists. Failures here are not fatal.
            if not current_response or result.command_id is not None:
                await self._best_effort_ack(
                    fire_id=fire_id,
                    command_id=result.command_id,
                    status="success",
                )
            if current_response:
                return result
        else:
            m.fires_total.labels(status="failed").inc()
            try:
                m.per_schedule_fires_total.labels(
                    schedule_id=str(schedule_id),
                    schedule_name=schedule_name,
                    status="failed",
                ).inc()
            except Exception:
                logger.debug("per-schedule counter inc failed", exc_info=True)
            logger.warning(
                "z4j.scheduler.dispatch: fire FAILED schedule_id=%s "
                "fire_id=%s error_code=%s message=%s elapsed=%.3fs",
                schedule_id,
                fire_id,
                result.error_code,
                result.error_message,
                elapsed,
            )
            if current_response:
                return result
            await self._best_effort_ack(
                fire_id=fire_id,
                command_id=None,
                status="failed",
                error=result.error_message or result.error_code or "unknown",
            )
            # Surface to the engine so it knows not to advance.
            raise FireDispatchError(
                f"fire failed: code={result.error_code!r} message={result.error_message!r}",
            )
        return None

    async def advance_cursor(
        self,
        *,
        entry: ScheduleEntry,
        prepared: PreparedFire,
    ) -> CursorTransitionResult:
        """Persist one current-protocol zero-work catch-up transition."""

        from z4j_scheduler.storage._protocol import CURRENT_PROTOCOL_EPOCH

        # Same reasoning as make_fire_request: submit what this process
        # computes, not the value the Brain streamed for the row. The Brain
        # compares the submission against its own computation, so echoing the
        # row made it compare the Brain to itself -- and made any change to the
        # cadence closure (or the Python version, which is in the fingerprint)
        # refuse every cursor advance for every pre-existing schedule.
        from z4j_scheduler.tick.cadence import (
            CADENCE_SEMANTICS_VERSION,
            cadence_runtime_fingerprint,
        )

        if entry.control_token is None:
            raise ValueError("durable cursor advance requires a current schedule")
        return await self._client.advance_schedule_cursor(
            project_id=entry.project_id,
            schedule_id=entry.id,
            observed_control_token=entry.control_token,
            definition_digest=entry.definition_digest,
            expected_schedule_revision=entry.schedule_revision,
            expected_last_run_at=entry.last_fire_at,
            expected_next_run_at=entry.next_fire_at,
            skipped_through=prepared.scheduled_for,
            prepared_next_run_at=prepared.next_run_at,
            scheduler_protocol_epoch=CURRENT_PROTOCOL_EPOCH,
            cadence_semantics_version=CADENCE_SEMANTICS_VERSION,
            cadence_runtime_fingerprint=cadence_runtime_fingerprint(),
        )

    # ------------------------------------------------------------------
    # Retry loop
    # ------------------------------------------------------------------

    async def _fire_with_retry(
        self,
        *,
        schedule_id: UUID,
        fire_id: UUID,
        scheduled_for: datetime,
        fired_at: datetime,
        triggered_by_user_id: str = "",
        schedule_entry: ScheduleEntry | None = None,
        prepared_fire: PreparedFire | None = None,
        scheduler_protocol_epoch: int = 0,
    ) -> FireResult:
        """Single fire with retry on transient gRPC errors."""
        attempt = 0
        max_attempts = max(1, 1 + self._settings.fire_retry_max)
        backoff = self._settings.fire_retry_backoff_seconds

        while True:
            attempt += 1
            try:
                if schedule_entry is None:
                    return await self._client.fire_schedule(
                        schedule_id=schedule_id,
                        fire_id=fire_id,
                        scheduled_for=scheduled_for,
                        fired_at=fired_at,
                        triggered_by_user_id=triggered_by_user_id,
                    )
                return await self._client.fire_schedule(
                    schedule_id=schedule_id,
                    fire_id=fire_id,
                    scheduled_for=scheduled_for,
                    fired_at=fired_at,
                    triggered_by_user_id=triggered_by_user_id,
                    schedule_entry=schedule_entry,
                    prepared_fire=prepared_fire,
                    scheduler_protocol_epoch=scheduler_protocol_epoch,
                )
            except grpc.aio.AioRpcError as exc:
                if exc.code() not in _RETRYABLE_STATUS_CODES or attempt >= max_attempts:
                    logger.warning(
                        "z4j.scheduler.dispatch: gRPC error %s on attempt %d/%d - giving up",
                        exc.code(),
                        attempt,
                        max_attempts,
                    )
                    raise
                # Capped exponential + small jitter so a flock of
                # retrying schedulers don't all retry at the same
                # instant.
                delay = min(
                    backoff * (2 ** (attempt - 1)),
                    10.0,
                )
                delay *= 1.0 + random.uniform(-0.2, 0.2)  # noqa: S311 - jitter, not crypto
                logger.info(
                    "z4j.scheduler.dispatch: transient gRPC error %s; "
                    "retrying in %.2fs (attempt %d/%d)",
                    exc.code(),
                    delay,
                    attempt,
                    max_attempts,
                )
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Ack
    # ------------------------------------------------------------------

    async def _best_effort_ack(
        self,
        *,
        fire_id: UUID,
        command_id: UUID | None,
        status: str,
        error: str | None = None,
    ) -> None:
        """Send acknowledge_result. Failures are logged and swallowed.

        The ack lets brain stamp ``schedules.last_run_at`` for the
        operator's view. A missed ack means brain's view is mildly
        stale until the next fire; never worth raising back to the
        engine for.
        """
        try:
            await self._client.acknowledge_result(
                fire_id=fire_id,
                command_id=command_id,
                status=status,
                error=error,
            )
        except Exception:
            logger.exception(
                "z4j.scheduler.dispatch: acknowledge_result failed for "
                "fire_id=%s; brain view will refresh on next fire",
                fire_id,
            )


class FireDispatchError(RuntimeError):
    """Raised when a FireSchedule call fails after all retries."""


__all__ = ["FireDispatchError", "FireDispatcher", "derive_fire_id"]
