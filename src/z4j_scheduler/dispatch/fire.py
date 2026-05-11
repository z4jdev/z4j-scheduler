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
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import NAMESPACE_DNS, UUID, uuid5

import grpc

from z4j_scheduler.observability import metrics as m

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage._models import FireResult
    from z4j_scheduler.storage.brain_client import BrainClient

logger = logging.getLogger("z4j.scheduler.dispatch")

#: Namespace for deterministic fire_id derivation. Pinned so the
#: same (schedule_id, scheduled_for_iso) pair always produces the
#: same fire_id across scheduler restarts and across HA instances.
_FIRE_ID_NAMESPACE = uuid5(NAMESPACE_DNS, "z4j-scheduler.fire-id.v1")

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
    canonical = (
        f"{schedule_id}:"
        f"{truncated.isoformat()}"
    )
    return uuid5(_FIRE_ID_NAMESPACE, canonical)


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
    ) -> object:
        """Fire a schedule on operator demand. Returns the FireResult.

        Used by the scheduler-side TriggerSchedule gRPC handler when
        the dashboard's "fire now" button is clicked. Differs from
        :meth:`dispatch` in two ways:

        - ``fire_id`` is a fresh :func:`uuid4` instead of being
          derived from ``scheduled_for`` (operator triggers are not
          tied to a scheduled time slot - they are extra fires on
          top of the normal cadence).
        - Returns the :class:`FireResult` so the handler can pass the
          ``command_id`` back to brain. The normal :meth:`dispatch`
          path is void because the tick engine doesn't need it.

        Same retry + ack semantics as :meth:`dispatch` so a flaky
        brain doesn't make the operator's click look like a no-op.
        """
        from uuid import uuid4 as _uuid4  # noqa: PLC0415

        fire_id = _uuid4()
        scheduled_for = datetime.now(UTC)
        fired_at = scheduled_for
        start = time.monotonic()

        result = await self._fire_with_retry(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
        )
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
        else:
            m.fires_total.labels(status="trigger_failed").inc()
            await self._best_effort_ack(
                fire_id=fire_id,
                command_id=None,
                status="failed",
                error=result.error_message or result.error_code or "unknown",
            )
        return result

    async def dispatch(
        self,
        *,
        schedule_id: UUID,
        scheduled_for: datetime,
        schedule_name: str = "",
    ) -> None:
        """Fire a schedule. Returns silently on success.

        Raises :class:`Exception` if every retry attempt fails - the
        tick engine treats this as "do not advance, retry next tick"
        per its contract. The engine clamps the next-tick interval
        so this does not become a tight loop.

        ``schedule_name`` is forwarded into per-schedule Prometheus
        labels (Phase 4). Optional - older callers that don't pass
        it get the empty-string label which Prometheus aggregates
        as the "unknown name" series.
        """
        fire_id = derive_fire_id(schedule_id, scheduled_for)
        fired_at = datetime.now(UTC)
        start = time.monotonic()

        result = await self._fire_with_retry(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
        )

        elapsed = time.monotonic() - start
        m.fire_latency_seconds.observe(elapsed)
        # Phase 4: per-schedule latency slice. Same value, additional
        # label dimension. Cardinality bounded by schedule count.
        try:
            m.per_schedule_fire_latency_seconds.labels(
                schedule_id=str(schedule_id),
                schedule_name=schedule_name,
            ).observe(elapsed)
        except Exception:  # noqa: BLE001
            # Metrics emission must never break a fire path. Swallow
            # silently - missing one data point is preferable to
            # missing the fire.
            logger.debug("per-schedule metric emission failed", exc_info=True)

        if result.success:
            status_label = "buffered" if result.buffered else "delivered"
            m.fires_total.labels(status=status_label).inc()
            try:
                m.per_schedule_fires_total.labels(
                    schedule_id=str(schedule_id),
                    schedule_name=schedule_name,
                    status=status_label,
                ).inc()
            except Exception:  # noqa: BLE001
                logger.debug("per-schedule counter inc failed", exc_info=True)
            logger.info(
                "z4j.scheduler.dispatch: fire %s schedule_id=%s fire_id=%s "
                "command_id=%s elapsed=%.3fs",
                status_label, schedule_id, fire_id,
                result.command_id, elapsed,
            )
            # Best-effort ack - brain doesn't actually need this in
            # the success path (it created the Command), but the
            # contract is symmetric and the integration tests rely
            # on it. Failures here are not fatal.
            await self._best_effort_ack(
                fire_id=fire_id,
                command_id=result.command_id,
                status="success",
            )
        else:
            m.fires_total.labels(status="failed").inc()
            try:
                m.per_schedule_fires_total.labels(
                    schedule_id=str(schedule_id),
                    schedule_name=schedule_name,
                    status="failed",
                ).inc()
            except Exception:  # noqa: BLE001
                logger.debug("per-schedule counter inc failed", exc_info=True)
            logger.warning(
                "z4j.scheduler.dispatch: fire FAILED schedule_id=%s "
                "fire_id=%s error_code=%s message=%s elapsed=%.3fs",
                schedule_id, fire_id, result.error_code,
                result.error_message, elapsed,
            )
            await self._best_effort_ack(
                fire_id=fire_id,
                command_id=None,
                status="failed",
                error=result.error_message or result.error_code or "unknown",
            )
            # Surface to the engine so it knows not to advance.
            raise FireDispatchError(
                f"fire failed: code={result.error_code!r} "
                f"message={result.error_message!r}",
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
    ) -> FireResult:
        """Single fire with retry on transient gRPC errors."""
        attempt = 0
        max_attempts = max(1, 1 + self._settings.fire_retry_max)
        backoff = self._settings.fire_retry_backoff_seconds

        while True:
            attempt += 1
            try:
                return await self._client.fire_schedule(
                    schedule_id=schedule_id,
                    fire_id=fire_id,
                    scheduled_for=scheduled_for,
                    fired_at=fired_at,
                )
            except grpc.aio.AioRpcError as exc:
                if (
                    exc.code() not in _RETRYABLE_STATUS_CODES
                    or attempt >= max_attempts
                ):
                    logger.warning(
                        "z4j.scheduler.dispatch: gRPC error %s on attempt "
                        "%d/%d - giving up",
                        exc.code(), attempt, max_attempts,
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
                    exc.code(), delay, attempt, max_attempts,
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
