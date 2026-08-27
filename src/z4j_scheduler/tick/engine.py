"""Main asyncio tick loop.

Single coroutine per scheduler instance. Sleeps until the next
schedule is due, checks the leader gate, dispatches if leader,
recomputes the next-fire and loops.

Design properties this module commits to:

- **No I/O knowledge.** The engine takes a ``cache``, a
  ``leader_gate`` callable, a ``dispatcher`` callable, and a
  ``clock`` callable. It does not know about gRPC, brain, or
  Postgres. Tests inject fakes for all four.
- **No background tasks.** The engine is one coroutine that the
  caller awaits via :meth:`run` (typically inside
  :class:`asyncio.TaskGroup`). Stop by setting the cancellation
  token (``stop_event``) - the loop checks on every wake.
- **Strict cooperation with the cache.** Mutations to the cache
  fire its ``changed`` event; the engine awaits a race between
  ``stop_event``, ``changed``, and the time-until-next-fire so it
  wakes when a cache change is delivered. This module makes no fixed
  end-to-end delivery-latency promise.
- **Catch-up is honest.** When a schedule's ``next_fire_at`` is in
  the past (after a brain-down outage, or a freshly-added
  schedule), the catch-up policy decides whether to fire 0, 1, or
  N times. The engine never silently swallows a missed fire.
- **Past one-shots dispatch immediately, then exhaust.** A
  one-shot whose target time has already passed when added to the
  cache fires once at its scheduled time, gets ``last_fire_at`` set,
  and computes no successor, so it does not fire again.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from z4j_scheduler.tick import cron as cron_mod
from z4j_scheduler.tick import interval as interval_mod
from z4j_scheduler.tick import one_shot as one_shot_mod
from z4j_scheduler.tick import solar as solar_mod
from z4j_scheduler.tick._entry import (
    schedule_cadence_identity,
    schedule_definition_changed,
)
from z4j_scheduler.tick.catch_up import plan_catch_up

if TYPE_CHECKING:
    from collections.abc import Callable

    from z4j_scheduler.storage._models import CursorTransitionResult, FireResult
    from z4j_scheduler.storage.cache import LocalQuarantine, ScheduleCache
    from z4j_scheduler.tick._entry import ScheduleEntry
    from z4j_scheduler.tick._prepared import PreparedFire

from z4j_scheduler.observability import metrics as m

logger = logging.getLogger("z4j.scheduler.tick")

#: Maximum sleep between wakeups even when the cache is empty - lets
#: the engine notice newly-added schedules within this bound even if
#: the cache's ``changed`` event was never set (defensive against
#: producer bugs that mutate state without firing the event).
_MAX_SLEEP_SECONDS = 30.0

#: Cooperative sleep duration when the watch stream is unhealthy and
#: the engine is skipping dispatch this iteration. Long enough for the
#: watch reconnect task to make progress, short enough that recovery
#: is sub-second once the watch stream comes back. Without a non-zero
#: sleep here, ``_sleep_until_next`` could return immediately when
#: past-due schedules sit in the cache, and the engine would spin
#: without ever yielding to the watch reconnect task -- the silent
#: deadlock that load testing surfaced as a 352-second silent dispatch
#: outage after a brain restart.
_UNHEALTHY_SLEEP_SECONDS = 1.0

#: RM12: when firing a schedule RAISES (a pathological cadence that overflows
#: _next_fire_for, a dispatcher bug, ...), the entry's next_fire_at is never
#: advanced, so it stays past-due and the engine would re-fire it every tick
#: (a hot-spin). Instead we push next_fire_at forward by an exponential
#: back-off so the broken schedule retries on a widening interval rather than
#: monopolising the loop. Base 1s, doubling, capped.
_BASE_FIRE_BACKOFF_SECONDS = 1.0
_MAX_FIRE_BACKOFF_SECONDS = 300.0
_MAX_FIRE_BACKOFF_EXPONENT = 8

#: M8: bound on how many due schedules fire concurrently within one tick. A
#: burst of due schedules cannot spawn unbounded coroutines, while a single
#: schedule's long catch-up drain no longer blocks every other due schedule's
#: on-time fire behind it.
_MAX_CONCURRENT_FIRES = 16

#: The maximum number of DISTINCT due schedules dispatched in a single
#: tick. The dispatch phase blocks until its whole batch drains, so an unbounded
#: batch means a promotion (a leadership grant that must un-park a follower's
#: slots) is not re-evaluated -- and a just-promoted follower's due slot not
#: fired -- until the entire current backlog completes. Capping the batch bounds
#: how long the loop takes to return to its top-of-iteration promotion re-check;
#: the remainder stays due and is picked up on the very next (immediate) iteration.
_MAX_DISPATCH_PER_TICK = 256


# M10/RM11: the definition-comparison helper now lives next to ScheduleEntry
# (tick/_entry.py) so the tick engine and the cache's compare-and-set write
# share ONE field list. Re-exported here under the original private name so the
# existing call sites keep working.
_schedule_definition_changed = schedule_definition_changed


#: Grace window for distinguishing "on-time" fires from "missed"
#: fires. A fire whose ``scheduled_for`` is within this many seconds
#: of ``now`` is treated as on-time and dispatched unconditionally;
#: a fire whose ``scheduled_for`` is further in the past is treated
#: as missed and the schedule's ``catch_up`` policy applies.
#:
#: Five seconds covers normal scheduler latency, gRPC round-trip
#: jitter, and small clock skew between scheduler instances. A fire
#: that's >5s late genuinely indicates the scheduler was down or
#: behind, and catch-up policy is the right behaviour.
_ON_TIME_GRACE_SECONDS = 5.0

#: How many iterations may fail BACK TO BACK before the loop stops absorbing and
#: lets the exception out. Absorbing forever would trade a loud crash for a
#: scheduler that looks alive and never fires, which is the worse of the two.
#: A single successful iteration resets the count.
_MAX_CONSECUTIVE_ITERATION_ERRORS = 10

#: Base delay after a failed iteration, doubled per consecutive failure and
#: capped. Stops a synchronously-raising path from spinning the event loop.
_ITERATION_ERROR_BACKOFF_BASE = 0.5
_ITERATION_ERROR_BACKOFF_MAX = 30.0

#: How long an on-time judgement may be held across retries before the slot is
#: honestly missed again.
#:
#: Freezing the judgement is right for the seconds a retry takes and wrong for
#: the hours an outage takes. A dispatch failure leaves next_fire_at untouched
#: and the schedule enabled, so nothing else in the engine releases the
#: entitlement while dispatch keeps failing. Without a ceiling, a 03:00 nightly
#: job whose brain was down could run at 09:00 under catch_up="skip", which is
#: precisely the "force on-time regardless of age" that removed for
#: handoff slots.
#:
#: Derived from the dispatch backoff cap so the two cannot drift: three capped
#: retries is a generous allowance for our own latency, and anything longer is
#: an outage, which is what catch_up is for.
_ENTITLEMENT_MAX_AGE_SECONDS = 3 * _MAX_FIRE_BACKOFF_SECONDS

#: PROMOTION-SCOPED grace for a slot THIS instance parked
#: as a follower and then fires as the just-promoted leader. The extra elapsed
#: time is our own promotion-detection latency, not a cluster outage, so such a
#: slot is on-time WITHIN this window; a slot OLDER than it is genuinely missed
#: and catch_up governs it. It applies ONLY to a follower-parked slot, so
#: steady-state leaders and single-instance deployments (which never carry a
#: handoff marker) keep the base on-time grace unchanged.
#:
#: This must be DERIVED from the deployment's configured leader
#: timing, not hard-coded. A fixed 30s was SHORTER than the supported
#: ``leader_heartbeat_seconds`` maximum (``le=60``), so a deployment with a slow
#: heartbeat promoted at ~T+60 and the promoted scheduler classified the slot it
#: had itself parked as missed, dropping it under catch_up="skip". The grace is
#: therefore ``base on-time grace + leader heartbeat + follower recheck``, which
#: bounds the realistic worst case: detecting the dead leader (one heartbeat)
#: plus this engine noticing it is now leader (one recheck).
# The floor is the grace this code shipped with BEFORE the derivation was
# introduced. Deriving from the heartbeat alone made the DEFAULT deployment
# stricter than its parent (2s heartbeat -> 12s), so a handoff slot ~20s late
# that used to fire was suddenly dropped under catch_up="skip". A fix for slow
# heartbeats must not tighten the common case: the derived value may only ever
# RAISE the window.
_PROMOTION_GRACE_FLOOR_SECONDS = 30.0


def _promotion_grace_for(
    *,
    leader_heartbeat_seconds: float,
    follower_recheck_seconds: float,
    base_grace_seconds: float = _ON_TIME_GRACE_SECONDS,
) -> float:
    """Worst-case failover latency a follower-parked slot may legitimately age by."""
    return max(
        _PROMOTION_GRACE_FLOOR_SECONDS,
        base_grace_seconds + leader_heartbeat_seconds + follower_recheck_seconds,
    )


#: Sentinel for :meth:`TickEngine._next_fire_for`'s
#: ``as_of_last_fire_at`` arg: "use ``entry.last_fire_at`` (legacy
#: behavior)." Python ``None`` is a meaningful caller value
#: (anchor at clock - schedule has never fired) so we need a
#: distinct sentinel.
_LAST_FIRE_AT_DEFAULT: object = object()


class LeaderGate(Protocol):
    """Per-project leader check. Implementations:

    - Single-instance mode: always returns True
    - HA mode: checks the Postgres advisory lock for the project
    """

    def is_leader(self, project_id: UUID) -> bool: ...


class Dispatcher(Protocol):
    """Single-fire dispatch. Implementations:

    - Production: gRPC ``FireSchedule(schedule_id, fire_id, ...)``
      to brain
    - Tests: fake that records calls

    The contract: the dispatcher is responsible for retry, deadline,
    and idempotency. The tick engine fires once per missed
    ``scheduled_for`` and trusts the dispatcher to handle the rest.
    """

    async def dispatch(
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
    ) -> FireResult | None: ...

    async def advance_cursor(
        self,
        *,
        entry: ScheduleEntry,
        prepared: PreparedFire,
    ) -> CursorTransitionResult: ...


class QuarantineSink(Protocol):
    """Queue a durable report after the cache has latched locally."""

    def enqueue(
        self,
        *,
        entry: ScheduleEntry,
        quarantine: LocalQuarantine,
    ) -> bool: ...


def _utc_now() -> datetime:
    """Default clock - used when the caller doesn't inject one."""
    return datetime.now(UTC)


class TickEngine:
    """Schedules → fires asyncio loop.

    Construction is cheap and synchronous. Drive the loop via
    :meth:`run` - typically inside an ``asyncio.TaskGroup`` so
    cancellation propagates cleanly on process shutdown.

    Args:
        cache: The schedule cache the engine reads from.
        leader_gate: Per-project leader check. Single-instance
            deployments pass an "always True" gate.
        dispatcher: Per-fire dispatch callable.
        clock: Wall-clock source. Override in tests.
        max_sleep_seconds: Cap on the wait between wakeups. Defensive
            against producer bugs - the engine will check the cache
            at least this often even if no ``changed`` event fires.
    """

    def __init__(
        self,
        *,
        cache: ScheduleCache,
        leader_gate: LeaderGate,
        dispatcher: Dispatcher,
        clock: Callable[[], datetime] = _utc_now,
        max_sleep_seconds: float = _MAX_SLEEP_SECONDS,
        max_consecutive_iteration_errors: int = _MAX_CONSECUTIVE_ITERATION_ERRORS,
        iteration_error_backoff_seconds: float = _ITERATION_ERROR_BACKOFF_BASE,
        watch_healthy: Callable[[], bool] | None = None,
        leader_heartbeat_seconds: float = 2.0,
        on_time_grace_seconds: float = _ON_TIME_GRACE_SECONDS,
        quarantine_reporter: QuarantineSink | None = None,
    ) -> None:
        self._cache = cache
        self._leader_gate = leader_gate
        self._dispatcher = dispatcher
        self._clock = clock
        self._max_sleep_seconds = max_sleep_seconds
        self._max_consecutive_iteration_errors = max_consecutive_iteration_errors
        self._iteration_error_backoff_seconds = iteration_error_backoff_seconds
        self._quarantine_reporter = quarantine_reporter
        # The promotion-scoped grace is derived from the DEPLOYMENT's
        # configured leader heartbeat, so a slow-heartbeat cluster (the supported
        # maximum is 60s) does not drop a slot this instance parked as a follower.
        self._leader_heartbeat_seconds = leader_heartbeat_seconds
        #: The base on-time grace. Deployment-configurable because the value
        #: that separates "jitter" from "missed" depends on the deployment's own
        #: dispatch latency, not on a constant chosen here.
        self._on_time_grace_seconds = on_time_grace_seconds
        self._stop_event = asyncio.Event()
        # When the watch stream is down, the cache may hold stale
        # state (e.g. an operator
        # disabled a schedule mid-outage; the disable event is
        # pending delivery). Refusing to fire during an unhealthy
        # window converts "fire stale state for ~30s" into
        # "miss a slot, catch_up handles it on recovery". The
        # default ``None`` means "always healthy" - tests and
        # single-process deployments without a separate
        # WatchStream don't pay the price.
        self._watch_healthy: Callable[[], bool] = (
            watch_healthy if watch_healthy is not None else (lambda: True)
        )
        # Per-schedule in-flight
        # guard. The dispatcher.dispatch await releases the event
        # loop for the duration of a gRPC round-trip (~5-50ms,
        # longer with retries). During that await a watch-stream
        # event for the same schedule_id can land on the cache,
        # firing ``cache.changed`` and waking the next iteration -
        # which then re-evaluates ``all_due`` and dispatches the
        # SAME schedule again before the in-flight
        # ``_advance_after_fire`` lands. Both fires mint the same
        # deterministic ``fire_id = uuid5(NAMESPACE, schedule_id +
        # scheduled_for_iso)`` and brain's
        # ``commands.idempotency_key`` UNIQUE collision (now
        # handled idempotently in CommandRepository.insert) makes
        # the second a no-op. Defence in depth: skip in-engine if
        # the schedule is already firing.
        self._in_flight: set[UUID] = set()
        # The sync
        # ``_next_fire_for`` helper used to mutate
        # ``entry.is_enabled = False`` directly when an expression
        # failed to parse. That mutation could land on an orphaned
        # entry if the WatchStream upserted concurrently, leaving
        # the broken schedule live. Now ``_next_fire_for`` records
        # the disable here; the async tick iteration drains the map
        # via ``cache.update_fire_state(is_enabled=False)`` which
        # serialises against ``cache.upsert``.
        #
        # RM12/M10: value is the ENTRY SNAPSHOT whose compute failed, so the
        # drain can pass it as ``expected_definition`` -- a definition-CAS. If a
        # watch upsert edited the cadence (e.g. the operator fixed the broken
        # expression) between the failed compute and the drain, the disable is
        # skipped rather than clobbering the now-valid schedule. Keyed by id so a
        # re-queued disable for the same schedule coalesces to the latest snapshot.
        self._pending_disables: dict[UUID, ScheduleEntry] = {}
        # RM12: consecutive fire-error counts per schedule, used to widen the
        # back-off. Cleared on the first clean fire; entries for schedules the
        # watch has removed are pruned against the live cache in
        # _compute_pending_next_fires (every iteration). So it stays bounded by
        # the number of live schedules.
        self._fire_error_counts: dict[UUID, int] = {}
        # P1-8b: retry DEADLINE for a schedule whose DISPATCH failed, kept
        # SEPARATE from next_fire_at so the retry re-fires the ORIGINAL slot (no
        # cadence drift) and does not dispatch at the back-off instant. The
        # due-loop skips a schedule until now >= its deadline; _sleep_until_next
        # wakes when the soonest deadline elapses. Pruned with _fire_error_counts.
        self._fire_backoff_until: dict[UUID, datetime] = {}
        # L3: the schedule DEFINITION snapshot captured when the back-off was
        # recorded, so the prune can tell an EDITED schedule (fixed cadence,
        # same id) from the still-broken one. The back-off/error state is keyed
        # by id alone; without this an operator who repairs a failing schedule
        # would have the corrected cadence inherit up to _MAX_FIRE_BACKOFF_SECONDS
        # of stale suppression before it could fire. Cleared on a definition
        # change in the prune, and alongside _fire_error_counts everywhere.
        self._fire_backoff_def: dict[UUID, ScheduleEntry] = {}
        # engine:449: a schedule whose NEXT-FIRE COMPUTATION raised (e.g. an
        # oversized interval -> OverflowError) is disabled locally. But the brain
        # keeps is_enabled=True (it does not validate interval magnitude) and
        # re-syncs the row, so the entry keeps coming back next_fire_at=None and
        # re-raising every tick/resync. Remember the (id -> failing expression) so
        # the full logger.exception traceback is emitted ONCE per broken
        # definition instead of storming the log on every re-sync; an operator's
        # edit changes the expression and re-arms the log. Pruned against the live
        # cache like the back-off counters.
        self._compute_quarantined: dict[UUID, str] = {}
        # H8: slots a NON-leader has already observed as due. A
        # follower must not advance ANY cadence (a one_shot cannot move past its
        # configured past-due time, and rewriting cron/interval corrupts the grid
        # + loses catch-up backlog), so it would re-process the same due slot every
        # tick (a hot loop). We PARK it here (id -> the parked next_fire_at) and
        # skip re-dispatch while a follower, WITHOUT touching next_fire_at /
        # last_fire_at. Cleared on promotion, on a slot change (echo/recompute), or
        # on removal.
        self._follower_parked: dict[UUID, datetime] = {}
        # Slots a follower OBSERVED live-due and merely handed off to the
        # (eventual) leader, keyed id -> the observed next_fire_at. When THIS
        # instance is promoted and fires such a slot, the delay is
        # promotion-DETECTION latency, not cluster unavailability, so it must be
        # treated as an ON-TIME fire (catch_up governs cluster outages, not a
        # live-observed handoff) -- otherwise the detection delay can push it past
        # the on-time grace and catch_up="skip" would drop a slot that was seen
        # due. Recorded when parking, consumed once on the promoted fire, pruned
        # with the park.
        # (Slot, cadence identity). The slot alone survived an edit that
        # changed the cadence but left next_fire_at untouched, and the marker
        # then made a slot fire on-time that was no longer on the schedule.
        self._follower_handoff: dict[UUID, tuple[datetime, tuple[object, ...]]] = {}
        #: Slots already judged on-time. Frozen so a failed dispatch cannot
        #: let the slot age out of its grace before the retry. Carries the
        #: cadence alongside the slot for the same reason _follower_handoff does:
        #: an edit that changes the cadence but leaves next_fire_at alone must
        #: not inherit an entitlement earned under the OLD cadence.
        self._slot_entitled: dict[UUID, tuple[datetime, tuple[object, ...], datetime]] = {}
        # While any entry is parked, a promotion must be noticed promptly.
        # The parked entries are excluded from the sleep wake computation (they
        # are past-due but must not force an immediate wake), and a leadership
        # change does NOT set cache.changed, so without this a just-promoted
        # follower would wait the full max-sleep before firing its due slot. When
        # something is parked we cap the sleep to this short re-check interval so
        # the loop re-evaluates leadership within a bounded delay. A single-
        # instance (always-leader) deployment never parks, so it never pays this.
        self._follower_recheck_seconds: float = 5.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the tick loop until :meth:`stop` is called.

        Caller pattern (production):

            async with asyncio.TaskGroup() as tg:
                tg.create_task(engine.run())
                ...

        The loop:
        1. Recomputes ``next_fire_at`` on every entry that lacks one.
        2. Asks the cache for the earliest due schedule.
        3. Sleeps until that schedule's next_fire_at OR the cache
           changes OR :meth:`stop` is called - whichever is first.
        4. On wake, processes every schedule whose ``next_fire_at <=
           now``, dispatching per the catch-up policy.
        5. Loops.
        """
        logger.info("z4j.scheduler.tick: engine starting")
        consecutive_errors = 0
        try:
            while not self._stop_event.is_set():
                try:
                    await self._iteration()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # This loop runs as a child of main's asyncio.TaskGroup, and
                    # a TaskGroup cancels every sibling when one child raises. An
                    # exception escaping a single iteration therefore did not just
                    # stop scheduling: it took down the watch stream, the metrics
                    # server and the process with it, for every project the
                    # deployment serves. Each path known to raise is guarded where
                    # it raises (the fire path and the next-fire computation both
                    # quarantine the offending schedule); this is the backstop for
                    # the paths that are not.
                    consecutive_errors += 1
                    m.engine_iteration_failures_total.inc()
                    logger.exception(
                        "z4j.scheduler.tick: iteration raised (%d consecutive); "
                        "continuing after backoff",
                        consecutive_errors,
                    )
                    if consecutive_errors >= self._max_consecutive_iteration_errors:
                        # Absorbing forever would leave a scheduler that looks
                        # healthy and never fires. Past this many back-to-back
                        # failures nothing is being scheduled anyway, so let the
                        # fault out where a supervisor can see it.
                        # critical, not error: the traceback is already on the
                        # line above, and the scheduler is about to stop
                        # scheduling entirely.
                        logger.critical(
                            "z4j.scheduler.tick: %d consecutive iteration "
                            "failures; giving up so the fault is visible",
                            consecutive_errors,
                        )
                        raise
                    await self._pause_after_iteration_error(consecutive_errors)
                else:
                    consecutive_errors = 0
        except asyncio.CancelledError:
            logger.info("z4j.scheduler.tick: engine cancelled")
            raise
        finally:
            logger.info("z4j.scheduler.tick: engine stopped")

    async def _pause_after_iteration_error(self, consecutive_errors: int) -> None:
        """Back off after a failed iteration, waking early on stop.

        A path that raises synchronously would otherwise spin the event loop and
        starve the watch reconnect, which is the same starvation the unhealthy
        watch branch guards against.
        """
        delay = min(
            _ITERATION_ERROR_BACKOFF_MAX,
            self._iteration_error_backoff_seconds * (2 ** min(consecutive_errors - 1, 6)),
        )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

    async def stop(self) -> None:
        """Signal the loop to exit on its next iteration.

        Idempotent. Safe to call from any coroutine.
        """
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    async def _iteration(self) -> None:
        """One pass of the tick loop. Public for testing."""
        # Cooperative yield. Guarantees every iteration of the run()
        # loop releases the event loop at least once, even when this
        # iteration's logic returns synchronously (no real awaits).
        # Without this, a tight no-await branch -- such as the unhealthy-
        # watch path below when cache contains past-due entries -- can
        # starve the watch reconnect task and the scheduler deadlocks
        # by starvation. Cheap (tens of microseconds).
        await asyncio.sleep(0)

        # Step 1: every entry needs a next_fire_at; compute on-demand
        # for fresh ones.
        await self._compute_pending_next_fires()

        # Step 2: dispatch anything that's due now.
        # Skip schedules already in flight from a previous iteration.
        # See ``self._in_flight``
        # docstring in __init__ for the full race description.
        # Also: refuse to fire if the watch stream is unhealthy -
        # cache state may be stale (operator-disabled schedules
        # could still be in cache awaiting the disable event).
        if not self._watch_healthy():
            logger.debug(
                "z4j.scheduler.tick: watch stream unhealthy; skipping dispatch this iteration",
            )
            # Fixed-duration cooperative wait so the watch reconnect
            # task and other asyncio coroutines can run. Past-due
            # schedules in the cache cause _sleep_until_next to
            # return immediately (no await), so without this branch
            # the engine spins on its own task indefinitely: watch
            # never reconnects, /metrics never responds, the engine
            # never escapes the unhealthy state. Manual scheduler
            # restart is the only recovery -- which is exactly the
            # silent-data-loss-on-brain-restart symptom that load
            # testing surfaced.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_UNHEALTHY_SLEEP_SECONDS,
                )
            return
        now = self._clock()
        due = await self._cache.all_due(before=now)
        if due:
            # M8: fire due schedules CONCURRENTLY (bounded), not sequentially.
            # A single schedule's fire_all_missed drain can be up to the 10k
            # slot cap; awaiting each inline delayed every OTHER due schedule's
            # on-time fire behind it.
            # P1-8b: skip a schedule that is in fire back-off (a prior dispatch
            # failed) until its retry deadline, so we neither hot-spin on it nor
            # advance its next_fire_at (the retry re-fires the original slot).
            runnable = [
                e
                for e in due
                if e.id not in self._in_flight
                and self._fire_backoff_until.get(e.id, now) <= now
                # A one_shot/clocked slot this follower already observed
                # is parked; skip re-dispatch until promotion (the leader gate in
                # _is_follower_parked releases it) or the slot changes.
                and not self._is_follower_parked(e)
            ]
            # Cap the per-tick dispatch batch so the loop returns to its
            # top-of-iteration promotion re-check within a bounded time even under
            # a large backlog. The remainder stays due and is dispatched on the
            # next (immediate, since something is still due) iteration.
            if len(runnable) > _MAX_DISPATCH_PER_TICK:
                runnable = runnable[:_MAX_DISPATCH_PER_TICK]
            for e in runnable:
                self._in_flight.add(e.id)
            if runnable:
                # RM13: bound the number of SPAWNED coroutines, not just the
                # number that hold a semaphore. The old
                # ``gather(*(_run(e) for e in runnable))`` created ONE task per
                # due entry up front, so a backlog of N due schedules parked
                # N - 16 tasks blocked on a semaphore -- unbounded task/memory
                # growth on a large fleet or after a long pause. A fixed worker
                # pool draining a queue keeps at most ``_MAX_CONCURRENT_FIRES``
                # coroutines alive no matter how many schedules are due.
                queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue()
                for e in runnable:
                    queue.put_nowait(e)

                async def _worker() -> None:
                    # engine:344: honour a graceful stop between entries. Without
                    # this a stop signalled mid-fan-out still drains EVERY queued
                    # due entry (2,000 project-count scans after stop). Entries
                    # left in the queue are released from _in_flight below.
                    while not self._stop_event.is_set():
                        try:
                            entry = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            return
                        try:
                            ok = await self._fire_with_catch_up(entry, now=now)
                        except Exception:
                            # P1-8b: a fire that RAISES means the schedule's
                            # cadence is broken (e.g. an OverflowError on an
                            # oversized interval) or a post-dispatch computation
                            # failed after the task was already sent. Pushing
                            # next_fire_at forward (the old back-off) made it fire
                            # REAL tasks at the back-off instants, and re-firing a
                            # post-dispatch failure duplicates the task. Quarantine
                            # it (disable) so it stops firing; an operator fixes
                            # the definition and re-enables.
                            logger.exception(
                                "z4j.scheduler.tick: firing schedule_id=%s raised; "
                                "quarantining (disabling) the schedule",
                                entry.id,
                            )
                            await self._quarantine_after_fire_error(entry)
                        else:
                            if ok:
                                # Clean fire: reset the error back-off state.
                                self._fire_error_counts.pop(entry.id, None)
                                self._fire_backoff_until.pop(entry.id, None)
                                self._fire_backoff_def.pop(entry.id, None)
                            else:
                                # DISPATCH failed (no task sent). Back off so the
                                # SAME slot is retried on a widening interval
                                # without hot-spinning; not a clean fire, so the
                                # error count is NOT reset (engine:729).
                                await self._back_off_after_fire_error(entry)
                        finally:
                            self._in_flight.discard(entry.id)

                workers = [
                    asyncio.create_task(_worker())
                    for _ in range(min(_MAX_CONCURRENT_FIRES, len(runnable)))
                ]
                try:
                    await asyncio.gather(*workers, return_exceptions=True)
                finally:
                    # engine:344 + engine:328: any entry still queued was never
                    # dispatched (a graceful stop drained early, or the gather was
                    # cancelled). Its id was pre-added to _in_flight; release it so
                    # a reused engine does not leak it and it is re-evaluated on the
                    # next run. Runs single-threaded, so this drain cannot race a
                    # worker.
                    while not queue.empty():
                        with contextlib.suppress(asyncio.QueueEmpty):
                            self._in_flight.discard(queue.get_nowait().id)

        # Step 3: sleep until the next schedule OR a cache change OR
        # stop signal.
        await self._sleep_until_next()

    def _prune_stale_fire_state(self, snapshot: list[ScheduleEntry]) -> None:
        """Reconcile the per-schedule fire-error / quarantine bookkeeping.

        RM12: the engine has no schedule-removal hook (it reads the cache), so a
        schedule that errored and was then DELETED before ever firing cleanly
        would leak its counters forever. Pruning here (every iteration; the
        dicts are tiny) keeps them bounded by the number of live schedules.

        L3: additionally clears the back-off for a schedule still LIVE but whose
        CADENCE was edited under the same id -- an operator who fixes a failing
        schedule would otherwise wait out up to _MAX_FIRE_BACKOFF_SECONDS of
        stale suppression before the corrected cadence could fire. A same-cadence
        replacement (the benign ack echo) does NOT clear it -- the definition-CAS
        in schedule_definition_changed excludes anchor_at.
        """
        if not (
            self._fire_error_counts
            or self._fire_backoff_until
            or self._compute_quarantined
            or self._follower_parked
            or self._follower_handoff
            or self._slot_entitled
        ):
            return
        live_by_id = {entry.id: entry for entry in snapshot}
        # Drop a parked marker once the entry is GONE, or once its
        # next_fire_at no longer matches the parked slot (the leader advanced it,
        # or a fired one_shot's success echo set next_fire_at=None). Both the
        # due-filter and the sleep skip a next_fire_at=None entry BEFORE calling
        # _is_follower_parked, so the self-clean there never runs for a consumed
        # one_shot -- without this the marker would leak for the schedule's whole
        # lifetime.
        for sid, parked_at in list(self._follower_parked.items()):
            live = live_by_id.get(sid)
            if live is None or live.next_fire_at != parked_at:
                self._follower_parked.pop(sid, None)
        # The handoff record follows the same lifecycle -- drop it once the
        # entry is gone or its slot changed (a stale handoff must never make a
        # DIFFERENT later slot fire on-time).
        for sid, (handoff_at, cadence) in list(self._follower_handoff.items()):
            live = live_by_id.get(sid)
            # Drop it when the entry is gone, the slot moved, OR the cadence
            # itself was edited. An edit can leave next_fire_at unchanged, so the
            # slot comparison alone let a stale marker survive and fire a slot
            # the new cadence does not contain.
            # A DISABLE also invalidates it. A follower parks slot T, the
            # schedule is disabled and re-enabled with the same cadence and the
            # same next_fire_at, and on promotion T fired "on time" -- even
            # though catch_up="skip" would have discarded it, and even though
            # the operator had switched the schedule off across that very slot.
            # An observed disable is an explicit statement that this occurrence
            # should not run.
            if (
                live is None
                or not live.is_enabled
                or live.next_fire_at != handoff_at
                or schedule_cadence_identity(live) != cadence
            ):
                self._follower_handoff.pop(sid, None)
                self._slot_entitled.pop(sid, None)
        self._prune_slot_entitlements(live_by_id)
        for sid in [s for s in self._fire_error_counts if s not in live_by_id]:
            self._fire_error_counts.pop(sid, None)
        for sid in [s for s in self._fire_backoff_until if s not in live_by_id]:
            self._fire_backoff_until.pop(sid, None)
        for sid in [s for s in self._fire_backoff_def if s not in live_by_id]:
            self._fire_backoff_def.pop(sid, None)
        for sid, prior in list(self._fire_backoff_def.items()):
            live = live_by_id.get(sid)
            if live is not None and schedule_definition_changed(prior, live):
                self._fire_error_counts.pop(sid, None)
                self._fire_backoff_until.pop(sid, None)
                self._fire_backoff_def.pop(sid, None)
        # engine:449: drop the log-throttle record for a schedule the watch
        # removed, so if the same id is later re-created with a still-broken
        # expression it logs once more.
        for sid in [s for s in self._compute_quarantined if s not in live_by_id]:
            self._compute_quarantined.pop(sid, None)

    async def _compute_pending_next_fires(self) -> None:
        """Compute next_fire_at for any entry that lacks one.

        Called on every iteration so freshly-added schedules and
        schedules whose previous next_fire_at was just consumed get
        a new value before the next sleep.

        Mutations go through
        :meth:`ScheduleCache.update_fire_state` so a concurrent
        watch upsert can't leave us writing on an evicted entry.
        ``self._pending_disables`` is drained through the same
        atomic helper.
        """
        snapshot = await self._cache.snapshot()
        self._prune_stale_fire_state(snapshot)
        for entry in snapshot:
            if entry.next_fire_at is None and entry.is_enabled:
                try:
                    next_at = self._next_fire_for(entry)
                except Exception:
                    # engine:449: _next_fire_for's per-kind handlers catch only
                    # their OWN *ExpressionError, but the cadence math can raise
                    # OTHER exceptions -- e.g. timedelta(seconds=huge) raises
                    # OverflowError (an ArithmeticError, NOT an
                    # IntervalExpressionError) on an oversized interval. This is
                    # the ONLY unguarded _next_fire_for caller (the fire path's
                    # calls run inside _fire_with_catch_up, which the worker
                    # try/except quarantines). An uncaught raise here propagates
                    # through _iteration -> run() -> the TaskGroup and crashes the
                    # WHOLE scheduler, re-crashing on restart as brain re-syncs
                    # the row. Quarantine the one bad schedule locally instead
                    # (mirrors the fire-path quarantine, P1-8b). The brain does
                    # not validate interval magnitude and keeps re-syncing the row
                    # is_enabled=True, so throttle the traceback to ONCE per
                    # (id, failing expression) -- otherwise every re-sync storms
                    # the log.
                    if self._compute_quarantined.get(entry.id) != entry.expression:
                        logger.exception(
                            "z4j.scheduler.tick: next-fire computation raised for "
                            "schedule_id=%s (kind=%s, expression=%r); disabling "
                            "locally",
                            entry.id,
                            entry.kind,
                            entry.expression,
                        )
                        self._compute_quarantined[entry.id] = entry.expression
                    self._pending_disables[entry.id] = entry
                    continue
                # L5: the compute SUCCEEDED (including a legitimately-exhausted
                # one_shot that returns None) -- drop any stale quarantine record
                # so a subsequent genuine failure logs its traceback afresh
                # instead of being silently throttled against an old expression.
                self._compute_quarantined.pop(entry.id, None)
                if next_at is not None:
                    # M9: definition-CAS the compute write too. The next-fire we
                    # computed is for THIS snapshot's cadence; if a watch upsert
                    # replaced the entry with an edited cadence in between, its
                    # own recompute owns the fire-state and this write is skipped.
                    await self._cache.update_fire_state(
                        entry.id,
                        next_fire_at=next_at,
                        expected_definition=entry,
                    )
        # Apply any disables queued by ``_next_fire_for``. Pop into
        # a local dict so a concurrent producer can keep adding while
        # we drain the snapshot.
        if self._pending_disables:
            to_disable = dict(self._pending_disables)
            self._pending_disables.clear()
            for stored in to_disable.values():
                # M10: definition-CAS the disable. If the operator fixed the
                # broken cadence between the failed compute and here, the watch
                # upsert changed the definition and we must NOT disable the
                # now-valid schedule; the CAS skips it and the next iteration
                # computes its fire-state.
                await self._record_local_quarantine(
                    stored,
                    code="cadence_definition_invalid",
                    detail=f"{stored.kind}:{stored.expression}",
                )

    def _next_fire_for(  # noqa: PLR0911 - per-kind dispatch is idiomatic
        self,
        entry: ScheduleEntry,
        *,
        as_of_last_fire_at: datetime | object | None = _LAST_FIRE_AT_DEFAULT,
    ) -> datetime | None:
        """Compute next-fire for a single entry. None for completed one-shots.

        Callers can pass an explicit ``as_of_last_fire_at`` to
        override what the function sees as the last fire moment -
        without having to mutate ``entry.last_fire_at`` first. A
        "temporarily mutate the entry" pattern in
        ``_advance_after_fire`` would be a non-atomic write on
        the cache's live entry; concurrent readers (`all_due`,
        `next_due`) could observe the half-applied state. The
        anchor is purely a local function arg.

        ``as_of_last_fire_at=None`` (Python None) is a sentinel
        for "the schedule has never fired" (use clock as anchor).
        Pass the literal ``_NEVER`` object to mean "I want to
        explicitly anchor at None"; pass nothing to default to
        ``entry.last_fire_at`` (the legacy behavior).
        """
        if as_of_last_fire_at is _LAST_FIRE_AT_DEFAULT:
            last: datetime | None = entry.last_fire_at
        else:
            # Caller passed an explicit value (or None).
            last = as_of_last_fire_at  # type: ignore[assignment]
        if entry.kind == "cron":
            after = last if last is not None else self._clock()
            try:
                return cron_mod.next_fire(
                    entry.expression,
                    entry.timezone,
                    after,
                )
            except cron_mod.CronExpressionError:
                logger.exception(
                    "z4j.scheduler.tick: invalid cron expression for "
                    "schedule_id=%s; disabling locally",
                    entry.id,
                )
                # Disable in the local cache so we don't spin on it.
                # Brain will surface the error via the schedule's
                # validation; the operator's fix re-enables via watch.
                # Record the disable on the SHARED set the caller drains
                # rather than mutating ``entry.is_enabled`` directly
                # on the cache's live object. Concurrent
                # ``cache.upsert`` from the WatchStream replaces the
                # entry between this read and the mutation; the
                # disable then lands on an orphaned object and is
                # silently lost on the next iteration.
                self._pending_disables[entry.id] = entry
                return None
        if entry.kind == "interval":
            try:
                return interval_mod.next_fire(
                    entry.expression,
                    last_fire_at=last,
                    anchor_at=entry.anchor_at,
                )
            except interval_mod.IntervalExpressionError:
                logger.exception(
                    "z4j.scheduler.tick: invalid interval expression for "
                    "schedule_id=%s; disabling locally",
                    entry.id,
                )
                # Record the disable on the SHARED set the caller drains
                # rather than mutating ``entry.is_enabled`` directly
                # on the cache's live object. Concurrent
                # ``cache.upsert`` from the WatchStream replaces the
                # entry between this read and the mutation; the
                # disable then lands on an orphaned object and is
                # silently lost on the next iteration.
                self._pending_disables[entry.id] = entry
                return None
        if entry.kind in ("clocked", "one_shot"):
            try:
                return one_shot_mod.next_fire(
                    entry.expression,
                    last_fire_at=last,
                )
            except one_shot_mod.OneShotExpressionError:
                logger.exception(
                    "z4j.scheduler.tick: invalid clocked expression for "
                    "schedule_id=%s; disabling locally",
                    entry.id,
                )
                # Record the disable on the SHARED set the caller drains
                # rather than mutating ``entry.is_enabled`` directly
                # on the cache's live object. Concurrent
                # ``cache.upsert`` from the WatchStream replaces the
                # entry between this read and the mutation; the
                # disable then lands on an orphaned object and is
                # silently lost on the next iteration.
                self._pending_disables[entry.id] = entry
                return None
        if entry.kind == "solar":
            # Solar schedules are first-class per docs/SCHEDULER.md §5.1
            # and the API ``_KIND_VOCAB`` accepts them, but this
            # dispatch table never branched on them, every solar
            # schedule fell through to the "unknown kind" path
            # below, landing on ``_pending_disables`` on the very
            # first tick. The schedule appeared created in the
            # dashboard but never fired.
            anchor = last if last is not None else self._clock()
            try:
                return solar_mod.next_solar_fire(
                    entry.expression,
                    after=anchor,
                )
            except (ValueError, RuntimeError):
                logger.exception(
                    "z4j.scheduler.tick: invalid solar expression for "
                    "schedule_id=%s; disabling locally",
                    entry.id,
                )
                self._pending_disables[entry.id] = entry
                return None
        # Unknown kind - log + queue the disable. Same race
        # fix as above: route through the pending-set so a concurrent
        # WatchStream upsert can't drop the disable.
        logger.error(
            "z4j.scheduler.tick: unknown schedule kind %r for schedule_id=%s",
            entry.kind,
            entry.id,
        )
        self._pending_disables[entry.id] = entry
        return None

    # ------------------------------------------------------------------
    # Dispatch + catch-up
    # ------------------------------------------------------------------

    async def _fire_with_catch_up(  # noqa: PLR0911, PLR0912, PLR0915 -- explicit safety gates
        self,
        entry: ScheduleEntry,
        *,
        now: datetime,
    ) -> bool:
        """Resolve the catch-up plan for a due entry and dispatch.

        Returns True on a clean outcome (fired + advanced, non-leader recompute,
        nothing-to-fire, aborted, or definition-changed) and False when the
        DISPATCHER itself failed (no task was sent; caller should back off and
        retry the same slot, NOT treat it as a clean fire -- engine:729). A
        raised exception (a broken cadence / post-dispatch computation error) is
        NOT caught here; the caller quarantines the schedule so it cannot
        re-dispatch or hot-spin (engine:820 / P1-8b)."""
        # Leader gate: only the leader actually dispatches. Non-leader
        # instances still recompute next_fire_at so they're hot if
        # they take over.
        if not self._leader_gate.is_leader(entry.project_id):
            # Non-leader
            # instances must NOT advance ``last_fire_at`` to the
            # tick they did not actually fire. We DO recompute
            # ``next_fire_at`` so the standby doesn't spin on the
            # same overdue entry every iteration. Becoming leader
            # later preserves the missed backlog for catch_up
            # (because last_fire_at stayed None / the prior
            # value).
            #
            # For
            # FRESH schedules (last_fire_at is None) the slot
            # the standby was about to fire IS lost on
            # promotion-after-this-tick, because there is no
            # anchor for catch_up to walk back to (anchor lookup
            # uses last_fire_at, which is None). This is a
            # bounded degraded mode (only fresh schedules, only
            # during a 1-3s failover window) and the alternative
            # (return without advancing) burns a CPU loop on the
            # standby until promotion. Documented in
            # docs/SCHEDULER.md §HA-failover-corner-cases.
            await self._advance_after_fire(entry, last_fire_at=None)
            return True

        scheduled_for = entry.next_fire_at
        if scheduled_for is None:
            return True

        # Distinguish on-time fires from missed fires. A fire whose
        # scheduled_for is within the grace window of now is "current"
        # and ALWAYS dispatched - the catch-up policy does not apply
        # to current fires (it would defeat the entire purpose of the
        # schedule). A fire whose scheduled_for is more than the grace
        # window in the past is "missed" - we were down or behind, and
        # the schedule's catch_up policy decides what to do.
        lateness_seconds = (now - scheduled_for).total_seconds()
        # One-shot schedules
        # are intentionally past-dated by importers / migrators / a
        # restored-backup workflow. ``one_shot.next_fire`` always
        # returns the configured timestamp regardless of how far in
        # the past it is; pre-fix the catch-up branch then ran
        # ``plan_catch_up("skip", ...)`` which returned ``[]``, the
        # engine advanced ``last_fire_at`` to ``scheduled_for``, and
        # the next ``next_fire`` call returned None, the one-shot
        # was permanently consumed without ever firing. For
        # one-shot we always fire exactly once regardless of
        # lateness; the caller's ``catch_up`` policy is meaningless
        # for a single-fire schedule.
        # ``advance_anchor`` is the slot we stamp as ``last_fire_at`` after
        # dispatching, so ``next_fire_at`` recomputes PAST everything we
        # just handled. For a missed cron backlog it must be the LAST
        # missed slot, not ``scheduled_for`` (the FIRST) -- otherwise the
        # engine re-enters once per slot and re-fires the whole backlog
        # (B3). For on-time / non-cron fires it stays ``scheduled_for``.
        # A slot this instance observed live-due while a FOLLOWER and is
        # now firing as the promoted leader -- the elapsed time is our own
        # promotion-detection latency, not cluster unavailability.
        # Do NOT consume the marker HERE (before dispatch). If the
        # dispatch below fails and the caller retries this slot, the retry must
        # still see the handoff status; the marker is consumed only AFTER a
        # successful dispatch (post-loop, below), and reconcile drops it once the
        # slot advances.
        _cadence = schedule_cadence_identity(entry)
        _handoff = self._follower_handoff.get(entry.id)
        was_follower_handoff = _handoff is not None and _handoff == (
            scheduled_for,
            _cadence,
        )
        advance_anchor = scheduled_for
        pending_discard: tuple[int, datetime, datetime, float] | None = None
        # Apply a PROMOTION-SCOPED grace to a handoff slot (base grace +
        # the promotion re-check interval), rather than FORCING it on-time
        # regardless of age. A slot within promotion_grace fires on-time; one
        # OLDER than that is honestly missed and catch_up governs it (the old
        # force-on-time mislabelled a genuinely-overdue slot). The base grace is
        # unchanged for steady-state leaders / single-instance -- they never carry
        # a marker -- so their on-time/skip semantics do not move at all. If
        # promotion took longer than follower_recheck (e.g. a longer leader-lease
        # TTL) the slot is correctly classified missed rather than silently
        # on-time.
        effective_grace = (
            _promotion_grace_for(
                leader_heartbeat_seconds=self._leader_heartbeat_seconds,
                follower_recheck_seconds=self._follower_recheck_seconds,
                base_grace_seconds=self._on_time_grace_seconds,
            )
            if was_follower_handoff
            else self._on_time_grace_seconds
        )
        # FREEZE the entitlement once granted. The classification is
        # recomputed on every attempt against a moving clock, so a slot judged
        # on-time at the first attempt could exceed the grace by the retry a
        # second later -- and under catch_up="skip" the retry then produced an
        # EMPTY plan, advanced past the slot, and stamped last_fire_at without
        # ever dispatching it. A dispatch failure must not silently change what
        # the slot IS. Once entitled, it stays entitled until it succeeds or the
        # marker is dropped for another reason.
        #
        # The grant covers EVERY on-time slot, not only a handoff one. fixed
        # this shape for the handoff path, but nothing in its reasoning is
        # specific to a handoff: a steady-state LEADER recomputes lateness on
        # each attempt against the same moving clock, and its own dispatch
        # backoff routinely exceeds the 5s base grace. That lost the slot in
        # silence and recorded it as fired.
        # The grant carries the time it was made, and expires. See
        # _ENTITLEMENT_MAX_AGE_SECONDS: without a ceiling the freeze survives an
        # arbitrarily long outage and fires a slot the policy exists to discard.
        _prior = self._slot_entitled.get(entry.id)
        if _prior is not None and (_prior[0], _prior[1]) == (scheduled_for, _cadence):
            granted_at = _prior[2]
            entitled = (now - granted_at).total_seconds() <= _ENTITLEMENT_MAX_AGE_SECONDS
        else:
            granted_at = now
            entitled = False
        if lateness_seconds <= effective_grace:
            self._slot_entitled[entry.id] = (scheduled_for, _cadence, granted_at)
            entitled = True
        if entry.kind in ("clocked", "one_shot") or entitled or lateness_seconds <= effective_grace:
            plan = [scheduled_for]
        else:
            # For cron schedules with ``fire_all_missed``,
            # materialise the full backlog of missed slots between
            # the last known fire and the current scheduled_for.
            # Without this, the missed list would always be a
            # single element, so all three catch_up policies
            # (skip / fire_one_missed / fire_all_missed) would
            # produce indistinguishable behavior for cron.
            #
            # ``fires_between`` returns every cron slot in the
            # window; the planner then trims per policy:
            #   - skip               -> []
            #   - fire_one_missed    -> [last_slot]
            #   - fire_all_missed    -> [all slots]
            #
            # Both cron AND interval schedules have a well-defined list of
            # missed slots, so materialise the full backlog for either and
            # let plan_catch_up coalesce per policy. H4: interval previously
            # fell into the single-slot else-branch, which made
            # fire_one_missed (and skip) re-fire the whole interval backlog
            # one tick at a time -- byte-for-byte the B3 storm, on interval
            # instead of cron. one_shot / clocked never reach here (handled
            # above);: solar now materialises its backlog too (via iterated
            # next_solar_fire), so it no longer stays single-slot.
            if entry.kind == "cron":
                missed_times = self._compute_missed_cron_slots(
                    entry,
                    scheduled_for=scheduled_for,
                    now=now,
                )
            elif entry.kind == "interval":
                missed_times = self._compute_missed_interval_slots(
                    entry,
                    scheduled_for=scheduled_for,
                    now=now,
                )
            elif entry.kind == "solar":
                # Solar now materialises its full backlog too, so
                # fire_one_missed coalesces to the latest slot and the anchor
                # jumps past the whole backlog in one pass (no per-tick re-entry
                # storm after a multi-boundary solar outage).
                missed_times = self._compute_missed_solar_slots(
                    entry,
                    scheduled_for=scheduled_for,
                    now=now,
                )
            else:
                missed_times = [scheduled_for]
            plan = plan_catch_up(
                entry.catch_up,
                missed_times=missed_times,
                now=now,
            )
            # A discard here is the catch_up policy working as configured, but
            # it used to happen in total silence: no log line, no metric. An
            # operator whose catch_up="skip" schedule stopped producing work had
            # nothing to look at.
            #
            # Recorded, not reported, at this point. This is the DECISION, and
            # the decision is recomputed on every retry of a failing cursor
            # advance, so reporting here counted the same backlog again on each
            # attempt. It is reported by _report_discarded_slots, from the paths
            # that have actually advanced past these slots.
            _discarded = len(missed_times) - len(plan)
            if _discarded > 0:
                pending_discard = (
                    _discarded,
                    missed_times[0],
                    missed_times[-1],
                    lateness_seconds,
                )
            # Advance past the WHOLE backlog in one pass -- the last missed
            # slot -- regardless of how many the policy chose to fire.
            if missed_times:
                advance_anchor = missed_times[-1]

        # Boundary D: finish every fallible successor computation for this
        # bounded dispatch batch before its first task may be sent. The old
        # path called ``_next_fire_for`` from ``_advance_after_fire`` after the
        # dispatcher returned; a parse/overflow failure there left an accepted
        # task with no reconstructible cursor transition.
        from z4j_scheduler.tick._prepared import PreparedFire

        bounded_plan = plan[:_MAX_DISPATCH_PER_TICK]
        prepared_by_slot = {
            moment: PreparedFire(
                scheduled_for=moment,
                next_run_at=self._next_fire_for(
                    entry,
                    as_of_last_fire_at=moment,
                ),
            )
            for moment in bounded_plan
        }
        prepared_skip = (
            PreparedFire(
                scheduled_for=advance_anchor,
                next_run_at=self._next_fire_for(
                    entry,
                    as_of_last_fire_at=advance_anchor,
                ),
            )
            if not plan
            else None
        )

        if prepared_skip is not None and entry.control_token is not None:
            try:
                transition = await self._dispatcher.advance_cursor(
                    entry=entry,
                    prepared=prepared_skip,
                )
            except Exception:
                logger.exception(
                    "z4j.scheduler.tick: durable no-work cursor advance failed "
                    "for schedule_id=%s; retaining the original slot",
                    entry.id,
                )
                return False
            if transition.disposition not in {"applied", "idempotent"}:
                if transition.disposition == "cadence_semantics_mismatch":
                    raise RuntimeError(
                        "Brain rejected the scheduler cadence semantics",
                    )
                return False
            if (
                transition.committed_last_run_at != prepared_skip.scheduled_for
                or transition.committed_next_run_at != prepared_skip.next_run_at
            ):
                raise RuntimeError(
                    "Brain committed a cursor different from the prepared transition",
                )
            if (
                transition.live_control_token != entry.control_token
                or transition.live_revision <= entry.schedule_revision
            ):
                return False
            await self._cache.apply_cursor_transition(
                entry.id,
                expected_control_token=entry.control_token,
                expected_revision=entry.schedule_revision,
                expected_last_run_at=entry.last_fire_at,
                expected_next_run_at=entry.next_fire_at,
                committed_revision=transition.live_revision,
                committed_last_run_at=transition.live_last_run_at,
                committed_next_run_at=transition.live_next_run_at,
            )
            self._consume_follower_handoff(entry.id, was_follower_handoff)
            self._report_discarded_slots(entry, pending_discard)
            return True

        # A3: hand the dispatcher this project's schedule count (from the
        # cache the engine already owns) so IT can decide whether the
        # fire-variance histogram carries a per-schedule label -- that
        # cardinality threshold lives with the metric rather than here. The
        # engine's own counters are aggregate-only for the same reason: it has
        # no schedule-count gate to weigh a per-schedule label against.
        # Resolved once per fire, not per missed moment.
        project_schedule_count = await self._cache.count_for_project(
            entry.project_id,
        )

        # M14: a fire_all_missed backlog can be up to the 10k slot cap. The
        # drain must NOT monopolise the engine: between dispatches we
        # re-check the stop signal AND the schedule's LIVE enabled state, and
        # yield to the loop. Otherwise a graceful stop() waited for the whole
        # backlog, an operator disabling the schedule mid-storm (the natural
        # remediation) was ignored until it drained, and every other due
        # schedule's on-time fire queued behind this one coroutine.
        last_dispatched: datetime | None = None
        dispatched_this_tick = 0
        aborted = False
        definition_changed = False
        current_protocol = entry.control_token is not None
        for moment in plan:
            if self._stop_event.is_set():
                aborted = True
                break
            # (Fairness): bound how many slots of ONE schedule's backlog we
            # drain per tick. A single fire_all_missed schedule can produce up to
            # the 10k cap; draining it all in this coroutine would starve every
            # OTHER due schedule's on-time fire (and delay a promotion re-check)
            # until it finished. Stop after a bounded chunk and advance to what we
            # dispatched; the aborted-partial path re-evaluates the remainder on
            # the next tick, so a huge backlog drains a chunk per tick while
            # everything else keeps firing on time. A handoff / on-time slot is a
            # single-element plan and never hits this.
            if dispatched_this_tick >= _MAX_DISPATCH_PER_TICK:
                aborted = True
                break
            # Re-read the entry from the cache: the watch stream may have
            # delivered a disable (or removal) mid-drain. A snapshot
            # ``entry`` cannot see it. Halt the drain if it is no longer
            # enabled or was deleted.
            live = await self._cache.get(entry.id)
            if live is None or not live.is_enabled:
                aborted = True
                break
            # M10: also halt if the DEFINITION changed mid-drain (expression /
            # interval / kind / timezone / catch_up / anchor). ``plan`` was
            # computed from the stale snapshot; dispatching it now would fire
            # the OLD backlog AND stamp a next_fire_at from the old cadence
            # into the freshly-edited schedule. The edit path recomputes
            # next_fire itself, so we abort WITHOUT advancing (below).
            if _schedule_definition_changed(entry, live):
                definition_changed = True
                break
            try:
                prepared_fire = prepared_by_slot[moment]
                expected_control_token = entry.control_token
                expected_revision = entry.schedule_revision
                expected_last_run_at = entry.last_fire_at
                expected_next_run_at = entry.next_fire_at
                fire_result = await self._dispatcher.dispatch(
                    schedule_id=entry.id,
                    scheduled_for=moment,
                    schedule_name=entry.name,
                    engine=entry.engine,
                    project_id=entry.project_id,
                    project_schedule_count=project_schedule_count,
                    prepared_fire=prepared_fire,
                    schedule_entry=entry,
                )
            except Exception:
                logger.exception(
                    "z4j.scheduler.tick: dispatcher raised for "
                    "schedule_id=%s scheduled_for=%s; will retry on next tick",
                    entry.id,
                    moment,
                )
                # Do NOT advance -- the task was NOT sent. Signal a dispatch
                # failure so the caller backs off and retries the SAME slot
                # (next_fire_at is unchanged), instead of treating this as a
                # clean fire and hot-spinning on it (engine:729).
                if last_dispatched is not None and not current_protocol:
                    prior_prepared = prepared_by_slot[last_dispatched]
                    await self._apply_prepared_advance(entry, prior_prepared)
                return False
            if current_protocol:
                if fire_result is None or fire_result.disposition is None:
                    logger.error(
                        "z4j.scheduler.tick: current fire returned no typed "
                        "disposition schedule_id=%s scheduled_for=%s",
                        entry.id,
                        moment,
                    )
                    return False
                if fire_result.disposition == "cadence_semantics_mismatch":
                    await self._record_local_quarantine(
                        entry,
                        code="cadence_semantics_mismatch",
                        detail="Brain and scheduler cadence runtimes differ",
                    )
                    return True
                if fire_result.disposition in {
                    "terminal_quarantined",
                    "slot_resolved_refresh",
                    "stale_control_refresh",
                    "legacy_upgrade_required",
                }:
                    if expected_control_token is not None:
                        await self._stop_until_brain_supersedes(
                            entry,
                            disposition=fire_result.disposition,
                            expected_control_token=expected_control_token,
                            refused_at_revision=fire_result.live_revision,
                        )
                    return True
                if fire_result.disposition != "accepted":
                    return False
                if (
                    expected_control_token is None
                    or fire_result.acceptance_revision <= expected_revision
                    or fire_result.accepted_last_run_at != prepared_fire.scheduled_for
                    or fire_result.accepted_next_run_at != prepared_fire.next_run_at
                    or fire_result.live_control_token is None
                    or fire_result.live_revision < fire_result.acceptance_revision
                    or fire_result.live_revision <= expected_revision
                    or fire_result.live_last_run_at is None
                ):
                    logger.error(
                        "z4j.scheduler.tick: malformed accepted fire evidence "
                        "schedule_id=%s scheduled_for=%s",
                        entry.id,
                        moment,
                    )
                    return False
                accepted_cursor = (
                    fire_result.accepted_last_run_at,
                    fire_result.accepted_next_run_at,
                )
                live_cursor = (
                    fire_result.live_last_run_at,
                    fire_result.live_next_run_at,
                )
                if (
                    fire_result.live_revision == fire_result.acceptance_revision
                    and live_cursor != accepted_cursor
                ):
                    logger.error(
                        "z4j.scheduler.tick: accepted fire returned a conflicting "
                        "same-revision cursor schedule_id=%s",
                        entry.id,
                    )
                    return False
                if fire_result.live_control_token != expected_control_token:
                    await self._stop_until_brain_supersedes(
                        entry,
                        disposition="accepted_under_rotated_control",
                        expected_control_token=expected_control_token,
                        refused_at_revision=fire_result.live_revision,
                    )
                    return True
                applied = await self._cache.apply_cursor_transition(
                    entry.id,
                    expected_control_token=expected_control_token,
                    expected_revision=expected_revision,
                    expected_last_run_at=expected_last_run_at,
                    expected_next_run_at=expected_next_run_at,
                    committed_revision=fire_result.live_revision,
                    committed_last_run_at=fire_result.live_last_run_at,
                    committed_next_run_at=fire_result.live_next_run_at,
                )
                if not applied:
                    return True
                if fire_result.live_revision != fire_result.acceptance_revision:
                    return True
            last_dispatched = moment
            # This counts successful dispatches, not loop iterations; enumerate
            # would incorrectly include aborted or failed slots.
            dispatched_this_tick += 1  # noqa: SIM113
            # Be polite to the event loop on long backlogs so heartbeats,
            # watch updates, and other schedules are not starved.
            await asyncio.sleep(0)

        if definition_changed:
            # M10: the schedule was re-defined mid-drain. Do NOT advance --
            # stamping last_fire_at / next_fire_at from the pre-edit cadence
            # would corrupt the new definition (which recomputes its own next
            # fire). The new cadence governs from the next tick.
            return True
        if aborted:
            # Partial drain (stop requested, or the schedule was disabled /
            # removed mid-storm). Advance only past what we ACTUALLY
            # dispatched so we neither re-fire those slots nor lose the
            # untouched tail: the remainder is re-evaluated on the next tick
            # (if still leader + enabled) or after re-enable. If nothing was
            # dispatched, do not advance at all.
            if last_dispatched is not None:
                if not current_protocol:
                    await self._apply_prepared_advance(
                        entry,
                        prepared_by_slot[last_dispatched],
                    )
                self._consume_follower_handoff(entry.id, was_follower_handoff)
            return True

        # Advance: stamp last_fire_at and recompute next_fire_at.
        # Even when the catch-up plan was empty (skip on a missed fire),
        # we still advance past the whole missed backlog (``advance_anchor``
        # = the last missed slot) - otherwise we'd re-evaluate the same
        # slots every iteration and re-fire them (B3).
        if not current_protocol:
            prepared_advance = prepared_skip or prepared_by_slot[advance_anchor]
            await self._apply_prepared_advance(entry, prepared_advance)
        # Consume the handoff marker only now that the slot was actually
        # dispatched. A failed dispatch returned False above without reaching
        # here, so the marker survives for the retry.
        self._consume_follower_handoff(entry.id, was_follower_handoff)
        self._report_discarded_slots(entry, pending_discard)
        return True

    async def _apply_prepared_advance(
        self,
        entry: ScheduleEntry,
        prepared: PreparedFire,
    ) -> None:
        """Mirror an already-computed transition without cadence work."""

        await self._cache.update_fire_state(
            entry.id,
            last_fire_at=prepared.scheduled_for,
            next_fire_at=prepared.next_run_at,
            expected_definition=entry,
        )

    def _is_follower_parked(self, entry: ScheduleEntry) -> bool:
        """True if this NON-leader has already observed ``entry``'s
        current due slot (a one_shot/clocked it cannot advance past).

        Self-cleans a stale park: if the slot moved (a watch echo adopted new
        state, or a recompute), or if this instance is now the LEADER for the
        project (promotion -- the leader must fire the parked slot), the park is
        dropped and the entry is treated as un-parked.
        """
        parked = self._follower_parked.get(entry.id)
        if parked is None:
            return False
        if parked != entry.next_fire_at:
            self._follower_parked.pop(entry.id, None)
            return False
        if self._leader_gate.is_leader(entry.project_id):
            self._follower_parked.pop(entry.id, None)
            return False
        return True

    def _report_discarded_slots(
        self,
        entry: ScheduleEntry,
        pending: tuple[int, datetime, datetime, float] | None,
    ) -> None:
        """Log and count slots the catch_up policy dropped.

        Called only from the paths that have durably advanced past those slots.
        Reporting at the decision point instead re-counted the whole backlog on
        every retry of a failing cursor advance, so one stuck schedule inflated
        the counter without bound.
        """
        if pending is None:
            return
        discarded, oldest, newest, lateness_seconds = pending
        m.slots_discarded_total.labels(catch_up=entry.catch_up).inc(discarded)
        logger.warning(
            "z4j.scheduler.tick: catch_up=%s dropped %d missed slot(s) for "
            "schedule_id=%s name=%r without firing (oldest=%s newest=%s, "
            "%.1fs late); these occurrences will not run",
            entry.catch_up,
            discarded,
            entry.id,
            entry.name or "",
            oldest,
            newest,
            lateness_seconds,
        )

    def _prune_slot_entitlements(self, live_by_id: dict[UUID, ScheduleEntry]) -> None:
        """Drop on-time entitlements that no longer describe a live pending slot.

        An entitlement granted to a LEADER-observed slot has no accompanying
        ``_follower_handoff`` marker, so the handoff reconciliation never reaches
        it. Without this it would survive for the process lifetime.

        It is dropped once the schedule is gone or disabled (per a disable is
        an explicit statement that this occurrence should not run), or once
        ``next_fire_at`` has moved off the entitled slot. A dispatch FAILURE
        leaves ``next_fire_at`` untouched, so a moved slot means the fire already
        landed and the entitlement has served its purpose.
        """
        now = self._clock()
        for sid, (entitled_at, _cadence, granted_at) in list(self._slot_entitled.items()):
            live = live_by_id.get(sid)
            if (
                live is None
                or not live.is_enabled
                or live.next_fire_at != entitled_at
                or (now - granted_at).total_seconds() > _ENTITLEMENT_MAX_AGE_SECONDS
            ):
                self._slot_entitled.pop(sid, None)

    def _consume_follower_handoff(self, schedule_id: UUID, was_follower_handoff: bool) -> None:
        """Drop the follower-handoff marker AFTER a successful dispatch
        of the slot it covered. Called only on the success paths, never after a
        failed dispatch (which returns early), so a retry of the same slot
        preserves the handoff status and its promotion-scoped grace.

        The entitlement is dropped unconditionally: it is now granted to every
        on-time slot, so gating its release on was_follower_handoff would strand
        a marker per schedule for the process lifetime."""
        if was_follower_handoff:
            self._follower_handoff.pop(schedule_id, None)
        self._slot_entitled.pop(schedule_id, None)

    async def _advance_after_fire(
        self,
        entry: ScheduleEntry,
        *,
        last_fire_at: datetime | None,
    ) -> None:
        """Update entry's last_fire_at + recompute next_fire_at.

        The engine NEVER mutates the live cache entry directly:
        ``entry.last_fire_at = last_fire_at`` would run outside
        the cache lock, so a concurrent reader (`all_due`,
        `next_due`) could observe a half-applied state. Instead
        we pass the new anchor through
        ``_next_fire_for(as_of_last_fire_at=...)`` and let
        ``cache.update_fire_state`` write under its lock.

        ``last_fire_at=None`` is a meaningful "leave it alone"
        signal from the non-leader path (we did NOT actually fire,
        so don't pretend we did).
        """
        if last_fire_at is None:
            # Non-leader path.: a FOLLOWER must NOT mutate the
            # authoritative fire-state (last_fire_at OR next_fire_at). The prior
            # Recompute anchored next_fire_at on NOW, which:
            #   - rewrote an OVERDUE slot to a FUTURE one, so a promoted leader
            #     then classified the skipped backlog (last_fire..now) as on-time
            #     and NEVER ran it (H7 -- permanent catch-up loss); and
            #   - shifted an INTERVAL schedule off its real grid onto a now-based
            #     cadence (a daily interval anchored at midnight, observed by a
            #     follower at 18:00, became an 18:00 cadence -- H8), which a later
            #     promotion could then persist as a phantom slot / mint a fire id
            #     from an already-fired real boundary.
            # Instead PARK the entry on its CURRENT next_fire_at: the park excludes
            # it from the due filter AND the sleep wake (so a behind follower does
            # not hot-loop) while leaving last_fire_at and next_fire_at exactly as
            # the leader / persistence set them. On promotion the park self-clears
            # (_is_follower_parked sees is_leader) and the LEADER path fires the
            # real slot and replays the catch_up backlog from the true
            # last_fire_at. This is the mechanism the one_shot path already used;
            # it now covers EVERY cadence type (cron / interval / solar / one_shot
            # / clocked) uniformly, so no cadence is ever rewritten by a follower.
            # Remember we OBSERVED this exact slot live-due as a follower.
            # If we are later promoted and fire it, the elapsed time is our own
            # promotion-detection latency, not a cluster outage, so it fires
            # on-time (see _fire_with_catch_up). Both markers are only meaningful
            # for a slot with a concrete next_fire_at: parking None recorded a
            # park that _is_follower_parked then read back as "not parked", and
            # it contradicted this dict's own datetime annotation. The due filter
            # and the sleep both skip a next_fire_at=None entry before we get
            # here, so this guard changes no reachable behaviour.
            if entry.next_fire_at is not None:
                self._follower_parked[entry.id] = entry.next_fire_at
                self._follower_handoff[entry.id] = (
                    entry.next_fire_at,
                    schedule_cadence_identity(entry),
                )
        else:
            # Leader path: anchor on the slot we just fired so the
            # next-fire computation walks forward from there.
            next_at = self._next_fire_for(
                entry,
                as_of_last_fire_at=last_fire_at,
            )
            await self._cache.update_fire_state(
                entry.id,
                last_fire_at=last_fire_at,
                next_fire_at=next_at,
                # RM11: this is the path (including the empty catch-up plan at
                # the call site above) that previously stamped a snapshot-
                # derived next_fire_at unconditionally. Guard it so a cadence
                # edit landing between the snapshot and here is not clobbered.
                expected_definition=entry,
            )

    async def _back_off_after_fire_error(
        self,
        entry: ScheduleEntry,
    ) -> None:
        """A DISPATCH failed (no task was sent). Retry the SAME slot after an
        exponential back-off, recorded in a SEPARATE ``_fire_backoff_until``
        deadline -- NOT by overwriting ``next_fire_at``.

        The old code stamped ``next_fire_at = now + backoff``, which (P1-8b) made
        the retry dispatch with ``scheduled_for`` = the back-off instant instead
        of the original slot (drifting the cadence and, for a raising fire,
        re-dispatching real tasks at +1s/+3s/+7s). Now next_fire_at stays at the
        original slot; the due-loop skips the entry until its deadline and
        _sleep_until_next wakes when the deadline elapses, so there is no
        hot-spin and no drift.

        M12: the deadline base is a FRESH ``self._clock()`` read taken HERE, not
        the iteration-start ``now``. A slow dispatch (up to the per-fire timeout,
        several seconds) elapses between the iteration's clock snapshot and this
        point; anchoring the back-off on the stale snapshot yields a deadline
        partly (or wholly) in the past, so the due-loop stops skipping the entry
        immediately and hot-spins the retry with no real back-off. Reading the
        clock now makes the full ``backoff`` window count from the failure.
        """
        count = self._fire_error_counts.get(entry.id, 0) + 1
        self._fire_error_counts[entry.id] = count
        backoff = min(
            _MAX_FIRE_BACKOFF_SECONDS,
            _BASE_FIRE_BACKOFF_SECONDS * (2 ** min(count - 1, _MAX_FIRE_BACKOFF_EXPONENT)),
        )
        self._fire_backoff_until[entry.id] = self._clock() + timedelta(seconds=backoff)
        # L3: remember the cadence this back-off is for, so a later operator edit
        # to the same id clears the suppression instead of inheriting it.
        self._fire_backoff_def[entry.id] = entry

    async def _quarantine_after_fire_error(self, entry: ScheduleEntry) -> None:
        """P1-8b: a fire that RAISED means the schedule is broken -- its cadence
        cannot be computed (e.g. an OverflowError on an oversized interval), or a
        post-dispatch step failed after the task was already sent. Re-firing
        would re-dispatch a duplicate and hot-spin, so DISABLE the schedule
        instead of retrying. The disable is guarded by the live definition
        (RM11), so an operator editing the schedule to something valid is not
        clobbered. Clears any back-off state for the id.
        """
        self._fire_error_counts.pop(entry.id, None)
        self._fire_backoff_until.pop(entry.id, None)
        self._fire_backoff_def.pop(entry.id, None)
        await self._record_local_quarantine(
            entry,
            code="post_dispatch_state_ambiguous",
            detail=f"{entry.kind}:{entry.expression}",
        )

    async def _stop_until_brain_supersedes(
        self,
        entry: ScheduleEntry,
        *,
        disposition: str,
        expected_control_token: UUID,
        refused_at_revision: int,
    ) -> bool:
        """Hold one generation locally and say so where an operator will look.

        A schedule that silently stops ticking is the hardest kind of incident
        to work: the row reads enabled in the dashboard, the Brain shows no
        refusal it kept, and the only evidence lives in this process's memory.
        The log line names both the reason and the Brain revision the stop is
        waiting to see, so an operator can tell "waiting to resync" apart from
        "held until someone acts" without attaching a debugger.
        """

        latched = await self._cache.latch_current_stop(
            entry.id,
            expected_control_token=expected_control_token,
            refused_at_revision=refused_at_revision,
        )
        if not latched:
            return False
        logger.warning(
            "z4j.scheduler.tick: stopped schedule_id=%s locally after "
            "disposition=%s; resumes once Brain state reaches revision %s",
            entry.id,
            disposition,
            refused_at_revision if refused_at_revision > 0 else "(a new control generation)",
        )
        return True

    async def _record_local_quarantine(
        self,
        entry: ScheduleEntry,
        *,
        code: str,
        detail: str,
    ) -> bool:
        """Latch first, then enqueue that exact current generation."""

        latched = await self._cache.quarantine_locally(
            entry.id,
            expected_definition=entry,
            code=code,
            detail=detail,
        )
        if not latched or self._quarantine_reporter is None:
            return latched
        quarantine = await self._cache.local_quarantine(entry.id)
        if quarantine is not None:
            self._quarantine_reporter.enqueue(
                entry=entry,
                quarantine=quarantine,
            )
        return latched

    def _compute_missed_cron_slots(
        self,
        entry: ScheduleEntry,
        *,
        scheduled_for: datetime,
        now: datetime,
    ) -> list[datetime]:
        """Return every cron slot in (last_fire_at, now] -- the FULL backlog.

        B3 fix: the window upper bound is ``now``, NOT ``scheduled_for``
        (the FIRST missed slot). Bounding at ``scheduled_for`` returned a
        one-element list on recovery, so ``plan_catch_up`` could not
        distinguish skip / fire_one_missed / fire_all_missed -- the engine
        advanced one slot, ``_sleep_until_next`` returned immediately (still
        past-due), and ``run()`` re-fired, so EVERY missed slot dispatched
        regardless of the catch-up policy (a fire storm of duplicate,
        possibly non-idempotent side-effects on every restart). Computing
        the whole backlog to ``now`` in one pass lets the planner coalesce
        correctly and the caller advance past the entire backlog at once.

        When ``last_fire_at`` is unknown (fresh schedule, post-restart with
        no brain echo yet), anchor at ``scheduled_for`` so only the current
        slot fires -- without an anchor a brand-new ``fire_all_missed``
        schedule would try to fire from epoch.

        Slots are capped at :data:`cron_mod.fires_between`'s default (10k);
        a 365-day outage of a minute cron is closer to half a million slots
        and would wedge the dispatcher queue. Operators with very long
        outages should manually trim the schedule before re-enabling.
        """
        # A FRESH schedule (no anchor yet -- a promotion / restart before
        # any brain echo) that is PAST-DUE must still materialise the FULL backlog
        # from scheduled_for to now. Returning the single current slot let
        # fire_one_missed fire the oldest slot, advance one step, and re-enter
        # next tick -- re-firing the whole backlog one slot at a time (the B3 storm
        # on a fresh schedule). Anchor the window at scheduled_for (NOT epoch), so
        # a fresh fire_all_missed never fires from epoch: the window is
        # [scheduled_for, now].
        after_anchor = entry.last_fire_at if entry.last_fire_at is not None else scheduled_for
        try:
            slots = cron_mod.fires_between(
                entry.expression,
                entry.timezone,
                after=after_anchor,
                until=now,
            )
        except cron_mod.CronExpressionError:
            # Bad expression - fall back to the single slot rather
            # than raising. The engine's ``_next_fire_for`` has its
            # own error handling for parse failures.
            return [scheduled_for]
        if entry.last_fire_at is None:
            # fires_between is exclusive of ``after``; include scheduled_for itself
            # as the first missed slot of a fresh past-due schedule.
            slots = [scheduled_for, *slots]
        if not slots:
            # Defensive: every well-formed (last_fire_at, now] window for a
            # past-due cron schedule contains at least ``scheduled_for``
            # (the first missed slot). If croniter disagrees (boundary /
            # one-shot-ish expression) still fire the requested slot.
            return [scheduled_for]
        return slots

    def _compute_missed_interval_slots(
        self,
        entry: ScheduleEntry,
        *,
        scheduled_for: datetime,
        now: datetime,
    ) -> list[datetime]:
        """Return every interval slot in (last_fire_at, now] -- the FULL backlog.

        H4: the mirror of :meth:`_compute_missed_cron_slots` for interval
        schedules. Without it interval returned a one-element missed list, so
        plan_catch_up could not distinguish skip / fire_one_missed /
        fire_all_missed, and fire_one_missed re-fired the entire backlog one
        slot per tick (a duplicate, possibly non-idempotent, side-effect
        storm on every restart -- the same failure B3 fixed for cron).

        When ``last_fire_at`` is unknown (fresh schedule) anchor at
        ``scheduled_for`` so only the current slot fires. Slots are capped at
        :func:`interval_mod.fires_between`'s default (10k).
        """
        # Fresh past-due interval materialises the FULL backlog from
        # scheduled_for (see _compute_missed_cron_slots for the rationale).
        after_anchor = entry.last_fire_at if entry.last_fire_at is not None else scheduled_for
        try:
            slots = interval_mod.fires_between(
                entry.expression,
                after=after_anchor,
                until=now,
            )
        except interval_mod.IntervalExpressionError:
            # Bad expression - fall back to the single slot rather than
            # raising; ``_next_fire_for`` has its own parse-error handling.
            return [scheduled_for]
        if entry.last_fire_at is None:
            slots = [scheduled_for, *slots]
        if not slots:
            # Defensive: a past-due interval window contains at least the
            # first missed slot. If the math disagrees (clock skew, a
            # last_fire_at newer than now) still fire the requested slot.
            return [scheduled_for]
        return slots

    def _compute_missed_solar_slots(
        self,
        entry: ScheduleEntry,
        *,
        scheduled_for: datetime,
        now: datetime,
    ) -> list[datetime]:
        """Every solar slot in the missed window -- the FULL backlog.

        The mirror of :meth:`_compute_missed_cron_slots` / interval for solar.
        Without it solar fell into the single-slot else-branch, so
        ``fire_one_missed`` (and skip) re-fired the whole solar backlog one slot
        per tick after a multi-boundary outage (the B3 storm, on solar). Anchored
        at ``scheduled_for`` for a fresh schedule so it never enumerates
        from epoch; bounded at :func:`solar_mod.fires_between`'s 10k cap.
        """
        after_anchor = entry.last_fire_at if entry.last_fire_at is not None else scheduled_for
        try:
            slots = solar_mod.fires_between(
                entry.expression,
                after=after_anchor,
                until=now,
            )
        except Exception:
            # A bad solar expression / missing astral -> fall back to the single
            # slot rather than raising; _next_fire_for handles the parse error.
            return [scheduled_for]
        if entry.last_fire_at is None:
            slots = [scheduled_for, *slots]
        if not slots:
            return [scheduled_for]
        return slots

    # ------------------------------------------------------------------
    # Sleep coordination
    # ------------------------------------------------------------------

    async def _sleep_until_next(self) -> None:  # noqa: PLR0912  wake-time + park + backoff scan
        """Sleep until the next schedule, a cache change, or stop.

        Uses :meth:`asyncio.wait` over the cache's ``changed`` event
        + the engine's ``stop_event`` + a timeout. First wake wins.
        Clears the cache's event after consuming it so subsequent
        mutations re-fire it.
        """
        # Wake at the EARLIEST effective-ready time across enabled schedules.
        # For a schedule in fire back-off, "ready" is its retry deadline, not its
        # (past-due, un-advanced) next_fire_at -- otherwise a backed-off past-due
        # entry would make us return immediately every tick (a busy-spin). This
        # is the same O(n) cost as the cache's next_due() scan plus back-off
        # awareness.
        now = self._clock()
        wake_at: datetime | None = None
        has_parked = False
        for entry in await self._cache.snapshot():
            if not entry.is_enabled or entry.next_fire_at is None:
                continue
            # H8: a parked follower slot is past-due but must NOT
            # force an immediate wake (that is the hot loop); treat it as
            # not-ready.: remember that SOMETHING is parked so the sleep is
            # capped for a prompt post-promotion re-check below.
            if self._is_follower_parked(entry):
                has_parked = True
                continue
            ready = entry.next_fire_at
            backoff = self._fire_backoff_until.get(entry.id)
            if backoff is not None and backoff > ready:
                ready = backoff
            if wake_at is None or ready < wake_at:
                wake_at = ready
        if wake_at is None:
            timeout = self._max_sleep_seconds
        else:
            wait_seconds = (wake_at - now).total_seconds()
            # Clamp: never sleep less than 0 (already ready, exit immediately),
            # never sleep more than the max bound.
            timeout = max(0.0, min(self._max_sleep_seconds, wait_seconds))
        # A promotion is not signalled via cache.changed, so bound the
        # sleep to the re-check interval whenever a parked entry exists -- else a
        # just-promoted follower would wait the full max-sleep before firing its
        # (now-eligible) slot.
        if has_parked and timeout > self._follower_recheck_seconds:
            timeout = self._follower_recheck_seconds

        if timeout <= 0:
            # Already due - return immediately, the next iteration
            # picks it up.
            return

        # Race: cache changed OR stop signalled OR timeout. We don't
        # need the (done, pending) sets - we re-check each event by
        # name on the next iteration.
        change_task = asyncio.create_task(self._cache.changed.wait())
        stop_task = asyncio.create_task(self._stop_event.wait())
        try:
            await asyncio.wait(
                {change_task, stop_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Cancel
            # AND await the loser tasks so the asyncio loop's
            # "Task was destroyed but it is pending" warning
            # doesn't fire under shutdown, and the cache.changed
            # event's waiter list is drained promptly.
            for task in (change_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                change_task,
                stop_task,
                return_exceptions=True,
            )

        # Consume the cache event so subsequent mutations re-trigger it.
        if self._cache.changed.is_set():
            self._cache.changed.clear()


__all__ = ["Dispatcher", "LeaderGate", "TickEngine"]
