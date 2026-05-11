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
  responds within ~100ms of any schedule change.
- **Catch-up is honest.** When a schedule's ``next_fire_at`` is in
  the past (after a brain-down outage, or a freshly-added
  schedule), the catch-up policy decides whether to fire 0, 1, or
  N times. The engine never silently swallows a missed fire.
- **Past one-shots dispatch immediately, then auto-disable.** A
  one-shot whose target time has already passed when added to the
  cache fires once at its scheduled time, gets ``last_fire_at`` set,
  and never fires again - the brain-side handler flips
  ``is_enabled=False`` after the result acknowledgement.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from z4j_scheduler.tick import cron as cron_mod
from z4j_scheduler.tick import interval as interval_mod
from z4j_scheduler.tick import one_shot as one_shot_mod
from z4j_scheduler.tick import solar as solar_mod
from z4j_scheduler.tick.catch_up import plan_catch_up

if TYPE_CHECKING:
    from collections.abc import Callable

    from z4j_scheduler.storage.cache import ScheduleCache
    from z4j_scheduler.tick._entry import ScheduleEntry

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
    ) -> None: ...


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
        watch_healthy: Callable[[], bool] | None = None,
    ) -> None:
        self._cache = cache
        self._leader_gate = leader_gate
        self._dispatcher = dispatcher
        self._clock = clock
        self._max_sleep_seconds = max_sleep_seconds
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
        # the disable here; the async tick iteration drains the set
        # via ``cache.update_fire_state(is_enabled=False)`` which
        # serialises against ``cache.upsert``.
        self._pending_disables: set[UUID] = set()

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
        try:
            while not self._stop_event.is_set():
                await self._iteration()
        except asyncio.CancelledError:
            logger.info("z4j.scheduler.tick: engine cancelled")
            raise
        finally:
            logger.info("z4j.scheduler.tick: engine stopped")

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
                "z4j.scheduler.tick: watch stream unhealthy; "
                "skipping dispatch this iteration",
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
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_UNHEALTHY_SLEEP_SECONDS,
                )
            except (asyncio.TimeoutError, TimeoutError):
                pass
            return
        now = self._clock()
        due = await self._cache.all_due(before=now)
        if due:
            for entry in due:
                if entry.id in self._in_flight:
                    continue
                self._in_flight.add(entry.id)
                try:
                    await self._fire_with_catch_up(entry, now=now)
                finally:
                    self._in_flight.discard(entry.id)

        # Step 3: sleep until the next schedule OR a cache change OR
        # stop signal.
        await self._sleep_until_next()

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
        for entry in await self._cache.snapshot():
            if entry.next_fire_at is None and entry.is_enabled:
                next_at = self._next_fire_for(entry)
                if next_at is not None:
                    await self._cache.update_fire_state(
                        entry.id, next_fire_at=next_at,
                    )
        # Apply any disables queued by ``_next_fire_for``. Pop into
        # a local list so a concurrent producer can keep adding while
        # we drain the snapshot.
        if self._pending_disables:
            to_disable = list(self._pending_disables)
            self._pending_disables.clear()
            for sched_id in to_disable:
                await self._cache.update_fire_state(
                    sched_id, is_enabled=False,
                )

    def _next_fire_for(  # noqa: PLR0911 - per-kind dispatch is idiomatic
        self,
        entry: ScheduleEntry,
        *,
        as_of_last_fire_at: datetime | None | object = _LAST_FIRE_AT_DEFAULT,
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
                    entry.expression, entry.timezone, after,
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
                self._pending_disables.add(entry.id)
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
                self._pending_disables.add(entry.id)
                return None
        if entry.kind in ("clocked", "one_shot"):
            try:
                return one_shot_mod.next_fire(
                    entry.expression, last_fire_at=last,
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
                self._pending_disables.add(entry.id)
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
                    entry.expression, after=anchor,
                )
            except (ValueError, RuntimeError):
                logger.exception(
                    "z4j.scheduler.tick: invalid solar expression for "
                    "schedule_id=%s; disabling locally",
                    entry.id,
                )
                self._pending_disables.add(entry.id)
                return None
        # Unknown kind - log + queue the disable. Same R7-MED race
        # fix as above: route through the pending-set so a concurrent
        # WatchStream upsert can't drop the disable.
        logger.error(
            "z4j.scheduler.tick: unknown schedule kind %r for schedule_id=%s",
            entry.kind, entry.id,
        )
        self._pending_disables.add(entry.id)
        return None

    # ------------------------------------------------------------------
    # Dispatch + catch-up
    # ------------------------------------------------------------------

    async def _fire_with_catch_up(
        self,
        entry: ScheduleEntry,
        *,
        now: datetime,
    ) -> None:
        """Resolve the catch-up plan for a due entry and dispatch."""
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
            return

        scheduled_for = entry.next_fire_at
        if scheduled_for is None:
            return

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
        if entry.kind in ("clocked", "one_shot"):
            plan = [scheduled_for]
        elif lateness_seconds <= _ON_TIME_GRACE_SECONDS:
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
            # Interval / one_shot / solar schedules don't have a
            # well-defined "list of missed slots" the way cron
            # does; fall back to the single-slot behavior for
            # them. fire_all_missed on those kinds remains a
            # documented degraded mode.
            if entry.kind == "cron":
                missed_times = self._compute_missed_cron_slots(
                    entry, scheduled_for=scheduled_for,
                )
            else:
                missed_times = [scheduled_for]
            plan = plan_catch_up(
                entry.catch_up,
                missed_times=missed_times,
                now=now,
            )

        for moment in plan:
            try:
                await self._dispatcher.dispatch(
                    schedule_id=entry.id,
                    scheduled_for=moment,
                    schedule_name=entry.name,
                )
            except Exception:
                logger.exception(
                    "z4j.scheduler.tick: dispatcher raised for "
                    "schedule_id=%s scheduled_for=%s; will retry on next tick",
                    entry.id, moment,
                )
                # Do NOT advance - we want to retry on next tick. The
                # dispatcher itself owns the retry contract; a raise
                # here means the dispatcher gave up.
                return

        # Advance: stamp last_fire_at and recompute next_fire_at.
        # Even when the catch-up plan was empty (skip on a missed
        # fire), we still advance past the missed slot - otherwise
        # we'd re-evaluate the same scheduled_for every iteration.
        await self._advance_after_fire(entry, last_fire_at=scheduled_for)

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
        from z4j_scheduler.storage.cache import _UNSET as _CACHE_UNSET  # noqa: PLC0415

        if last_fire_at is None:
            # Non-leader path: anchor on entry's CURRENT
            # last_fire_at (legacy behavior - we do not advance).
            next_at = self._next_fire_for(entry)
            await self._cache.update_fire_state(
                entry.id,
                last_fire_at=_CACHE_UNSET,
                next_fire_at=next_at,
            )
        else:
            # Leader path: anchor on the slot we just fired so the
            # next-fire computation walks forward from there.
            next_at = self._next_fire_for(
                entry, as_of_last_fire_at=last_fire_at,
            )
            await self._cache.update_fire_state(
                entry.id,
                last_fire_at=last_fire_at,
                next_fire_at=next_at,
            )

    def _compute_missed_cron_slots(
        self,
        entry: ScheduleEntry,
        *,
        scheduled_for: datetime,
    ) -> list[datetime]:
        """Return every cron slot in (last_fire_at, scheduled_for].

        Audit fix (Apr 2026 follow-up) for the ``fire_all_missed``
        silent-contract violation. When ``last_fire_at`` is unknown
        (fresh schedule, post-restart with no brain echo yet), we
        anchor at ``scheduled_for`` itself so only the current slot
        fires. Without an anchor we'd have no defensible upper
        bound on "how far back to walk" and a brand-new schedule
        with ``fire_all_missed`` would attempt to fire from epoch.

        Slots are capped at :data:`cron_mod.fires_between`'s
        default (10k); a 365-day outage of a minute cron is closer
        to half a million slots and would wedge the dispatcher
        queue. Operators with very long outages should manually
        trim the schedule before re-enabling.
        """
        if entry.last_fire_at is None:
            return [scheduled_for]
        try:
            slots = cron_mod.fires_between(
                entry.expression,
                entry.timezone,
                after=entry.last_fire_at,
                until=scheduled_for,
            )
        except cron_mod.CronExpressionError:
            # Bad expression - fall back to the single slot rather
            # than raising. The engine's ``_next_fire_for`` has its
            # own error handling for parse failures.
            return [scheduled_for]
        if not slots:
            # Defensive: every well-formed (last_fire_at,
            # scheduled_for] window for an enabled cron schedule
            # contains at least the current scheduled_for. If
            # croniter disagrees (e.g. expression is one-shot-ish
            # and the slot already passed), ensure we still fire
            # the requested slot.
            return [scheduled_for]
        # Always include scheduled_for as the trailing slot. If
        # ``fires_between`` already returned it (boundary cases) the
        # de-dup keeps the list strictly increasing.
        #
        # Compare in UTC.
        # During DST fall-back (Nov first-Sunday in US/Eastern, the
        # 1am-2am window exists twice, once at fold=0 in DST, once
        # at fold=1 standard). ``fires_between`` returns both
        # ambiguous slots; ``scheduled_for`` is one of them. The
        # naive ``slots[-1] != scheduled_for`` compares wall-clock
        # equality and reports True for the fold-different twin,
        # silently de-duping a legitimate second fire that would
        # otherwise be the second of the two ambiguous slots.
        # Comparing in UTC distinguishes the folds correctly because
        # they have distinct UTC offsets.
        scheduled_utc = scheduled_for.astimezone(UTC)
        last_slot_utc = slots[-1].astimezone(UTC)
        if last_slot_utc != scheduled_utc:
            slots.append(scheduled_for)
        return slots

    # ------------------------------------------------------------------
    # Sleep coordination
    # ------------------------------------------------------------------

    async def _sleep_until_next(self) -> None:
        """Sleep until the next schedule, a cache change, or stop.

        Uses :meth:`asyncio.wait` over the cache's ``changed`` event
        + the engine's ``stop_event`` + a timeout. First wake wins.
        Clears the cache's event after consuming it so subsequent
        mutations re-fire it.
        """
        next_entry = await self._cache.next_due()
        if next_entry is None or next_entry.next_fire_at is None:
            timeout = self._max_sleep_seconds
        else:
            wait_seconds = (
                next_entry.next_fire_at - self._clock()
            ).total_seconds()
            # Clamp: never sleep less than 0 (already past due, exit
            # immediately), never sleep more than the max bound.
            timeout = max(0.0, min(self._max_sleep_seconds, wait_seconds))

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
                change_task, stop_task, return_exceptions=True,
            )

        # Consume the cache event so subsequent mutations re-trigger it.
        if self._cache.changed.is_set():
            self._cache.changed.clear()


__all__ = ["Dispatcher", "LeaderGate", "TickEngine"]
