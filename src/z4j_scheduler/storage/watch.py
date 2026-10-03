"""WatchSchedules stream consumer - keeps the cache hot.

Long-lived async task that:

1. Subscribes to brain's ``WatchSchedules`` stream
2. Translates each event to a cache mutation
3. On stream drop, backs off + reconnects + does a full
   :meth:`BrainClient.list_schedules` re-sync to catch any events
   missed during the outage, then resumes ``watch_schedules``
4. Periodically (every ``full_resync_interval_seconds``) does an
   independent full re-sync even when the watch stream is healthy.
   Defensive against silent watch-event loss (a row mutated inside
   a transaction that committed but whose NOTIFY payload was lost
   to a connection blip; brain restarts that orphan a backlog of
   pending events; bugs we haven't found yet). The default cadence
   is 15 minutes and is operator-configurable.
5. Tracks the legacy stream's latest ``resume_token`` across reconnects. The
   current protocol instead resumes from an exact revision cursor.
6. Publishes its own health (``z4j_scheduler_watch_healthy``) and the
   cache's schedule counts (``z4j_scheduler_schedules_loaded``). The
   readiness endpoint and the tick engine read the same health
   transitions, so a stream that has failed for good shows up in all
   three places at once.

Consumer pattern:

    async with asyncio.TaskGroup() as tg:
        tg.create_task(watch.run())
        tg.create_task(engine.run())
        ...

Stop by setting the cancellation token (call :meth:`stop`) - the
loop checks on every iteration.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING, Literal

import grpc

from z4j_scheduler.observability import metrics as m

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.storage.cache import ScheduleCache

logger = logging.getLogger("z4j.scheduler.watch")

#: Backoff bounds for reconnect after a stream drop. We start at
#: 0.5s and double up to ``max`` with jitter.
_BACKOFF_INITIAL = 0.5
_BACKOFF_MAX = 30.0
_BACKOFF_JITTER = 0.3

#: Default cadence for the defensive periodic full re-sync. Matches
#: the spec's recommended 15-minute interval and the Settings default
#: ``reconcile_interval_seconds=900``.
_DEFAULT_FULL_RESYNC_INTERVAL_SECONDS = 900.0

#: ``project`` label value when one stream covers every project the
#: certificate binds (``project_id=None``). Mirrors the ``*`` wildcard the
#: project-scope setting used for the same meaning.
_ALL_PROJECTS_LABEL = "*"


class WatchStream:
    """Async task that keeps a :class:`ScheduleCache` synchronised.

    Args:
        client: An open :class:`BrainClient`.
        cache: The cache to write into.
        project_id: If set, watches only this project. ``None`` (the
            default) watches every project the scheduler is enrolled
            with.
        full_resync_interval_seconds: Cadence for the defensive
            periodic full re-sync that runs in parallel with the
            watch stream. Defaults to 15 minutes. Set to ``0`` to
            disable (only the on-reconnect sync runs). Negative
            values are coerced to ``0``. The same cadence is how
            long the stream must stay healthy before a drop clears
            the reconnect penalty; ``0`` leaves that at the
            15-minute default.
        reconnect_backoff_max_seconds: Maximum delay between stream reconnect
            attempts. Defaults to 30 seconds.
        clock: Monotonic clock used to time how long the stream has been
            unhealthy. Defaults to :func:`time.monotonic`; tests inject a
            fake.
    """

    def __init__(
        self,
        *,
        client: BrainClient,
        cache: ScheduleCache,
        project_id: UUID | None = None,
        full_resync_interval_seconds: float = (_DEFAULT_FULL_RESYNC_INTERVAL_SECONDS),
        reconnect_backoff_max_seconds: float = _BACKOFF_MAX,
        protocol_mode: Literal["legacy", "current"] = "legacy",
        protocol_selector: (Callable[[], Awaitable[Literal["legacy", "current"]]] | None) = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._client = client
        self._cache = cache
        self._project_id = project_id
        self._stop_event = asyncio.Event()
        self._resume_token = ""
        self._protocol_mode = protocol_mode
        self._protocol_selector = protocol_selector
        self._revision_cursor = 0
        # The reconnect-driven sync and the periodic-timer sync race
        # against each other on first connect (both fire at startup).
        # The lock makes whichever wins exclusive so we don't issue
        # two ``list_schedules`` calls in parallel that fight over
        # cache state.
        self._sync_lock = asyncio.Lock()
        self._full_resync_interval_seconds = max(
            0.0,
            full_resync_interval_seconds,
        )
        self._reconnect_backoff_max_seconds = max(
            0.1,
            reconnect_backoff_max_seconds,
        )
        # Expose a ``is_healthy`` flag the tick engine reads on
        # every iteration. If the watch stream drops (network
        # blip, brain restart) the cache holds its last-known
        # state until reconnect+resync; without this gate, an
        # operator who disabled a schedule during the outage
        # would still see the schedule fire because the
        # disable-event was on the wire but never delivered.
        # With the gate: stream-down → engine refuses to fire
        # (catch_up will handle the gap on recovery). False
        # during the backoff/reconnect window; flips True the
        # moment a stream iteration succeeds.
        self._is_healthy = False
        # When the stream last became unhealthy, on ``clock``; ``None``
        # while healthy and before the first failure. The readiness gate
        # compares it against the on-time grace.
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._unhealthy_since: float | None = None
        # When the stream last became healthy, on ``clock``; ``None`` while
        # down. A healthy stretch of one full re-sync interval clears the
        # reconnect penalty (see ``_mark_unhealthy``).
        self._healthy_since: float | None = None
        # Whether the open stream has delivered a frame. A stream the brain
        # refused at its capacity cap, or dropped before its first frame, was
        # healthy for one round trip and never served; ``_mark_unhealthy``
        # reads this to decide whether the outage ended at all.
        self._served_since_open = False
        # The outage start ``_mark_healthy`` cleared, kept until the stream
        # proves itself so a refusal can hand it back.
        self._outage_start_before_open: float | None = None
        # A stream that delivered no frame and dropped sooner than this after
        # opening is treated as refused. A refusal is torn down within one
        # round trip; a stream that outlived the reconnect ceiling has at
        # least outlasted the flap it would otherwise be one cycle of, and an
        # idle brain's stream, which also sends nothing, lives far longer.
        self._unserved_stream_floor_seconds = self._reconnect_backoff_max_seconds
        # ``schedules_loaded`` as last published, keyed by ``(project, kind)``.
        # A full sync rebuilds it from one cache walk; a membership-changing
        # event moves one count, so a ten-thousand-row import streamed as
        # events costs ten thousand dictionary updates, not ten thousand walks.
        self._loaded_counts: dict[tuple[str, str], int] = {}
        # The penalty clears only after the stream has stayed up this long.
        # It follows the full re-sync cadence (``reconcile_interval_seconds``):
        # a stream that outlived a whole re-sync cycle has proven the brain is
        # serving it, which a stream rejected at the capacity cap never does.
        # A disabled periodic re-sync (``0``) falls back to the default cadence
        # rather than clearing the penalty on every reconnect.
        self._backoff_reset_after_seconds: float = (
            self._full_resync_interval_seconds
            if self._full_resync_interval_seconds > 0
            else _DEFAULT_FULL_RESYNC_INTERVAL_SECONDS
        )
        self._project_label = str(project_id) if project_id is not None else _ALL_PROJECTS_LABEL
        m.watch_healthy.labels(project=self._project_label).set(0.0)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the watch + periodic-resync loops until stop is called.

        Spawns a sibling task for the periodic full re-sync timer so
        the defensive sweep keeps running even when the watch stream
        is healthy. The two share a sync lock so they never overlap
        a ``list_schedules`` call.
        """
        logger.info(
            "z4j.scheduler.watch: stream consumer starting "
            "(project_id=%s, full_resync_interval=%.0fs)",
            self._project_id,
            self._full_resync_interval_seconds,
        )
        try:
            tasks = [asyncio.create_task(self._watch_loop())]
            if self._full_resync_interval_seconds > 0:
                tasks.append(
                    asyncio.create_task(self._periodic_resync_loop()),
                )
            try:
                # Block until ``stop()`` flips the event.
                await self._stop_event.wait()
            finally:
                for t in tasks:
                    t.cancel()
                # Drain. ``return_exceptions=True`` so a cancellation
                # mid-await doesn't bubble out of teardown.
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            logger.info("z4j.scheduler.watch: stream consumer stopped")

    @property
    def is_healthy(self) -> bool:
        """True iff the live stream has connected at least once and
        is not currently in the backoff/reconnect window.

        The tick engine consults this flag and refuses to
        dispatch when False. Bounds the "stale cache" exposure
        during a stream drop to "wait until next reconnect"
        instead of "fire whatever the cache last saw, possibly
        minutes old."
        """
        return self._is_healthy

    @property
    def project_label(self) -> str:
        """``project`` label value this stream publishes under."""
        return self._project_label

    def unhealthy_for_seconds(self) -> float | None:
        """Seconds the stream has been continuously unhealthy.

        ``None`` while healthy, and before the first failure: a stream that
        is still performing its initial sync has not failed, and readiness
        already waits on ``cache_initial_sync`` for that window.
        """
        if self._unhealthy_since is None:
            return None
        return max(0.0, self._clock() - self._unhealthy_since)

    def _mark_healthy(self) -> None:
        """Stream opened: the cache is live again.

        The outage clock is cleared here, not at the first frame, so an idle
        brain, which opens the stream and then has nothing to send, reads as
        healthy. Its start is kept aside: a stream refused or dropped before
        its first frame hands it back in ``_mark_unhealthy``, because that
        outage never ended.
        """
        if not self._is_healthy:
            logger.info(
                "z4j.scheduler.watch: stream healthy (project=%s)",
                self._project_label,
            )
            self._healthy_since = self._clock()
            self._outage_start_before_open = self._unhealthy_since
            self._served_since_open = False
        self._is_healthy = True
        self._unhealthy_since = None
        m.watch_healthy.labels(project=self._project_label).set(1.0)

    def _mark_served(self) -> None:
        """First frame of the open stream: the brain is serving it.

        From here a drop is a new outage, whatever the stream's age.
        """
        if not self._served_since_open:
            self._served_since_open = True
            self._outage_start_before_open = None

    def _mark_unhealthy(self) -> None:
        """Stream down: the engine stops dispatching and the outage clock runs.

        The timestamp is kept across repeated reconnect failures so the
        readiness gate sees one continuous outage, not a series of fresh
        ones that each restart the grace. A stream that opened after a
        successful full sync and was then refused, or dropped before its
        first frame, is the same outage still running: the sync proved the
        brain reachable, not the Watch served, and ``_mark_healthy`` had
        cleared the clock. The start it kept aside is restored, so a brain
        that lists schedules but refuses every Watch trips ``/ready`` after
        the grace instead of restarting the clock on every reconnect.

        A stream that stayed healthy for a full re-sync interval before this
        drop has earned a clean slate: the reconnect penalty resets so the
        next attempt is fast. A shorter stretch keeps it, so a flapping
        stream, or a brain shedding load at its capacity cap, still sees the
        delay grow.
        """
        if self._is_healthy and self._healthy_since is not None:
            healthy_for = self._clock() - self._healthy_since
            cleared = healthy_for >= self._backoff_reset_after_seconds
            if cleared and self._reconnect_attempts:
                logger.info(
                    "z4j.scheduler.watch: healthy for %.0fs, "
                    "reconnect penalty cleared (project=%s)",
                    healthy_for,
                    self._project_label,
                )
                self._reconnect_attempts = 0
            if (
                not self._served_since_open
                and self._outage_start_before_open is not None
                and healthy_for < self._unserved_stream_floor_seconds
            ):
                logger.info(
                    "z4j.scheduler.watch: stream dropped after %.1fs without a "
                    "frame; the outage that began %.0fs ago continues (project=%s)",
                    healthy_for,
                    self._clock() - self._outage_start_before_open,
                    self._project_label,
                )
                self._unhealthy_since = self._outage_start_before_open
        self._healthy_since = None
        self._outage_start_before_open = None
        if self._unhealthy_since is None:
            self._unhealthy_since = self._clock()
        self._is_healthy = False
        m.watch_healthy.labels(project=self._project_label).set(0.0)

    async def _watch_loop(self) -> None:
        """Original reconnect-with-backoff watch loop."""
        first_attempt = True
        while not self._stop_event.is_set():
            if not first_attempt:
                m.watch_stream_reconnects_total.inc()
            first_attempt = False
            try:
                await self._sync_then_watch()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Stream dropped, brain unreachable, or some other
                # transient. Mark unhealthy so the tick engine
                # stops firing until reconnect lands.
                self._mark_unhealthy()
                logger.exception(
                    "z4j.scheduler.watch: stream loop error; backing off + reconnecting",
                )
                await self._backoff_or_stop()
                continue
            # Clean stream end (no exception). Brain closed the
            # stream gracefully or there were no events to process.
            # Always backoff before reconnecting so:
            # 1. A brain that's restarting doesn't see a tight
            #    reconnect loop
            # 2. The loop yields control to the asyncio scheduler
            #    (an empty stream returns immediately and would
            #    spin without checking stop_event otherwise)
            self._mark_unhealthy()
            await self._backoff_or_stop()

    async def _periodic_resync_loop(self) -> None:
        """Independent timer that triggers the defensive full re-sync.

        Runs in parallel with the watch loop. The first iteration
        sleeps the full interval (the watch loop's startup sync
        already covers the boot case) and then keeps firing on a
        fixed cadence until stopped.

        Failures here log + retry on the next tick - we never let a
        transient brain blip kill the timer entirely, because doing
        so would silently disable the whole defensive layer.
        """
        interval = self._full_resync_interval_seconds
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval,
                )
                # ``wait_for`` returned cleanly only if stop fired -
                # exit the loop.
                return
            except TimeoutError:
                pass
            if self._stop_event.is_set():
                return
            try:
                await self._full_sync()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Defensive: a single failed defensive sweep is
                # logged and we wait for the next tick. Never let
                # this loop die.
                logger.warning(
                    "z4j.scheduler.watch: periodic full re-sync failed; will retry on next tick",
                    exc_info=True,
                )

    async def stop(self) -> None:
        """Signal the loop to exit on its next iteration. Idempotent."""
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    async def _sync_then_watch(self) -> None:
        """One full cycle: list-sync, then watch until disconnect.

        After the full sync, the resume token is forwarded to
        ``now()`` so the subsequent WatchSchedules stream does
        NOT replay events older than ``sync_started_at``.
        Otherwise every reconnect would produce 2x delivery of
        every event in the (resume_token, sync_done) window
        because the stream's catch-up replays the same rows the
        full sync just landed. The cache's ``upsert`` is
        idempotent so behavior would still be correct, but the
        duplicate I/O would triple load on a flapping connection.

        We capture ``sync_started_at`` BEFORE the sync starts so
        any event committed during the sync window is still
        delivered by the LISTEN stream after stream-start (no
        events lost). Anything strictly older than
        ``sync_started_at`` was definitely covered by the sync's
        full snapshot.
        """
        await self._refresh_protocol_mode()
        if self._protocol_mode == "current":
            await self._full_sync()
            await self._stream()
            return

        from datetime import UTC, datetime

        sync_started_at_iso = datetime.now(UTC).isoformat()
        await self._full_sync()
        # Advance resume_token past the sync window. Use an
        # ``or`` guard so we never DOWN-grade a token the stream
        # was already past (paranoia - shouldn't happen because
        # we only get here on reconnect, but cheap to assert).
        if not self._resume_token or self._resume_token < sync_started_at_iso:
            self._resume_token = sync_started_at_iso

        # Now subscribe to the live stream until something breaks.
        await self._stream()

    async def _refresh_protocol_mode(self) -> None:
        """Re-negotiate on every stream connection and reject mode changes."""

        if self._protocol_selector is None:
            return
        selected = await self._protocol_selector()
        if selected != self._protocol_mode:
            from z4j_scheduler.storage._protocol import (
                ProtocolNegotiationError,
            )

            raise ProtocolNegotiationError(
                "Brain protocol mode changed across reconnect; restart the "
                "scheduler to rebuild mode-specific subsystems",
            )

    async def _full_sync(self) -> None:
        """Fetch every schedule from brain and reconcile the cache.

        Two-pass:

        1. Upsert every schedule the brain returns. New rows land in
           cache; existing rows are refreshed.
        2. Sweep deletes - any cache id that the brain didn't return
           is removed. Catches DELETED events that were missed during
           a stream outage or while the watch reconnect was racing.

        The sweep set is computed relative to a snapshot taken
        BEFORE the list_schedules call starts, not the current
        cache. Otherwise a brand-new schedule that the live
        ``_stream`` upserted DURING the list_schedules read
        window would be in the post-sync snapshot but NOT in
        ``fresh_ids`` (because list_schedules returned before
        brain wrote that row), and the sweep would evict it. The
        next periodic full-resync (15 min) would rediscover it -
        until then the schedule would be invisible to the
        scheduler. Sweeping only ids that already existed before
        the sync started leaves concurrent landings alone.

        Serialised behind ``_sync_lock`` so a periodic-timer sync
        racing the on-reconnect sync can't issue overlapping
        ``list_schedules`` calls and clobber each other's state.
        """
        if self._protocol_mode == "current":
            await self._full_sync_current()
            return

        async with self._sync_lock:
            # Snapshot the cache BEFORE list_schedules so we know
            # which ids were live at sync-start. Any id the live
            # stream adds during the list call is intentionally
            # excluded from the sweep candidate set.
            pre_sync_ids = {e.id for e in await self._cache.snapshot()}
            entries = []
            async for entry in self._client.list_schedules(self._project_id):
                entries.append(entry)
            if entries:
                # Echo-safe apply -- a full-sync re-read must not
                # clobber the engine's authoritative fire-state on same-cadence
                # rows (brain is authoritative for the DEFINITION, the engine for
                # fire-state). New rows and real cadence edits still replace.
                await self._cache.apply_watch_updates(entries)
            # Sweep deletes. Only consider ids that were present
            # BEFORE the sync started and that brain did NOT
            # return. Concurrently-added ids (from _stream events
            # during the sync window) are left in the cache.
            fresh_ids = {e.id for e in entries}
            stale_ids = [sid for sid in pre_sync_ids if sid not in fresh_ids]
            for sid in stale_ids:
                await self._cache.remove(sid)
            if stale_ids:
                logger.info(
                    "z4j.scheduler.watch: full sync swept %d stale schedule(s) from cache",
                    len(stale_ids),
                )
        await self._publish_schedule_counts()
        logger.info(
            "z4j.scheduler.watch: full sync loaded %d schedule(s)",
            len(entries),
        )

    async def _full_sync_current(self) -> None:
        """Atomically install one validated current-protocol snapshot."""

        async with self._sync_lock:
            snapshot = await self._client.list_schedule_snapshot(self._project_id)
            await self._cache.apply_completed_snapshot(snapshot)
            self._revision_cursor = max(
                self._revision_cursor,
                snapshot.watermark,
            )
        await self._publish_schedule_counts()
        logger.info(
            "z4j.scheduler.watch: current snapshot installed at revision %d",
            snapshot.watermark,
        )

    async def _count_membership(self, schedule_id: UUID | None) -> tuple[str, str] | None:
        """The ``(project, kind)`` series a cached schedule counts under.

        ``None`` when the id is absent from the cache. Compared before and
        after an event lands, so a fire-ack echo, which keeps both, costs two
        O(1) lookups and no recount.
        """
        if schedule_id is None:
            return None
        entry = await self._cache.get(schedule_id)
        if entry is None:
            return None
        return (str(entry.project_id), entry.kind)

    async def _publish_schedule_counts(self) -> None:
        """Publish ``schedules_loaded`` from what the cache actually holds.

        The cache indexes counts per project only and the gauge is declared
        per ``(project, kind)``, so this walks one snapshot. It runs after
        every full sync and nowhere else: events move the walked counts one
        at a time through :meth:`_move_schedule_count`, and the next full
        sync rebuilds them from the cache again.
        """
        counts: dict[tuple[str, str], int] = {}
        for entry in await self._cache.snapshot():
            key = (str(entry.project_id), entry.kind)
            counts[key] = counts.get(key, 0) + 1
        self._loaded_counts = counts
        m.set_schedules_loaded(counts)

    def _move_schedule_count(
        self,
        before: tuple[str, str] | None,
        after: tuple[str, str] | None,
    ) -> None:
        """Move one schedule between ``schedules_loaded`` series and publish.

        ``before`` and ``after`` are the event subject's ``(project, kind)``
        membership on either side of the cache mutation, ``None`` for absent.
        Equal memberships (a fire-ack echo, a checkpoint) publish nothing. A
        walk here cost 5 ms at ten thousand cached schedules, which made a
        ten-thousand-row import streamed as events a minute of watch-task
        CPU; moving one count is constant. A change this path cannot see (a
        tombstone sweep inside a snapshot install) is corrected by the next
        full sync, within one reconcile interval.
        """
        if before == after:
            return
        if before is not None:
            remaining = self._loaded_counts.get(before, 0) - 1
            if remaining > 0:
                self._loaded_counts[before] = remaining
            else:
                self._loaded_counts.pop(before, None)
        if after is not None:
            self._loaded_counts[after] = self._loaded_counts.get(after, 0) + 1
        m.set_schedules_loaded(self._loaded_counts)

    async def _stream(self) -> None:
        """Process WatchSchedules events until the stream ends."""
        if self._protocol_mode == "current":
            await self._stream_current()
            return
        # Stream successfully opened (the iterator is live) - flip
        # to healthy so the tick engine resumes firing. We do this
        # BEFORE consuming the first event because brain may have
        # zero events to send (the cache is up-to-date) and we
        # don't want to wait indefinitely for the first event to
        # mark the stream healthy.
        self._mark_healthy()
        async for event in self._client.watch_schedules(
            self._project_id,
            resume_token=self._resume_token,
        ):
            if self._stop_event.is_set():
                break
            self._mark_served()
            self._resume_token = event.resume_token
            subject = event.schedule.id if event.schedule is not None else event.deleted_id
            before = await self._count_membership(subject)
            if event.kind == "deleted":
                if event.deleted_id is not None:
                    await self._cache.remove(event.deleted_id)
            elif event.schedule is not None:
                # CREATED + UPDATED.: apply echo-safely so a benign
                # fire-ack echo (same cadence, advanced last_run_at) does not
                # overwrite the engine's computed next_fire_at/anchor_at and
                # drift the cadence. A real cadence edit still replaces wholesale.
                await self._cache.apply_watch_update(event.schedule)
            else:
                # Defensive - shouldn't happen given the conversion
                # contract in _convert.event_from_pb.
                logger.warning(
                    "z4j.scheduler.watch: ignoring malformed event %r",
                    event,
                )
            self._move_schedule_count(before, await self._count_membership(subject))

    async def _stream_current(self) -> None:
        """Apply V2 events/checkpoints strictly after the snapshot cursor."""

        from z4j_scheduler.storage._models import ScheduleChange
        from z4j_scheduler.storage._watch_v2 import OrderedWatchApplier

        applier = OrderedWatchApplier(
            cache=self._cache,
            project_id=self._project_id,
            after_revision=self._revision_cursor,
        )
        self._mark_healthy()
        async for frame in self._client.watch_schedules_v2(
            self._project_id,
            after_revision=applier.cursor,
        ):
            if self._stop_event.is_set():
                break
            self._mark_served()
            subject: UUID | None = None
            if isinstance(frame, ScheduleChange):
                subject = frame.schedule.id if frame.schedule is not None else frame.deleted_id
            before = await self._count_membership(subject)
            await applier.apply(frame)
            # A periodic snapshot may have advanced the shared cursor farther
            # while this stream was live; never move it backwards.
            self._revision_cursor = max(self._revision_cursor, applier.cursor)
            self._move_schedule_count(before, await self._count_membership(subject))

    # ------------------------------------------------------------------
    # Backoff
    # ------------------------------------------------------------------

    # The reconnect penalty escalates across drops and clears only after the
    # stream has stayed healthy for one full re-sync interval
    # (``reconcile_interval_seconds``); ``_mark_unhealthy`` applies the reset
    # at the moment the length of the healthy stretch is known.
    #
    # The placement matters. Clearing before the ``async for`` cleared before
    # the RPC had started: the brain rejects Watch at a per-certificate and a
    # global capacity cap, that rejection arrives before any frame, and the
    # scheduler re-synced two or three times a second against a brain that
    # was reachable and deliberately shedding load. Clearing inside the loop
    # body never fired on an idle brain, which yields no frames. Measuring the
    # healthy stretch on the clock answers both: a rejected stream is healthy
    # for milliseconds and keeps its penalty, an idle stream still ages, and a
    # stream that outlived a whole re-sync cycle has proven the brain is
    # serving it. ``tests/unit/test_reconnect_backoff_resets.py`` pins the
    # flap, the healthy stretch and the fast first reconnect.

    async def _backoff_or_stop(self) -> None:
        """Exponential backoff with jitter, but wake immediately on stop."""
        # Compute next delay - simple capped doubling with random
        # jitter. Persist between iterations via instance state so a
        # rapid-fire reconnect storm gets progressively longer pauses.
        delay = min(
            self._reconnect_backoff_max_seconds,
            _BACKOFF_INITIAL * (2 ** min(self._reconnect_attempts, 6)),
        )
        delay *= 1.0 + random.uniform(-_BACKOFF_JITTER, _BACKOFF_JITTER)  # noqa: S311 - jitter, not crypto
        delay = max(0.1, delay)
        self._reconnect_attempts += 1

        logger.info(
            "z4j.scheduler.watch: reconnecting in %.2fs (attempt %d)",
            delay,
            self._reconnect_attempts,
        )
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except TimeoutError:
            # Backoff completed normally - try again.
            return

    # Track reconnect attempts on the instance so the backoff grows
    # across iterations of the outer ``while`` loop.
    _reconnect_attempts: int = 0


# Suppress unused-import warning - grpc is imported for the
# StatusCode/AioRpcError types the test path may want to reference,
# even if the current implementation only relies on the catch-all
# Exception handler in _sync_then_watch.
_ = grpc

__all__ = ["WatchStream"]
