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
   is 15 minutes which is well within the worst-case staleness any
   operator should tolerate.
5. Tracks the latest ``resume_token`` so brain can pick up from
   where we left off when the stream protocol supports resume
   (currently brain emits monotonically-increasing tokens; the
   actual resume semantics land brain-side in Phase 1 server work)

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
from typing import TYPE_CHECKING

import grpc

if TYPE_CHECKING:  # pragma: no cover
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
            values are coerced to ``0``.
    """

    def __init__(
        self,
        *,
        client: BrainClient,
        cache: ScheduleCache,
        project_id: UUID | None = None,
        full_resync_interval_seconds: float = (_DEFAULT_FULL_RESYNC_INTERVAL_SECONDS),
    ) -> None:
        self._client = client
        self._cache = cache
        self._project_id = project_id
        self._stop_event = asyncio.Event()
        self._resume_token = ""
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

    async def _watch_loop(self) -> None:
        """Original reconnect-with-backoff watch loop."""
        while not self._stop_event.is_set():
            try:
                await self._sync_then_watch()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Stream dropped, brain unreachable, or some other
                # transient. Mark unhealthy so the tick engine
                # stops firing until reconnect lands.
                self._is_healthy = False
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
            self._is_healthy = False
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
                await self._cache.upsert_many(entries)
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
        logger.info(
            "z4j.scheduler.watch: full sync loaded %d schedule(s)",
            len(entries),
        )

    async def _stream(self) -> None:
        """Process WatchSchedules events until the stream ends."""
        # Stream successfully opened (the iterator is live) - flip
        # to healthy so the tick engine resumes firing. We do this
        # BEFORE consuming the first event because brain may have
        # zero events to send (the cache is up-to-date) and we
        # don't want to wait indefinitely for the first event to
        # mark the stream healthy.
        self._is_healthy = True
        async for event in self._client.watch_schedules(
            self._project_id,
            resume_token=self._resume_token,
        ):
            if self._stop_event.is_set():
                break
            self._resume_token = event.resume_token
            if event.kind == "deleted":
                if event.deleted_id is not None:
                    await self._cache.remove(event.deleted_id)
            elif event.schedule is not None:
                # CREATED + UPDATED both upsert.
                await self._cache.upsert(event.schedule)
            else:
                # Defensive - shouldn't happen given the conversion
                # contract in _convert.event_from_pb.
                logger.warning(
                    "z4j.scheduler.watch: ignoring malformed event %r",
                    event,
                )

    # ------------------------------------------------------------------
    # Backoff
    # ------------------------------------------------------------------

    async def _backoff_or_stop(self) -> None:
        """Exponential backoff with jitter, but wake immediately on stop."""
        # Compute next delay - simple capped doubling with random
        # jitter. Persist between iterations via instance state so a
        # rapid-fire reconnect storm gets progressively longer pauses.
        delay = min(
            _BACKOFF_MAX,
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
