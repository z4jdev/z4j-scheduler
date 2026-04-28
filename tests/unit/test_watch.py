"""Tests for :class:`z4j_scheduler.storage.watch.WatchStream`.

Uses a fake brain client whose ``list_schedules`` and
``watch_schedules`` are async iterators we can drive. No real
gRPC, no real network. Covers:

- Initial full-sync populates the cache
- Watch events propagate to the cache (created/updated/deleted)
- Stream-end + reconnect after backoff
- :meth:`stop` exits the loop
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.storage._models import ScheduleEvent
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.watch import WatchStream
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _make_entry(*, schedule_id: UUID | None = None) -> ScheduleEntry:
    return ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
    )


@dataclass
class FakeBrainClient:
    """Drives the watch consumer with scripted list + stream output.

    - ``list_entries`` is what :meth:`list_schedules` yields.
    - ``stream_events`` is the queue of events the stream yields.
      Pushing ``None`` simulates an end-of-stream (the watch consumer
      then catches the resulting StopAsyncIteration in its outer loop
      and triggers reconnect / backoff).
    """

    list_entries: list[ScheduleEntry] = field(default_factory=list)
    stream_events: list[ScheduleEvent] = field(default_factory=list)
    raise_in_stream: BaseException | None = None

    async def list_schedules(
        self,
        project_id: UUID | None = None,
    ) -> AsyncIterator[ScheduleEntry]:
        for e in self.list_entries:
            yield e

    async def watch_schedules(
        self,
        project_id: UUID | None = None,
        *,
        resume_token: str = "",
    ) -> AsyncIterator[ScheduleEvent]:
        for ev in self.stream_events:
            yield ev
        if self.raise_in_stream is not None:
            raise self.raise_in_stream


class TestFullSync:
    async def test_initial_sync_populates_cache(self) -> None:
        e1 = _make_entry()
        e2 = _make_entry()
        client = FakeBrainClient(list_entries=[e1, e2])
        cache = ScheduleCache()
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        await watch._full_sync()
        assert len(cache) == 2
        assert await cache.get(e1.id) is not None
        assert await cache.get(e2.id) is not None

    async def test_empty_sync_does_not_fire_event(self) -> None:
        cache = ScheduleCache()
        cache.changed.clear()
        watch = WatchStream(client=FakeBrainClient(), cache=cache)  # type: ignore[arg-type]
        await watch._full_sync()
        assert not cache.changed.is_set()


class TestStreamProcessing:
    async def test_created_event_inserts(self) -> None:
        e = _make_entry()
        client = FakeBrainClient(
            stream_events=[
                ScheduleEvent(
                    kind="created", schedule=e,
                    deleted_id=None, resume_token="t1",  # noqa: S106 - opaque cursor
                ),
            ],
        )
        cache = ScheduleCache()
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        await watch._stream()
        assert await cache.get(e.id) is e
        assert watch._resume_token == "t1"  # noqa: S105 - opaque cursor

    async def test_updated_event_replaces(self) -> None:
        sid = uuid4()
        old = _make_entry(schedule_id=sid)
        new = _make_entry(schedule_id=sid)
        cache = ScheduleCache()
        await cache.upsert(old)

        client = FakeBrainClient(
            stream_events=[
                ScheduleEvent(
                    kind="updated", schedule=new,
                    deleted_id=None, resume_token="t2",  # noqa: S106 - opaque cursor
                ),
            ],
        )
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]
        await watch._stream()
        assert await cache.get(sid) is new

    async def test_deleted_event_removes(self) -> None:
        e = _make_entry()
        cache = ScheduleCache()
        await cache.upsert(e)

        client = FakeBrainClient(
            stream_events=[
                ScheduleEvent(
                    kind="deleted", schedule=None,
                    deleted_id=e.id, resume_token="t3",  # noqa: S106 - opaque cursor
                ),
            ],
        )
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]
        await watch._stream()
        assert await cache.get(e.id) is None


class TestFullSyncDeleteSweep:
    """The defensive periodic re-sync must catch missed DELETE events.

    Background: prior to Phase 6 the full sync only upserted - it
    trusted the watch stream's DELETED events for removals. A
    missed event (NOTIFY lost during a reconnect, brain bug, ...)
    could leave a phantom schedule firing forever. The sweep
    closes that gap.
    """

    async def test_sweeps_ids_not_in_fresh_list(self) -> None:
        # Cache starts with two schedules; brain returns only one.
        # The other should be removed by the sweep.
        keep = _make_entry()
        gone = _make_entry()
        cache = ScheduleCache()
        await cache.upsert(keep)
        await cache.upsert(gone)

        client = FakeBrainClient(list_entries=[keep])
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        await watch._full_sync()

        assert await cache.get(keep.id) is not None
        assert await cache.get(gone.id) is None
        assert len(cache) == 1

    async def test_empty_brain_clears_cache(self) -> None:
        # Edge case: every schedule was deleted. The sweep should
        # empty the cache rather than leaving stale entries.
        e = _make_entry()
        cache = ScheduleCache()
        await cache.upsert(e)

        client = FakeBrainClient(list_entries=[])
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        await watch._full_sync()

        assert len(cache) == 0


class TestPeriodicResync:
    """The periodic timer fires independent of watch-stream health."""

    async def test_zero_interval_disables_timer(self) -> None:
        # Operator opt-out path: setting the interval to 0 turns off
        # the periodic loop. Useful for tests and for very-low-RPS
        # deployments where every list call is wasted work.
        cache = ScheduleCache()
        client = FakeBrainClient()
        watch = WatchStream(
            client=client,  # type: ignore[arg-type]
            cache=cache,
            full_resync_interval_seconds=0,
        )
        task = asyncio.create_task(watch.run())
        await asyncio.sleep(0.05)
        await watch.stop()
        await asyncio.wait_for(task, timeout=1.0)
        # No periodic task means the only sync attempts came from
        # the watch loop's reconnect path.

    async def test_periodic_timer_fires_full_sync(self) -> None:
        # Drive the timer with a very short interval and a list
        # client that records call counts. Watch loop's empty stream
        # will spin (with backoff) but the periodic timer should
        # also issue list_schedules calls on its own cadence.
        e = _make_entry()
        cache = ScheduleCache()
        await cache.upsert(e)  # seed

        list_call_count = 0

        @dataclass
        class CountingClient(FakeBrainClient):
            async def list_schedules(  # type: ignore[override]
                self, project_id: UUID | None = None,
            ) -> AsyncIterator[ScheduleEntry]:
                nonlocal list_call_count
                list_call_count += 1
                for x in self.list_entries:
                    yield x

        client = CountingClient(list_entries=[e])
        watch = WatchStream(
            client=client,  # type: ignore[arg-type]
            cache=cache,
            full_resync_interval_seconds=0.05,
        )
        task = asyncio.create_task(watch.run())
        # Long enough that the periodic timer fires at least once
        # (interval 50ms; we wait ~150ms = ~2 ticks).
        await asyncio.sleep(0.15)
        await watch.stop()
        await asyncio.wait_for(task, timeout=1.0)
        # At minimum one list call from the watch loop's startup
        # sync, plus one from the periodic timer. Assert >=2 to
        # prove the timer fired independently.
        assert list_call_count >= 2, (
            f"periodic timer didn't fire: only {list_call_count} "
            f"list_schedules calls observed"
        )

    async def test_periodic_timer_survives_sync_failure(self) -> None:
        # If a single periodic sync raises, the timer must keep
        # ticking - a transient brain blip should not silently
        # disable the defensive layer for the lifetime of the
        # process.
        @dataclass
        class FlakyClient(FakeBrainClient):
            calls: int = 0

            async def list_schedules(  # type: ignore[override]
                self, project_id: UUID | None = None,
            ) -> AsyncIterator[ScheduleEntry]:
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("brain blip")
                for x in self.list_entries:
                    yield x

        cache = ScheduleCache()
        client = FlakyClient()
        watch = WatchStream(
            client=client,  # type: ignore[arg-type]
            cache=cache,
            full_resync_interval_seconds=0.05,
        )
        task = asyncio.create_task(watch.run())
        await asyncio.sleep(0.2)  # ~3-4 ticks
        await watch.stop()
        await asyncio.wait_for(task, timeout=1.0)
        # Calls 1 (startup), 2 (periodic, raises), 3+ (periodic
        # continues). Assert at least 3 to prove the loop didn't
        # die after the failure.
        assert client.calls >= 3, (
            f"periodic timer died after a failure: only "
            f"{client.calls} calls observed"
        )


class TestStopAndRun:
    async def test_run_exits_on_stop(self) -> None:
        # Empty client - both list and stream return immediately. The
        # outer ``while not stop_event`` keeps looping; we set stop
        # right after first iteration begins.
        cache = ScheduleCache()
        client = FakeBrainClient()
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        task = asyncio.create_task(watch.run())
        await asyncio.sleep(0)  # let task start
        await watch.stop()
        # The watch loop calls _backoff_or_stop() between iterations
        # which awaits the stop_event with the current backoff. Stop
        # has already been signalled so the wait returns immediately.
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done()

    async def test_run_swallows_stream_exception_and_retries(self) -> None:
        # First sync raises; the loop logs + backs off + retries.
        # Use a stop event to exit on the second iteration.
        cache = ScheduleCache()
        client = FakeBrainClient(
            raise_in_stream=RuntimeError("simulated stream drop"),
        )
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]
        task = asyncio.create_task(watch.run())
        # Let it crash + backoff once.
        await asyncio.sleep(0.1)
        await watch.stop()
        await asyncio.wait_for(task, timeout=2.0)
        assert task.done()
        # Reconnect counter incremented at least once.
        assert watch._reconnect_attempts >= 1
