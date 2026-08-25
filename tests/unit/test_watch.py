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
from z4j_scheduler.storage._models import (
    ScannedThrough,
    ScheduleEvent,
    ScheduleSnapshot,
)
from z4j_scheduler.storage._protocol import ProtocolNegotiationError
from z4j_scheduler.storage._snapshot import snapshot_digest
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
                    kind="created",
                    schedule=e,
                    deleted_id=None,
                    resume_token="t1",
                ),
            ],
        )
        cache = ScheduleCache()
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]

        await watch._stream()
        assert await cache.get(e.id) is e
        assert watch._resume_token == "t1"

    async def test_updated_event_replaces(self) -> None:
        sid = uuid4()
        old = _make_entry(schedule_id=sid)
        new = _make_entry(schedule_id=sid)
        cache = ScheduleCache()
        await cache.upsert(old)

        client = FakeBrainClient(
            stream_events=[
                ScheduleEvent(
                    kind="updated",
                    schedule=new,
                    deleted_id=None,
                    resume_token="t2",
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
                    kind="deleted",
                    schedule=None,
                    deleted_id=e.id,
                    resume_token="t3",
                ),
            ],
        )
        watch = WatchStream(client=client, cache=cache)  # type: ignore[arg-type]
        await watch._stream()
        assert await cache.get(e.id) is None


class TestCurrentProtocolSync:
    async def test_reconnect_rejects_legacy_to_current_mode_change(self) -> None:
        async def select_current():
            return "current"

        watch = WatchStream(
            client=FakeBrainClient(),  # type: ignore[arg-type]
            cache=ScheduleCache(),
            protocol_mode="legacy",
            protocol_selector=select_current,  # type: ignore[arg-type]
        )

        with pytest.raises(ProtocolNegotiationError, match="restart"):
            await watch._refresh_protocol_mode()

    async def test_reconnect_rejects_current_to_legacy_downgrade(self) -> None:
        async def select_legacy():
            return "legacy"

        watch = WatchStream(
            client=FakeBrainClient(),  # type: ignore[arg-type]
            cache=ScheduleCache(),
            protocol_mode="current",
            protocol_selector=select_legacy,  # type: ignore[arg-type]
        )

        with pytest.raises(ProtocolNegotiationError, match="restart"):
            await watch._refresh_protocol_mode()

    async def test_snapshot_then_watch_uses_revision_cursor(self) -> None:
        project_id = uuid4()
        row = ScheduleEntry(
            id=uuid4(),
            project_id=project_id,
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
            control_token=uuid4(),
            schedule_revision=10,
            definition_digest="d" * 64,
            cadence_semantics_version=1,
            cadence_runtime_fingerprint="f" * 64,
        )
        row.next_fire_at = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
        unfinished = ScheduleSnapshot(
            snapshot_id=uuid4(),
            project_id=project_id,
            watermark=10,
            rows=(row,),
            digest="",
        )
        snapshot = ScheduleSnapshot(
            snapshot_id=unfinished.snapshot_id,
            project_id=project_id,
            watermark=10,
            rows=(row,),
            digest=snapshot_digest(unfinished),
        )

        class CurrentClient:
            seen_after_revision: int | None = None

            async def list_schedule_snapshot(self, requested_project_id):
                assert requested_project_id == project_id
                return snapshot

            async def watch_schedules_v2(
                self,
                requested_project_id,
                *,
                after_revision,
            ):
                assert requested_project_id == project_id
                self.seen_after_revision = after_revision
                yield ScannedThrough(revision=12, server_revision=12)

        client = CurrentClient()
        cache = ScheduleCache()
        watch = WatchStream(
            client=client,  # type: ignore[arg-type]
            cache=cache,
            project_id=project_id,
            protocol_mode="current",
        )

        await watch._full_sync()
        await watch._stream()

        assert await cache.get(row.id) is row
        assert client.seen_after_revision == 10
        assert watch._revision_cursor == 12
        assert await cache.project_watermark(project_id) == 10

    async def test_current_empty_snapshot_removes_stale_runnable_row(self) -> None:
        project_id = uuid4()
        stale = ScheduleEntry(
            id=uuid4(),
            project_id=project_id,
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
            control_token=uuid4(),
            schedule_revision=7,
            definition_digest="d" * 64,
            cadence_semantics_version=1,
            cadence_runtime_fingerprint="f" * 64,
        )
        stale.next_fire_at = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
        unfinished = ScheduleSnapshot(
            snapshot_id=uuid4(),
            project_id=project_id,
            watermark=10,
            rows=(),
            digest="",
        )
        snapshot = ScheduleSnapshot(
            snapshot_id=unfinished.snapshot_id,
            project_id=project_id,
            watermark=10,
            rows=(),
            digest=snapshot_digest(unfinished),
        )

        class EmptyClient:
            async def list_schedule_snapshot(self, requested_project_id):
                assert requested_project_id == project_id
                return snapshot

        cache = ScheduleCache()
        await cache.upsert(stale)
        watch = WatchStream(
            client=EmptyClient(),  # type: ignore[arg-type]
            cache=cache,
            project_id=project_id,
            protocol_mode="current",
        )
        await watch._full_sync()

        assert await cache.get(stale.id) is None
        assert watch._revision_cursor == 10

    async def test_current_mode_supports_authenticated_all_project_scope(
        self,
    ) -> None:
        first = _make_entry()
        first.control_token = uuid4()
        first.schedule_revision = 10
        first.definition_digest = "d" * 64
        first.cadence_semantics_version = 1
        first.cadence_runtime_fingerprint = "f" * 64
        second = _make_entry()
        second.control_token = uuid4()
        second.schedule_revision = 11
        second.definition_digest = "e" * 64
        second.cadence_semantics_version = 1
        second.cadence_runtime_fingerprint = "f" * 64
        unfinished = ScheduleSnapshot(
            snapshot_id=uuid4(),
            project_id=None,
            watermark=11,
            rows=(first, second),
            digest="",
        )
        snapshot = ScheduleSnapshot(
            snapshot_id=unfinished.snapshot_id,
            project_id=None,
            watermark=11,
            rows=unfinished.rows,
            digest=snapshot_digest(unfinished),
        )

        class AllScopeClient:
            async def list_schedule_snapshot(self, requested_project_id):
                assert requested_project_id is None
                return snapshot

            async def watch_schedules_v2(
                self,
                requested_project_id,
                *,
                after_revision,
            ):
                assert requested_project_id is None
                assert after_revision == 11
                yield ScannedThrough(revision=12, server_revision=12)

        cache = ScheduleCache()
        watch = WatchStream(
            client=AllScopeClient(),  # type: ignore[arg-type]
            cache=cache,
            protocol_mode="current",
        )
        await watch._full_sync()
        await watch._stream()

        assert await cache.get(first.id) is first
        assert await cache.get(second.id) is second
        assert await cache.project_watermark(first.project_id) == 11
        assert await cache.project_watermark(second.project_id) == 11
        assert watch._revision_cursor == 12


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

    async def test_zero_interval_disables_timer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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

        watch_started = asyncio.Event()
        periodic_started = asyncio.Event()

        async def controlled_watch_loop() -> None:
            watch_started.set()
            await watch._stop_event.wait()

        async def forbidden_periodic_loop() -> None:
            periodic_started.set()
            await watch._stop_event.wait()

        monkeypatch.setattr(watch, "_watch_loop", controlled_watch_loop)
        monkeypatch.setattr(watch, "_periodic_resync_loop", forbidden_periodic_loop)

        task = asyncio.create_task(watch.run())
        try:
            await asyncio.wait_for(watch_started.wait(), timeout=1.0)
            # Let every task created by ``run`` take a scheduling turn.  If the
            # zero guard regresses, the patched periodic loop becomes observable.
            await asyncio.sleep(0)
            assert not periodic_started.is_set()
        finally:
            await watch.stop()
            await asyncio.wait_for(task, timeout=1.0)

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
                self,
                project_id: UUID | None = None,
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
            f"periodic timer didn't fire: only {list_call_count} list_schedules calls observed"
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
                self,
                project_id: UUID | None = None,
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
            f"periodic timer died after a failure: only {client.calls} calls observed"
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

    async def test_reconnect_backoff_honors_configured_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cache = ScheduleCache()
        client = FakeBrainClient()
        watch = WatchStream(
            client=client,  # type: ignore[arg-type]
            cache=cache,
            reconnect_backoff_max_seconds=0.25,
        )
        watch._reconnect_attempts = 20
        captured: list[float] = []

        async def _capture_wait(
            awaitable: object,
            *,
            timeout: float,  # noqa: ASYNC109 - asyncio.wait_for test double
        ) -> None:
            captured.append(timeout)
            awaitable.close()  # type: ignore[attr-defined]
            raise TimeoutError

        monkeypatch.setattr("z4j_scheduler.storage.watch.random.uniform", lambda *_: 0.0)
        monkeypatch.setattr("z4j_scheduler.storage.watch.asyncio.wait_for", _capture_wait)

        await watch._backoff_or_stop()

        assert captured == [0.25]
