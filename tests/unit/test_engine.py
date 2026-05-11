"""Tests for :class:`z4j_scheduler.tick.engine.TickEngine`.

The engine is fully testable without I/O: we inject a controllable
clock, a fake leader gate, and a fake dispatcher that records
calls.

Test categories:

- single-iteration semantics (cron / interval / one_shot fire correctly)
- catch-up policy interactions
- leader-gate enforcement
- error handling (bad expression, dispatcher raises)
- sleep coordination + stop semantics
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick.engine import TickEngine

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class ManualClock:
    """Test clock - returns whatever was last set via :meth:`advance_to`."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance_to(self, when: datetime) -> None:
        self._now = when

    def advance(self, delta: timedelta) -> None:
        self._now += delta


class AlwaysLeader:
    def is_leader(self, project_id: UUID) -> bool:
        return True


class NeverLeader:
    def is_leader(self, project_id: UUID) -> bool:
        return False


class PerProjectLeader:
    """Leader for an explicit set of project ids."""

    def __init__(self, leader_for: set[UUID]) -> None:
        self._leader_for = leader_for

    def is_leader(self, project_id: UUID) -> bool:
        return project_id in self._leader_for


@dataclass
class RecordedFire:
    schedule_id: UUID
    scheduled_for: datetime


@dataclass
class RecordingDispatcher:
    """Records every dispatch call. Optionally raises N times to test
    the engine's "leave next_fire_at unchanged on dispatcher failure"
    semantics.
    """

    fires: list[RecordedFire] = field(default_factory=list)
    raise_count: int = 0

    async def dispatch(
        self,
        *,
        schedule_id: UUID,
        scheduled_for: datetime,
        schedule_name: str = "",  # Phase 4: per-schedule metric label
    ) -> None:
        if self.raise_count > 0:
            self.raise_count -= 1
            raise RuntimeError("simulated dispatcher failure")
        self.fires.append(
            RecordedFire(schedule_id=schedule_id, scheduled_for=scheduled_for),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_cron_entry(
    *,
    expression: str = "0 * * * *",
    schedule_id: UUID | None = None,
    project_id: UUID | None = None,
    catch_up: str = "skip",
    is_enabled: bool = True,
    last_fire_at: datetime | None = None,
) -> ScheduleEntry:
    return ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind="cron",
        expression=expression,
        timezone="UTC",
        is_enabled=is_enabled,
        catch_up=catch_up,  # type: ignore[arg-type]
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
        last_fire_at=last_fire_at,
    )


def make_interval_entry(
    *,
    expression: str = "5m",
    schedule_id: UUID | None = None,
    project_id: UUID | None = None,
    last_fire_at: datetime | None = None,
    anchor_at: datetime | None = None,
) -> ScheduleEntry:
    return ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind="interval",
        expression=expression,
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=anchor_at or datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        last_fire_at=last_fire_at,
    )


def make_one_shot_entry(
    *,
    expression: str,
    schedule_id: UUID | None = None,
    project_id: UUID | None = None,
    last_fire_at: datetime | None = None,
) -> ScheduleEntry:
    return ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind="one_shot",
        expression=expression,
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
        last_fire_at=last_fire_at,
    )


# ---------------------------------------------------------------------------
# Tests - single iteration
# ---------------------------------------------------------------------------


class TestComputeNextFire:
    async def test_cron_gets_next_fire_computed_on_iteration(self) -> None:
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 4, 26, 14, 30, tzinfo=UTC))
        entry = make_cron_entry()  # "0 * * * *" - top of every hour
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(), clock=clock,
        )
        await engine._compute_pending_next_fires()

        # Next "0 * * * *" after 14:30 is 15:00.
        assert entry.next_fire_at == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)

    async def test_interval_first_fire_aligns_to_anchor(self) -> None:
        cache = ScheduleCache()
        anchor = datetime(2026, 4, 26, 12, 3, tzinfo=UTC)
        entry = make_interval_entry(expression="5m", anchor_at=anchor)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(anchor),
        )
        await engine._compute_pending_next_fires()
        # 5-min boundary after 12:03 is 12:05.
        assert entry.next_fire_at == datetime(2026, 4, 26, 12, 5, tzinfo=UTC)

    async def test_one_shot_returns_target(self) -> None:
        cache = ScheduleCache()
        entry = make_one_shot_entry(expression="2026-05-01T09:00:00Z")
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
        )
        await engine._compute_pending_next_fires()
        assert entry.next_fire_at == datetime(2026, 5, 1, 9, 0, tzinfo=UTC)

    async def test_invalid_cron_disables_locally(self) -> None:
        cache = ScheduleCache()
        entry = make_cron_entry(expression="not a cron")
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
        )
        await engine._compute_pending_next_fires()
        assert entry.is_enabled is False
        assert entry.next_fire_at is None


# ---------------------------------------------------------------------------
# Tests - dispatch + catch_up
# ---------------------------------------------------------------------------


class TestDispatchPath:
    async def test_due_entry_dispatches(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC))

        entry = make_cron_entry()
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
        )
        await engine._iteration()

        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].schedule_id == entry.id
        assert dispatcher.fires[0].scheduled_for == datetime(
            2026, 4, 26, 15, 0, 0, tzinfo=UTC,
        )
        # last_fire_at advanced + next_fire_at recomputed.
        assert entry.last_fire_at == datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        assert entry.next_fire_at == datetime(2026, 4, 26, 16, 0, 0, tzinfo=UTC)

    async def test_not_due_does_not_dispatch(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 14, 0, tzinfo=UTC))

        entry = make_cron_entry()
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,  # so the iteration returns fast
        )
        await engine._iteration()
        assert dispatcher.fires == []

    async def test_disabled_does_not_dispatch(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        entry = make_cron_entry(is_enabled=False)
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        assert dispatcher.fires == []

    async def test_skip_policy_advances_without_dispatch(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        # Wall-clock is 1 hour past the schedule's next_fire - it's
        # "missed". Skip policy = no dispatch, but still advance.
        clock = ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC))

        entry = make_cron_entry(catch_up="skip")
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        # Skip => no dispatch.
        assert dispatcher.fires == []
        # But last_fire_at advanced past the missed slot, so we
        # don't re-evaluate it next iteration.
        assert entry.last_fire_at == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        # And next_fire_at recomputed past the missed slot.
        assert entry.next_fire_at == datetime(2026, 4, 26, 16, 0, tzinfo=UTC)

    async def test_fire_one_missed_dispatches_once(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC))

        entry = make_cron_entry(catch_up="fire_one_missed")
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].scheduled_for == datetime(
            2026, 4, 26, 15, 0, tzinfo=UTC,
        )


class TestLeaderGate:
    async def test_non_leader_does_not_dispatch(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        entry = make_cron_entry()
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=NeverLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        assert dispatcher.fires == []
        # Non-leader still advanced so it doesn't spin on the same entry.
        assert entry.next_fire_at == datetime(2026, 4, 26, 16, 0, tzinfo=UTC)

    async def test_per_project_leader_filtering(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        leader_project = uuid4()
        non_leader_project = uuid4()

        leader_entry = make_cron_entry(project_id=leader_project)
        leader_entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        other_entry = make_cron_entry(project_id=non_leader_project)
        other_entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert_many([leader_entry, other_entry])

        engine = TickEngine(
            cache=cache,
            leader_gate=PerProjectLeader({leader_project}),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        # Only the leader project's entry dispatched.
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].schedule_id == leader_entry.id


class TestDispatcherFailure:
    async def test_dispatcher_raise_does_not_advance(self) -> None:
        """If dispatcher raises, last_fire_at + next_fire_at stay
        put so the next iteration retries the same fire."""
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher(raise_count=1)
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        entry = make_cron_entry()
        original_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry.next_fire_at = original_next
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=0.01,
        )
        # First iteration: dispatcher raises - no advance.
        await engine._iteration()
        assert entry.last_fire_at is None
        assert entry.next_fire_at == original_next
        assert dispatcher.fires == []

        # Second iteration: dispatcher succeeds - advance.
        await engine._iteration()
        assert entry.last_fire_at == original_next
        assert len(dispatcher.fires) == 1


# ---------------------------------------------------------------------------
# Tests - sleep coordination + stop
# ---------------------------------------------------------------------------


class TestSleepAndStop:
    async def test_run_exits_on_stop(self) -> None:
        cache = ScheduleCache()
        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
            max_sleep_seconds=10.0,
        )
        # Start the loop, then immediately stop.
        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0)  # let the task start
        await engine.stop()
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done()

    async def test_run_wakes_on_cache_change(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        engine = TickEngine(
            cache=cache, leader_gate=AlwaysLeader(),
            dispatcher=dispatcher, clock=clock,
            max_sleep_seconds=10.0,  # would sleep forever without cache event
        )
        task = asyncio.create_task(engine.run())
        try:
            # Engine sleeps until the cache changes.
            await asyncio.sleep(0.05)
            # Now insert a due schedule. Cache change fires the event,
            # engine wakes and dispatches.
            entry = make_cron_entry()
            entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
            await cache.upsert(entry)
            # Give engine time to process the cache change.
            for _ in range(50):
                await asyncio.sleep(0.01)
                if dispatcher.fires:
                    break
            assert len(dispatcher.fires) == 1
        finally:
            await engine.stop()
            await asyncio.wait_for(task, timeout=1.0)


# ---------------------------------------------------------------------------
# Tests - cooperative yield under unhealthy watch (Bug #5 regression)
# ---------------------------------------------------------------------------


class TestUnhealthyWatchCooperativeYield:
    """Regression tests for the silent-deadlock-on-brain-restart bug.

    When the watch stream is unhealthy AND the cache holds past-due
    schedules, the engine's ``_iteration`` used to return synchronously
    via ``_sleep_until_next`` (which finds past-due entries and returns
    immediately, no real ``await``). Combined with the outer ``run()``
    loop, this produced a tight no-yield spin that starved every other
    asyncio task, including the watch reconnect task -- so the engine
    could never escape the unhealthy state. The fix is a fixed
    cooperative wait in the unhealthy branch + a top-of-iteration
    ``asyncio.sleep(0)``.
    """

    async def test_iteration_yields_when_watch_unhealthy_and_cache_past_due(
        self,
    ) -> None:
        """The unhealthy branch must yield to the event loop.

        Construct: cache holds a past-due schedule, watch_healthy
        returns False. Run a sibling asyncio task in parallel with
        ``engine.run()``. The sibling MUST make progress within
        a small bounded time -- if the engine's unhealthy branch
        doesn't yield, the sibling is starved and this test times
        out.
        """
        cache = ScheduleCache()
        # Past-due cron entry (last fire well in the past)
        entry = make_cron_entry(
            expression="* * * * *",
            last_fire_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
        entry.next_fire_at = datetime(2020, 1, 1, 0, 1, tzinfo=UTC)
        await cache.upsert(entry)

        clock = ManualClock(datetime(2026, 4, 26, 12, 0, tzinfo=UTC))

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
            max_sleep_seconds=10.0,
            watch_healthy=lambda: False,  # the trigger
        )

        # Sibling task that just increments a counter on every yield.
        # If the engine starves the loop, this counter stays at 0.
        sibling_progress = [0]

        async def sibling() -> None:
            for _ in range(20):
                await asyncio.sleep(0.005)
                sibling_progress[0] += 1

        engine_task = asyncio.create_task(engine.run())
        sibling_task = asyncio.create_task(sibling())
        try:
            # Sibling needs ~100ms to complete its 20 iterations.
            # Generous bound: 2 seconds. If the engine's unhealthy
            # branch starves the loop the sibling makes no progress
            # and this wait_for times out.
            await asyncio.wait_for(sibling_task, timeout=2.0)
        finally:
            await engine.stop()
            await asyncio.wait_for(engine_task, timeout=1.0)

        assert sibling_progress[0] == 20, (
            f"sibling task starved by engine spin "
            f"(progress={sibling_progress[0]}/20); the unhealthy "
            f"branch failed to yield to the event loop"
        )

    async def test_iteration_yields_top_of_loop_when_no_real_awaits(
        self,
    ) -> None:
        """Belt-and-suspenders: every iteration yields at the top.

        Even branches that don't reach the unhealthy-watch path
        should yield once per iteration so a tight loop can never
        starve siblings. We exercise the empty-cache path
        (``_compute_pending_next_fires`` short-circuits, no dispatch,
        ``_sleep_until_next`` finds nothing due and sleeps), and
        confirm a sibling makes progress.
        """
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 4, 26, 12, 0, tzinfo=UTC))

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
            max_sleep_seconds=0.05,  # short so the loop iterates fast
            watch_healthy=lambda: True,
        )

        sibling_progress = [0]

        async def sibling() -> None:
            for _ in range(10):
                await asyncio.sleep(0.005)
                sibling_progress[0] += 1

        engine_task = asyncio.create_task(engine.run())
        sibling_task = asyncio.create_task(sibling())
        try:
            await asyncio.wait_for(sibling_task, timeout=2.0)
        finally:
            await engine.stop()
            await asyncio.wait_for(engine_task, timeout=1.0)

        assert sibling_progress[0] == 10
