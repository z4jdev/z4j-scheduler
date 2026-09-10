"""A backlog must stop starting work when its operating conditions change."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick._prepared import PreparedFire
from z4j_scheduler.tick.engine import TickEngine

pytestmark = pytest.mark.asyncio
_START = datetime(2026, 9, 8, 12, tzinfo=UTC)
_NOW = _START + timedelta(minutes=3)


@dataclass
class Conditions:
    healthy: bool = True
    leader: bool = True

    def is_leader(self, project_id: UUID) -> bool:
        return self.leader


def _entry(*, current: bool = True) -> ScheduleEntry:
    entry = ScheduleEntry(
        id=uuid4(),
        project_id=uuid4(),
        kind="interval",
        expression="1m",
        timezone="UTC",
        is_enabled=True,
        catch_up="fire_all_missed",
        anchor_at=_START,
        last_fire_at=_START,
        control_token=uuid4() if current else None,
        schedule_revision=40 if current else 0,
        definition_digest="d" * 64 if current else "",
        cadence_semantics_version=1 if current else 0,
        cadence_runtime_fingerprint="f" * 64 if current else "",
    )
    entry.next_fire_at = _START + timedelta(minutes=1)
    return entry


class InterruptingDispatcher:
    """Accept one slot, then change a condition while that RPC is in flight."""

    def __init__(self, conditions: Conditions, interrupt: str | None) -> None:
        self.conditions = conditions
        self.interrupt = interrupt
        self.slots: list[tuple[UUID, datetime]] = []

    async def dispatch(
        self,
        *,
        schedule_entry: ScheduleEntry,
        prepared_fire: PreparedFire,
        **_kwargs: object,
    ) -> FireResult | None:
        self.slots.append((schedule_entry.id, prepared_fire.scheduled_for))
        if len(self.slots) == 1 and self.interrupt is not None:
            setattr(self.conditions, self.interrupt, False)
        if schedule_entry.control_token is None:
            return None
        revision = schedule_entry.schedule_revision + 1
        return FireResult(
            command_id=uuid4(),
            error_code=None,
            error_message=None,
            buffered=False,
            disposition="accepted",
            acceptance_revision=revision,
            accepted_last_run_at=prepared_fire.scheduled_for,
            accepted_next_run_at=prepared_fire.next_run_at,
            live_control_token=schedule_entry.control_token,
            live_revision=revision,
            live_last_run_at=prepared_fire.scheduled_for,
            live_next_run_at=prepared_fire.next_run_at,
        )

    async def advance_cursor(self, **_kwargs: object) -> None:
        pytest.fail("an unhealthy queued schedule must not discard its backlog")


def _engine(cache: ScheduleCache, conditions: Conditions, dispatcher: object) -> TickEngine:
    return TickEngine(
        cache=cache,
        leader_gate=conditions,
        dispatcher=dispatcher,  # type: ignore[arg-type] -- injected fault driver
        clock=lambda: _NOW,
        watch_healthy=lambda: conditions.healthy,
        max_sleep_seconds=0.001,
        iteration_error_backoff_seconds=0,
        max_consecutive_iteration_errors=2,
    )


@pytest.mark.parametrize("current", [False, True], ids=["legacy", "current"])
@pytest.mark.parametrize("interrupt", ["healthy", "leader"], ids=["watch-loss", "demotion"])
async def test_partial_backlog_stops_and_recovers_without_repeating_slots(
    current: bool,
    interrupt: str,
) -> None:
    cache, conditions = ScheduleCache(), Conditions()
    entry = _entry(current=current)
    await cache.upsert(entry)
    dispatcher = InterruptingDispatcher(conditions, interrupt)
    engine = _engine(cache, conditions, dispatcher)

    await engine._iteration()

    first = _START + timedelta(minutes=1)
    assert dispatcher.slots == [(entry.id, first)]
    assert entry.last_fire_at == first
    assert entry.next_fire_at == _START + timedelta(minutes=2)
    assert entry.is_enabled
    assert not engine._in_flight

    setattr(conditions, interrupt, True)
    await engine._iteration()

    assert dispatcher.slots == [(entry.id, _START + timedelta(minutes=i)) for i in (1, 2, 3)]
    assert entry.next_fire_at == _START + timedelta(minutes=4)
    assert not engine._fire_backoff_until


@pytest.mark.parametrize("interrupt", ["healthy", "leader"])
async def test_conditions_are_checked_after_reading_live_state(interrupt: str) -> None:
    """The cache lock is an await point after the initial condition check."""
    cache, conditions = ScheduleCache(), Conditions()
    entry = _entry()
    await cache.upsert(entry)
    real_get = cache.get

    async def interrupted_get(schedule_id: UUID) -> ScheduleEntry | None:
        live = await real_get(schedule_id)
        setattr(conditions, interrupt, False)
        return live

    cache.get = interrupted_get  # type: ignore[method-assign]
    dispatcher = InterruptingDispatcher(conditions, interrupt)
    engine = _engine(cache, conditions, dispatcher)

    await engine._fire_with_catch_up(entry, now=_NOW)

    assert dispatcher.slots == []
    assert entry.last_fire_at == _START
    assert entry.next_fire_at == _START + timedelta(minutes=1)


async def test_queued_schedules_stop_when_watch_fails_and_all_resume() -> None:
    """More than one worker batch stays intact during a shared watch outage."""
    cache, conditions = ScheduleCache(), Conditions()
    entries = [_entry() for _ in range(300)]
    for entry in entries:
        await cache.upsert(entry)
    dispatcher = InterruptingDispatcher(conditions, "healthy")
    engine = _engine(cache, conditions, dispatcher)

    await engine._iteration()
    assert len(dispatcher.slots) == 1
    assert not engine._in_flight

    conditions.healthy = True
    for _ in range(3):
        await engine._iteration()

    expected = {(entry.id, _START + timedelta(minutes=i)) for entry in entries for i in (1, 2, 3)}
    assert set(dispatcher.slots) == expected
    assert len(dispatcher.slots) == len(expected)
    assert all(entry.next_fire_at == _START + timedelta(minutes=4) for entry in entries)


@pytest.mark.parametrize("reason", ["watch", "stop"])
async def test_interrupted_skip_does_not_advance_a_durable_cursor(reason: str) -> None:
    cache, conditions = ScheduleCache(), Conditions(healthy=reason != "watch")
    entry = _entry()
    entry.catch_up = "skip"
    await cache.upsert(entry)
    dispatcher = InterruptingDispatcher(conditions, "healthy")
    engine = _engine(cache, conditions, dispatcher)
    if reason == "stop":
        await engine.stop()

    assert await engine._fire_with_catch_up(entry, now=_NOW)
    assert entry.next_fire_at == _START + timedelta(minutes=1)
    assert entry.schedule_revision == 40


async def test_project_demotion_leaves_other_projects_running() -> None:
    cache, conditions = ScheduleCache(), Conditions()
    demoted, unaffected = _entry(), _entry()
    await cache.upsert(demoted)
    await cache.upsert(unaffected)

    class ProjectDemotion(InterruptingDispatcher):
        async def dispatch(self, *, schedule_entry, **kwargs):
            result = await super().dispatch(schedule_entry=schedule_entry, **kwargs)
            if schedule_entry.id == demoted.id:
                conditions.leader = False
            return result

    dispatcher = ProjectDemotion(conditions, None)
    engine = _engine(cache, conditions, dispatcher)
    # The first project loses its lock while the second retains its own lock.
    conditions.is_leader = lambda project_id: (  # type: ignore[method-assign]
        project_id != demoted.project_id or conditions.leader
    )

    await engine._iteration()

    assert [slot for sid, slot in dispatcher.slots if sid == demoted.id] == [
        _START + timedelta(minutes=1),
    ]
    assert [slot for sid, slot in dispatcher.slots if sid == unaffected.id] == [
        _START + timedelta(minutes=i) for i in (1, 2, 3)
    ]
    assert unaffected.next_fire_at == _START + timedelta(minutes=4)
    assert demoted.next_fire_at == _START + timedelta(minutes=2)


async def test_worker_failure_preserves_siblings_and_recovers_on_next_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache, conditions = ScheduleCache(), Conditions()
    broken, unaffected = _entry(), _entry()
    await cache.upsert(broken)
    await cache.upsert(unaffected)
    dispatcher = InterruptingDispatcher(conditions, None)
    engine = _engine(cache, conditions, dispatcher)
    original = engine._fire_with_catch_up

    async def fail_one(entry: ScheduleEntry, *, now: datetime) -> bool:
        if entry.id == broken.id:
            raise ValueError("injected cadence fault")
        return await original(entry, now=now)

    with monkeypatch.context() as patch:
        patch.setattr(engine, "_fire_with_catch_up", fail_one)
        patch.setattr(
            engine,
            "_quarantine_after_fire_error",
            AsyncMock(side_effect=RuntimeError("storage")),
        )
        with pytest.raises(ExceptionGroup, match="dispatch workers failed"):
            await engine._iteration()

    assert unaffected.next_fire_at == _START + timedelta(minutes=4)
    assert broken.next_fire_at == _START + timedelta(minutes=1)
    assert not engine._in_flight

    await engine._iteration()
    expected = {
        (entry.id, _START + timedelta(minutes=i))
        for entry in (broken, unaffected)
        for i in (1, 2, 3)
    }
    assert set(dispatcher.slots) == expected
    assert len(dispatcher.slots) == len(expected)


async def test_worker_recovery_failure_reaches_iteration_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache, conditions = ScheduleCache(), Conditions()
    entry = _entry()
    await cache.upsert(entry)
    engine = _engine(cache, conditions, InterruptingDispatcher(conditions, "healthy"))
    # A cadence failure is contained by quarantine; a failure to latch that
    # quarantine must reach run()'s bounded supervisor instead of vanishing.
    monkeypatch.setattr(engine, "_fire_with_catch_up", AsyncMock(side_effect=ValueError("cadence")))
    recovery = AsyncMock(side_effect=RuntimeError("quarantine storage unavailable"))
    monkeypatch.setattr(engine, "_quarantine_after_fire_error", recovery)

    with pytest.raises(ExceptionGroup, match="dispatch workers failed") as exc:
        await asyncio.wait_for(engine.run(), timeout=2)

    assert recovery.await_count == 2
    assert any("quarantine storage unavailable" in str(e) for e in exc.value.exceptions)
    assert not engine._in_flight
    assert entry.next_fire_at == _START + timedelta(minutes=1)


async def test_worker_cancellation_propagates_and_releases_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache, conditions = ScheduleCache(), Conditions()
    for _ in range(40):
        await cache.upsert(_entry())
    engine = _engine(cache, conditions, InterruptingDispatcher(conditions, "healthy"))
    monkeypatch.setattr(
        engine, "_fire_with_catch_up", AsyncMock(side_effect=asyncio.CancelledError)
    )

    with pytest.raises(asyncio.CancelledError):
        await engine._iteration()

    assert not engine._in_flight
