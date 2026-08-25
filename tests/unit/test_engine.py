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
from z4j_scheduler.storage._models import CursorTransitionResult, FireResult
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
        **_kwargs: object,  # A3: engine / project_id / project_schedule_count
    ) -> None:
        if self.raise_count > 0:
            self.raise_count -= 1
            raise RuntimeError("simulated dispatcher failure")
        self.fires.append(
            RecordedFire(schedule_id=schedule_id, scheduled_for=scheduled_for),
        )


@dataclass
class RecordingQuarantineReporter:
    reports: list[tuple[ScheduleEntry, object]] = field(default_factory=list)

    def enqueue(self, *, entry: ScheduleEntry, quarantine: object) -> bool:
        self.reports.append((entry, quarantine))
        return True


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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
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
            cache=cache,
            leader_gate=AlwaysLeader(),
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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
        )
        await engine._compute_pending_next_fires()
        assert entry.next_fire_at == datetime(2026, 5, 1, 9, 0, tzinfo=UTC)

    async def test_invalid_cron_disables_locally(self) -> None:
        cache = ScheduleCache()
        entry = make_cron_entry(expression="not a cron")
        entry.control_token = uuid4()
        entry.schedule_revision = 1
        entry.definition_digest = "d" * 64
        entry.cadence_semantics_version = 1
        entry.cadence_runtime_fingerprint = "f" * 64
        await cache.upsert(entry)
        reporter = RecordingQuarantineReporter()

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
            quarantine_reporter=reporter,  # type: ignore[arg-type]
        )
        await engine._compute_pending_next_fires()
        assert entry.is_enabled is False
        assert entry.next_fire_at is None
        assert reporter.reports[0][0] is entry


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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
        )
        await engine._iteration()

        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].schedule_id == entry.id
        assert dispatcher.fires[0].scheduled_for == datetime(
            2026,
            4,
            26,
            15,
            0,
            0,
            tzinfo=UTC,
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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        # Skip => no dispatch.
        assert dispatcher.fires == []
        # A fresh skip now materialises the full backlog and advances the
        # anchor past the LAST missed slot (16:00) in one pass, so next_fire_at
        # recomputes to 17:00. (The old single-slot fresh branch advanced only to
        # 15:00, leaving 16:00 still past-due for the next tick.)
        assert entry.last_fire_at == datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
        assert entry.next_fire_at == datetime(2026, 4, 26, 17, 0, tzinfo=UTC)


class TestCatchUpBacklogB3:
    """B3 regression: on recovery the engine must compute the WHOLE missed
    backlog to ``now`` in one pass and coalesce per policy, then advance
    ``last_fire_at`` past the entire backlog. Pre-fix it bounded the window
    at the FIRST missed slot, so every policy dispatched one slot and the
    engine re-entered per slot -- ``fire_one_missed`` fired the entire
    backlog (a duplicate-side-effect storm) just like ``fire_all_missed``.

    Scenario: */5 cron, last fired 15:00, next_fire 15:05 (first missed),
    wall-clock now 16:00 -> a backlog of 15:05..16:00 (~12 slots).
    """

    def _missed_entry(self, catch_up: str) -> ScheduleEntry:
        entry = make_cron_entry(
            expression="*/5 * * * *",
            catch_up=catch_up,
            last_fire_at=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        entry.next_fire_at = datetime(2026, 4, 26, 15, 5, tzinfo=UTC)
        return entry

    async def _engine(self, entry: ScheduleEntry, dispatcher: RecordingDispatcher) -> TickEngine:
        cache = ScheduleCache()
        await cache.upsert(entry)
        return TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )

    async def test_fire_one_missed_coalesces_to_single_dispatch(self) -> None:
        entry = self._missed_entry("fire_one_missed")
        dispatcher = RecordingDispatcher()
        engine = await self._engine(entry, dispatcher)
        # Run several iterations: pre-fix these would keep re-firing slot
        # by slot; the fix advances past the whole backlog on iteration 1.
        for _ in range(3):
            await engine._iteration()
        assert len(dispatcher.fires) == 1, [f.scheduled_for for f in dispatcher.fires]
        # The single fire is the most-recent missed slot, and last_fire_at
        # jumped past the entire backlog (not just to 15:05).
        assert dispatcher.fires[0].scheduled_for == entry.last_fire_at
        assert entry.last_fire_at >= datetime(2026, 4, 26, 15, 55, tzinfo=UTC)
        assert entry.next_fire_at > datetime(2026, 4, 26, 16, 0, tzinfo=UTC)

    async def test_fire_all_missed_dispatches_full_backlog(self) -> None:
        entry = self._missed_entry("fire_all_missed")
        dispatcher = RecordingDispatcher()
        engine = await self._engine(entry, dispatcher)
        await engine._iteration()
        # The entire backlog fires in ONE pass (pre-fix: exactly 1).
        assert len(dispatcher.fires) >= 10
        fired = [f.scheduled_for for f in dispatcher.fires]
        assert len(set(fired)) == len(fired), "slots must be distinct"
        assert entry.last_fire_at == max(fired)

    async def test_skip_advances_past_whole_backlog(self) -> None:
        entry = self._missed_entry("skip")
        dispatcher = RecordingDispatcher()
        engine = await self._engine(entry, dispatcher)
        await engine._iteration()
        assert dispatcher.fires == []
        # Even with nothing fired, last_fire_at must clear the whole
        # backlog so the engine does not churn slot-by-slot next tick.
        assert entry.last_fire_at >= datetime(2026, 4, 26, 15, 55, tzinfo=UTC)

    async def test_fire_one_missed_dispatches_once(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC))

        entry = make_cron_entry(catch_up="fire_one_missed")
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        # A FRESH fire_one_missed materialises the full backlog and
        # coalesces to the LATEST slot (16:00), firing exactly ONCE and advancing
        # past the whole backlog. The old single-slot fresh branch fired the
        # OLDEST (15:00) and then re-entered next tick to fire the rest -- the very
        # storm fire_one_missed is meant to prevent.
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].scheduled_for == datetime(
            2026,
            4,
            26,
            16,
            0,
            tzinfo=UTC,
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
            cache=cache,
            leader_gate=NeverLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        assert dispatcher.fires == []
        # A NON-leader does NOT advance/rewrite fire-state; it PARKS the
        # entry on its current next_fire_at (no hot-loop, no cadence corruption)
        # so a promoted leader fires the REAL slot, not a rewritten one.
        assert entry.next_fire_at == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        assert engine._follower_parked.get(entry.id) == entry.next_fire_at

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
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()
        # Only the leader project's entry dispatched.
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].schedule_id == leader_entry.id


class TestDispatcherFailure:
    async def test_dispatcher_failure_backs_off_then_retries_same_slot(self) -> None:
        """engine:729 + P1-8b: a dispatch failure does NOT advance next_fire_at
        AND is NOT retried on the immediate next tick (no hot-spin). It is backed
        off on a SEPARATE deadline and retries the ORIGINAL slot once the deadline
        elapses (scheduled_for = the original slot, no cadence drift)."""
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher(raise_count=1)
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))

        sid = uuid4()
        entry = make_cron_entry(schedule_id=sid)
        original_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry.next_fire_at = original_next
        await cache.upsert(entry)

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        # First iteration: dispatch fails -> backed off, next_fire UNCHANGED,
        # nothing dispatched.
        await engine._iteration()
        assert entry.last_fire_at is None
        assert entry.next_fire_at == original_next  # NOT advanced/overwritten
        assert dispatcher.fires == []
        assert sid in engine._fire_backoff_until

        # Immediate second iteration (same clock): SKIPPED (still in back-off) --
        # the old code hot-spun here (engine:729); now it does not re-fire.
        await engine._iteration()
        assert dispatcher.fires == []

        # After the back-off deadline: retries the ORIGINAL slot and succeeds.
        clock.advance_to(engine._fire_backoff_until[sid] + timedelta(seconds=1))
        await engine._iteration()
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].scheduled_for == original_next  # no drift
        assert entry.last_fire_at == original_next
        assert sid not in engine._fire_backoff_until  # cleared on success


class TestPreparedCadenceTransition:
    async def test_no_cadence_math_runs_after_dispatch(self) -> None:
        from z4j_scheduler.tick._prepared import PreparedFire

        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))
        entry = make_cron_entry()
        entry.next_fire_at = clock()
        await cache.upsert(entry)

        class GuardedDispatcher:
            sent = False
            prepared: PreparedFire | None = None

            async def dispatch(
                self,
                *,
                prepared_fire: PreparedFire | None = None,
                **_kwargs: object,
            ) -> None:
                assert prepared_fire is not None
                self.prepared = prepared_fire
                self.sent = True

        dispatcher = GuardedDispatcher()

        class GuardedEngine(TickEngine):
            def _next_fire_for(self, *args, **kwargs):
                if dispatcher.sent:
                    raise AssertionError("cadence computation ran after dispatch")
                return super()._next_fire_for(*args, **kwargs)

        engine = GuardedEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()

        assert dispatcher.prepared == PreparedFire(
            scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
            next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        assert entry.last_fire_at == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        assert entry.next_fire_at == datetime(2026, 4, 26, 16, 0, tzinfo=UTC)

    async def test_later_batch_computation_failure_sends_nothing(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        clock = ManualClock(datetime(2026, 4, 26, 15, 20, tzinfo=UTC))
        entry = make_cron_entry(
            expression="*/5 * * * *",
            catch_up="fire_all_missed",
            last_fire_at=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        entry.next_fire_at = datetime(2026, 4, 26, 15, 5, tzinfo=UTC)
        await cache.upsert(entry)

        class FailingPlanner(TickEngine):
            def _next_fire_for(self, *args, **kwargs):
                as_of_last_fire_at = kwargs.get("as_of_last_fire_at")
                if as_of_last_fire_at == datetime(
                    2026,
                    4,
                    26,
                    15,
                    10,
                    tzinfo=UTC,
                ):
                    raise OverflowError("later prepared successor failed")
                return super()._next_fire_for(*args, **kwargs)

        engine = FailingPlanner(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        await engine._iteration()

        assert dispatcher.fires == []
        assert entry.is_enabled is False


class TestDurableSkipNoWork:
    def _current_entry(self) -> ScheduleEntry:
        entry = make_cron_entry(catch_up="skip")
        entry.control_token = uuid4()
        entry.schedule_revision = 40
        entry.definition_digest = "d" * 64
        entry.cadence_semantics_version = 1
        entry.cadence_runtime_fingerprint = "f" * 64
        entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        return entry

    async def test_current_skip_persists_before_local_advance(self) -> None:
        cache = ScheduleCache()
        entry = self._current_entry()
        await cache.upsert(entry)

        class DurableDispatcher(RecordingDispatcher):
            async def advance_cursor(self, *, entry, prepared):
                return CursorTransitionResult(
                    disposition="applied",
                    committed_revision=41,
                    committed_last_run_at=prepared.scheduled_for,
                    committed_next_run_at=prepared.next_run_at,
                    live_control_token=entry.control_token,
                    live_revision=41,
                    live_last_run_at=prepared.scheduled_for,
                    live_next_run_at=prepared.next_run_at,
                    error_code=None,
                    error_message=None,
                )

        dispatcher = DurableDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )
        await engine._iteration()

        assert dispatcher.fires == []
        assert entry.schedule_revision == 41
        assert entry.last_fire_at == datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
        assert entry.next_fire_at == datetime(2026, 4, 26, 17, 0, tzinfo=UTC)

    async def test_transport_failure_keeps_original_cursor(self) -> None:
        cache = ScheduleCache()
        entry = self._current_entry()
        await cache.upsert(entry)

        class FailingDispatcher(RecordingDispatcher):
            async def advance_cursor(self, *, entry, prepared):
                raise RuntimeError("response lost")

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=FailingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, 16, 0, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )
        await engine._iteration()

        assert entry.schedule_revision == 40
        assert entry.last_fire_at is None
        assert entry.next_fire_at == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        assert entry.id in engine._fire_backoff_until


class TestCurrentFireProgress:
    @staticmethod
    def _entry(
        *,
        catch_up: str = "fire_all_missed",
        next_fire_at: datetime,
        last_fire_at: datetime | None,
    ) -> ScheduleEntry:
        entry = make_interval_entry(
            expression="5m",
            last_fire_at=last_fire_at,
        )
        entry.catch_up = catch_up  # type: ignore[assignment]
        entry.control_token = uuid4()
        entry.schedule_revision = 40
        entry.definition_digest = "d" * 64
        entry.cadence_semantics_version = 1
        entry.cadence_runtime_fingerprint = "f" * 64
        entry.next_fire_at = next_fire_at
        return entry

    async def test_multi_slot_batch_uses_each_accepted_prefix_revision(self) -> None:
        cache = ScheduleCache()
        entry = self._entry(
            next_fire_at=datetime(2026, 4, 26, 15, 5, tzinfo=UTC),
            last_fire_at=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        await cache.upsert(entry)

        class CurrentDispatcher(RecordingDispatcher):
            calls: list[tuple[datetime, int, datetime | None, datetime | None]]

            def __init__(self) -> None:
                super().__init__()
                self.calls = []

            async def dispatch(
                self,
                *,
                scheduled_for,
                schedule_entry,
                prepared_fire,
                **_kwargs,
            ):
                self.calls.append(
                    (
                        scheduled_for,
                        schedule_entry.schedule_revision,
                        schedule_entry.last_fire_at,
                        schedule_entry.next_fire_at,
                    ),
                )
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

        dispatcher = CurrentDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(datetime(2026, 4, 26, 15, 15, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )

        await engine._iteration()

        assert [call[:2] for call in dispatcher.calls] == [
            (datetime(2026, 4, 26, 15, 5, tzinfo=UTC), 40),
            (datetime(2026, 4, 26, 15, 10, tzinfo=UTC), 41),
            (datetime(2026, 4, 26, 15, 15, tzinfo=UTC), 42),
        ]
        assert entry.schedule_revision == 43
        assert entry.last_fire_at == datetime(2026, 4, 26, 15, 15, tzinfo=UTC)
        assert entry.next_fire_at == datetime(2026, 4, 26, 15, 20, tzinfo=UTC)

    async def test_response_loss_keeps_and_retries_the_same_slot(self) -> None:
        cache = ScheduleCache()
        slot = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=datetime(2026, 4, 26, 14, 55, tzinfo=UTC),
        )
        await cache.upsert(entry)

        class LossThenAccepted(RecordingDispatcher):
            slots: list[datetime]

            def __init__(self) -> None:
                super().__init__()
                self.slots = []

            async def dispatch(
                self,
                *,
                scheduled_for,
                schedule_entry,
                prepared_fire,
                **_kwargs,
            ):
                self.slots.append(scheduled_for)
                if len(self.slots) == 1:
                    raise RuntimeError("response lost")
                return FireResult(
                    command_id=uuid4(),
                    error_code=None,
                    error_message=None,
                    buffered=False,
                    disposition="accepted",
                    acceptance_revision=41,
                    accepted_last_run_at=prepared_fire.scheduled_for,
                    accepted_next_run_at=prepared_fire.next_run_at,
                    live_control_token=schedule_entry.control_token,
                    live_revision=41,
                    live_last_run_at=prepared_fire.scheduled_for,
                    live_next_run_at=prepared_fire.next_run_at,
                )

        dispatcher = LossThenAccepted()
        clock = ManualClock(slot)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )

        await engine._iteration()
        assert entry.schedule_revision == 40
        assert entry.next_fire_at == slot
        clock.advance(timedelta(seconds=2))
        await engine._iteration()

        assert dispatcher.slots == [slot, slot]
        assert entry.schedule_revision == 41

    async def test_terminal_disposition_latches_until_control_rotation(self) -> None:
        """A refusal that names no live revision can only end on a new one.

        There is nothing to wait for: a brain answering that the schedule does
        not exist has no live row to name, and a peer that answers without one
        has told this scheduler nothing it can act on. Waiting for the next
        revision would be waiting for a fact that was never promised, so the
        stop stays until the control generation is superseded outright.
        """
        cache = ScheduleCache()
        slot = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=datetime(2026, 4, 26, 14, 55, tzinfo=UTC),
        )
        await cache.upsert(entry)

        class TerminalDispatcher(RecordingDispatcher):
            calls = 0

            async def dispatch(self, **_kwargs):
                self.calls += 1
                return FireResult(
                    command_id=uuid4(),
                    error_code="terminal",
                    error_message=None,
                    buffered=False,
                    disposition="terminal_quarantined",
                )

        dispatcher = TerminalDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(slot),
            max_sleep_seconds=0.01,
        )

        await engine._iteration()
        await engine._iteration()

        assert dispatcher.calls == 1
        assert entry.is_enabled is False
        same_generation = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=entry.last_fire_at,
        )
        same_generation.id = entry.id
        same_generation.project_id = entry.project_id
        same_generation.control_token = entry.control_token
        same_generation.schedule_revision = 41
        await cache.apply_watch_update(same_generation)
        assert same_generation.is_enabled is False

        replacement = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=entry.last_fire_at,
        )
        replacement.id = entry.id
        replacement.project_id = entry.project_id
        replacement.schedule_revision = 42
        await cache.apply_watch_update(replacement)
        assert replacement.is_enabled is True

    async def test_a_fire_that_raced_a_hold_resumes_when_the_hold_is_released(
        self,
    ) -> None:
        """The release has to be enough. Restarting the process is not a remedy.

        An operator holds a schedule while a fire for the due slot is already in
        flight. Brain allocates a revision for the hold and answers the fire
        with a refresh, because the state the fire expected is no longer live.
        The scheduler stops the schedule locally, which is right: its view is
        behind and it must not keep firing from it.

        A hold deliberately keeps the same control token, since holding a
        schedule does not redefine it. So a stop that ends only on a new token
        cannot end here at all: the release carries the same token, and the
        schedule stays dark with nothing in the dashboard to explain it. What
        moves is the revision, and this asserts the release is what brings the
        schedule back.
        """
        cache = ScheduleCache()
        slot = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=datetime(2026, 4, 26, 14, 55, tzinfo=UTC),
        )
        await cache.upsert(entry)
        # Brain allocates revisions globally, so the hold's is not simply the
        # next one this schedule saw.
        held_revision = entry.schedule_revision + 5

        class RacedByHold(RecordingDispatcher):
            calls = 0

            async def dispatch(self, *, schedule_entry, **_kwargs):
                self.calls += 1
                return FireResult(
                    command_id=None,
                    error_code="stale_control",
                    error_message="schedule control state changed",
                    buffered=False,
                    disposition="stale_control_refresh",
                    live_control_token=schedule_entry.control_token,
                    live_revision=held_revision,
                    live_last_run_at=schedule_entry.last_fire_at,
                    live_next_run_at=slot,
                )

        dispatcher = RacedByHold()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(slot),
            max_sleep_seconds=0.01,
        )

        await engine._iteration()
        await engine._iteration()
        assert dispatcher.calls == 1, "the stop must hold while nothing has changed"
        assert entry.is_enabled is False

        def _echo(*, revision: int, enabled: bool) -> ScheduleEntry:
            echo = self._entry(
                catch_up="skip",
                next_fire_at=slot,
                last_fire_at=entry.last_fire_at,
            )
            echo.id = entry.id
            echo.project_id = entry.project_id
            echo.control_token = entry.control_token
            echo.schedule_revision = revision
            echo.is_enabled = enabled
            return echo

        # An unrelated edit that landed before the hold. It is newer than what
        # this scheduler had and older than what refused the fire, so it is not
        # the state the stop is waiting for and must not end it.
        interim = _echo(revision=held_revision - 2, enabled=True)
        await cache.apply_watch_update(interim)
        assert interim.is_enabled is False, (
            "a stop that ends before the refusing state arrives is not a stop"
        )
        await engine._iteration()
        assert dispatcher.calls == 1

        # Brain projects a hold as not-enabled; there is no separate field.
        held = _echo(revision=held_revision, enabled=False)
        await cache.apply_watch_update(held)
        assert held.is_enabled is False
        await engine._iteration()
        assert dispatcher.calls == 1, "a held schedule must not fire"

        released = _echo(revision=held_revision + 1, enabled=True)
        await cache.apply_watch_update(released)
        assert released.is_enabled is True, (
            "the release carries the same control token, so a token-keyed stop "
            "would clamp it here and the schedule would never tick again"
        )

        await engine._iteration()
        assert dispatcher.calls == 2

    async def test_a_stale_view_resumes_on_the_state_that_refused_it(self) -> None:
        """Nothing is holding this schedule, so nothing further should be needed.

        A refresh means only that this scheduler's view was behind. Once the
        very revision the Brain named has arrived, the schedule is enabled, the
        cursor is Brain's own, and there is no remaining reason not to fire.
        Waiting for a revision beyond it would strand the slot until somebody
        happened to edit the schedule, which is a missed fire caused entirely
        by the scheduler's own bookkeeping.
        """
        cache = ScheduleCache()
        slot = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=datetime(2026, 4, 26, 14, 55, tzinfo=UTC),
        )
        await cache.upsert(entry)
        live_revision = entry.schedule_revision + 3

        class StaleThenAccepted(RecordingDispatcher):
            calls = 0

            async def dispatch(self, *, schedule_entry, prepared_fire, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    return FireResult(
                        command_id=None,
                        error_code="stale_control",
                        error_message="schedule control state changed",
                        buffered=False,
                        disposition="stale_control_refresh",
                        live_control_token=schedule_entry.control_token,
                        live_revision=live_revision,
                        live_last_run_at=schedule_entry.last_fire_at,
                        live_next_run_at=slot,
                    )
                accepted = schedule_entry.schedule_revision + 1
                return FireResult(
                    command_id=uuid4(),
                    error_code=None,
                    error_message=None,
                    buffered=False,
                    disposition="accepted",
                    acceptance_revision=accepted,
                    accepted_last_run_at=prepared_fire.scheduled_for,
                    accepted_next_run_at=prepared_fire.next_run_at,
                    live_control_token=schedule_entry.control_token,
                    live_revision=accepted,
                    live_last_run_at=prepared_fire.scheduled_for,
                    live_next_run_at=prepared_fire.next_run_at,
                )

        dispatcher = StaleThenAccepted()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(slot),
            max_sleep_seconds=0.01,
        )

        await engine._iteration()
        assert dispatcher.calls == 1
        assert entry.is_enabled is False

        caught_up = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=entry.last_fire_at,
        )
        caught_up.id = entry.id
        caught_up.project_id = entry.project_id
        caught_up.control_token = entry.control_token
        caught_up.schedule_revision = live_revision
        await cache.apply_watch_update(caught_up)
        assert caught_up.is_enabled is True

        await engine._iteration()
        assert dispatcher.calls == 2
        assert caught_up.last_fire_at == slot

    async def test_untyped_success_response_does_not_advance(self) -> None:
        cache = ScheduleCache()
        slot = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        entry = self._entry(
            catch_up="skip",
            next_fire_at=slot,
            last_fire_at=datetime(2026, 4, 26, 14, 55, tzinfo=UTC),
        )
        await cache.upsert(entry)

        class UntypedDispatcher(RecordingDispatcher):
            async def dispatch(self, **_kwargs):
                return FireResult(
                    command_id=uuid4(),
                    error_code=None,
                    error_message=None,
                    buffered=False,
                )

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=UntypedDispatcher(),
            clock=ManualClock(slot),
            max_sleep_seconds=0.01,
        )
        await engine._iteration()

        assert entry.schedule_revision == 40
        assert entry.next_fire_at == slot
        assert entry.id in engine._fire_backoff_until


# ---------------------------------------------------------------------------
# Tests - sleep coordination + stop
# ---------------------------------------------------------------------------


class TestSleepAndStop:
    async def test_run_exits_on_stop(self) -> None:
        cache = ScheduleCache()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
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
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
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


# ---------------------------------------------------------------------------
# Round-2 regression tests (RM11 / RM12 / RM13)
# ---------------------------------------------------------------------------


class TestUpdateFireStateCompareAndSet:
    """RM11: a fire-state write guarded by ``expected_definition`` must be
    SKIPPED when the live entry's DEFINITION changed under it (a concurrent
    cadence edit). Otherwise a next_fire_at computed from the OLD cadence is
    stamped onto the freshly-edited schedule."""

    async def test_write_skipped_when_definition_changed(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        old = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(old)
        # Concurrent edit: a NEW definition (1h cadence) replaces the entry.
        await cache.upsert(make_interval_entry(expression="1h", schedule_id=sid))

        stale_next = datetime(2000, 1, 1, tzinfo=UTC)
        applied = await cache.update_fire_state(
            sid,
            next_fire_at=stale_next,
            expected_definition=old,
        )
        assert applied is False
        live = await cache.get(sid)
        assert live is not None
        assert live.expression == "1h"
        assert live.next_fire_at != stale_next  # NOT clobbered by the old cadence

    async def test_write_applies_when_definition_unchanged(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        entry = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(entry)
        when = datetime(2030, 1, 1, tzinfo=UTC)
        applied = await cache.update_fire_state(
            sid,
            next_fire_at=when,
            expected_definition=entry,
        )
        assert applied is True
        live = await cache.get(sid)
        assert live is not None
        assert live.next_fire_at == when

    async def test_same_definition_replacement_does_not_block_engine_advance(self) -> None:
        # M8/H7/L4: the definition-CAS compares only the CADENCE (kind /
        # expression / timezone / catch_up), NOT anchor_at and NOT a monotonic
        # revision. A same-definition replacement -- the benign fire-ack watch
        # echo, which re-sends the identical cadence with an advanced
        # last_run_at/anchor_at -- must NOT block the engine's own authoritative
        # fire-state advance. (The abandoned revision CAS was too strict here:
        # the echo bumped the revision and wrongly skipped the engine's write.)
        cache = ScheduleCache()
        sid = uuid4()
        snap = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(snap)  # the engine's snapshot
        # The ack echo: SAME cadence, re-upserted (a fresh object identity).
        echo = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(echo)
        engine_next = datetime(2031, 6, 1, tzinfo=UTC)
        # The engine's write, snapshotted at `snap`, still APPLIES because the
        # cadence is unchanged -- the engine is authoritative for fire state.
        assert await cache.update_fire_state(
            sid,
            next_fire_at=engine_next,
            expected_definition=snap,
        )
        live = await cache.get(sid)
        assert live is not None
        assert live.next_fire_at == engine_next

    async def test_in_place_fire_state_write_leaves_definition_stable(self) -> None:
        # In-place fire-state writes touch only last_fire_at / next_fire_at, not
        # the cadence, so a second CAS with the SAME snapshot still applies (an
        # advance that arrives in place, not via upsert, never spuriously skips a
        # follow-up write).
        cache = ScheduleCache()
        sid = uuid4()
        entry = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            sid, next_fire_at=datetime(2030, 1, 1, tzinfo=UTC), expected_definition=entry
        )
        # Second CAS with the same snapshot: the definition is still unchanged.
        assert await cache.update_fire_state(
            sid, last_fire_at=datetime(2030, 1, 1, tzinfo=UTC), expected_definition=entry
        )
        live = await cache.get(sid)
        assert live is not None
        assert live.last_fire_at == datetime(2030, 1, 1, tzinfo=UTC)


class TestNextFireComputeCrashGuard:
    """engine:449: a cadence whose next-fire COMPUTATION raises a
    non-*ExpressionError (e.g. OverflowError from timedelta on an oversized
    interval) must NOT crash the tick engine. The per-kind handlers catch only
    their own expression error, so _compute_pending_next_fires guards the call
    and quarantines (disables) the one bad schedule locally instead."""

    async def test_oversized_interval_disables_not_crashes(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        # 99999999999999d overflows timedelta -> OverflowError (not an
        # IntervalExpressionError), the exact case that crashed the engine.
        entry = make_interval_entry(expression="99999999999999d", schedule_id=sid)
        await cache.upsert(entry)  # next_fire_at is None -> compute path runs
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )
        # Previously raised OverflowError out of the engine coroutine; must not.
        await engine._compute_pending_next_fires()
        live = await cache.get(sid)
        assert live is not None
        assert live.is_enabled is False  # quarantined locally, engine survives

    async def test_full_iteration_survives_oversized_interval(self) -> None:
        # End-to-end: a whole _iteration() over a poison schedule does not raise.
        cache = ScheduleCache()
        entry = make_interval_entry(expression="99999999999999d", schedule_id=uuid4())
        await cache.upsert(entry)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )
        await engine._iteration()  # must complete cleanly

    async def test_poison_schedule_logs_once_across_resyncs(self, caplog) -> None:
        # engine:449: the brain does not validate interval magnitude and keeps
        # re-syncing the row is_enabled=True, so a poison schedule keeps coming
        # back next_fire_at=None and re-raising. The traceback must be emitted
        # ONCE per (id, expression), not stormed on every re-sync.
        import logging

        cache = ScheduleCache()
        sid = uuid4()
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=ManualClock(datetime(2026, 4, 26, tzinfo=UTC)),
            max_sleep_seconds=0.01,
        )

        def _count() -> int:
            return sum(
                1 for r in caplog.records if "next-fire computation raised" in r.getMessage()
            )

        with caplog.at_level(logging.ERROR, logger="z4j.scheduler.tick"):
            await cache.upsert(make_interval_entry(expression="99999999999999d", schedule_id=sid))
            await engine._compute_pending_next_fires()
            assert _count() == 1
            # Simulate a brain re-sync: the row returns is_enabled=True,
            # next_fire_at=None, SAME broken expression.
            await cache.upsert(make_interval_entry(expression="99999999999999d", schedule_id=sid))
            await engine._compute_pending_next_fires()
            assert _count() == 1  # throttled: no second traceback
            # An operator edit (DIFFERENT expression, still broken) re-arms the log.
            await cache.upsert(make_interval_entry(expression="88888888888888d", schedule_id=sid))
            await engine._compute_pending_next_fires()
            assert _count() == 2


class TestGracefulStopDrain:
    """engine:344 + engine:328: a stop signalled before/during the due fan-out
    must halt draining (not run every queued entry), and no due id may be left
    stranded in _in_flight (which would leak in a reused engine)."""

    async def test_stop_halts_fanout_and_releases_in_flight(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher()
        now = datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        clock = ManualClock(now)
        for _ in range(50):
            e = make_interval_entry(expression="5m", schedule_id=uuid4())
            e.next_fire_at = now  # all past-due
            await cache.upsert(e)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        # Count _fire_with_catch_up INVOCATIONS (dequeues), not fires. A fire is
        # never dispatched once stop is set (the fire method aborts internally),
        # so fires==0 whether or not the worker-loop stop check (engine:344) is
        # present -- the load-bearing observable is how many entries the workers
        # DEQUEUE. With the fix they exit without dequeuing; with `while True`
        # they dequeue all 50.
        calls = {"n": 0}
        real_fire = engine._fire_with_catch_up

        async def _counting(entry, *, now):
            calls["n"] += 1
            return await real_fire(entry, now=now)

        engine._fire_with_catch_up = _counting  # type: ignore[method-assign]
        await engine.stop()  # signalled BEFORE the iteration
        await engine._iteration()
        # engine:344: workers see the stop and exit WITHOUT dequeuing the 50
        # entries (a reverted `while True` would call _fire_with_catch_up 50x).
        assert calls["n"] == 0
        # engine:328: every pre-added id is released (no leak in a reused engine).
        assert engine._in_flight == set()
        assert dispatcher.fires == []


class TestFireErrorBackoff:
    """P1-8b: a fire that RAISES quarantines (disables) the schedule -- it must
    not re-dispatch a duplicate or fire real tasks at the back-off instants. A
    DISPATCH failure (no task sent) backs off on a SEPARATE deadline and retries
    the ORIGINAL slot with no cadence drift."""

    async def test_fire_raise_quarantines_schedule(self, monkeypatch) -> None:
        cache = ScheduleCache()
        now = datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        clock = ManualClock(now)
        sid = uuid4()
        entry = make_interval_entry(expression="5m", schedule_id=sid)
        entry.next_fire_at = now  # past-due
        await cache.upsert(entry)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
            max_sleep_seconds=0.01,
        )
        calls = {"n": 0}

        async def _boom(_entry, *, now) -> None:
            calls["n"] += 1
            raise OverflowError("pathological cadence")

        monkeypatch.setattr(engine, "_fire_with_catch_up", _boom)

        await engine._iteration()
        # Attempted once, then QUARANTINED (disabled) -- NOT pushed forward
        # (which would fire real tasks at the back-off instants, P1-8b).
        assert calls["n"] == 1
        live = await cache.get(sid)
        assert live is not None
        assert live.is_enabled is False
        assert sid not in engine._fire_error_counts
        assert sid not in engine._fire_backoff_until
        # Disabled -> not re-fired on the next iteration (same clock).
        await engine._iteration()
        assert calls["n"] == 1

    async def test_backoff_widens_on_repeated_dispatch_failure(self) -> None:
        cache = ScheduleCache()
        dispatcher = RecordingDispatcher(raise_count=5)  # dispatch keeps failing
        now = datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        clock = ManualClock(now)
        sid = uuid4()
        entry = make_cron_entry(schedule_id=sid)
        entry.next_fire_at = now
        await cache.upsert(entry)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        # First dispatch failure -> count 1, a back-off deadline.
        await engine._iteration()
        assert engine._fire_error_counts[sid] == 1
        first = (engine._fire_backoff_until[sid] - clock()).total_seconds()
        # Advance past the deadline; retry fails again -> count 2, WIDER back-off.
        clock.advance_to(engine._fire_backoff_until[sid] + timedelta(seconds=1))
        await engine._iteration()
        assert engine._fire_error_counts[sid] == 2
        second = (engine._fire_backoff_until[sid] - clock()).total_seconds()
        assert second > first  # exponential widening
        # next_fire_at was never overwritten by the back-off.
        live = await cache.get(sid)
        assert live.next_fire_at == now

    async def test_error_count_pruned_when_schedule_removed_rm12(self) -> None:
        # RM12: a schedule that errored (leaving a back-off count) and is then
        # REMOVED from the cache (a watch delete) must not leak its count -- the
        # compute pass reconciles the counters against the live cache.
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC))
        sid = uuid4()
        entry = make_interval_entry(schedule_id=sid)
        await cache.upsert(entry)
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        # Simulate a lingering back-off counter for a now-broken schedule.
        engine._fire_error_counts[sid] = 3
        # The watch removes the schedule.
        await cache.remove(sid)
        # A compute pass reconciles against the live cache and prunes it.
        await engine._compute_pending_next_fires()
        assert sid not in engine._fire_error_counts


class TestBoundedFireSpawn:
    """RM13: the number of SPAWNED fire coroutines is bounded by
    ``_MAX_CONCURRENT_FIRES`` regardless of how many schedules are due -- the
    old ``gather(*(_run(e) for e in runnable))`` created one task per due entry
    (a backlog of N parked N - 16 tasks). Every due schedule still fires."""

    async def test_spawned_tasks_bounded_not_one_per_due(self) -> None:
        from z4j_scheduler.tick.engine import _MAX_CONCURRENT_FIRES

        cache = ScheduleCache()
        now = datetime(2026, 4, 26, 15, 0, 0, tzinfo=UTC)
        clock = ManualClock(now)
        release = asyncio.Event()

        class BlockingDispatcher:
            def __init__(self) -> None:
                self.fires: list[UUID] = []
                self.entered = 0

            async def dispatch(self, *, schedule_id, scheduled_for, **_kw) -> None:
                self.entered += 1
                await release.wait()
                self.fires.append(schedule_id)

        dispatcher = BlockingDispatcher()
        n = 200
        for _ in range(n):
            e = make_cron_entry()
            e.next_fire_at = now
            await cache.upsert(e)

        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=clock,
            max_sleep_seconds=0.01,
        )
        tick = asyncio.create_task(engine._iteration())
        # Let the worker pool spin up and block at dispatch.
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

        # The distinguishing property: total live tasks is on the order of the
        # worker cap, NOT one-per-due. The old code would have ~n tasks parked
        # on the semaphore here.
        assert len(asyncio.all_tasks()) <= _MAX_CONCURRENT_FIRES + 10
        # And at most the cap of dispatches are in-flight at once.
        assert dispatcher.entered <= _MAX_CONCURRENT_FIRES

        release.set()
        await asyncio.wait_for(tick, timeout=5.0)
        assert len(dispatcher.fires) == n  # every due schedule fired


class TestWatchEchoMerge:
    """A brain WatchSchedules echo (CREATED/UPDATED) must not clobber
    the engine's authoritative fire-state. apply_watch_update replaces wholesale
    only on a genuinely-new id or a real cadence edit; a same-cadence echo (the
    fire-ack re-send with an advanced anchor and null next_run_at) preserves the
    engine's last_fire_at / next_fire_at / anchor_at."""

    async def test_same_cadence_echo_preserves_engine_fire_state(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        engine_anchor = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        entry = make_interval_entry(expression="1m", schedule_id=sid, anchor_at=engine_anchor)
        await cache.upsert(entry)
        # Engine advances its own fire-state.
        engine_next = datetime(2026, 4, 26, 12, 1, tzinfo=UTC)
        engine_last = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        assert await cache.update_fire_state(
            sid,
            last_fire_at=engine_last,
            next_fire_at=engine_next,
            expected_definition=entry,
        )
        # Fire-ack echo: SAME cadence, advanced anchor (= new last_run_at), and
        # next_fire_at defaulting to None (brain has no next_run_at).
        echo = make_interval_entry(
            expression="1m",
            schedule_id=sid,
            anchor_at=datetime(2026, 4, 26, 12, 0, 0, 250000, tzinfo=UTC),
        )
        assert echo.next_fire_at is None
        await cache.apply_watch_update(echo)
        live = await cache.get(sid)
        assert live is not None
        # Engine fire-state preserved -- NOT clobbered by the echo.
        assert live.next_fire_at == engine_next
        assert live.last_fire_at == engine_last
        assert live.anchor_at == engine_anchor

    async def test_cadence_edit_replaces_wholesale(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        entry = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            sid,
            next_fire_at=datetime(2030, 1, 1, tzinfo=UTC),
            expected_definition=entry,
        )
        # A real cadence edit (1m -> 1h): replace wholesale so the engine
        # recomputes from the new definition (next_fire_at resets to None).
        edited = make_interval_entry(expression="1h", schedule_id=sid)
        await cache.apply_watch_update(edited)
        live = await cache.get(sid)
        assert live is not None
        assert live.expression == "1h"
        assert live.next_fire_at is None  # engine recomputes next tick

    async def test_created_entry_inserted_wholesale(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        fresh = make_interval_entry(expression="30s", schedule_id=sid)
        await cache.apply_watch_update(fresh)  # not previously cached
        live = await cache.get(sid)
        assert live is not None
        assert live.expression == "30s"


class TestWatchEchoLeaderFollowerRH4:
    """The echo-merge preserves engine fire-state only on the LEADER. A
    FOLLOWER adopts brain's advancement (stores the echo wholesale) so it
    converges instead of busy-spinning / replaying after failover."""

    async def test_follower_adopts_echo_state(self) -> None:
        cache = ScheduleCache()
        cache.is_leader = lambda _pid: False  # this instance is a FOLLOWER
        sid = uuid4()
        entry = make_interval_entry(expression="1m", schedule_id=sid)
        await cache.upsert(entry)
        # Follower's stale local fire-state.
        assert await cache.update_fire_state(
            sid,
            next_fire_at=datetime(2026, 5, 1, 12, 1, tzinfo=UTC),
            expected_definition=entry,
        )
        # Leader fired 12:01; brain echo: same cadence, advanced anchor, no
        # next_run_at (None).
        echo = make_interval_entry(
            expression="1m",
            schedule_id=sid,
            anchor_at=datetime(2026, 5, 1, 12, 1, 0, 250000, tzinfo=UTC),
        )
        await cache.apply_watch_update(echo)
        live = await cache.get(sid)
        assert live is not None
        # Adopted the echo -- NOT preserved. next_fire_at is None (recomputed
        # forward next tick), anchor is the brain-advanced one.
        assert live.next_fire_at is None
        assert live.anchor_at == datetime(2026, 5, 1, 12, 1, 0, 250000, tzinfo=UTC)

    async def test_leader_preserves_engine_state(self) -> None:
        cache = ScheduleCache()
        cache.is_leader = lambda _pid: True  # this instance is the LEADER
        sid = uuid4()
        anchor = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        entry = make_interval_entry(expression="1m", schedule_id=sid, anchor_at=anchor)
        await cache.upsert(entry)
        engine_next = datetime(2026, 4, 26, 12, 1, tzinfo=UTC)
        assert await cache.update_fire_state(
            sid,
            next_fire_at=engine_next,
            expected_definition=entry,
        )
        echo = make_interval_entry(
            expression="1m",
            schedule_id=sid,
            anchor_at=datetime(2026, 4, 26, 12, 0, 0, 250000, tzinfo=UTC),
        )
        await cache.apply_watch_update(echo)
        live = await cache.get(sid)
        assert live is not None
        # Leader is authoritative -> engine fire-state preserved.
        assert live.next_fire_at == engine_next
        assert live.anchor_at == anchor


class TestFollowerNoHotLoopR7:
    """H8: a NON-leader (follower) must not hot-loop on a past-due
    slot -- but it must also NOT mutate the authoritative fire-state. It PARKS the
    entry on its current next_fire_at (every cadence type), leaving next_fire_at
    and last_fire_at exactly as persisted, so a promoted leader fires the REAL
    slot and its catch_up backlog rather than a follower-rewritten phantom."""

    async def test_follower_interval_parks_not_rewrites(self) -> None:
        # A follower must NOT rewrite an interval's next_fire_at onto a
        # now-based grid (the anchor-on-now bug). It parks the REAL
        # past-due slot, leaving next_fire_at + last_fire_at untouched.
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 5, 1, 12, 30, 5, tzinfo=UTC))
        entry = make_interval_entry(
            expression="1m", anchor_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        )
        await cache.upsert(entry)
        past_slot = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
        # A behind follower: next_fire_at is a PAST slot.
        assert await cache.update_fire_state(
            entry.id,
            next_fire_at=past_slot,
            expected_definition=entry,
        )
        engine = TickEngine(
            cache=cache,
            leader_gate=NeverLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        live = await cache.get(entry.id)
        assert live is not None
        # NOT advanced / rewritten -- the real grid slot is preserved.
        assert live.next_fire_at == past_slot
        assert live.last_fire_at is None  # follower did NOT fire -> anchor kept
        # Parked so the follower does not hot-loop on it.
        assert engine._follower_parked.get(entry.id) == past_slot
        assert engine._is_follower_parked(live) is True

    async def test_follower_one_shot_parks_not_spins(self) -> None:
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 5, 1, 13, 0, tzinfo=UTC))
        target = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)  # past-due
        entry = make_one_shot_entry(expression=target.isoformat())
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            entry.id, next_fire_at=target, expected_definition=entry
        )
        engine = TickEngine(
            cache=cache,
            leader_gate=NeverLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        live = await cache.get(entry.id)
        assert live is not None
        # one_shot cannot advance -> PARKED (next_fire_at kept for promotion).
        assert live.next_fire_at == target
        assert engine._follower_parked.get(entry.id) == target
        # The due filter / sleep will skip it while a follower.
        assert engine._is_follower_parked(live) is True

    async def test_promotion_releases_the_park(self) -> None:
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 5, 1, 13, 0, tzinfo=UTC))
        target = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        pid = uuid4()
        entry = make_one_shot_entry(expression=target.isoformat(), project_id=pid)
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            entry.id, next_fire_at=target, expected_definition=entry
        )
        gate = PerProjectLeader(set())  # start as follower
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        live = await cache.get(entry.id)
        assert engine._is_follower_parked(live) is True
        # Promote: the leader must be able to fire the parked slot.
        gate._leader_for.add(pid)
        assert engine._is_follower_parked(live) is False
        assert entry.id not in engine._follower_parked  # park released

    async def test_follower_handoff_fires_on_time_r9_m4(self) -> None:
        # A slot a follower observed live-due and handed off
        # fires ON-TIME on promotion when its lateness is within the
        # PROMOTION-SCOPED grace (promotion-detection latency), even though it is
        # past the base 5s on-time grace -- catch_up (which governs cluster
        # outages, not a live handoff) must NOT classify it missed and drop it.
        cache = ScheduleCache()
        # 20s late: past the 5s base grace, still within the promotion grace at
        # DEFAULT settings.: this value guards against the derivation
        # tightening the default deployment below its inherited 30s window.
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        clock = ManualClock(datetime(2026, 5, 1, 12, 0, 20, tzinfo=UTC))
        pid = uuid4()
        # catch_up="skip" would DROP a genuinely-missed slot -- so a dispatch
        # proves the handoff on-time path, not catch-up.
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())  # follower
        dispatcher = RecordingDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        # Follower observes it due -> parks + records the handoff.
        await engine._advance_after_fire(entry, last_fire_at=None)
        assert engine._follower_handoff.get(entry.id)[0] == slot
        # Promote, then fire: despite skip + lateness>grace, the handoff fires it
        # ON-TIME exactly once and consumes the record.
        gate._leader_for.add(pid)
        live = await cache.get(entry.id)
        ok = await engine._fire_with_catch_up(live, now=clock())
        assert ok
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].scheduled_for == slot
        assert entry.id not in engine._follower_handoff  # consumed

    async def test_handoff_marker_dropped_when_the_cadence_is_edited_r13(
        self,
    ) -> None:
        # The handoff marker recorded only the schedule
        # id and the slot. An edit that changes the CADENCE while leaving
        # next_fire_at untouched left the marker intact, so on promotion the
        # slot fired "on time" even though it is not an occurrence of the edited
        # schedule at all. The marker now carries a cadence identity and is
        # pruned when that changes.
        from z4j_scheduler.tick._entry import schedule_cadence_identity

        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        clock = ManualClock(datetime(2026, 5, 1, 12, 0, 20, tzinfo=UTC))
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.expression = "0 * * * *"
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())  # follower
        dispatcher = RecordingDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        recorded = engine._follower_handoff.get(entry.id)
        assert recorded is not None and recorded[0] == slot

        # The schedule is edited to a DIFFERENT cadence, but next_fire_at is
        # unchanged -- which is exactly what defeated the old slot-only guard.
        edited = await cache.get(entry.id)
        edited.expression = "30 * * * *"
        await cache.upsert(edited)
        assert edited.next_fire_at == slot
        assert schedule_cadence_identity(edited) != recorded[1]

        engine._prune_stale_fire_state([edited])
        assert entry.id not in engine._follower_handoff, (
            "a cadence edit must drop the handoff marker; keeping it fires a "
            "slot that is no longer on the schedule"
        )

    async def test_failed_handoff_dispatch_keeps_its_entitlement_r14(self) -> None:
        # The handoff classification was recomputed on
        # every attempt against a moving clock. A slot judged on-time at the
        # first promoted attempt could exceed the grace by the retry a second
        # later, and under catch_up="skip" the retry then produced an EMPTY plan,
        # advanced past the slot, and consumed the marker WITHOUT dispatching it.
        # A dispatch failure must not silently change what the slot is.
        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        # First attempt sits just inside the default 30s promotion grace.
        clock = ManualClock(datetime(2026, 5, 1, 12, 0, 29, tzinfo=UTC))
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())
        dispatcher = RecordingDispatcher(raise_count=1)  # first dispatch fails
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        gate._leader_for.add(pid)
        live = await cache.get(entry.id)

        assert await engine._fire_with_catch_up(live, now=clock()) is False
        # The clock moves PAST the grace before the retry.
        clock.advance_to(datetime(2026, 5, 1, 12, 0, 45, tzinfo=UTC))
        assert await engine._fire_with_catch_up(live, now=clock()) is True
        assert len(dispatcher.fires) == 1, (
            "the retry let the slot age out of its grace and skipped it; a "
            "failed dispatch must not change the slot's classification"
        )
        assert dispatcher.fires[0].scheduled_for == slot

    async def test_disable_invalidates_the_handoff_marker_r14(self) -> None:
        # A follower parks slot T, the schedule is
        # disabled and re-enabled with the same cadence and the same
        # next_fire_at, and on promotion T fired "on time" -- even though
        # catch_up="skip" would discard it and the operator had switched the
        # schedule off across that very slot. An observed disable is an explicit
        # statement that this occurrence should not run.
        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        clock = ManualClock(datetime(2026, 5, 1, 12, 0, 20, tzinfo=UTC))
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        assert engine._follower_handoff.get(entry.id) is not None

        disabled = await cache.get(entry.id)
        disabled.is_enabled = False
        engine._prune_stale_fire_state([disabled])
        assert entry.id not in engine._follower_handoff, (
            "a disable must drop the handoff marker; keeping it fires an "
            "occurrence the operator switched the schedule off across"
        )
        assert entry.id not in engine._handoff_entitled

    async def test_derived_grace_never_tightens_the_default_r12(self) -> None:
        # Deriving the promotion grace from the heartbeat must only ever
        # RAISE the window. Derived-from-heartbeat alone gave the DEFAULT
        # deployment 12s (base 5 + heartbeat 2 + recheck 5), which is STRICTER
        # than the 30s this code shipped with, so a ~20s handoff that used to
        # fire was suddenly dropped under catch_up="skip".
        from z4j_scheduler.tick.engine import _promotion_grace_for

        default = _promotion_grace_for(leader_heartbeat_seconds=2.0, follower_recheck_seconds=5.0)
        assert default >= 30.0, "the derivation must not tighten the default window"
        # ...while a slow heartbeat still widens it (the HIGH-1 fix).
        slow = _promotion_grace_for(leader_heartbeat_seconds=60.0, follower_recheck_seconds=5.0)
        assert slow > default and slow >= 65.0

    async def test_promotion_grace_covers_the_configured_leader_heartbeat_r11(
        self,
    ) -> None:
        # The promotion grace must be DERIVED from the deployment's
        # leader heartbeat, not hard-coded. With the supported maximum heartbeat
        # (60s), a follower-parked slot that ages ~60s during failover is still
        # an on-time handoff and must fire, even under catch_up="skip". A fixed
        # 30s grace dropped it.
        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        clock = ManualClock(datetime(2026, 5, 1, 12, 1, 0, tzinfo=UTC))  # 60s late
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())
        dispatcher = RecordingDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
            leader_heartbeat_seconds=60.0,  # the supported maximum
        )
        await engine._advance_after_fire(entry, last_fire_at=None)  # park + mark
        assert engine._follower_handoff.get(entry.id)[0] == slot
        gate._leader_for.add(pid)
        live = await cache.get(entry.id)
        assert await engine._fire_with_catch_up(live, now=clock())
        assert len(dispatcher.fires) == 1
        assert dispatcher.fires[0].scheduled_for == slot

    async def test_follower_handoff_beyond_promotion_grace_defers_to_catch_up_r10_h13(
        self,
    ) -> None:
        # A handoff slot OLDER than the promotion grace is genuinely
        # missed, so catch_up governs it. The old force-on-time fired an
        # arbitrarily-old parked slot on-time and bypassed catch_up entirely.
        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        # 1 hour late -- far past any realistic promotion latency.
        clock = ManualClock(datetime(2026, 5, 1, 13, 0, 0, tzinfo=UTC))
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())
        dispatcher = RecordingDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)  # park + mark
        assert engine._follower_handoff.get(entry.id)[0] == slot
        gate._leader_for.add(pid)
        live = await cache.get(entry.id)
        ok = await engine._fire_with_catch_up(live, now=clock())
        assert ok
        # skip governs a genuinely-missed slot -> NOT fired (not force-on-time).
        assert dispatcher.fires == []

    async def test_follower_handoff_marker_survives_failed_dispatch_r10_h12(
        self,
    ) -> None:
        # The handoff marker is consumed only AFTER a successful
        # dispatch. A dispatch failure (return False) must PRESERVE it so the
        # retry still treats the slot as an on-time handoff, not a missed slot to
        # skip.
        cache = ScheduleCache()
        slot = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        clock = ManualClock(datetime(2026, 5, 1, 12, 0, 20, tzinfo=UTC))  # within grace
        pid = uuid4()
        entry = make_cron_entry(catch_up="skip", project_id=pid)
        entry.next_fire_at = slot
        await cache.upsert(entry)
        assert await cache.update_fire_state(entry.id, next_fire_at=slot, expected_definition=entry)
        gate = PerProjectLeader(set())
        dispatcher = RecordingDispatcher(raise_count=1)  # first dispatch fails
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        assert engine._follower_handoff.get(entry.id)[0] == slot
        gate._leader_for.add(pid)
        live = await cache.get(entry.id)
        # First attempt: dispatch raises -> False, marker MUST survive.
        ok = await engine._fire_with_catch_up(live, now=clock())
        assert ok is False
        assert engine._follower_handoff.get(entry.id)[0] == slot  # H12: preserved
        # Retry: dispatch succeeds -> fired on-time, marker now consumed.
        ok2 = await engine._fire_with_catch_up(live, now=clock())
        assert ok2 is True
        assert len(dispatcher.fires) == 1
        assert entry.id not in engine._follower_handoff  # consumed after success

    async def test_backlog_drain_capped_per_tick_for_fairness_r10(self) -> None:
        # (Fairness): one schedule's fire_all_missed backlog drains at most
        # _MAX_DISPATCH_PER_TICK slots per tick, so it cannot monopolise the
        # engine and starve other due schedules; the remainder is re-evaluated on
        # the next tick.
        from z4j_scheduler.tick.engine import _MAX_DISPATCH_PER_TICK

        cache = ScheduleCache()
        pid = uuid4()
        n_over = _MAX_DISPATCH_PER_TICK + 20  # backlog bigger than one tick's cap
        slot0 = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        now = slot0 + timedelta(minutes=n_over)
        clock = ManualClock(now)
        entry = make_cron_entry(
            expression="* * * * *",
            catch_up="fire_all_missed",
            project_id=pid,
            last_fire_at=slot0,
        )
        entry.next_fire_at = slot0 + timedelta(minutes=1)
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            entry.id,
            last_fire_at=slot0,
            next_fire_at=slot0 + timedelta(minutes=1),
            expected_definition=entry,
        )
        gate = PerProjectLeader({pid})  # leader
        dispatcher = RecordingDispatcher()
        engine = TickEngine(
            cache=cache,
            leader_gate=gate,
            dispatcher=dispatcher,
            clock=clock,
        )
        # First tick: only a bounded chunk is dispatched.
        live = await cache.get(entry.id)
        assert await engine._fire_with_catch_up(live, now=clock())
        assert len(dispatcher.fires) == _MAX_DISPATCH_PER_TICK
        # Next tick drains the remainder (all slots fired across two ticks).
        live2 = await cache.get(entry.id)
        assert await engine._fire_with_catch_up(live2, now=clock())
        assert len(dispatcher.fires) == n_over

    async def test_prune_cleans_stale_park_marker_r8_l1(self) -> None:
        # A parked marker must be dropped once the entry's next_fire_at no
        # longer matches the parked slot (the leader advanced it, or a fired
        # one_shot's echo set it to None). The due-filter/sleep skip a
        # next_fire_at=None entry BEFORE the self-clean in _is_follower_parked, so
        # _prune_stale_fire_state has to handle it or the marker leaks forever.
        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 5, 1, 13, 0, tzinfo=UTC))
        target = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        entry = make_one_shot_entry(expression=target.isoformat())
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            entry.id, next_fire_at=target, expected_definition=entry
        )
        engine = TickEngine(
            cache=cache,
            leader_gate=NeverLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
        )
        await engine._advance_after_fire(entry, last_fire_at=None)
        assert entry.id in engine._follower_parked
        # The leader advances the entry to a DIFFERENT slot (echo). The stale park
        # (old slot) must be pruned so it does not suppress the new slot.
        new_slot = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)
        assert await cache.update_fire_state(
            entry.id, next_fire_at=new_slot, expected_definition=entry
        )
        engine._prune_stale_fire_state(await cache.snapshot())
        assert entry.id not in engine._follower_parked  # stale marker cleaned

    async def test_sleep_caps_timeout_when_parked_r8_m3(self) -> None:
        # A promotion is not signalled via cache.changed, and parked
        # entries are excluded from the wake computation, so the sleep is capped
        # to the re-check interval whenever something is parked -- otherwise a
        # just-promoted follower would wait the full max-sleep before firing.
        import time

        cache = ScheduleCache()
        clock = ManualClock(datetime(2026, 5, 1, 13, 0, tzinfo=UTC))
        target = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        entry = make_one_shot_entry(expression=target.isoformat())
        await cache.upsert(entry)
        assert await cache.update_fire_state(
            entry.id, next_fire_at=target, expected_definition=entry
        )
        engine = TickEngine(
            cache=cache,
            leader_gate=NeverLeader(),
            dispatcher=RecordingDispatcher(),
            clock=clock,
            max_sleep_seconds=300.0,
        )
        engine._follower_recheck_seconds = 0.05
        await engine._advance_after_fire(entry, last_fire_at=None)
        # Parking (via update_fire_state) leaves cache.changed SET, which
        # would end the sleep immediately and pass even if the cap were removed.
        # Clear it so the ONLY way the sleep can end quickly is the re-check cap,
        # then assert a TWO-SIDED bound -- the lower bound proves the sleep
        # actually waited the cap (removing the cap makes it wait the 300s
        # max_sleep and the test hangs/fails).
        engine._cache.changed.clear()
        t0 = time.monotonic()
        await engine._sleep_until_next()
        elapsed = time.monotonic() - t0
        # Capped to ~recheck interval (0.05s), NOT the 300s max_sleep.
        assert 0.04 <= elapsed < 2.0, elapsed
