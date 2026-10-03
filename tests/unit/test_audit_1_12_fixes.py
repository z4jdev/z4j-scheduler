"""Release-audit fixes: the watch outage clock, the cost of schedule counts,
the arq weekday base, the taskiq cron offset and the discard accounting.

Each class pins one finding the way the audit framed it: the failing case
next to a control that already passed, so a regression and a fix that
over-reaches both show.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import ModuleType
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.exporters._client import ExportedSchedule
from z4j_scheduler.importers._core import ImportedSchedule
from z4j_scheduler.observability import metrics as m
from z4j_scheduler.storage._models import CursorTransitionResult, ScheduleEvent
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.watch import WatchStream
from z4j_scheduler.tick._entry import ScheduleEntry

from .test_engine import AlwaysLeader, ManualClock, RecordingDispatcher


def _entry(
    *,
    project_id: UUID | None = None,
    schedule_id: UUID | None = None,
    kind: str = "cron",
) -> ScheduleEntry:
    return ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind=kind,  # type: ignore[arg-type]
        expression="0 * * * *" if kind == "cron" else "60",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
    )


def _created(project_id: UUID, token: str) -> ScheduleEvent:
    return ScheduleEvent(
        kind="created",
        schedule=_entry(project_id=project_id),
        deleted_id=None,
        resume_token=token,
    )


def _loaded(project_id: UUID, kind: str) -> float | None:
    return m.default_registry.get_sample_value(
        "z4j_scheduler_schedules_loaded",
        {"project": str(project_id), "kind": kind},
    )


# ---------------------------------------------------------------------------
# Fake brain clients
# ---------------------------------------------------------------------------


@dataclass
class ListOkWatchRejected:
    """Brain reachable (the full sync lists fine) but Watch refused before any
    frame, the shape of a brain shedding load at its capacity cap.

    ``idle_before_drop`` lets the second and later streams stay open, with
    nothing to send, for that long on the injected clock before dropping.
    """

    list_entries: list[ScheduleEntry] = field(default_factory=list)
    watch_calls: int = 0
    idle_before_drop: float = 0.0
    clock: list[float] = field(default_factory=lambda: [0.0])

    async def list_schedules(self, project_id: UUID | None = None) -> AsyncIterator[ScheduleEntry]:
        for entry in self.list_entries:
            yield entry

    async def watch_schedules(
        self,
        project_id: UUID | None = None,
        *,
        resume_token: str = "",
    ) -> AsyncIterator[ScheduleEvent]:
        self.watch_calls += 1
        if self.watch_calls > 1:
            self.clock[0] += self.idle_before_drop
        raise RuntimeError("RESOURCE_EXHAUSTED: watch capacity cap")
        yield  # pragma: no cover


@dataclass
class ListFails:
    """Brain unreachable: the full sync itself fails."""

    async def list_schedules(self, project_id: UUID | None = None) -> AsyncIterator[ScheduleEntry]:
        raise RuntimeError("UNAVAILABLE")
        yield  # pragma: no cover

    async def watch_schedules(
        self,
        project_id: UUID | None = None,
        *,
        resume_token: str = "",
    ) -> AsyncIterator[ScheduleEvent]:
        raise AssertionError("never reached")
        yield  # pragma: no cover


@dataclass
class ServesThenDrops:
    """Watch opens, delivers ``stream_events``, then drops."""

    list_entries: list[ScheduleEntry] = field(default_factory=list)
    stream_events: list[ScheduleEvent] = field(default_factory=list)

    async def list_schedules(self, project_id: UUID | None = None) -> AsyncIterator[ScheduleEntry]:
        for entry in self.list_entries:
            yield entry

    async def watch_schedules(
        self,
        project_id: UUID | None = None,
        *,
        resume_token: str = "",
    ) -> AsyncIterator[ScheduleEvent]:
        for event in self.stream_events:
            yield event
        raise RuntimeError("stream dropped")


async def _drive(
    client: object,
    *,
    cycles: int,
    backoff_s: float,
    clock: list[float],
) -> tuple[WatchStream, list[float]]:
    """Run the watch loop for ``cycles`` reconnects, reading the outage clock
    at each backoff before advancing the clock by ``backoff_s``."""
    watch = WatchStream(
        client=client,  # type: ignore[arg-type]
        cache=ScheduleCache(),
        project_id=uuid4(),
        full_resync_interval_seconds=0,
        clock=lambda: clock[0],
    )
    readings: list[float] = []
    count = 0

    async def backoff() -> None:
        nonlocal count
        count += 1
        readings.append(watch.unhealthy_for_seconds() or 0.0)
        clock[0] += backoff_s
        if count >= cycles:
            await watch.stop()

    watch._backoff_or_stop = backoff  # type: ignore[method-assign]
    await watch._watch_loop()
    return watch, readings


# ---------------------------------------------------------------------------
# 1. The outage clock survives a refused Watch
# ---------------------------------------------------------------------------


class TestWatchOutageClock:
    """A Watch refused after a successful full sync is the same outage.

    The full sync proves the brain reachable, not the stream served. Before
    the fix ``_mark_healthy`` cleared the clock at open, so a brain that
    listed schedules and refused every Watch read as a series of fresh
    thirty-second blips, under the grace, and ``/ready`` never tripped.
    """

    async def test_a_refused_watch_after_a_successful_sync_keeps_one_outage(self) -> None:
        clock = [1_000.0]
        client = ListOkWatchRejected(list_entries=[_entry()], clock=clock)

        watch, readings = await _drive(client, cycles=6, backoff_s=30.0, clock=clock)

        assert client.watch_calls == 6
        assert readings == pytest.approx([0.0, 30.0, 60.0, 90.0, 120.0, 150.0])
        assert watch.unhealthy_for_seconds() == pytest.approx(180.0)
        assert watch.is_healthy is False

    async def test_a_failing_sync_reads_the_same_way(self) -> None:
        """Control: the outage the brain-unreachable case already timed."""
        clock = [1_000.0]

        watch, readings = await _drive(ListFails(), cycles=6, backoff_s=30.0, clock=clock)

        assert readings == pytest.approx([0.0, 30.0, 60.0, 90.0, 120.0, 150.0])
        assert watch.unhealthy_for_seconds() == pytest.approx(180.0)

    async def test_a_served_stream_that_drops_starts_a_fresh_outage(self) -> None:
        """Control: one frame proves the brain served the stream."""
        clock = [1_000.0]
        project_id = uuid4()
        client = ServesThenDrops(stream_events=[_created(project_id, "t1")])

        watch, readings = await _drive(client, cycles=3, backoff_s=30.0, clock=clock)

        # Each drop is a new outage: the reading at every backoff is zero.
        assert readings == pytest.approx([0.0, 0.0, 0.0])
        assert watch.unhealthy_for_seconds() == pytest.approx(30.0)

    @pytest.mark.parametrize(
        ("idle_for", "expected"),
        [
            # Dropped inside the reconnect ceiling with nothing served: refused.
            (29.0, [0.0, 59.0]),
            # Outlived the ceiling before dropping: a real stream, a new outage.
            (31.0, [0.0, 0.0]),
        ],
    )
    async def test_an_idle_stream_is_refused_only_inside_the_ceiling(
        self,
        idle_for: float,
        expected: list[float],
    ) -> None:
        clock = [1_000.0]
        client = ListOkWatchRejected(
            list_entries=[_entry()],
            clock=clock,
            idle_before_drop=idle_for,
        )

        _, readings = await _drive(client, cycles=2, backoff_s=30.0, clock=clock)

        assert readings == pytest.approx(expected)


# ---------------------------------------------------------------------------
# 4. Schedule counts move; they do not walk
# ---------------------------------------------------------------------------


class TestScheduleCountsScale:
    async def test_ten_thousand_events_move_counts_without_walking_the_cache(self) -> None:
        """A membership change moves one count; only a full sync walks.

        The walk cost 5 ms at ten thousand cached schedules, so an import of
        that size streamed as events cost the watch task about a minute.
        """
        project_id = uuid4()
        cache = ScheduleCache()
        client = ServesThenDrops(
            stream_events=[_created(project_id, str(i)) for i in range(10_000)],
        )
        watch = WatchStream(client=client, cache=cache, project_id=project_id)  # type: ignore[arg-type]
        await watch._full_sync()

        walks = 0
        original_snapshot = cache.snapshot

        async def counting_snapshot() -> list[ScheduleEntry]:
            nonlocal walks
            walks += 1
            return await original_snapshot()

        cache.snapshot = counting_snapshot  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="stream dropped"):
            await watch._stream()

        assert len(cache) == 10_000
        assert walks == 0
        assert _loaded(project_id, "cron") == 10_000.0

        # Control: the full sync still walks, once for its delete-sweep set
        # and once to publish, and its count is the cache's (the empty
        # listing sweeps every row).
        await watch._full_sync()
        assert walks == 2
        assert _loaded(project_id, "cron") is None


# ---------------------------------------------------------------------------
# 2. arq weekdays count from Monday
# ---------------------------------------------------------------------------


def _exported(expression: str, *, engine: str, timezone: str = "UTC") -> ExportedSchedule:
    return ExportedSchedule(
        id=str(uuid4()),
        name="weekly",
        engine=engine,
        kind="cron",
        expression=expression,
        task_name="pkg.job",
        timezone=timezone,
        queue=None,
        args=[],
        kwargs={},
        is_enabled=True,
    )


class TestArqWeekdayBase:
    """arq matches ``dt.weekday()`` (Monday is 0); cron's 0 is Sunday."""

    def test_integer_weekdays_shift_to_the_cron_base(self) -> None:
        pytest.importorskip("arq")
        from arq import cron
        from z4j_scheduler.importers.arq import _cron_job_to_cron_string

        async def job(ctx: object) -> None:  # pragma: no cover
            return None

        assert _cron_job_to_cron_string(cron(job, weekday=0, hour=8, minute=0)) == "0 8 * * 1"
        assert _cron_job_to_cron_string(cron(job, weekday=6, hour=8, minute=0)) == "0 8 * * 0"
        assert (
            _cron_job_to_cron_string(cron(job, weekday={0, 1, 2, 3, 4}, hour=9, minute=30))
            == "30 9 * * 1,2,3,4,5"
        )
        # Controls: an alias names its day and needs no shift, and an unset
        # weekday is still every day.
        assert _cron_job_to_cron_string(cron(job, weekday="mon", hour=8, minute=0)) == "0 8 * * 1"
        assert _cron_job_to_cron_string(cron(job, weekday="SUN", hour=8, minute=0)) == "0 8 * * 0"
        assert _cron_job_to_cron_string(cron(job, hour=8, minute=0)) == "0 8 * * *"

    def test_the_imported_expression_fires_on_arq_day(self) -> None:
        """The day arq would fire on and the day the expression names agree."""
        pytest.importorskip("arq")
        croniter = pytest.importorskip("croniter")
        from arq.cron import next_cron
        from z4j_scheduler.importers.arq import _arq_weekday_to_cron

        saturday = datetime(2026, 10, 3, 0, 0, 0)
        assert saturday.strftime("%A") == "Saturday"
        for weekday in range(7):
            arq_day = next_cron(
                saturday,
                month=None,
                day=None,
                weekday=weekday,
                hour=8,
                minute=0,
                second=0,
                microsecond=0,
            )
            expression = f"0 8 * * {_arq_weekday_to_cron(weekday)}"
            cron_day = croniter.croniter(expression, saturday).get_next(datetime)
            assert cron_day.strftime("%A") == arq_day.strftime("%A"), weekday

    def test_export_shifts_back_and_expands_a_range(self) -> None:
        from z4j_scheduler.exporters import arq as exp

        assert "weekday=0" in exp.render([_exported("0 8 * * 1", engine="arq")])
        assert "weekday=6" in exp.render([_exported("0 8 * * 0", engine="arq")])
        assert "weekday=6" in exp.render([_exported("0 8 * * 7", engine="arq")])
        assert "weekday={0, 1, 2, 3, 4}" in exp.render(
            [_exported("30 9 * * 1,2,3,4,5", engine="arq")],
        )
        assert "weekday={0, 1, 2, 3, 4}" in exp.render([_exported("30 9 * * 1-5", engine="arq")])
        # Controls: every day is None, and the other fields are not shifted.
        every_day = exp.render([_exported("0 8 * * *", engine="arq")])
        assert "weekday=None" in every_day
        assert "hour=8" in every_day

    def test_round_trip_keeps_the_days(self) -> None:
        pytest.importorskip("arq")
        from arq import cron
        from z4j_scheduler.exporters import arq as exp
        from z4j_scheduler.importers.arq import _cron_job_to_cron_string

        async def job(ctx: object) -> None:  # pragma: no cover
            return None

        imported = _cron_job_to_cron_string(cron(job, weekday={0, 1, 2, 3, 4}, hour=9, minute=30))
        out = exp.render([_exported(imported, engine="arq")])
        assert "weekday={0, 1, 2, 3, 4}" in out

        imported = _cron_job_to_cron_string(cron(job, weekday=6, hour=9, minute=30))
        out = exp.render([_exported(imported, engine="arq")])
        assert "weekday=6" in out


# ---------------------------------------------------------------------------
# 3. taskiq cron_offset is the schedule timezone
# ---------------------------------------------------------------------------


class TestTaskiqCronOffset:
    def test_import_carries_cron_offset_as_the_timezone(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from z4j_scheduler.importers.taskiq import _label_entry_to_schedule

        def imported(entry: dict[str, object]) -> ImportedSchedule:
            schedule = _label_entry_to_schedule(
                task_name="pkg.nightly",
                entry=entry,
                idx=0,
                project_slug="p",
                engine="taskiq",
                default_queue=None,
                default_timezone="UTC",
            )
            assert schedule is not None
            return schedule

        assert imported({"cron": "0 9 * * *", "cron_offset": "Europe/Berlin"}).timezone == (
            "Europe/Berlin"
        )
        # Control: without an offset taskiq evaluates in UTC and so does the
        # importer's default.
        assert imported({"cron": "0 9 * * *"}).timezone == "UTC"
        # A timedelta offset names no zone: the default applies and is logged.
        with caplog.at_level(logging.WARNING, logger="z4j.scheduler.importers.taskiq"):
            assert imported({"cron": "0 9 * * *", "cron_offset": timedelta(hours=2)}).timezone == (
                "UTC"
            )
        assert "cron_offset" in caplog.text

    def test_import_reads_the_offset_off_a_real_broker(self) -> None:
        pytest.importorskip("taskiq")
        from taskiq import InMemoryBroker
        from z4j_scheduler.importers.taskiq import read_taskiq_broker

        broker = InMemoryBroker()

        @broker.task(schedule=[{"cron": "0 9 * * *", "cron_offset": "Europe/Berlin"}])
        async def nightly() -> None:  # pragma: no cover
            return None

        module = ModuleType("z4j_test_taskiq_offset")
        module.broker = broker  # type: ignore[attr-defined]
        sys.modules["z4j_test_taskiq_offset"] = module
        try:
            schedules = read_taskiq_broker(
                broker_path="z4j_test_taskiq_offset:broker",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_taskiq_offset"]

        assert [s.timezone for s in schedules] == ["Europe/Berlin"]

    def test_export_emits_the_timezone_as_cron_offset(self) -> None:
        from z4j_scheduler.exporters import taskiq as exp

        out = exp.render([_exported("0 9 * * *", engine="taskiq", timezone="Europe/Berlin")])
        assert '"cron": "0 9 * * *"' in out
        assert '"cron_offset": "Europe/Berlin"' in out


# ---------------------------------------------------------------------------
# 5. A gap is one transition; the metric counts its slots
# ---------------------------------------------------------------------------


class TestDiscardAccounting:
    """The brain's ``skipped_slots_24h`` counts ``skip_no_work`` transitions.

    The scheduler records one transition per gap, carrying the last slot it
    moved past and no slot count, so that field reads gaps; the slots are
    counted in ``z4j_scheduler_slots_discarded_total``. This pins the two
    numbers for a three-slot gap, which the documentation now states.
    """

    async def test_a_three_slot_gap_is_one_transition_and_three_slots(self) -> None:
        from z4j_scheduler.tick._prepared import PreparedFire
        from z4j_scheduler.tick.engine import TickEngine

        t0 = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        entry = ScheduleEntry(
            id=uuid4(),
            project_id=uuid4(),
            kind="interval",
            expression="5m",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=t0,
            last_fire_at=t0,
        )
        entry.control_token = uuid4()
        entry.schedule_revision = 40
        entry.definition_digest = "d" * 64
        entry.cadence_semantics_version = 1
        entry.cadence_runtime_fingerprint = "f" * 64
        entry.next_fire_at = t0 + timedelta(minutes=5)
        cache = ScheduleCache()
        await cache.upsert(entry)

        @dataclass
        class RecordingTransitions(RecordingDispatcher):
            skipped_through: list[datetime] = field(default_factory=list)

            async def advance_cursor(
                self,
                *,
                entry: ScheduleEntry,
                prepared: PreparedFire,
            ) -> CursorTransitionResult:
                self.skipped_through.append(prepared.scheduled_for)
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

        dispatcher = RecordingTransitions()
        # Three slots (+5, +10, +15 min) are due and the newest is a minute
        # late, past the default grace: all three are missed.
        engine = TickEngine(
            cache=cache,
            leader_gate=AlwaysLeader(),
            dispatcher=dispatcher,
            clock=ManualClock(t0 + timedelta(minutes=16)),
            max_sleep_seconds=0.01,
        )
        discarded_before = m.slots_discarded_total.labels(catch_up="skip")._value.get()

        await engine._iteration()

        assert dispatcher.fires == []
        assert dispatcher.skipped_through == [t0 + timedelta(minutes=15)]
        assert m.slots_discarded_total.labels(catch_up="skip")._value.get() - discarded_before == 3
