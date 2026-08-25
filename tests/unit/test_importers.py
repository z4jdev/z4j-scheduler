"""Tests for the migration importers.

Cron and core (``ImportedSchedule`` / ``render_jsonl`` /
``BrainImportClient``) are dependency-free and tested directly.
celery / rq / apscheduler integration paths require their respective
optional deps installed; we test the parsing and dispatch helpers
with hand-built fakes so the test suite remains hermetic.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from z4j_scheduler.importers._core import (
    BrainImportClient,
    ImportedSchedule,
    render_jsonl,
)

# =====================================================================
# Core
# =====================================================================


class TestImportedSchedule:
    def test_to_dict_includes_computed_hash(self) -> None:
        sched = ImportedSchedule(
            project_slug="p",
            name="every-hour",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="tasks.heartbeat",
        )
        d = sched.to_dict()
        assert d["source_hash"]
        assert len(d["source_hash"]) == 64  # sha256 hex

    def test_compute_hash_deterministic(self) -> None:
        sched1 = ImportedSchedule(
            project_slug="p",
            name="x",
            engine="celery",
            kind="cron",
            expression="* * * * *",
            task_name="t",
            args=[1, 2],
            kwargs={"a": "b"},
        )
        sched2 = ImportedSchedule(
            project_slug="p",
            name="x",
            engine="celery",
            kind="cron",
            expression="* * * * *",
            task_name="t",
            args=[1, 2],
            kwargs={"a": "b"},
        )
        # ``project_slug`` is intentionally NOT part of the hash; the
        # hash is content-only (re-import idempotency).
        assert sched1.compute_hash() == sched2.compute_hash()

    def test_compute_hash_changes_on_edit(self) -> None:
        sched = ImportedSchedule(
            project_slug="p",
            name="x",
            engine="celery",
            kind="cron",
            expression="* * * * *",
            task_name="t",
        )
        before = sched.compute_hash()
        sched.expression = "0 * * * *"
        assert before != sched.compute_hash()


class TestRenderJsonl:
    def test_one_line_per_schedule(self) -> None:
        scheds = [
            ImportedSchedule(
                project_slug="p",
                name=f"n{i}",
                engine="celery",
                kind="cron",
                expression="0 * * * *",
                task_name="t",
            )
            for i in range(3)
        ]
        rendered = render_jsonl(scheds)
        lines = rendered.splitlines()
        assert len(lines) == 3
        for line in lines:
            payload = json.loads(line)
            assert "source_hash" in payload

    def test_empty_input(self) -> None:
        assert render_jsonl([]) == ""


# =====================================================================
# BrainImportClient
# =====================================================================


class TestBrainImportClient:
    @pytest.mark.asyncio
    async def test_empty_upload_returns_zero(self) -> None:
        client = BrainImportClient(brain_url="http://brain")
        result = await client.upload(project_slug="p", schedules=[])
        # ``upload`` returns a counters dict: every bucket should
        # be zero on an empty input (no rows to insert / update /
        # leave unchanged / fail).
        assert result == {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "failed": 0,
        }

    @pytest.mark.asyncio
    async def test_upload_404_raises_clear_error(self) -> None:
        # Stand-in for httpx.AsyncClient that returns a 404 to
        # mimic an old brain without the import endpoint.
        client = BrainImportClient(brain_url="http://brain")

        class _Response:
            status_code = 404

            def raise_for_status(self) -> None:
                raise AssertionError("should have been short-circuited")

        class _Client:
            async def __aenter__(self) -> _Client:
                return self

            async def __aexit__(self, *_args: object) -> None:
                return None

            async def post(self, *_args: object, **_kw: object) -> _Response:
                return _Response()

        with (
            patch("httpx.AsyncClient", return_value=_Client()),
            pytest.raises(RuntimeError, match="schedules:import"),
        ):
            await client.upload(
                project_slug="p",
                schedules=[
                    ImportedSchedule(
                        project_slug="p",
                        name="n",
                        engine="celery",
                        kind="cron",
                        expression="* * * * *",
                        task_name="t",
                    ),
                ],
            )


# =====================================================================
# cron importer
# =====================================================================


class TestReadCrontab:
    def _write_crontab(self, tmp_path: Path, lines: list[str]) -> Path:
        path = tmp_path / "crontab"
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def test_simple_crontab(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            [
                "# header comment",
                "",
                "0 * * * * /usr/bin/run-hourly",
                "*/5 * * * * /usr/bin/run-five-min",
            ],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="myapp.shell.exec",
        )
        assert len(scheds) == 2
        assert scheds[0].kind == "cron"
        assert scheds[0].expression == "0 * * * *"
        assert scheds[0].args == ["/usr/bin/run-hourly"]
        assert scheds[0].task_name == "myapp.shell.exec"
        assert scheds[0].source == "imported_cron"
        assert scheds[1].expression == "*/5 * * * *"

    def test_shortcut_lines_expanded(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            [
                "@hourly /opt/bin/cleanup",
                "@daily /opt/bin/nightly",
            ],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="task.exec",
        )
        assert scheds[0].expression == "0 * * * *"
        assert scheds[1].expression == "0 0 * * *"

    def test_reboot_skipped(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            ["@reboot /opt/bin/startup"],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="task.exec",
        )
        assert scheds == []

    def test_env_directive_ignored(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            [
                "MAILTO=ops@example.com",
                "PATH=/usr/local/bin:/usr/bin",
                "0 * * * * /opt/bin/run",
            ],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="task.exec",
        )
        assert len(scheds) == 1
        assert scheds[0].args == ["/opt/bin/run"]

    def test_etc_crontab_user_column(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            [
                "# /etc/crontab style",
                "0 * * * * root /usr/bin/run-as-root",
            ],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="task.exec",
            has_user_column=True,
        )
        assert len(scheds) == 1
        assert scheds[0].args == ["/usr/bin/run-as-root"]

    def test_invalid_expression_skipped(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        path = self._write_crontab(
            tmp_path,
            [
                "bogus expression here",
                "0 * * * * /opt/bin/valid",
            ],
        )
        scheds = read_crontab(
            crontab_path=path,
            project_slug="p",
            task_prefix="task.exec",
        )
        # Only the valid line survives.
        assert len(scheds) == 1
        assert scheds[0].args == ["/opt/bin/valid"]

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import read_crontab

        with pytest.raises(FileNotFoundError):
            read_crontab(
                crontab_path=tmp_path / "no-such-file",
                project_slug="p",
                task_prefix="task.exec",
            )


# =====================================================================
# celery importer (parsing without celery installed)
# =====================================================================


class TestCeleryClassify:
    """Test the schedule-classification helper without booting celery."""

    def test_timedelta_to_interval(self) -> None:
        from datetime import timedelta

        from z4j_scheduler.importers.celery import _classify_schedule

        kind, expr, tz = _classify_schedule(timedelta(seconds=30), "UTC")
        assert kind == "interval"
        assert expr == "30s"
        assert tz == "UTC"

    def test_zero_timedelta_rejected(self) -> None:
        from datetime import timedelta

        from z4j_scheduler.importers.celery import (
            _classify_schedule,
            _UnsupportedScheduleError,
        )

        with pytest.raises(_UnsupportedScheduleError):
            _classify_schedule(timedelta(0), "UTC")

    def test_unknown_type_rejected(self) -> None:
        from z4j_scheduler.importers.celery import (
            _classify_schedule,
            _UnsupportedScheduleError,
        )

        with pytest.raises(_UnsupportedScheduleError):
            _classify_schedule(object(), "UTC")


class TestCeleryAppLoader:
    def test_bad_path_format_rejected(self) -> None:
        from z4j_scheduler.importers.celery import _load_celery_app

        with pytest.raises(ValueError, match=r"module\.path:attr"):
            _load_celery_app("not_a_module_path_format")

    def test_missing_module_clear_error(self) -> None:
        from z4j_scheduler.importers.celery import _load_celery_app

        with pytest.raises(RuntimeError, match="failed to import"):
            _load_celery_app("definitely_not_a_real_module_xyz:app")


# =====================================================================
# rq importer (job mapper without redis)
# =====================================================================


class TestRqJobMapper:
    def test_cron_meta_string(self) -> None:
        from z4j_scheduler.importers.rq import _job_to_schedule

        job = SimpleNamespace(
            id="job-1",
            func_name="myapp.tasks.heartbeat",
            args=(),
            kwargs={},
            origin="default",
            meta={"cron_string": "0 * * * *"},
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="rq",
            default_queue=None,
        )
        assert sched.kind == "cron"
        assert sched.expression == "0 * * * *"
        assert sched.queue == "default"
        assert sched.source == "imported_rq"

    def test_local_timezone_cron_uses_operator_zone(self) -> None:
        from z4j_scheduler.importers.rq import _job_to_schedule

        job = SimpleNamespace(
            id="job-local",
            func_name="myapp.tasks.heartbeat",
            args=(),
            kwargs={},
            origin="default",
            meta={
                "cron_string": "0 9 * * *",
                "use_local_timezone": True,
            },
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="rq",
            default_queue=None,
            default_timezone="America/New_York",
        )
        assert sched.timezone == "America/New_York"

    def test_interval_meta(self) -> None:
        from z4j_scheduler.importers.rq import _job_to_schedule

        job = SimpleNamespace(
            id="job-2",
            func_name="myapp.tasks.poll",
            args=(),
            kwargs={},
            origin=None,
            meta={"interval": 60},
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="rq",
            default_queue="q-fallback",
        )
        assert sched.kind == "interval"
        assert sched.expression == "60s"
        assert sched.queue == "q-fallback"

    def test_one_shot_uses_scheduled_at(self) -> None:
        from z4j_scheduler.importers.rq import _job_to_schedule

        when = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        job = SimpleNamespace(
            id="job-3",
            func_name="myapp.tasks.bg",
            args=(),
            kwargs={},
            origin=None,
            meta={},
            scheduled_at=when,
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="rq",
            default_queue=None,
        )
        assert sched.kind == "clocked"
        assert sched.expression == when.isoformat()

    def test_one_shot_uses_sorted_set_score_as_utc(self) -> None:
        from z4j_scheduler.importers.rq import _job_to_schedule

        job = SimpleNamespace(
            id="job-score",
            func_name="myapp.tasks.bg",
            args=(),
            kwargs={},
            origin=None,
            meta={},
        )
        score_time = datetime(2026, 4, 26, 15, 0)
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="rq",
            default_queue=None,
            scheduled_time=score_time,
            scheduled_time_is_utc=True,
        )
        assert sched.kind == "clocked"
        assert sched.expression == "2026-04-26T15:00:00+00:00"

    def test_ambiguous_naive_one_shot_is_rejected(self) -> None:
        from z4j_scheduler.importers.rq import (
            _job_to_schedule,
            _UnsupportedJobError,
        )

        job = SimpleNamespace(
            id="job-naive",
            func_name="myapp.tasks.bg",
            args=(),
            kwargs={},
            origin=None,
            meta={"scheduled_at": datetime(2026, 11, 1, 1, 30)},
        )
        with pytest.raises(_UnsupportedJobError, match="timezone-naive"):
            _job_to_schedule(
                job=job,
                project_slug="p",
                engine="rq",
                default_queue=None,
                default_timezone="America/New_York",
            )

    def test_reader_requests_sorted_set_times(self, monkeypatch) -> None:
        import redis
        import rq_scheduler
        from z4j_scheduler.importers.rq import read_rq_scheduler

        requested: list[bool] = []
        job = SimpleNamespace(
            id="job-score",
            func_name="myapp.tasks.bg",
            args=(),
            kwargs={},
            origin=None,
            meta={},
        )

        class Scheduler:
            def __init__(self, *, connection):
                self.connection = connection

            def get_jobs(self, *, with_times=False):
                requested.append(with_times)
                yield job, datetime(2026, 4, 26, 15, 0)

        monkeypatch.setattr(redis.Redis, "from_url", lambda _url: object())
        monkeypatch.setattr(rq_scheduler, "Scheduler", Scheduler)
        rows = read_rq_scheduler(
            redis_url="redis://localhost/0",
            project_slug="p",
        )
        assert requested == [True]
        assert rows[0].expression == "2026-04-26T15:00:00+00:00"

    def test_cli_forwards_timezone_to_rq_importer(self, monkeypatch) -> None:
        from z4j_scheduler import cli
        from z4j_scheduler.importers import rq as rq_importer

        captured = {}

        def read(**kwargs):
            captured.update(kwargs)
            return []

        monkeypatch.setattr(rq_importer, "read_rq_scheduler", read)
        cli._do_import(
            source="rq",
            project="p",
            celery_app=None,
            django_settings=None,
            redis_url="redis://localhost/0",
            jobstore_url=None,
            jobstore_alias="default",
            crontab=None,
            task_prefix=None,
            has_user_column=False,
            queue=None,
            timezone="America/New_York",
            engine=None,
        )
        assert captured["default_timezone"] == "America/New_York"

    def test_unknown_shape_raises(self) -> None:
        from z4j_scheduler.importers.rq import (
            _job_to_schedule,
            _UnsupportedJobError,
        )

        job = SimpleNamespace(
            id="job-4",
            func_name="t.t",
            args=(),
            kwargs={},
            origin=None,
            meta={},
        )
        with pytest.raises(_UnsupportedJobError):
            _job_to_schedule(
                job=job,
                project_slug="p",
                engine="rq",
                default_queue=None,
            )


# =====================================================================
# apscheduler importer (job mapper without apscheduler)
# =====================================================================


# Stand-in trigger classes with the right __name__ for the
# type(trigger).__name__ dispatch in _job_to_schedule.
class _FakeCronTrigger:
    def __init__(self) -> None:
        self.fields = (
            SimpleNamespace(name="minute", expressions=("0",)),
            SimpleNamespace(name="hour", expressions=("*",)),
            SimpleNamespace(name="day", expressions=("*",)),
            SimpleNamespace(name="month", expressions=("*",)),
            SimpleNamespace(name="day_of_week", expressions=("*",)),
        )
        self.timezone = "UTC"


# Real APScheduler classes are named CronTrigger / IntervalTrigger /
# DateTrigger - the mapper looks at type(trigger).__name__. Rebind
# the class names so the dispatch matches.
_FakeCronTrigger.__name__ = "CronTrigger"


class _FakeIntervalTrigger:
    def __init__(self, interval: object) -> None:
        self.interval = interval
        self.timezone = "UTC"


_FakeIntervalTrigger.__name__ = "IntervalTrigger"


class _FakeDateTrigger:
    def __init__(self, run_date: datetime) -> None:
        self.run_date = run_date
        self.timezone = "UTC"


_FakeDateTrigger.__name__ = "DateTrigger"


class _FakeAndTrigger:
    pass


_FakeAndTrigger.__name__ = "AndTrigger"


class TestApsJobMapper:
    def test_cron_trigger(self) -> None:
        from z4j_scheduler.importers.apscheduler import _job_to_schedule

        job = SimpleNamespace(
            id="aps-1",
            trigger=_FakeCronTrigger(),
            args=(),
            kwargs={},
            func_ref="mymod:func",
            next_run_time=datetime(2026, 4, 26, tzinfo=UTC),
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="apscheduler",
            default_queue=None,
        )
        assert sched.kind == "cron"
        assert sched.expression == "0 * * * *"
        assert sched.task_name == "mymod:func"
        assert sched.is_enabled is True

    def test_interval_trigger_3x_shape(self) -> None:
        from datetime import timedelta

        from z4j_scheduler.importers.apscheduler import _job_to_schedule

        job = SimpleNamespace(
            id="aps-2",
            trigger=_FakeIntervalTrigger(timedelta(seconds=300)),
            args=(),
            kwargs={},
            func_ref="mod:func",
            next_run_time=None,  # paused
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="apscheduler",
            default_queue=None,
        )
        assert sched.kind == "interval"
        assert sched.expression == "300s"
        assert sched.is_enabled is False

    def test_date_trigger(self) -> None:
        from z4j_scheduler.importers.apscheduler import _job_to_schedule

        when = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        job = SimpleNamespace(
            id="aps-3",
            trigger=_FakeDateTrigger(when),
            args=(),
            kwargs={},
            func_ref="mod:func",
            next_run_time=when,
        )
        sched = _job_to_schedule(
            job=job,
            project_slug="p",
            engine="apscheduler",
            default_queue=None,
        )
        assert sched.kind == "clocked"
        assert sched.expression == when.isoformat()

    def test_combining_trigger_rejected(self) -> None:
        from z4j_scheduler.importers.apscheduler import (
            _job_to_schedule,
            _UnsupportedTriggerError,
        )

        job = SimpleNamespace(
            id="aps-4",
            trigger=_FakeAndTrigger(),
            args=(),
            kwargs={},
            func_ref="mod:func",
            next_run_time=None,
        )
        with pytest.raises(_UnsupportedTriggerError):
            _job_to_schedule(
                job=job,
                project_slug="p",
                engine="apscheduler",
                default_queue=None,
            )


class TestApsCronTrigger3x:
    """Exercise the mapper against real APScheduler 3.x field objects."""

    def test_default_second_is_losslessly_rendered(self) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import _render_cron_trigger

        trigger = cron_module.CronTrigger(minute=0)

        assert _render_cron_trigger(trigger) == "0 * * * *"

    @pytest.mark.parametrize(
        "day_of_week",
        ["mon", "sun", "mon-fri", "mon,wed,fri"],
    )
    def test_named_weekdays_match_croniter_semantics(
        self,
        day_of_week: str,
    ) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import _render_cron_trigger
        from z4j_scheduler.tick.cron import next_fire

        trigger = cron_module.CronTrigger(
            day_of_week=day_of_week,
            hour=12,
            minute=0,
            second=0,
            timezone="UTC",
        )
        base = datetime(2026, 8, 2, tzinfo=UTC)  # Sunday
        expression = _render_cron_trigger(trigger)

        assert next_fire(expression, "UTC", base) == trigger.get_next_fire_time(
            base,
            base,
        )

    @pytest.mark.parametrize("day_of_week", ["0", "6", "*/2"])
    def test_numeric_or_stepped_weekday_fails_closed(
        self,
        day_of_week: str,
    ) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )

        trigger = cron_module.CronTrigger(
            day_of_week=day_of_week,
            hour=12,
            minute=0,
            second=0,
            timezone="UTC",
        )

        with pytest.raises(_UnsupportedTriggerError, match="numbers Monday as 0"):
            _render_cron_trigger(trigger)

    def test_numeric_weekday_negative_control_changes_day(self) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )
        from z4j_scheduler.tick.cron import next_fire

        trigger = cron_module.CronTrigger(
            day_of_week="0",
            hour=12,
            minute=0,
            second=0,
            timezone="UTC",
        )
        base = datetime(2026, 8, 2, tzinfo=UTC)  # Sunday

        # The old literal projection means Sunday to croniter and Monday to
        # APScheduler, so the two next-fire dates differ by a day.
        assert next_fire("0 12 * * 0", "UTC", base) != (trigger.get_next_fire_time(base, base))
        with pytest.raises(_UnsupportedTriggerError):
            _render_cron_trigger(trigger)

    def test_day_and_weekday_conjunction_fails_closed(self) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )
        from z4j_scheduler.tick.cron import next_fire

        trigger = cron_module.CronTrigger(
            day="1",
            day_of_week="mon",
            hour=12,
            minute=0,
            second=0,
            timezone="UTC",
        )
        base = datetime(2026, 8, 2, tzinfo=UTC)

        # croniter's default OR rule fires on the next Monday. APScheduler's
        # AND rule waits for a Monday that is also the first of its month.
        assert next_fire("0 12 1 * mon", "UTC", base) != (trigger.get_next_fire_time(base, base))
        with pytest.raises(_UnsupportedTriggerError, match="requires both"):
            _render_cron_trigger(trigger)

    @pytest.mark.parametrize("day", ["last", "3rd fri"])
    def test_special_day_expression_fails_closed(self, day: str) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )

        trigger = cron_module.CronTrigger(day=day, hour=12, minute=0, second=0)

        with pytest.raises(_UnsupportedTriggerError, match="day expression"):
            _render_cron_trigger(trigger)

    @pytest.mark.parametrize(
        "modifier",
        [
            {"start_date": datetime(2026, 8, 5, tzinfo=UTC)},
            {"end_date": datetime(2026, 8, 5, tzinfo=UTC)},
            {"jitter": 30},
        ],
    )
    def test_unrepresentable_modifier_fails_closed(
        self,
        modifier: dict[str, object],
    ) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )

        trigger = cron_module.CronTrigger(minute=0, **modifier)

        with pytest.raises(_UnsupportedTriggerError, match="only modifiers"):
            _render_cron_trigger(trigger)

    @pytest.mark.parametrize(
        ("constraint", "expected_detail"),
        [
            ({"year": "2026", "minute": 0}, "year='2026'"),
            ({"week": "1", "minute": 0}, "week='1'"),
            ({"second": "30", "minute": 0}, "second='30'"),
            ({"second": "*", "minute": 0}, "second='*'"),
        ],
    )
    def test_dimensions_missing_from_five_field_cron_fail_closed(
        self,
        constraint: dict[str, object],
        expected_detail: str,
    ) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )

        trigger = cron_module.CronTrigger(**constraint)

        with pytest.raises(
            _UnsupportedTriggerError,
            match="cannot be represented by five-field cron",
        ) as excinfo:
            _render_cron_trigger(trigger)
        assert expected_detail in str(excinfo.value)

    def test_negative_control_old_projection_would_broaden_trigger(self) -> None:
        cron_module = pytest.importorskip("apscheduler.triggers.cron")
        from z4j_scheduler.importers.apscheduler import (
            _render_cron_trigger,
            _UnsupportedTriggerError,
        )

        trigger = cron_module.CronTrigger(year="2026", second="30", minute=0)

        def _old_five_field_projection() -> str:
            values = {
                field.name: ",".join(str(expr) for expr in field.expressions)
                for field in trigger.fields
            }
            return "{minute} {hour} {day} {month} {day_of_week}".format(**values)

        # Before the fix, both constraints were discarded and this broad
        # schedule was imported as every hour in every year at second zero.
        assert _old_five_field_projection() == "0 * * * *"
        with pytest.raises(_UnsupportedTriggerError):
            _render_cron_trigger(trigger)

    def test_fresh_importer_loads_persisted_sqlalchemy_jobstore(
        self,
        tmp_path: Path,
    ) -> None:
        pytest.importorskip("sqlalchemy")
        aps_jobstore = pytest.importorskip("apscheduler.jobstores.sqlalchemy")
        aps_scheduler = pytest.importorskip("apscheduler.schedulers.background")
        aps_date = pytest.importorskip("apscheduler.triggers.date")
        from datetime import timedelta

        from sqlalchemy.pool import NullPool
        from z4j_scheduler.importers.apscheduler import read_apscheduler

        db_path = tmp_path / "apscheduler.sqlite"
        jobstore_url = f"sqlite:///{db_path}"

        writer = aps_scheduler.BackgroundScheduler()
        writer.add_jobstore(
            aps_jobstore.SQLAlchemyJobStore(
                url=jobstore_url,
                engine_options={"poolclass": NullPool},
            )
        )
        writer_started = False
        try:
            writer.start(paused=True)
            writer_started = True
            # A future one-shot job remains present for the fresh reader. The
            # importer starts paused, so enumeration never executes jobs.
            writer.add_job(
                "builtins:print",
                trigger=aps_date.DateTrigger(
                    run_date=datetime.now(UTC) + timedelta(days=365),
                ),
                args=["must not execute during import"],
                id="persisted-job",
                misfire_grace_time=60,
            )
        finally:
            if writer_started:
                writer.shutdown()

        # Negative control for the old importer: a fresh stopped scheduler has
        # not started its persistent store, so get_jobs() sees no stored rows.
        stopped_reader = aps_scheduler.BackgroundScheduler()
        stopped_jobstore = aps_jobstore.SQLAlchemyJobStore(
            url=jobstore_url,
            engine_options={"poolclass": NullPool},
        )
        stopped_reader.add_jobstore(stopped_jobstore)
        try:
            assert stopped_reader.get_jobs(jobstore="default") == []
        finally:
            stopped_jobstore.shutdown()

        imported = read_apscheduler(
            jobstore_url=jobstore_url,
            project_slug="p",
        )

        assert [schedule.name for schedule in imported] == ["persisted-job"]
        assert imported[0].kind == "clocked"
        assert imported[0].task_name == "builtins:print"
