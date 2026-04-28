"""Round-5 importer + exporter parity tests.

Brings huey, arq, taskiq, and dramatiq up to the same coverage
the round-1 work established for celery, rq, apscheduler, cron.

Round-5 closes the documented "Importer/exporter parity gap" from
the security audit.
"""

from __future__ import annotations

import sys
import textwrap
from types import ModuleType

import pytest

from z4j_scheduler.exporters._client import ExportedSchedule


# =====================================================================
# Huey
# =====================================================================


class TestHueyImporter:
    def test_reads_periodic_tasks_from_real_huey_instance(self) -> None:
        pytest.importorskip("huey")
        from huey import MemoryHuey, crontab

        from z4j_scheduler.importers.huey import read_huey_app

        # Build a Huey instance with a periodic task in a synthetic
        # module so the importer can resolve "module:attr".
        mod = ModuleType("z4j_test_huey_mod")
        huey_inst = MemoryHuey("imp-test", immediate=False)

        @huey_inst.periodic_task(crontab(minute="*/15", hour="9-17"))
        def daily_report() -> None:
            return None

        mod.huey_inst = huey_inst
        sys.modules["z4j_test_huey_mod"] = mod
        try:
            schedules = read_huey_app(
                app_path="z4j_test_huey_mod:huey_inst",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_huey_mod"]

        assert len(schedules) == 1
        s = schedules[0]
        assert s.kind == "cron"
        # Crontab fields rendered as 5-field cron string. Huey 3.x
        # pre-expands the spec to integer sets, so we get the
        # comma-list form (``0,15,30,45``) rather than the original
        # ``*/15`` step form. Both are semantically equivalent.
        parts = s.expression.split()
        assert len(parts) == 5
        assert parts[0] == "0,15,30,45"  # minute
        assert parts[1] == "9,10,11,12,13,14,15,16,17"  # hour
        assert s.engine == "huey"
        assert s.source == "imported_huey"

    def test_resolve_app_missing_colon_raises(self) -> None:
        pytest.importorskip("huey")
        from z4j_scheduler.importers.huey import read_huey_app

        with pytest.raises(RuntimeError, match="module:attr"):
            read_huey_app(
                app_path="z4j_test.no_colon",
                project_slug="acme",
            )


class TestHueyExporter:
    def test_renders_cron_schedule_as_periodic_task(self) -> None:
        from z4j_scheduler.exporters.huey import render

        sched = ExportedSchedule(
            id="x", name="hourly", engine="huey", kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.hourly",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "@huey.periodic_task" in out
        assert "crontab(" in out
        assert "minute=\"0\"" in out
        assert "hour=\"*\"" in out

    def test_disabled_schedule_renders_as_comment(self) -> None:
        from z4j_scheduler.exporters.huey import render

        sched = ExportedSchedule(
            id="x", name="off", engine="huey", kind="cron",
            expression="0 * * * *", task_name="t",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=False,
        )
        out = render([sched])
        assert "DISABLED in z4j" in out
        # No active periodic_task line.
        active_lines = [
            line for line in out.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
            and "register_schedules" not in line
            and "from huey" not in line
            and "def " not in line
        ]
        assert all("@huey.periodic_task" not in line for line in active_lines)

    def test_interval_kind_is_skipped_with_comment(self) -> None:
        from z4j_scheduler.exporters.huey import render

        sched = ExportedSchedule(
            id="x", name="ival", engine="huey", kind="interval",
            expression="60s", task_name="t",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "not supported by huey" in out

    def test_unsafe_cron_field_refused(self) -> None:
        from z4j_scheduler.exporters.huey import render

        sched = ExportedSchedule(
            id="x", name="bad", engine="huey", kind="cron",
            expression="0 `whoami` * * *", task_name="t",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out
        # No crontab() call rendered for this schedule.
        active = [
            line for line in out.splitlines()
            if "crontab(" in line and not line.lstrip().startswith("#")
        ]
        # The only allowed crontab() reference comes from the
        # ``from huey import crontab`` import line which doesn't
        # have parentheses on it.
        assert active == []


# =====================================================================
# arq
# =====================================================================


class TestArqImporter:
    def test_reads_cron_jobs_from_worker_settings(self) -> None:
        pytest.importorskip("arq")
        from arq import cron

        from z4j_scheduler.importers.arq import read_arq_settings

        async def my_task(ctx) -> None:  # noqa: ANN001
            return None

        class _WorkerSettings:
            cron_jobs = [
                cron(my_task, name="daily-greeting", hour=8, minute=0),
                cron(my_task, name="bursts", hour={9, 17}, minute=0),
            ]

        mod = ModuleType("z4j_test_arq_mod")
        mod.WorkerSettings = _WorkerSettings
        sys.modules["z4j_test_arq_mod"] = mod
        try:
            schedules = read_arq_settings(
                settings_path="z4j_test_arq_mod:WorkerSettings",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_arq_mod"]

        assert len(schedules) == 2
        names = {s.name for s in schedules}
        assert names == {"daily-greeting", "bursts"}
        # Set-valued hour rendered as comma-list.
        bursts = next(s for s in schedules if s.name == "bursts")
        parts = bursts.expression.split()
        assert parts[1] == "9,17"

    def test_no_cron_jobs_returns_empty(self) -> None:
        pytest.importorskip("arq")
        from z4j_scheduler.importers.arq import read_arq_settings

        class _WorkerSettings:
            cron_jobs = []

        mod = ModuleType("z4j_test_arq_empty")
        mod.WorkerSettings = _WorkerSettings
        sys.modules["z4j_test_arq_empty"] = mod
        try:
            schedules = read_arq_settings(
                settings_path="z4j_test_arq_empty:WorkerSettings",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_arq_empty"]
        assert schedules == []


class TestArqExporter:
    def test_renders_cron_call_with_field_translation(self) -> None:
        from z4j_scheduler.exporters.arq import render

        sched = ExportedSchedule(
            id="x", name="daily", engine="arq", kind="cron",
            expression="0 8 * * *", task_name="myapp.tasks.daily",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "from arq import cron" in out
        assert "build_cron_jobs" in out
        assert "cron(by_name[\"daily\"]" in out
        # Field translation: "*" → None, "0" → 0, "8" → 8.
        assert "minute=0" in out
        assert "hour=8" in out
        assert "day=None" in out

    def test_set_valued_field_renders_as_python_set(self) -> None:
        from z4j_scheduler.exporters.arq import render

        sched = ExportedSchedule(
            id="x", name="bursts", engine="arq", kind="cron",
            expression="0 9,17 * * *", task_name="t",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "hour={9, 17}" in out


# =====================================================================
# Taskiq
# =====================================================================


class TestTaskiqImporter:
    @pytest.mark.asyncio
    async def test_reads_cron_label_from_broker(self) -> None:
        pytest.importorskip("taskiq")
        from taskiq import InMemoryBroker

        from z4j_scheduler.importers.taskiq import read_taskiq_broker

        broker = InMemoryBroker()

        @broker.task(schedule=[{"cron": "0 3 * * *"}])
        async def nightly_cleanup() -> None:
            return None

        mod = ModuleType("z4j_test_taskiq_mod")
        mod.broker = broker
        sys.modules["z4j_test_taskiq_mod"] = mod
        try:
            schedules = read_taskiq_broker(
                broker_path="z4j_test_taskiq_mod:broker",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_taskiq_mod"]

        assert len(schedules) == 1
        s = schedules[0]
        assert s.kind == "cron"
        assert s.expression == "0 3 * * *"
        assert s.engine == "taskiq"
        assert s.source == "imported_taskiq"

    @pytest.mark.asyncio
    async def test_reads_one_shot_time_label(self) -> None:
        pytest.importorskip("taskiq")
        from taskiq import InMemoryBroker

        from z4j_scheduler.importers.taskiq import read_taskiq_broker

        broker = InMemoryBroker()

        @broker.task(schedule=[{"time": "2026-12-31T23:59:59+00:00"}])
        async def newyear() -> None:
            return None

        mod = ModuleType("z4j_test_taskiq_oneshot")
        mod.broker = broker
        sys.modules["z4j_test_taskiq_oneshot"] = mod
        try:
            schedules = read_taskiq_broker(
                broker_path="z4j_test_taskiq_oneshot:broker",
                project_slug="acme",
            )
        finally:
            del sys.modules["z4j_test_taskiq_oneshot"]

        assert len(schedules) == 1
        assert schedules[0].kind == "one_shot"
        assert "2026-12-31" in schedules[0].expression


class TestTaskiqExporter:
    def test_renders_label_attachment_for_cron(self) -> None:
        from z4j_scheduler.exporters.taskiq import render

        sched = ExportedSchedule(
            id="x", name="nightly", engine="taskiq", kind="cron",
            expression="0 3 * * *", task_name="myapp.tasks.cleanup",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "by_name.get(\"myapp.tasks.cleanup\")" in out
        assert "labels['schedule']" in out
        assert '"cron": "0 3 * * *"' in out

    def test_one_shot_renders_time_label(self) -> None:
        from z4j_scheduler.exporters.taskiq import render

        sched = ExportedSchedule(
            id="x", name="ny", engine="taskiq", kind="one_shot",
            expression="2026-12-31T23:59:59Z",
            task_name="myapp.tasks.newyear",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert '"time": "2026-12-31T23:59:59Z"' in out

    def test_interval_skipped(self) -> None:
        from z4j_scheduler.exporters.taskiq import render

        sched = ExportedSchedule(
            id="x", name="ival", engine="taskiq", kind="interval",
            expression="60s", task_name="t",
            timezone="UTC", queue=None, args=[], kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "not supported by taskiq" in out


# =====================================================================
# Dramatiq (guidance stubs)
# =====================================================================


class TestDramatiqStubs:
    def test_importer_raises_with_guidance(self) -> None:
        from z4j_scheduler.importers.dramatiq import read_dramatiq

        with pytest.raises(RuntimeError, match="Dramatiq has no built-in scheduler"):
            read_dramatiq()

    def test_exporter_returns_guidance_comment(self) -> None:
        from z4j_scheduler.exporters.dramatiq import render

        out = render([])
        assert "Dramatiq has no native scheduler" in out
        # Every line is a comment.
        for line in out.splitlines():
            if line.strip():
                assert line.lstrip().startswith("#")
