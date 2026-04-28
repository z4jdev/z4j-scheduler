"""Tests for the reverse-export renderers.

The HTTP fetch is covered by the e2e test in
``tests/integration/test_export_e2e.py``. These tests are pure
rendering: take a list of :class:`ExportedSchedule` rows and verify
the output is parseable / matches the target format.
"""

from __future__ import annotations

import ast
import json

import pytest

from z4j_scheduler.exporters import apscheduler, celery, cron, rq
from z4j_scheduler.exporters._client import ExportedSchedule


def _schedule(
    name: str = "every-hour",
    *,
    kind: str = "cron",
    expression: str = "0 * * * *",
    enabled: bool = True,
    queue: str | None = None,
    args: list | None = None,
    kwargs: dict | None = None,
) -> ExportedSchedule:
    return ExportedSchedule(
        id="00000000-0000-0000-0000-000000000001",
        name=name,
        engine="celery",
        kind=kind,
        expression=expression,
        task_name=f"myapp.tasks.{name.replace('-', '_')}",
        timezone="UTC",
        queue=queue,
        args=args or [],
        kwargs=kwargs or {},
        catch_up="skip",
        is_enabled=enabled,
        scheduler="z4j-scheduler",
        source="dashboard",
    )


# =====================================================================
# Celery
# =====================================================================


class TestCeleryRender:
    def test_cron_emits_crontab_call(self) -> None:
        out = celery.render([_schedule("hourly")])
        assert "from celery.schedules import crontab" in out
        assert "from datetime import timedelta" in out
        assert "beat_schedule = {" in out
        assert "\"hourly\"" in out
        # Cron expression rendered as keyword crontab(...) call.
        assert "crontab(" in out
        assert "minute=\"0\"" in out
        assert "hour=\"*\"" in out

    def test_interval_emits_timedelta(self) -> None:
        out = celery.render([_schedule("poll", kind="interval", expression="60s")])
        assert "timedelta(seconds=60)" in out

    def test_interval_minutes_unit(self) -> None:
        out = celery.render([_schedule("poll", kind="interval", expression="5m")])
        assert "timedelta(seconds=300)" in out

    def test_disabled_emits_warning_comment(self) -> None:
        out = celery.render([_schedule("off", enabled=False)])
        assert "# NOTE: disabled in z4j" in out

    def test_one_shot_emits_warning(self) -> None:
        out = celery.render(
            [_schedule("once", kind="one_shot", expression="2026-04-30T00:00:00Z")],
        )
        assert "one-shot trigger" in out

    def test_queue_emits_options(self) -> None:
        out = celery.render([_schedule("hourly", queue="critical")])
        assert "\"queue\": \"critical\"" in out

    def test_output_is_valid_python_module(self) -> None:
        # Critical: celery operators paste the output into their
        # config. If it doesn't parse, the export is broken.
        out = celery.render([
            _schedule("a"),
            _schedule("b", kind="interval", expression="30s"),
            _schedule("c", queue="q"),
        ])
        try:
            ast.parse(out)
        except SyntaxError as exc:
            pytest.fail(f"celery render produced invalid Python: {exc}\n{out}")


# =====================================================================
# RQ
# =====================================================================


class TestRqRender:
    def test_cron_emits_scheduler_cron(self) -> None:
        out = rq.render([_schedule("hourly")])
        assert "scheduler.cron(" in out
        assert "cron_string=\"0 * * * *\"" in out

    def test_interval_emits_scheduler_schedule(self) -> None:
        out = rq.render([_schedule("poll", kind="interval", expression="60s")])
        assert "scheduler.schedule(" in out
        assert "interval=60" in out

    def test_one_shot_emits_enqueue_at(self) -> None:
        out = rq.render(
            [_schedule("once", kind="one_shot", expression="2026-04-30T00:00:00")],
        )
        assert "scheduler.enqueue_at(" in out

    def test_disabled_emits_commented_call(self) -> None:
        out = rq.render([_schedule("off", enabled=False)])
        # Commented call so re-import is safe-by-default.
        assert "# NOTE: disabled in z4j" in out

    def test_register_function_present(self) -> None:
        out = rq.render([_schedule("a")])
        assert "def register_schedules(scheduler):" in out

    def test_output_is_valid_python_module(self) -> None:
        out = rq.render([_schedule("a"), _schedule("b", kind="interval", expression="30s")])
        try:
            ast.parse(out)
        except SyntaxError as exc:
            pytest.fail(f"rq render produced invalid Python: {exc}\n{out}")


# =====================================================================
# APScheduler
# =====================================================================


class TestApsRender:
    def test_cron_emits_keyword_fields(self) -> None:
        out = apscheduler.render([_schedule("hourly")])
        assert "scheduler.add_job(" in out
        assert "\"cron\"" in out
        assert "minute=\"0\"" in out
        assert "hour=\"*\"" in out
        assert "id=\"00000000-0000-0000-0000-000000000001\"" in out

    def test_interval_emits_seconds(self) -> None:
        out = apscheduler.render([_schedule("p", kind="interval", expression="2h")])
        assert "\"interval\"" in out
        assert "seconds=7200" in out

    def test_one_shot_emits_date_trigger(self) -> None:
        out = apscheduler.render(
            [_schedule("once", kind="one_shot", expression="2026-04-30T00:00:00")],
        )
        assert "\"date\"" in out
        assert "run_date=datetime.fromisoformat" in out

    def test_disabled_emits_paused(self) -> None:
        out = apscheduler.render([_schedule("off", enabled=False)])
        assert "paused=True" in out

    def test_output_is_valid_python_module(self) -> None:
        out = apscheduler.render([_schedule("a"), _schedule("b", kind="interval", expression="60s")])
        try:
            ast.parse(out)
        except SyntaxError as exc:
            pytest.fail(f"apscheduler render produced invalid Python: {exc}\n{out}")


# =====================================================================
# Cron
# =====================================================================


class TestCronRender:
    def test_cron_emits_unmodified_expression(self) -> None:
        out = cron.render([_schedule("hourly")])
        # Header explains WRAPPER convention.
        assert "WRAPPER=" in out
        # Schedule line with the original expression + task name.
        assert "0 * * * * $WRAPPER myapp.tasks.hourly" in out

    def test_interval_30_seconds_skipped(self) -> None:
        # 30s is sub-minute, can't be expressed in cron. The
        # warning lands as two-line comment block; check for the
        # phrase ignoring whitespace+linewraps so a future
        # rewording with the same intent still passes.
        out = cron.render([_schedule("sub", kind="interval", expression="30s")])
        # The warning is split across two comment lines, with `#`
        # markers in between. Check the salient half.
        assert "5-field cron" in out
        # No actual cron line for it.
        assert "$WRAPPER myapp.tasks.sub" not in out

    def test_interval_5m_emits_slash_5(self) -> None:
        out = cron.render([_schedule("p", kind="interval", expression="5m")])
        assert "*/5 * * * * $WRAPPER" in out

    def test_interval_1h_emits_hourly(self) -> None:
        out = cron.render([_schedule("p", kind="interval", expression="1h")])
        assert "0 * * * * $WRAPPER" in out

    def test_disabled_commented_out(self) -> None:
        out = cron.render([_schedule("off", enabled=False)])
        assert "# DISABLED in z4j" in out

    def test_one_shot_explanatory_comment(self) -> None:
        out = cron.render(
            [_schedule("once", kind="one_shot", expression="2026-04-30T00:00:00")],
        )
        assert "no native one-shot" in out


# =====================================================================
# Empty + sanity
# =====================================================================


class TestEmpty:
    def test_celery_empty(self) -> None:
        out = celery.render([])
        assert "beat_schedule = {" in out
        assert "}" in out
        ast.parse(out)

    def test_rq_empty(self) -> None:
        out = rq.render([])
        assert "no z4j schedules to migrate" in out
        ast.parse(out)

    def test_aps_empty(self) -> None:
        out = apscheduler.render([])
        assert "no z4j schedules to migrate" in out
        ast.parse(out)

    def test_cron_empty(self) -> None:
        out = cron.render([])
        # Just the header + WRAPPER variable, no schedule lines.
        assert "WRAPPER=" in out


# =====================================================================
# §17.5 reverse-migration "back-out plan" verification.
#
# The spec promises: "operators can paste it into their settings.py
# to revert to celery-beat. This is part of the trust contract."
# Parseable Python (the existing tests) is necessary but not
# sufficient - the rendered output must also evaluate into a
# beat_schedule dict whose semantics match the source schedules.
# These tests exec the output with celery's symbols stubbed out so
# we can pin the dict shape without requiring celery in the test
# environment.
# =====================================================================


def _exec_celery_export(source: str) -> dict:
    """Exec a celery export and return its ``beat_schedule`` dict.

    Stubs out ``crontab`` and ``timedelta`` with capture functions so
    the resulting dict carries inspectable representations of the
    schedule expressions instead of opaque celery objects. Lets the
    test assert "the operator gets a 5-minute crontab" without
    importing celery itself.
    """
    namespace: dict = {}

    class _CaptureCrontab:
        def __init__(self, **kwargs: object) -> None:
            self.kind = "crontab"
            self.kwargs = kwargs

        def __repr__(self) -> str:  # pragma: no cover - debug only
            return f"crontab({self.kwargs})"

    class _CaptureTimedelta:
        def __init__(self, **kwargs: object) -> None:
            self.kind = "timedelta"
            self.seconds = int(kwargs.get("seconds", 0))

        def __repr__(self) -> str:  # pragma: no cover - debug only
            return f"timedelta(seconds={self.seconds})"

    # Strip the import lines so we don't need celery / datetime to
    # be importable in their real form. The exec sees the captures
    # under the same names so the rest of the module body works
    # unchanged.
    body_lines = [
        line for line in source.splitlines()
        if not line.startswith("from celery.schedules")
        and not line.startswith("from datetime")
    ]
    body = "\n".join(body_lines)
    namespace["crontab"] = _CaptureCrontab
    namespace["timedelta"] = _CaptureTimedelta
    exec(compile(body, "<celery-export>", "exec"), namespace)  # noqa: S102
    return namespace["beat_schedule"]


class TestCeleryExecRoundTrip:
    """The rendered celery output must exec to a usable beat_schedule."""

    def test_cron_exec_recovers_minute_hour(self) -> None:
        out = celery.render([
            _schedule("nightly", expression="0 3 * * *"),
        ])
        beat = _exec_celery_export(out)
        assert "nightly" in beat
        entry = beat["nightly"]
        # Operator-facing contract: each entry has ``task``,
        # ``schedule``, ``args``, ``kwargs``.
        assert entry["task"] == "myapp.tasks.nightly"
        assert entry["schedule"].kind == "crontab"
        assert entry["schedule"].kwargs["minute"] == "0"
        assert entry["schedule"].kwargs["hour"] == "3"
        assert entry["args"] == []
        assert entry["kwargs"] == {}

    def test_interval_exec_recovers_seconds(self) -> None:
        out = celery.render([
            _schedule("poll", kind="interval", expression="5m"),
        ])
        beat = _exec_celery_export(out)
        entry = beat["poll"]
        assert entry["schedule"].kind == "timedelta"
        assert entry["schedule"].seconds == 300

    def test_queue_exec_lands_in_options(self) -> None:
        out = celery.render([
            _schedule("hourly", queue="critical"),
        ])
        beat = _exec_celery_export(out)
        assert beat["hourly"]["options"]["queue"] == "critical"

    def test_args_kwargs_round_trip(self) -> None:
        out = celery.render([
            _schedule(
                "report",
                args=["weekly", 7],
                kwargs={"format": "pdf", "redact": True},
            ),
        ])
        beat = _exec_celery_export(out)
        entry = beat["report"]
        assert entry["args"] == ["weekly", 7]
        assert entry["kwargs"] == {"format": "pdf", "redact": True}

    def test_multiple_schedules_all_recovered(self) -> None:
        out = celery.render([
            _schedule("a", expression="*/5 * * * *"),
            _schedule("b", kind="interval", expression="60s"),
            _schedule("c", queue="low"),
        ])
        beat = _exec_celery_export(out)
        assert set(beat.keys()) == {"a", "b", "c"}
        assert beat["a"]["schedule"].kwargs["minute"] == "*/5"
        assert beat["b"]["schedule"].seconds == 60
        assert beat["c"]["options"]["queue"] == "low"


class TestRqExecRoundTrip:
    """RQ output is bash-style script - validate the shape via AST."""

    def test_rq_emits_scheduler_call_per_schedule(self) -> None:
        out = rq.render([
            _schedule("hourly"),
            _schedule("poll", kind="interval", expression="30s"),
        ])
        # Expect one ``scheduler.cron(...)`` and one
        # ``scheduler.schedule(...)`` call for the two schedules.
        assert out.count("scheduler.cron(") == 1
        assert out.count("scheduler.schedule(") == 1
        # Both task names land in the script.
        assert "myapp.tasks.hourly" in out
        assert "myapp.tasks.poll" in out


class TestApsExecRoundTrip:
    """APScheduler output is plain ``add_job(...)`` calls."""

    def test_aps_emits_add_job_per_schedule(self) -> None:
        out = apscheduler.render([
            _schedule("hourly"),
            _schedule("poll", kind="interval", expression="60s"),
        ])
        assert out.count("scheduler.add_job(") == 2
        # Each gets a unique job_id derived from the brain UUID so
        # operators can identify and replace the schedule cleanly.
        assert "id=" in out

    def test_disabled_jobs_emit_paused_true(self) -> None:
        # APScheduler supports a native ``paused=True`` job state, so
        # disabled rows still register with the scheduler but do not
        # fire until ``scheduler.resume_job(id)`` is called. Distinct
        # from the rq exporter which comments the call out (rq has no
        # native pause). Operators who want a hard "do not load" must
        # delete the schedule in z4j first.
        out = apscheduler.render([
            _schedule("a"),
            _schedule("b", enabled=False),
        ])
        assert out.count("scheduler.add_job(") == 2
        assert "paused=True" in out


class TestCronExecRoundTrip:
    """Cron output is a crontab(5) text file."""

    def test_cron_lines_use_5_field_format(self) -> None:
        out = cron.render([
            _schedule("hourly"),  # cron 0 * * * *
            _schedule("daily", expression="0 3 * * *"),
        ])
        # Find every non-comment, non-empty line that's not the
        # WRAPPER assignment - those should be schedule lines.
        import re
        schedule_lines = [
            line for line in out.splitlines()
            if line.strip()
            and not line.lstrip().startswith("#")
            and not line.startswith("WRAPPER")
        ]
        assert len(schedule_lines) == 2
        for line in schedule_lines:
            # First five whitespace-separated fields are the cron
            # spec; the rest is the command. Normalise tabs first
            # since cron is whitespace-agnostic but our renderer
            # uses single spaces.
            fields = re.split(r"\s+", line, maxsplit=5)
            assert len(fields) == 6, (
                f"cron line malformed: {line!r}"
            )
            # Field 6 is the command; it must reference the wrapper
            # so operators can drop a single setup-script in once
            # rather than per-line.
            assert "$WRAPPER" in fields[5]
