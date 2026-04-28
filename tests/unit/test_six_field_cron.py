"""Tests for the optional 6-field cron expression with seconds.

docs/SCHEDULER.md §5.1: *"cron - any standard 5-field expression
(with optional 6th seconds field for higher resolution where the
engine supports it)."*

croniter natively supports the 6-field form
``minute hour dom month dow second``. The pieces of z4j-scheduler
that touch cron expressions had to learn to accept length 6:

- ``next_fire`` (cron.py) - already passes through to croniter, no
  code change needed; this test pins the contract.
- The shadow comparator's ``_predict_cron`` - same, just a pin.
- The dashboard DST-warning helper - extended to accept length 6
  (the trailing seconds field doesn't affect DST analysis since
  DST transitions happen on hour boundaries).
- The celery exporter - emits a warning comment above the dict
  entry when the seconds field is dropped on export (celery's
  ``crontab()`` is minute-resolution only).

The 6-field form is **not** an opt-in to high-frequency scheduling
- z4j still adds ~10-30ms decoupling per fire (§6). The form
exists for operators who need a fire at a specific second-within-
minute boundary (e.g. trading systems that fire at exactly the
30th second of each minute for clock-aligned snapshots).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from z4j_scheduler.exporters import celery as celery_exporter
from z4j_scheduler.exporters._client import ExportedSchedule
from z4j_scheduler.importers._core import ImportedSchedule
from z4j_scheduler.tick.cron import is_valid, next_fire
from z4j_scheduler.verify.shadow_comparator import predict_fires


class TestNextFireAcceptsSixField:
    def test_is_valid_recognises_six_field(self) -> None:
        # Both shapes parse.
        assert is_valid("0 * * * *") is True
        assert is_valid("0 * * * * 30") is True

    def test_six_field_fires_at_specified_second(self) -> None:
        # ``0 * * * * 30`` = at minute=0, second=30. The next match
        # strictly after 12:00:00 is 12:00:30 (same hour - second=30
        # is the only constraint that "advances" past anchor).
        anchor = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        nxt = next_fire("0 * * * * 30", "UTC", anchor)
        assert nxt == datetime(2026, 4, 27, 12, 0, 30, tzinfo=UTC)
        # The fire after that jumps to the next hour's minute=0
        # because second=30 is already past for minute=0.
        nxt2 = next_fire("0 * * * * 30", "UTC", nxt)
        assert nxt2 == datetime(2026, 4, 27, 13, 0, 30, tzinfo=UTC)

    def test_six_field_subsequent_fires_evenly_spaced(self) -> None:
        # ``* * * * * 30`` = the 30th second of every minute.
        # Two consecutive fires must be exactly 60 seconds apart.
        anchor = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        first = next_fire("* * * * * 30", "UTC", anchor)
        second = next_fire("* * * * * 30", "UTC", first)
        assert (second - first).total_seconds() == 60.0


class TestPredictFiresAcceptsSixField:
    def test_six_field_in_window_yields_per_minute_fires(self) -> None:
        # 5-minute window, fires at second 30 of every minute = 5 fires.
        sched = ImportedSchedule(
            project_slug="p",
            name="tick",
            engine="celery",
            kind="cron",
            expression="* * * * * 30",
            task_name="app.tick",
            timezone="UTC",
            args=[],
            kwargs={},
            is_enabled=True,
            source="test",
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(minutes=5),
        )
        assert len(fires) == 5
        # All fires land on the 30th second.
        assert all(f.fire_time.second == 30 for f in fires)


class TestCeleryExporterDegradesSixField:
    def _exported(self, expression: str) -> ExportedSchedule:
        return ExportedSchedule(
            id="00000000-0000-0000-0000-000000000001",
            name="tick",
            engine="celery",
            kind="cron",
            expression=expression,
            task_name="app.tick",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            catch_up="skip",
            is_enabled=True,
            scheduler="z4j-scheduler",
            source="dashboard",
        )

    def test_six_field_renders_warning_comment(self) -> None:
        out = celery_exporter.render([
            self._exported("0 * * * * 30"),
        ])
        # Operator MUST see the seconds-loss warning, otherwise
        # they pasted a schedule into celery that runs differently.
        assert "seconds-precision" in out
        assert "celery-beat's crontab()" in out
        assert "minute-resolution only" in out

    def test_six_field_drops_seconds_in_crontab_call(self) -> None:
        out = celery_exporter.render([
            self._exported("0 3 * * * 30"),
        ])
        # The crontab() call must contain the 5-field shape; the
        # seconds field MUST NOT appear inside crontab(...).
        assert "crontab(minute=\"0\", hour=\"3\"" in out
        # And the rendered file must still parse as Python.
        import ast
        try:
            ast.parse(out)
        except SyntaxError as exc:
            pytest.fail(f"6-field export produced invalid Python: {exc}\n{out}")

    def test_five_field_renders_without_warning(self) -> None:
        # No spurious warning when the input is a normal 5-field cron.
        out = celery_exporter.render([self._exported("0 3 * * *")])
        assert "seconds-precision" not in out
