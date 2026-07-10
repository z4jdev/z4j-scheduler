"""Tests for the Phase-5 CLI additions.

Covers:

- ``schedules add/list/trigger/disable/enable`` subcommand registration
- ``import --verify`` flag wiring (the helper is unit-tested here;
  the full HTTP path is covered by integration smoke)
- Django management command shape (importable + dispatches)

The full e2e against a real brain is covered by
``tests/integration/test_export_e2e.py`` style tests; these are
the fast unit-level regressions that catch typer arg-spec drift.
"""

from __future__ import annotations

import sys
from unittest.mock import patch

from typer.testing import CliRunner
from z4j_scheduler.cli import app

runner = CliRunner()


# =====================================================================
# schedules subcommand registration
# =====================================================================


class TestSchedulesSubcommandsExist:
    def test_schedules_help_lists_all_subcommands(self) -> None:
        result = runner.invoke(app, ["schedules", "--help"])
        assert result.exit_code == 0
        # All five Phase-5 subcommands present in --help.
        for cmd in ("add", "list", "trigger", "disable", "enable"):
            assert cmd in result.stdout, f"`schedules {cmd}` missing from help: {result.stdout}"

    def test_schedules_add_help_lists_required_options(self) -> None:
        result = runner.invoke(app, ["schedules", "add", "--help"])
        assert result.exit_code == 0
        # Required options are documented.
        for opt in ("--project", "--name", "--kind", "--expression", "--task-name"):
            assert opt in result.stdout, f"`add` missing required option {opt}"


# =====================================================================
# import --verify flag is wired
# =====================================================================


class TestImportVerifyFlag:
    def test_help_shows_verify_flag(self) -> None:
        result = runner.invoke(app, ["import", "--help"])
        assert result.exit_code == 0
        assert "--verify" in result.stdout

    def test_verify_helper_classifies_correctly(self) -> None:
        """The diff helper buckets schedules by source_hash match."""
        from z4j_scheduler.cli import _print_verify_diff
        from z4j_scheduler.importers._core import ImportedSchedule

        # Source has three schedules: one new, one with changed
        # expression (update), one identical to brain (unchanged).
        # Brain also has a fourth row that source dropped (delete).
        new_sched = ImportedSchedule(
            project_slug="p",
            name="new",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
            source="src",
        )
        changed = ImportedSchedule(
            project_slug="p",
            name="changed",
            engine="celery",
            kind="cron",
            expression="*/15 * * * *",
            task_name="t",  # different from brain
            source="src",
        )
        identical = ImportedSchedule(
            project_slug="p",
            name="identical",
            engine="celery",
            kind="cron",
            expression="0 0 * * *",
            task_name="t",
            source="src",
        )
        identical_hash = identical.compute_hash()

        # Mock httpx.get so the helper sees brain's current view.
        class _FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> list:
                return [
                    {
                        "name": "changed",
                        "source": "src",
                        "source_hash": "old-hash",
                    },
                    {
                        "name": "identical",
                        "source": "src",
                        "source_hash": identical_hash,
                    },
                    {
                        "name": "removed-by-source",
                        "source": "src",
                        "source_hash": "x",
                    },
                ]

        captured: list[str] = []

        def _fake_echo(msg, *args, **kw):
            captured.append(str(msg))

        with (
            patch("httpx.get", return_value=_FakeResponse()),
            patch("typer.echo", _fake_echo),
        ):
            _print_verify_diff(
                schedules=[new_sched, changed, identical],
                brain_url="http://brain",
                api_token=None,
                project="p",
            )

        joined = "\n".join(captured)
        # Each schedule lands in the right bucket.
        assert "INSERT: new" in joined
        assert "UPDATE: changed" in joined
        assert "DELETE: removed-by-source" in joined
        # Counts in the summary line.
        assert "insert=1" in joined
        assert "update=1" in joined
        assert "unchanged=1" in joined
        assert "delete=1" in joined


# =====================================================================
# Django management command import shape
# =====================================================================


class TestDjangoCommandImportable:
    def test_command_module_imports(self) -> None:
        # Module is shaped to be importable even without Django -
        # BaseCommand falls back to ``object``. Importing without
        # Django installed shouldn't crash the package.
        from z4j_scheduler.django_app.management.commands import (
            z4j_schedules,
        )

        assert hasattr(z4j_schedules, "Command")

    def test_command_has_subcommand_choices(self) -> None:
        # Sanity: the command wires the four subcommands the spec
        # promises (sync, list, diff, trigger).
        import inspect

        from z4j_scheduler.django_app.management.commands import (
            z4j_schedules,
        )

        source = inspect.getsource(z4j_schedules)
        for cmd in ("sync", "list", "diff", "trigger"):
            assert f'"{cmd}"' in source, f"missing subcommand {cmd!r}"

    def test_app_config_optionally_imports(self) -> None:
        # The AppConfig falls back to ``object`` when Django is
        # absent so the package import doesn't blow up.
        from z4j_scheduler.django_app import apps

        assert hasattr(apps, "Z4JSchedulerConfig")


# =====================================================================
# Django framework helper - reconcile_from_settings
# =====================================================================


class TestDjangoReconcileFromSettings:
    def test_returns_none_when_django_missing(self) -> None:
        from z4j_scheduler.declarative.frameworks.django import (
            reconcile_from_settings,
        )

        # Pretend django.conf.settings has no Z4J_SCHEDULES.
        with patch.dict(sys.modules, {"django.conf": None}):
            result = reconcile_from_settings()
        # Either Django absent → None, or settings missing → None.
        assert result is None
