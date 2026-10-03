"""CLI wiring for the Huey, arq, taskiq and Dramatiq importers and exporters.

``import --from`` and ``export --to`` dispatch lazily into
``z4j_scheduler.importers.<engine>`` and ``z4j_scheduler.exporters.<engine>``.
These tests run the Typer command end to end with the importer or exporter
module mocked at that boundary, so they pin the flag names, the keyword
arguments each reader receives, the renderer each target resolves to, and the
exit codes, without needing a Huey instance, an arq ``WorkerSettings`` class,
a taskiq broker, or a brain. The readers and renderers themselves are covered
by ``test_importers_exporters_round5.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner
from z4j_scheduler.cli import app
from z4j_scheduler.exporters._client import ExportedSchedule

runner = CliRunner()

IMPORT_SOURCES = (
    # (--from value, locator flag, locator value, reader path, reader kwarg)
    (
        "huey",
        "--huey-app",
        "myapp.tasks:huey",
        "z4j_scheduler.importers.huey.read_huey_app",
        "app_path",
    ),
    (
        "arq",
        "--arq-settings",
        "myapp.worker:WorkerSettings",
        "z4j_scheduler.importers.arq.read_arq_settings",
        "settings_path",
    ),
    (
        "taskiq",
        "--taskiq-broker",
        "myapp.tkq:broker",
        "z4j_scheduler.importers.taskiq.read_taskiq_broker",
        "broker_path",
    ),
)


# =====================================================================
# import --from huey / arq / taskiq
# =====================================================================


class TestEngineImportersAreWired:
    @pytest.mark.parametrize(("source", "flag", "locator", "reader", "kwarg"), IMPORT_SOURCES)
    def test_from_value_dispatches_to_its_reader_with_shared_options(
        self,
        source: str,
        flag: str,
        locator: str,
        reader: str,
        kwarg: str,
    ) -> None:
        with patch(reader, return_value=[]) as read:
            result = runner.invoke(
                app,
                [
                    "import",
                    "--from",
                    source,
                    flag,
                    locator,
                    "--project",
                    "demo",
                    "--queue",
                    "reports",
                    "--timezone",
                    "Europe/Berlin",
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0, result.output
        read.assert_called_once_with(
            **{kwarg: locator},
            project_slug="demo",
            engine=source,
            default_queue="reports",
            default_timezone="Europe/Berlin",
        )
        assert "[dry-run] would push 0 schedule(s)" in result.output

    @pytest.mark.parametrize(("source", "flag", "locator", "reader", "kwarg"), IMPORT_SOURCES)
    def test_engine_override_is_forwarded(
        self,
        source: str,
        flag: str,
        locator: str,
        reader: str,
        kwarg: str,
    ) -> None:
        with patch(reader, return_value=[]) as read:
            result = runner.invoke(
                app,
                [
                    "import",
                    "--from",
                    source,
                    flag,
                    locator,
                    "--project",
                    "demo",
                    "--engine",
                    "custom-engine",
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0, result.output
        assert read.call_args.kwargs["engine"] == "custom-engine"
        assert read.call_args.kwargs[kwarg] == locator

    @pytest.mark.parametrize(("source", "flag", "locator", "reader", "kwarg"), IMPORT_SOURCES)
    def test_missing_locator_is_a_usage_error_naming_the_flag(
        self,
        source: str,
        flag: str,
        locator: str,
        reader: str,
        kwarg: str,
    ) -> None:
        del locator, kwarg
        with patch(reader, return_value=[]) as read:
            result = runner.invoke(
                app,
                ["import", "--from", source, "--project", "demo", "--dry-run"],
            )

        assert result.exit_code == 2
        assert f"{flag} is required for --from {source}" in result.output
        read.assert_not_called()

    def test_from_value_is_case_insensitive(self) -> None:
        with patch("z4j_scheduler.importers.huey.read_huey_app", return_value=[]) as read:
            result = runner.invoke(
                app,
                [
                    "import",
                    "--from",
                    "HUEY",
                    "--huey-app",
                    "myapp.tasks:huey",
                    "--project",
                    "demo",
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0, result.output
        read.assert_called_once()

    def test_unknown_source_lists_the_full_set(self) -> None:
        result = runner.invoke(
            app,
            ["import", "--from", "quartz", "--project", "demo", "--dry-run"],
        )

        assert result.exit_code == 2
        assert "huey, arq, taskiq" in result.output
        assert "dramatiq" in result.output

    def test_help_lists_the_engine_sources_and_their_flags(self) -> None:
        result = runner.invoke(app, ["import", "--help"])

        assert result.exit_code == 0
        for text in ("huey", "arq", "taskiq", "dramatiq"):
            assert text in result.output
        for flag in ("--huey-app", "--arq-settings", "--taskiq-broker"):
            assert flag in result.output


# =====================================================================
# import --from dramatiq (guidance only)
# =====================================================================


class TestDramatiqImportIsGuidanceOnly:
    def test_path_reaches_the_importer_module(self) -> None:
        with patch("z4j_scheduler.importers.dramatiq.read_dramatiq", return_value=[]) as read:
            result = runner.invoke(
                app,
                ["import", "--from", "dramatiq", "--project", "demo", "--dry-run"],
            )

        assert result.exit_code == 0, result.output
        read.assert_called_once_with()

    def test_guidance_is_printed_and_the_command_exits_2(self) -> None:
        result = runner.invoke(
            app,
            ["import", "--from", "dramatiq", "--project", "demo", "--dry-run"],
        )

        assert result.exit_code == 2
        assert "Dramatiq has no built-in scheduler" in result.output
        # The guidance names real flags only.
        assert "--jobstore-url" in result.output
        assert "--apscheduler-app" not in result.output
        assert "Traceback" not in result.output


# =====================================================================
# export --to huey / arq / taskiq / dramatiq
# =====================================================================


def _one_schedule() -> list[ExportedSchedule]:
    return [
        ExportedSchedule(
            id="0f0e0d0c-0b0a-4908-8706-050403020100",
            name="nightly-cleanup",
            engine="huey",
            kind="cron",
            expression="0 3 * * *",
            task_name="myapp.tasks.nightly_cleanup",
        ),
    ]


class TestEngineExportersAreWired:
    @pytest.mark.parametrize(
        ("target", "renderer"),
        [
            ("huey", "z4j_scheduler.exporters.huey.render"),
            ("arq", "z4j_scheduler.exporters.arq.render"),
            ("taskiq", "z4j_scheduler.exporters.taskiq.render"),
            ("dramatiq", "z4j_scheduler.exporters.dramatiq.render"),
        ],
    )
    def test_to_value_resolves_its_renderer(self, target: str, renderer: str) -> None:
        schedules = _one_schedule()
        with (
            patch(
                "z4j_scheduler.exporters._client.fetch_schedules",
                new=AsyncMock(return_value=schedules),
            ),
            patch(renderer, return_value=f"# rendered for {target}\n") as render,
        ):
            result = runner.invoke(
                app,
                ["export", "--to", target, "--project", "demo", "--brain-url", "http://brain"],
            )

        assert result.exit_code == 0, result.output
        render.assert_called_once_with(schedules)
        assert f"# rendered for {target}" in result.output

    def test_to_value_is_case_insensitive(self) -> None:
        with (
            patch(
                "z4j_scheduler.exporters._client.fetch_schedules",
                new=AsyncMock(return_value=[]),
            ),
            patch("z4j_scheduler.exporters.arq.render", return_value="# arq\n") as render,
        ):
            result = runner.invoke(app, ["export", "--to", "ARQ", "--project", "demo"])

        assert result.exit_code == 0, result.output
        render.assert_called_once_with([])

    def test_dramatiq_export_renders_guidance_as_comments(self) -> None:
        with patch(
            "z4j_scheduler.exporters._client.fetch_schedules",
            new=AsyncMock(return_value=_one_schedule()),
        ):
            result = runner.invoke(app, ["export", "--to", "dramatiq", "--project", "demo"])

        assert result.exit_code == 0, result.output
        assert "Dramatiq has no native scheduler config" in result.output
        assert "--to apscheduler" in result.output
        assert "--to jsonl" not in result.output
        for line in result.output.splitlines():
            if line.strip():
                assert line.lstrip().startswith("#"), line

    def test_unknown_target_lists_the_full_set(self) -> None:
        with patch(
            "z4j_scheduler.exporters._client.fetch_schedules",
            new=AsyncMock(return_value=[]),
        ) as fetch:
            result = runner.invoke(app, ["export", "--to", "quartz", "--project", "demo"])

        assert result.exit_code == 2
        assert "huey, arq, taskiq, or dramatiq" in result.output
        fetch.assert_not_called()

    def test_help_lists_the_engine_targets(self) -> None:
        result = runner.invoke(app, ["export", "--help"])

        assert result.exit_code == 0
        for text in ("huey", "arq", "taskiq", "dramatiq"):
            assert text in result.output

    def test_brain_down_exits_two_with_a_message_not_a_traceback(self) -> None:
        import httpx

        with patch(
            "z4j_scheduler.exporters._client.fetch_schedules",
            new=AsyncMock(side_effect=httpx.ConnectError("All connection attempts failed")),
        ):
            result = runner.invoke(
                app,
                ["export", "--to", "dramatiq", "--project", "demo", "--brain-url", "http://b:1"],
            )

        assert result.exit_code == 2, result.output
        assert "brain not reachable at http://b:1" in result.output
        assert "ConnectError" in result.output
        assert "Traceback" not in result.output
        assert not isinstance(result.exception, httpx.HTTPError)

    def test_brain_refusal_exits_two_with_the_client_message(self) -> None:
        with patch(
            "z4j_scheduler.exporters._client.fetch_schedules",
            new=AsyncMock(side_effect=RuntimeError("brain returned 404 for project 'demo'")),
        ):
            result = runner.invoke(app, ["export", "--to", "huey", "--project", "demo"])

        assert result.exit_code == 2, result.output
        assert "export: brain returned 404 for project 'demo'" in result.output
        assert "Traceback" not in result.output
