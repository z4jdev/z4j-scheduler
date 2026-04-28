"""CLI smoke tests."""

from __future__ import annotations

from typer.testing import CliRunner
from z4j_scheduler.cli import app


def test_version_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    # Just check there's something version-shaped on stdout.
    assert any(c.isdigit() for c in result.stdout)


def test_help_lists_subcommands() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "serve" in result.stdout
    assert "version" in result.stdout
