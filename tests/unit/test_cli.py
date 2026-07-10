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


# ---------------------------------------------------------------------------
# info (A6): hits the local scheduler /info over HTTP
# ---------------------------------------------------------------------------


def _fake_info_payload() -> dict:
    return {
        "version": "1.6.7",
        "instance_id": "sched-abc",
        "started_at": "2026-06-01T00:00:00+00:00",
        "uptime_seconds": 42.5,
        "ready": True,
        "subsystems": {
            "brain_client_connected": True,
            "cache_initial_sync_complete": True,
            "leader_gate_initialised": True,
        },
        "schedules_loaded": 7,
    }


class _FakeResp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_info_prints_status(monkeypatch) -> None:
    import httpx

    monkeypatch.setattr(
        httpx,
        "get",
        lambda *a, **k: _FakeResp(_fake_info_payload()),
    )
    result = CliRunner().invoke(app, ["info", "--url", "http://localhost:7800"])
    assert result.exit_code == 0, result.output
    assert "sched-abc" in result.output
    assert "schedules_loaded  7" in result.output
    assert "leader_gate_initialised" in result.output


def test_info_json(monkeypatch) -> None:
    import json

    import httpx

    monkeypatch.setattr(
        httpx,
        "get",
        lambda *a, **k: _FakeResp(_fake_info_payload()),
    )
    result = CliRunner().invoke(app, ["info", "--json"])
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["instance_id"] == "sched-abc"
    assert parsed["schedules_loaded"] == 7


def test_info_unreachable_exits_1(monkeypatch) -> None:
    import httpx

    def _boom(*_a, **_k):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", _boom)
    result = CliRunner().invoke(app, ["info"])
    # A non-zero exit (not the old Phase-1 exit 2) is the contract for
    # shell scripts + health probes.
    assert result.exit_code == 1
