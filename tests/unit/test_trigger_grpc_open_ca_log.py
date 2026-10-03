"""The serving line of the trigger gRPC server states the open-CA posture at WARNING.

The scheduler docs promise a warning when ``Z4J_SCHEDULER_TRIGGER_GRPC_ALLOWED_CNS``
is empty. The decision-time ``trigger_grpc_open_ca`` warning has always fired;
the line that follows it, announcing the bound address, restated the open
state at INFO, so an operator filtering on WARNING saw a warning and then a
line that read as routine. These tests drive ``start()`` with the gRPC server
and credentials stubbed, and read the records the real logger emits.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("grpc")

from z4j_scheduler.settings import Settings
from z4j_scheduler.trigger_grpc import server as server_module

LOGGER = "z4j.scheduler.trigger_grpc.server"


class _FakeAioServer:
    def add_secure_port(self, address: str, credentials: object) -> int:
        self.address = address
        return 7701

    async def start(self) -> None:
        self.started = True


def _settings(tmp_path: Path, allowed_cns: list[str]) -> Settings:
    cert = tmp_path / "srv.crt"
    key = tmp_path / "srv.key"
    ca = tmp_path / "ca.crt"
    for path in (cert, key, ca):
        path.write_bytes(b"stubbed-below")
    return Settings(
        brain_grpc_url="brain:7701",
        brain_rest_url="http://brain:7700",
        brain_api_token="x" * 16,
        projects="acme",
        tls_cert=cert,
        tls_key=key,
        tls_ca=ca,
        trigger_grpc_enabled=True,
        trigger_grpc_bind_host="127.0.0.1",
        trigger_grpc_bind_port=0,
        trigger_grpc_tls_cert=cert,
        trigger_grpc_tls_key=key,
        trigger_grpc_tls_ca=ca,
        trigger_grpc_allowed_cns=allowed_cns,
        trigger_grpc_require_allowlist=False,
    )


async def _start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    allowed_cns: list[str],
) -> None:
    monkeypatch.setattr(server_module, "_build_server_credentials", lambda _settings: object())
    monkeypatch.setattr(server_module.grpc.aio, "server", lambda **_kwargs: _FakeAioServer())
    monkeypatch.setattr(
        server_module.pb_grpc,
        "add_SchedulerServiceServicer_to_server",
        lambda _servicer, _server: None,
    )
    server = server_module.TriggerGrpcServer(
        settings=_settings(tmp_path, allowed_cns),
        cache=MagicMock(),
        dispatcher=MagicMock(),
        leader_gate=MagicMock(),
    )
    await server.start()
    assert server.bound_port == 7701


def _serving_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == LOGGER and "serving on" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_empty_allow_list_serves_with_a_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)

    await _start(tmp_path, monkeypatch, allowed_cns=[])

    (serving,) = _serving_records(caplog)
    assert serving.levelno == logging.WARNING
    message = serving.getMessage()
    assert "open CA: any certificate the CA validates is accepted" in message
    assert "127.0.0.1:0" in message
    assert "Z4J_SCHEDULER_TRIGGER_GRPC_ALLOWED_CNS" in message
    assert getattr(serving, "event", None) == "trigger_grpc_open_ca"
    # The decision-time warning still fires first; the serving line agrees with it.
    warnings = [r for r in caplog.records if r.name == LOGGER and r.levelno == logging.WARNING]
    assert [getattr(r, "event", None) for r in warnings] == [
        "trigger_grpc_open_ca",
        "trigger_grpc_open_ca",
    ]


@pytest.mark.asyncio
async def test_populated_allow_list_serves_at_info_naming_the_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative control: the warning is about the empty list, not about serving."""
    caplog.set_level(logging.INFO, logger=LOGGER)

    await _start(tmp_path, monkeypatch, allowed_cns=["z4j-brain"])

    (serving,) = _serving_records(caplog)
    assert serving.levelno == logging.INFO
    assert "allow-list=('z4j-brain',)" in serving.getMessage()
    assert "open CA" not in serving.getMessage()
    assert not [r for r in caplog.records if r.name == LOGGER and r.levelno >= logging.WARNING]
