"""Settings validation - smoke tests.

Phase 0 commit ships only the basics; full settings tests land in
Phase 1 alongside the actual usages.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from z4j_scheduler.main import SchedulerApp
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage._models import PingInfo

_COMPATIBILITY_ONLY_FIELDS = {
    "database_url",
    "projects",
    "pro_license_key",
    "leader_poll_interval_seconds",
}


class _CompatibilityFieldTrap:
    """Delegate settings reads, but fail if runtime wiring reads old knobs."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def __getattr__(self, name: str):
        if name in _COMPATIBILITY_ONLY_FIELDS:
            raise AssertionError(f"runtime read deprecated setting {name!r}")
        return getattr(self._settings, name)


class _FakeBrainClient:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def ping(self) -> PingInfo:
        return PingInfo(
            brain_version="1.9.0",
            brain_time=datetime.now(UTC),
            scheduler_protocol_epoch=1,
        )

    async def negotiate_protocol(self, offered):
        return offered


class _AppWithFakeClient(SchedulerApp):
    def _build_brain_client(self):
        return _FakeBrainClient()


def _runtime_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "brain_grpc_url": "brain:7701",
        "brain_rest_url": "http://brain:7700",
        "environment": "dev",
        "insecure_grpc": True,
        "database_url": "postgresql://deprecated-database",
        "projects": "deprecated-project-filter",
        "pro_license_key": "deprecated-license",
        "leader_poll_interval_seconds": 59,
    }
    values.update(overrides)
    return Settings(**values, _env_file=None)  # type: ignore[arg-type,call-arg]


def test_settings_requires_brain_grpc_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", raising=False)
    monkeypatch.delenv("Z4J_SCHEDULER_BRAIN_REST_URL", raising=False)
    monkeypatch.delenv("Z4J_SCHEDULER_TLS_CERT", raising=False)
    monkeypatch.delenv("Z4J_SCHEDULER_TLS_KEY", raising=False)
    monkeypatch.delenv("Z4J_SCHEDULER_TLS_CA", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_settings_loads_from_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: object,
) -> None:
    cert = tmp_path / "scheduler.crt"  # type: ignore[operator]
    key = tmp_path / "scheduler.key"  # type: ignore[operator]
    ca = tmp_path / "brain-ca.crt"  # type: ignore[operator]
    cert.write_text("dummy")
    key.write_text("dummy")
    ca.write_text("dummy")

    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))

    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.brain_grpc_url == "brain:7701"
    assert settings.brain_rest_url == "http://brain:7700"
    assert settings.bind_port == 7800
    assert settings.log_level == "INFO"


@pytest.mark.asyncio
async def test_compatibility_settings_are_not_read_during_runtime_wiring() -> None:
    app = _AppWithFakeClient(
        _CompatibilityFieldTrap(_runtime_settings()),  # type: ignore[arg-type]
    )

    await app.start()
    try:
        assert app._watch is not None
        assert app._watch._project_id is None
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_postgres_leader_wiring_uses_replacement_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import z4j_scheduler.leader.postgres as postgres_module

    captured: dict[str, object] = {}

    class FakeBackend:
        def __init__(self, *, dsn: str) -> None:
            captured["dsn"] = dsn

    class FakeGate:
        def __init__(
            self,
            *,
            backend: object,
            namespace: str,
            heartbeat_seconds: float,
        ) -> None:
            captured["backend"] = backend
            captured["namespace"] = namespace
            captured["heartbeat_seconds"] = heartbeat_seconds

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(postgres_module, "AsyncpgLockBackend", FakeBackend)
    monkeypatch.setattr(
        postgres_module,
        "PostgresAdvisoryLockLeaderGate",
        FakeGate,
    )
    settings = _runtime_settings(
        leader_backend="postgres",
        leader_pg_dsn="postgresql://replacement-leader",
        leader_namespace="replacement-namespace",
        leader_heartbeat_seconds=7.0,
    )
    app = SchedulerApp(
        _CompatibilityFieldTrap(settings),  # type: ignore[arg-type]
    )

    gate = await app._build_leader_gate()

    assert isinstance(gate, FakeGate)
    assert captured["dsn"] == "postgresql://replacement-leader"
    assert isinstance(captured["backend"], FakeBackend)
    assert captured["namespace"] == "replacement-namespace"
    assert captured["heartbeat_seconds"] == 7.0
    assert captured["started"] is True


def test_fire_timeout_default_preserves_observed_rpc_deadline() -> None:
    assert Settings.model_fields["fire_timeout_seconds"].default == 10
