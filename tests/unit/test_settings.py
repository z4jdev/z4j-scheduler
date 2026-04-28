"""Settings validation - smoke tests.

Phase 0 commit ships only the basics; full settings tests land in
Phase 1 alongside the actual usages.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from z4j_scheduler.settings import Settings


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
