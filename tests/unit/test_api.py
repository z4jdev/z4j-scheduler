"""Tests for the FastAPI operational endpoints.

Uses :class:`fastapi.testclient.TestClient` (which is sync) - no
real network, no real lifespan. Constructs a fresh ``SchedulerState``
per test and asserts on the four endpoints' responses.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from z4j_scheduler.api._state import SchedulerState
from z4j_scheduler.api.app import create_app
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage.cache import ScheduleCache


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    cert = tmp_path / "scheduler.crt"
    key = tmp_path / "scheduler.key"
    ca = tmp_path / "brain-ca.crt"
    for p in (cert, key, ca):
        p.write_text("dummy")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))
    monkeypatch.setenv("Z4J_SCHEDULER_INSTANCE_ID", "test-instance")
    return Settings(_env_file=None)  # type: ignore[call-arg]


def _make_client(settings: Settings, *, ready: bool = True) -> TestClient:
    state = SchedulerState(settings=settings)
    if ready:
        state.brain_client_connected = True
        state.cache_initial_sync_complete = True
        state.leader_gate_initialised = True
    state.cache = ScheduleCache()
    return TestClient(create_app(state))


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_returns_alive(self, settings: Settings) -> None:
        client = _make_client(settings)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "alive"}

    def test_health_works_even_when_not_ready(self, settings: Settings) -> None:
        client = _make_client(settings, ready=False)
        # Health is liveness only - returns 200 even when not ready.
        assert client.get("/health").status_code == 200


# ---------------------------------------------------------------------------
# /ready
# ---------------------------------------------------------------------------


class TestReady:
    def test_ready_when_all_subsystems_up(self, settings: Settings) -> None:
        client = _make_client(settings, ready=True)
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

    def test_not_ready_when_brain_disconnected(self, settings: Settings) -> None:
        state = SchedulerState(settings=settings)
        state.cache_initial_sync_complete = True
        state.leader_gate_initialised = True
        # brain_client_connected stays False
        client = TestClient(create_app(state))
        response = client.get("/ready")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "not_ready"
        assert "brain_client" in body["missing"]

    def test_not_ready_lists_every_missing_subsystem(
        self, settings: Settings,
    ) -> None:
        state = SchedulerState(settings=settings)
        # No subsystems up.
        client = TestClient(create_app(state))
        response = client.get("/ready")
        assert response.status_code == 503
        body = response.json()
        assert set(body["missing"]) == {
            "brain_client",
            "cache_initial_sync",
            "leader_gate",
        }


# ---------------------------------------------------------------------------
# /info
# ---------------------------------------------------------------------------


class TestInfo:
    def test_info_returns_runtime_snapshot(self, settings: Settings) -> None:
        client = _make_client(settings)
        response = client.get("/info")
        assert response.status_code == 200
        body = response.json()
        assert body["instance_id"] == "test-instance"
        assert body["ready"] is True
        assert body["schedules_loaded"] == 0
        assert body["uptime_seconds"] >= 0
        assert "version" in body
        assert "started_at" in body

    def test_info_does_not_leak_secrets(self, settings: Settings) -> None:
        # Even if metrics_auth_token is set (it's not here, but if it
        # were), it must NOT appear in /info.
        client = _make_client(settings)
        body = client.get("/info").text
        # Cert paths shouldn't appear either - they're sensitive
        # configuration, not status. The string "tls_" should not
        # appear.
        assert "tls_" not in body.lower()

    def test_info_does_not_leak_topology(self, settings: Settings) -> None:
        """Audit fix S003 (1.4.0): /info must not expose brain URL.

        Pre-fix, /info returned ``brain_grpc_url`` and the projects
        list, which let any unauthenticated network-reachable caller
        learn the upstream brain URL and per-instance project
        bindings. With the scheduler's bind_host now defaulting to
        127.0.0.1 the network exposure is gone too, but defense in
        depth: redact these fields even when an operator opts back
        into 0.0.0.0 binding via a reverse proxy.
        """
        client = _make_client(settings)
        body = client.get("/info").json()
        assert "brain_grpc_url" not in body, (
            "leaks the upstream brain URL"
        )
        assert "projects" not in body, (
            "leaks the per-instance project bindings"
        )


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_metrics_returns_prometheus_format(
        self, settings: Settings,
    ) -> None:
        client = _make_client(settings)
        response = client.get("/metrics")
        assert response.status_code == 200
        # Prometheus exposition has a specific content type.
        assert "text/plain" in response.headers["content-type"]
        # And our metric names appear.
        text = response.text
        assert "z4j_scheduler_fires_total" in text
        assert "z4j_scheduler_schedules_loaded" in text

    def test_metrics_no_auth_when_token_unset(
        self, settings: Settings,
    ) -> None:
        client = _make_client(settings)
        # No Authorization header - 200 OK.
        assert client.get("/metrics").status_code == 200

    def test_metrics_requires_bearer_when_token_set(
        self, settings: Settings, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN", "supersecret123",
        )
        gated_settings = Settings(_env_file=None)  # type: ignore[call-arg]
        client = _make_client(gated_settings)

        # No header - 401.
        assert client.get("/metrics").status_code == 401

        # Wrong header - 401.
        response = client.get(
            "/metrics", headers={"Authorization": "Bearer wrong"},
        )
        assert response.status_code == 401

        # Correct header - 200.
        response = client.get(
            "/metrics",
            headers={"Authorization": "Bearer supersecret123"},
        )
        assert response.status_code == 200
