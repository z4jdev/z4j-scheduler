"""Tests for the FastAPI operational endpoints.

Uses :class:`fastapi.testclient.TestClient` (which is sync) - no
real network, no real lifespan. Constructs a fresh ``SchedulerState``
per test and asserts on the four endpoints' responses.
"""

from __future__ import annotations

from contextlib import closing
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
    # 1.6.5: the new fail-safe refuses to start a
    # production scheduler that binds to 0.0.0.0 with metrics on
    # and no auth token. Existing test fixtures used the default
    # ``environment="production"`` + default ``bind_host="0.0.0.0"``,
    # which now trips the validator. Override to dev so the fixture
    # constructs cleanly; tests that specifically exercise the
    # production fail-safe set the env explicitly themselves.
    monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "dev")
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
        with closing(_make_client(settings)) as client:
            response = client.get("/health")
            assert response.status_code == 200
            assert response.json() == {"status": "alive"}

    def test_health_works_even_when_not_ready(self, settings: Settings) -> None:
        with closing(_make_client(settings, ready=False)) as client:
            # Health is liveness only - returns 200 even when not ready.
            assert client.get("/health").status_code == 200


# ---------------------------------------------------------------------------
# /ready
# ---------------------------------------------------------------------------


class TestReady:
    def test_ready_when_all_subsystems_up(self, settings: Settings) -> None:
        with closing(_make_client(settings, ready=True)) as client:
            response = client.get("/ready")
            assert response.status_code == 200
            assert response.json() == {"status": "ready"}

    def test_not_ready_when_brain_disconnected(self, settings: Settings) -> None:
        state = SchedulerState(settings=settings)
        state.cache_initial_sync_complete = True
        state.leader_gate_initialised = True
        # brain_client_connected stays False
        with closing(TestClient(create_app(state))) as client:
            response = client.get("/ready")
            assert response.status_code == 503
            body = response.json()
            assert body["status"] == "not_ready"
            assert "brain_client" in body["missing"]

    def test_not_ready_lists_every_missing_subsystem(
        self,
        settings: Settings,
    ) -> None:
        state = SchedulerState(settings=settings)
        # No subsystems up.
        with closing(TestClient(create_app(state))) as client:
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
        with closing(_make_client(settings)) as client:
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
        with closing(_make_client(settings)) as client:
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
        with closing(_make_client(settings)) as client:
            body = client.get("/info").json()
            assert "brain_grpc_url" not in body, "leaks the upstream brain URL"
            assert "projects" not in body, "leaks the per-instance project bindings"


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_metrics_returns_prometheus_format(
        self,
        settings: Settings,
    ) -> None:
        with closing(_make_client(settings)) as client:
            response = client.get("/metrics")
            assert response.status_code == 200
            # Prometheus exposition has a specific content type.
            assert "text/plain" in response.headers["content-type"]
            # And our metric names appear.
            text = response.text
            assert "z4j_scheduler_fires_total" in text
            assert "z4j_scheduler_schedules_loaded" in text

    def test_metrics_no_auth_when_token_unset(
        self,
        settings: Settings,
    ) -> None:
        with closing(_make_client(settings)) as client:
            # No Authorization header - 200 OK.
            assert client.get("/metrics").status_code == 200

    def test_metrics_requires_bearer_when_token_set(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            "supersecret123",
        )
        gated_settings = Settings(_env_file=None)  # type: ignore[call-arg]
        with closing(_make_client(gated_settings)) as client:
            # No header - 401.
            assert client.get("/metrics").status_code == 401

            # Wrong header - 401.
            response = client.get(
                "/metrics",
                headers={"Authorization": "Bearer wrong"},
            )
            assert response.status_code == 401

            # Correct header - 200.
            response = client.get(
                "/metrics",
                headers={"Authorization": "Bearer supersecret123"},
            )
            assert response.status_code == 200

    # ------------------------------------------------------------------
    # 1.6.5 audit: honor the metrics_enabled toggle
    # ------------------------------------------------------------------

    def test_metrics_disabled_returns_404(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When ``Z4J_SCHEDULER_METRICS_ENABLED=false`` the /metrics
        route MUST not be mounted. Pre-1.6.5 the setting was
        unused: operators who set it to false still got 200 from
        the endpoint, undermining their declared intent.
        """
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "false")
        disabled_settings = Settings(_env_file=None)  # type: ignore[call-arg]
        assert disabled_settings.metrics_enabled is False, (
            "test fixture: env var should have parsed as False"
        )

        with closing(_make_client(disabled_settings)) as client:
            response = client.get("/metrics")
            assert response.status_code == 404, (
                "1.6.5 regression: when metrics_enabled=false the "
                "/metrics route MUST NOT be mounted (404), not 200. "
                f"Got {response.status_code}: {response.text[:200]}"
            )


# ---------------------------------------------------------------------------
# 1.6.5 audit: production fail-safe -- non-loopback bind + no auth
# token + metrics_enabled = ConfigError at startup, not a silent leak.
# ---------------------------------------------------------------------------


class TestR3L1ProductionMetricsFailSafe:
    """Production scheduler with the pre-1.6.5 default
    configuration (bind 0.0.0.0:7800, metrics on, no auth token)
    used to expose project + schedule labels to anyone who could
    reach the port. 1.6.5 makes this fail-fast at startup so an
    operator who didn't intend a public metrics surface notices
    on the first deploy attempt instead of weeks later when a
    security review finds the leak."""

    def test_prod_nonloopback_unauth_metrics_refuses_startup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
        # The dangerous combination:
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "production")
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "0.0.0.0")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "true")
        monkeypatch.delenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            raising=False,
        )

        with pytest.raises(Exception) as exc:
            Settings(_env_file=None)  # type: ignore[call-arg]
        msg = str(exc.value)
        # Error message must name the four-way condition + the
        # three fix options so the operator can recover without
        # reading the source.
        assert "metrics_auth_token" in msg or "/metrics" in msg, msg
        assert (
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN" in msg
            or "loopback" in msg
            or "METRICS_ENABLED" in msg
        ), f"error message missing fix-options guidance: {msg}"

    def test_prod_loopback_unauth_metrics_starts(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Loopback bind is the operator's deliberate choice
        (typical: reverse-proxy in front handles auth). Skip the
        fail-safe in that case."""
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
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "production")
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "true")
        monkeypatch.delenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            raising=False,
        )
        # Should NOT raise.
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.bind_host == "127.0.0.1"

    def test_prod_nonloopback_auth_token_starts(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "production")
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "0.0.0.0")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "true")
        monkeypatch.setenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            "supersecretthirtytwo_bytes_random!!",
        )
        # Should NOT raise: auth token closes the loop.
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.metrics_auth_token is not None

    def test_prod_metrics_disabled_starts_without_auth(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "production")
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "0.0.0.0")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "false")
        monkeypatch.delenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            raising=False,
        )
        # Metrics off -> no /metrics surface -> no need to gate.
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.metrics_enabled is False

    def test_dev_environment_skips_check(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dev / test / CI environments skip the check so
        fixtureless setups continue to work."""
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
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "dev")
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "0.0.0.0")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_ENABLED", "true")
        monkeypatch.delenv(
            "Z4J_SCHEDULER_METRICS_AUTH_TOKEN",
            raising=False,
        )
        # Should NOT raise even though prod combo would be refused.
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.environment == "dev"
