"""Smoke tests for observability/metrics + observability/logging.

Just enough to catch import-time errors and verify the metric
definitions and logger config wire correctly. The behavior
(actual scrape output) is covered by the API tests.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest
import structlog
from z4j_scheduler.observability import metrics as m
from z4j_scheduler.observability.logging import (
    configure_logging,
    reset_for_tests,
)
from z4j_scheduler.settings import Settings


def _raise_scheduler_logger_failure() -> None:
    raise RuntimeError("scheduler logger failure")


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
    return Settings(_env_file=None)  # type: ignore[call-arg]


class TestMetricDefinitions:
    def test_fires_total_increment(self) -> None:
        # Counter increment + read - smoke test that the labels
        # are accepted as defined.
        m.fires_total.labels(status="delivered").inc()
        m.fires_total.labels(status="failed").inc()
        m.fires_total.labels(status="buffered").inc()

    def test_fire_latency_observe(self) -> None:
        m.fire_latency_seconds.observe(0.050)
        m.fire_latency_seconds.observe(0.250)

    def test_is_leader_gauge(self) -> None:
        m.is_leader.labels(project="alpha").set(1)
        m.is_leader.labels(project="beta").set(0)

    def test_grpc_calls_total_label_combos(self) -> None:
        m.grpc_calls_total.labels(method="FireSchedule", status="ok").inc()
        m.grpc_calls_total.labels(
            method="FireSchedule",
            status="UNAVAILABLE",
        ).inc()

    def test_default_registry_exposes_metrics(self) -> None:
        # Every metric we declare should be registered on the
        # default registry. ``collect()`` yields ``Metric`` objects
        # whose ``name`` attribute is the bare metric name without
        # the ``_total`` suffix.
        names = {metric.name for metric in m.default_registry.collect()}
        # Our metrics show up on the registry. Counter names are
        # exposed without the ``_total`` suffix (prometheus_client
        # appends it when rendering).
        assert "z4j_scheduler_fires" in names
        assert "z4j_scheduler_schedules_loaded" in names


class TestLoggingConfig:
    def test_configure_idempotent(self, settings: Settings) -> None:
        reset_for_tests()
        configure_logging(settings)
        # Second call should not raise; should return without
        # reconfiguring.
        configure_logging(settings)

    def test_configure_console_mode(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("Z4J_SCHEDULER_LOG_JSON", "false")
        non_json_settings = Settings(_env_file=None)  # type: ignore[call-arg]
        reset_for_tests()
        configure_logging(non_json_settings)
        # Just verify no exception. The actual output format isn't
        # asserted; structlog wires it.

    def test_non_utf8_pipe_formats_traceback_without_unicode_failure(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        raw = io.BytesIO()
        stream = io.TextIOWrapper(raw, encoding="cp1252", errors="strict")
        monkeypatch.setattr(sys, "stderr", stream)
        monkeypatch.setenv("Z4J_SCHEDULER_LOG_JSON", "false")
        non_json_settings = Settings(_env_file=None)  # type: ignore[call-arg]
        reset_for_tests()
        try:
            configure_logging(non_json_settings)
            try:
                _raise_scheduler_logger_failure()
            except RuntimeError:
                structlog.get_logger("z4j.scheduler.non-utf8").exception(
                    "scheduler traceback remains writable",
                )
            stream.flush()
            rendered = raw.getvalue().decode("cp1252")
            assert "scheduler traceback remains writable" in rendered
            assert "RuntimeError: scheduler logger failure" in rendered
        finally:
            reset_for_tests()
            structlog.reset_defaults()
            stream.close()
