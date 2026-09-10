"""Operational metrics must change through real run/dispatch paths, not only .inc smoke tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.observability import metrics as m
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.watch import WatchStream

from .test_dispatch import FakeBrainClient, settings  # noqa: F401 - shared TLS/settings fixture
from .test_engine_run_resilience import _engine


def sample(name: str) -> float:
    return m.default_registry.get_sample_value(name) or 0.0


async def test_iterations_count_completed_work_but_not_errors_or_cancellation():
    engine = _engine(iteration_error_backoff_seconds=0.001)
    before = sample("z4j_scheduler_engine_iterations_total")
    failures = sample("z4j_scheduler_engine_iteration_failures_total")
    calls = 0

    async def iteration():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected iteration error")
        if calls == 3:
            raise asyncio.CancelledError

    engine._iteration = iteration
    with pytest.raises(asyncio.CancelledError):
        await engine.run()
    assert sample("z4j_scheduler_engine_iterations_total") - before == 1
    assert sample("z4j_scheduler_engine_iteration_failures_total") - failures == 1


@pytest.mark.parametrize("failure", [False, True])
async def test_watch_counts_actual_reconnect_attempts_after_clean_or_failed_stream(failure):
    stream = WatchStream(client=object(), cache=ScheduleCache(), full_resync_interval_seconds=0)
    before = sample("z4j_scheduler_watch_stream_reconnects_total")
    calls = 0

    async def sync():
        nonlocal calls
        calls += 1
        if calls == 3:
            await stream.stop()
        elif failure:
            raise RuntimeError("injected dropped watch")

    async def backoff():
        await asyncio.sleep(0)

    stream._sync_then_watch = sync
    stream._backoff_or_stop = backoff
    await stream._watch_loop()
    assert calls == 3
    assert sample("z4j_scheduler_watch_stream_reconnects_total") - before == 2


async def test_stopping_during_backoff_is_not_a_reconnect():
    stream = WatchStream(client=object(), cache=ScheduleCache(), full_resync_interval_seconds=0)
    before = sample("z4j_scheduler_watch_stream_reconnects_total")

    async def sync():
        return

    stream._sync_then_watch = sync
    stream._backoff_or_stop = stream.stop
    await stream._watch_loop()
    assert sample("z4j_scheduler_watch_stream_reconnects_total") == before


@pytest.mark.parametrize("lateness", [-2, 0, 12, 400])
async def test_dispatch_observes_nonnegative_drift(settings, lateness, monkeypatch):  # noqa: F811
    client = FakeBrainClient(
        fire_responses=[
            FireResult(command_id=uuid4(), error_code=None, error_message=None, buffered=False)
        ]
    )
    dispatcher = FireDispatcher(client=client, settings=settings)
    start = datetime(2026, 9, 8, tzinfo=UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return start + timedelta(seconds=lateness)

    monkeypatch.setattr("z4j_scheduler.dispatch.fire.datetime", Clock)
    count = sample("z4j_scheduler_tick_drift_seconds_count")
    total = sample("z4j_scheduler_tick_drift_seconds_sum")
    await dispatcher.dispatch(schedule_id=uuid4(), scheduled_for=start)
    assert sample("z4j_scheduler_tick_drift_seconds_count") - count == 1
    assert sample("z4j_scheduler_tick_drift_seconds_sum") - total == max(0, lateness)


@pytest.fixture
def detail_registry(monkeypatch):
    from collections import OrderedDict

    from prometheus_client import CollectorRegistry, Counter, Histogram

    registry = CollectorRegistry()
    monkeypatch.setattr(m, "MAX_DETAIL_SCHEDULES", 2)
    monkeypatch.setattr(m, "_detail_series", OrderedDict())
    monkeypatch.setattr(m, "_variance_series", OrderedDict())
    monkeypatch.setattr(
        m,
        "per_schedule_fires_total",
        Counter(
            "detail_fires", "test", ("schedule_id", "schedule_name", "status"), registry=registry
        ),
    )
    monkeypatch.setattr(
        m,
        "per_schedule_fire_latency_seconds",
        Histogram("detail_latency", "test", ("schedule_id", "schedule_name"), registry=registry),
    )
    monkeypatch.setattr(
        m,
        "fire_variance_seconds",
        Histogram("variance", "test", ("schedule_id", "engine", "project"), registry=registry),
    )
    return registry


def test_detail_churn_evicts_latency_and_all_statuses_but_retains_recent(detail_registry):
    for sid in ("old", "recent"):
        m.observe_schedule_latency(sid, sid, 0.1)
        for status in ("delivered", "buffered", "failed"):
            m.increment_schedule_fires(sid, sid, status)
    # Touch old: recent becomes the least recently used group.
    m.increment_schedule_fires("old", "old", "delivered")
    m.observe_schedule_latency("new", "new", 0.2)
    samples = [sample for metric in detail_registry.collect() for sample in metric.samples]
    assert {s.labels["schedule_id"] for s in samples} == {"old", "new"}
    assert (
        detail_registry.get_sample_value(
            "detail_fires_total",
            {"schedule_id": "old", "schedule_name": "old", "status": "delivered"},
        )
        == 2
    )
    # Repeated renames must obey the same lifetime cap.
    for i in range(20):
        m.observe_schedule_latency("new", f"rename-{i}", 0.1)
        m.increment_schedule_fires("new", f"rename-{i}", "delivered")
    samples = [sample for metric in detail_registry.collect() for sample in metric.samples]
    assert {s.labels["schedule_name"] for s in samples} == {"rename-18", "rename-19"}


def test_variance_bounds_project_and_schedule_churn(detail_registry):
    for i in range(20):
        m.observe_fire_variance(str(i), "celery", str(i), 0.2)
    m.observe_fire_variance("18", "celery", "18", 0.3)
    m.observe_fire_variance("", "rq", "large-project", 0.1)
    samples = [s for metric in detail_registry.collect() for s in metric.samples]
    assert {s.labels["project"] for s in samples} == {"18", "large-project"}
    assert (
        detail_registry.get_sample_value(
            "variance_count", {"schedule_id": "18", "engine": "celery", "project": "18"}
        )
        == 2
    )


async def test_evicted_detail_preserves_aggregate_dispatch_totals(detail_registry, settings):  # noqa: F811
    client = FakeBrainClient(
        fire_responses=[
            FireResult(command_id=uuid4(), error_code=None, error_message=None, buffered=False)
            for _ in range(4)
        ]
    )
    dispatcher = FireDispatcher(client=client, settings=settings)
    before = (
        m.default_registry.get_sample_value("z4j_scheduler_fires_total", {"status": "delivered"})
        or 0
    )
    count = sample("z4j_scheduler_tick_drift_seconds_count")
    for _ in range(4):
        await dispatcher.dispatch(schedule_id=uuid4(), scheduled_for=datetime.now(UTC))
    assert (
        m.default_registry.get_sample_value("z4j_scheduler_fires_total", {"status": "delivered"})
        - before
        == 4
    )
    assert sample("z4j_scheduler_tick_drift_seconds_count") - count == 4
    assert len(m._detail_series) == 2


def _remove_without_missing_guard(self, *labelvalues):
    """``MetricWrapperBase.remove`` as prometheus-client 0.21.0 and 0.21.1 ship it.

    Those releases end ``remove`` with an unguarded ``del``, so removing a label
    set that was never created raises KeyError. 0.22.0 added the membership check
    (prometheus/client_python#1077). The scheduler's dependency floor still admits
    0.21, so eviction has to hold under this behaviour as well.
    """
    if not self._labelnames:
        raise ValueError("No label names were set")
    if len(labelvalues) != len(self._labelnames):
        raise ValueError("Incorrect label count")
    labelvalues = tuple(str(value) for value in labelvalues)
    with self._lock:
        del self._metrics[labelvalues]


def test_churn_holds_under_unguarded_remove(detail_registry, monkeypatch):
    from prometheus_client.metrics import MetricWrapperBase

    monkeypatch.setattr(MetricWrapperBase, "remove", _remove_without_missing_guard)
    test_detail_churn_evicts_latency_and_all_statuses_but_retains_recent(detail_registry)


def test_remove_series_still_refuses_a_wrong_label_count(detail_registry, monkeypatch):
    from prometheus_client.metrics import MetricWrapperBase

    monkeypatch.setattr(MetricWrapperBase, "remove", _remove_without_missing_guard)
    with pytest.raises(ValueError, match="label count"):
        m._remove_series(m.per_schedule_fires_total, "only-one")


async def test_eviction_tolerates_never_created_series(detail_registry, settings, monkeypatch):  # noqa: F811
    from prometheus_client.metrics import MetricWrapperBase

    monkeypatch.setattr(MetricWrapperBase, "remove", _remove_without_missing_guard)
    client = FakeBrainClient(
        fire_responses=[
            FireResult(command_id=uuid4(), error_code=None, error_message=None, buffered=False)
            for _ in range(6)
        ]
    )
    dispatcher = FireDispatcher(client=client, settings=settings)
    ids = [uuid4() for _ in range(6)]
    for schedule_id in ids:
        await dispatcher.dispatch(schedule_id=schedule_id, scheduled_for=datetime.now(UTC))
    # Every group only ever delivered, so each eviction also asks to remove buffered
    # and failed series that were never created. The cap must still hold, and the
    # newest groups must keep the latency their dispatch observed.
    detail = [
        s
        for metric in detail_registry.collect()
        if metric.name != "variance"
        for s in metric.samples
    ]
    assert {s.labels["schedule_id"] for s in detail} == {str(ids[-2]), str(ids[-1])}
    for schedule_id in ids[-2:]:
        assert (
            detail_registry.get_sample_value(
                "detail_latency_count", {"schedule_id": str(schedule_id), "schedule_name": ""}
            )
            == 1
        )
