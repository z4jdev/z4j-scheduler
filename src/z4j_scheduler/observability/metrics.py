"""Prometheus metric definitions for z4j-scheduler.

All metrics are module-level instances so the FastAPI ``/metrics``
endpoint serializes them lazily on scrape. The
:class:`prometheus_client.REGISTRY` is the default singleton.

Per ``docs/SCHEDULER.md §5.10`` the metrics surface is:

- :data:`schedules_loaded` - gauge of how many schedules are in the
  cache, labelled by project and engine and kind
- :data:`fires_total` - counter of dispatched fires, labelled by
  status (delivered / buffered / failed)
- :data:`fire_latency_seconds` - histogram of fire dispatch
  end-to-end latency
- :data:`tick_drift_seconds` - histogram of how late each fire was
  vs its scheduled_for (lateness > grace = catch-up applied)
- :data:`is_leader` - gauge per project (0 / 1)
- :data:`grpc_calls_total` - counter labelled by method + status
- :data:`watch_stream_reconnects_total` - counter

Aggregate metrics retain every observation. Schedule detail and labelled
variance use separate LRU windows of at most 1,000 label groups each, so
schedule churn cannot grow these collectors indefinitely. Historical
per-schedule records belong in the brain database.

Histogram buckets are tuned for the latency targets in
``docs/SCHEDULER.md §23``:
  - fire_latency: 5ms / 10ms / 25ms / 50ms / 100ms / 250ms / 500ms /
    1s / 2.5s / 5s / +Inf
  - tick_drift:   100ms / 250ms / 500ms / 1s / 5s / 30s / 5min /
    1h / +Inf
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import suppress
from threading import Lock

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)

#: Default registry - shared with the FastAPI metrics endpoint. We
#: hold a reference here so tests can construct a private
#: ``CollectorRegistry`` and pass it to a fresh metric set if they
#: want isolation. Production uses the global REGISTRY.
default_registry = CollectorRegistry(auto_describe=True)

# ---------------------------------------------------------------------------
# Schedule cache state
# ---------------------------------------------------------------------------

schedules_loaded = Gauge(
    "z4j_scheduler_schedules_loaded",
    "Schedules currently in the in-memory cache.",
    labelnames=("project", "kind"),
    registry=default_registry,
)

# ---------------------------------------------------------------------------
# Fire dispatch
# ---------------------------------------------------------------------------

fires_total = Counter(
    "z4j_scheduler_fires_total",
    "Total schedule fires dispatched, labelled by terminal status.",
    labelnames=("status",),  # "delivered" | "buffered" | "failed"
    registry=default_registry,
)

fire_latency_seconds = Histogram(
    "z4j_scheduler_fire_latency_seconds",
    "FireSchedule RPC duration from call to result, including retries; excludes due-time wait and worker execution.",
    buckets=(
        0.005,
        0.010,
        0.025,
        0.050,
        0.100,
        0.250,
        0.500,
        1.0,
        2.5,
        5.0,
    ),
    registry=default_registry,
)

tick_drift_seconds = Histogram(
    "z4j_scheduler_tick_drift_seconds",
    "How late each fire was vs its scheduled_for time.",
    buckets=(
        0.100,
        0.250,
        0.500,
        1.0,
        5.0,
        30.0,
        300.0,
        3600.0,
    ),
    registry=default_registry,
)

# Dispatch-moment fire-time variance: ``fired_at - next_fire_at`` at the
# instant a schedule is dispatched (A3). Distinct from
# ``tick_drift_seconds`` (also lateness, but unlabelled and coarser
# bucketed): this one is sliced by engine + project so operators can
# graph p50/p95/p99 fire accuracy per project/engine. The ``schedule_id``
# label is emitted ONLY for projects with fewer than
# ``FIRE_VARIANCE_SCHEDULE_ID_MAX`` schedules -- above that the tick
# engine passes an empty ``schedule_id`` so per-schedule cardinality
# stays bounded on large tenants.
fire_variance_seconds = Histogram(
    "z4j_scheduler_fire_variance_seconds",
    "Fire-time variance (fired_at - next_fire_at) at dispatch, "
    "by engine + project (+ schedule_id for small projects).",
    labelnames=("schedule_id", "engine", "project"),
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=default_registry,
)

#: Cardinality guard: emit the per-schedule label on the fire-variance
#: histogram only while a project stays under this many schedules.
FIRE_VARIANCE_SCHEDULE_ID_MAX = 100

# ---------------------------------------------------------------------------
# Leadership
# ---------------------------------------------------------------------------

is_leader = Gauge(
    "z4j_scheduler_is_leader",
    "1 if this scheduler instance currently leads the project, else 0.",
    labelnames=("project",),
    registry=default_registry,
)

# ---------------------------------------------------------------------------
# gRPC traffic
# ---------------------------------------------------------------------------

grpc_calls_total = Counter(
    "z4j_scheduler_grpc_calls_total",
    "Outbound gRPC calls to brain, labelled by method and result.",
    labelnames=("method", "status"),
    registry=default_registry,
)

watch_stream_reconnects_total = Counter(
    "z4j_scheduler_watch_stream_reconnects_total",
    "Watch resynchronization/reconnection attempts after the initial attempt.",
    registry=default_registry,
)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

engine_iterations_total = Counter(
    "z4j_scheduler_engine_iterations_total",
    "Tick-engine iterations completed.",
    registry=default_registry,
)

#: Iterations that ended in an unhandled exception. The loop absorbs these and
#: continues, so nothing else would show that scheduling is degraded. A sustained
#: rate here means some path is raising on every pass and fires are being missed.
engine_iteration_failures_total = Counter(
    "z4j_scheduler_engine_iteration_failures_total",
    "Tick-engine iterations that raised an unhandled exception.",
    registry=default_registry,
)

#: Slots the catch_up policy deliberately did NOT fire. A discard is correct
#: behaviour for catch_up="skip" after a real outage, but it was previously
#: invisible: a schedule could stop producing work and nothing in the metrics or
#: the log said so. Alert on a non-zero rate here.
#:
#: Labelled by POLICY only, deliberately. The per-schedule counters above carry
#: a schedule_id because the dispatcher decides that, weighing the cardinality
#: against a live project schedule count. The tick engine has no such gate, so a
#: schedule_id here would mint an unbounded, never-released series from inside a
#: component that does not own that decision. The WARNING logged beside each
#: increment names the schedule, which is what an operator needs to act.
slots_discarded_total = Counter(
    "z4j_scheduler_slots_discarded_total",
    "Missed schedule slots dropped by the catch_up policy without firing.",
    labelnames=("catch_up",),
    registry=default_registry,
)

# ---------------------------------------------------------------------------
# Per-schedule (Phase 4)
# ---------------------------------------------------------------------------
#
# These slice the global counters above by ``schedule_id`` so the
# dashboard can render per-schedule fire-rate / latency / failure
# charts. IDs and names both label a series, so a rename creates a new
# group. The rolling LRU below bounds groups across renames and deletions,
# independently of the number of currently loaded schedules.

per_schedule_fires_total = Counter(
    "z4j_scheduler_per_schedule_fires_total",
    "Schedule fires per schedule, labelled by terminal status.",
    labelnames=("schedule_id", "schedule_name", "status"),
    registry=default_registry,
)

per_schedule_fire_latency_seconds = Histogram(
    "z4j_scheduler_per_schedule_fire_latency_seconds",
    "Per-schedule fire dispatch latency (FireSchedule call → result).",
    labelnames=("schedule_id", "schedule_name"),
    buckets=(
        0.005,
        0.010,
        0.025,
        0.050,
        0.100,
        0.250,
        0.500,
        1.0,
        2.5,
        5.0,
    ),
    registry=default_registry,
)

# Bound process memory when schedules are created, deleted or renamed over a
# long uptime. The aggregate metrics retain every observation; per-schedule
# detail is a rolling diagnostic window, not a durable history store.
MAX_DETAIL_SCHEDULES = 1_000
_detail_series: OrderedDict[tuple[str, str], None] = OrderedDict()
_variance_series: OrderedDict[tuple[str, str, str], None] = OrderedDict()
_detail_lock = Lock()


def _remove_series(metric: Counter | Histogram, *labelvalues: str) -> None:
    # Eviction removes every series a group could own, but most groups own only
    # some: a schedule usually reports one or two statuses, and a swallowed
    # emission error can leave a group without a latency series.
    # prometheus-client releases before 0.22.0 raise KeyError when removing a
    # label set that was never created. Unhandled, that aborts the eviction and
    # the observation that triggered it, so series outlive the cap. A series
    # that does not exist is already removed.
    with suppress(KeyError):
        metric.remove(*labelvalues)


def _retain_detail(schedule_id: str, schedule_name: str) -> None:
    key = (schedule_id, schedule_name)
    _detail_series[key] = None
    _detail_series.move_to_end(key)
    while len(_detail_series) > MAX_DETAIL_SCHEDULES:
        (old_id, old_name), _ = _detail_series.popitem(last=False)
        _remove_series(per_schedule_fire_latency_seconds, old_id, old_name)
        for status in ("delivered", "buffered", "failed"):
            _remove_series(per_schedule_fires_total, old_id, old_name, status)


def observe_schedule_latency(schedule_id: str, schedule_name: str, elapsed: float) -> None:
    with _detail_lock:
        _retain_detail(schedule_id, schedule_name)
        per_schedule_fire_latency_seconds.labels(schedule_id, schedule_name).observe(elapsed)


def increment_schedule_fires(schedule_id: str, schedule_name: str, status: str) -> None:
    with _detail_lock:
        _retain_detail(schedule_id, schedule_name)
        per_schedule_fires_total.labels(schedule_id, schedule_name, status).inc()


def observe_fire_variance(schedule_id: str, engine: str, project: str, seconds: float) -> None:
    with _detail_lock:
        key = (schedule_id, engine, project)
        _variance_series[key] = None
        _variance_series.move_to_end(key)
        while len(_variance_series) > MAX_DETAIL_SCHEDULES:
            previous, _ = _variance_series.popitem(last=False)
            _remove_series(fire_variance_seconds, *previous)
        fire_variance_seconds.labels(*key).observe(seconds)


__all__ = [
    "FIRE_VARIANCE_SCHEDULE_ID_MAX",
    "MAX_DETAIL_SCHEDULES",
    "default_registry",
    "engine_iterations_total",
    "fire_latency_seconds",
    "fire_variance_seconds",
    "fires_total",
    "grpc_calls_total",
    "increment_schedule_fires",
    "is_leader",
    "observe_fire_variance",
    "observe_schedule_latency",
    "per_schedule_fire_latency_seconds",
    "per_schedule_fires_total",
    "schedules_loaded",
    "tick_drift_seconds",
    "watch_stream_reconnects_total",
]
