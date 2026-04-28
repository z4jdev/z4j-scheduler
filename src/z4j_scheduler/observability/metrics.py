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

Labels are bounded - we never label on schedule_id (high cardinality);
project_id / engine_kind labels are fine because brain caps both.

Histogram buckets are tuned for the latency targets in
``docs/SCHEDULER.md §23``:
  - fire_latency: 5ms / 10ms / 25ms / 50ms / 100ms / 250ms / 500ms /
    1s / 2.5s / 5s / +Inf
  - tick_drift:   100ms / 250ms / 500ms / 1s / 5s / 30s / 5min /
    1h / +Inf
"""

from __future__ import annotations

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
    "End-to-end fire dispatch latency from FireSchedule call to result.",
    buckets=(
        0.005, 0.010, 0.025, 0.050, 0.100, 0.250,
        0.500, 1.0, 2.5, 5.0,
    ),
    registry=default_registry,
)

tick_drift_seconds = Histogram(
    "z4j_scheduler_tick_drift_seconds",
    "How late each fire was vs its scheduled_for time.",
    buckets=(
        0.100, 0.250, 0.500, 1.0, 5.0, 30.0,
        300.0, 3600.0,
    ),
    registry=default_registry,
)

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
    "Reconnects of the WatchSchedules stream after a drop.",
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

# ---------------------------------------------------------------------------
# Per-schedule (Phase 4)
# ---------------------------------------------------------------------------
#
# These slice the global counters above by ``schedule_id`` so the
# dashboard can render per-schedule fire-rate / latency / failure
# charts. The cardinality is bounded by the schedule count
# (typically <10k per cluster) - well within Prometheus comfort.
#
# We label by ``schedule_id`` (a UUID, stable across renames) AND
# ``schedule_name`` (the human label, also stable per identity).
# Including both lets operators query by either - the trade-off is
# 2× the label space, which is fine at this cardinality.

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
        0.005, 0.010, 0.025, 0.050, 0.100, 0.250,
        0.500, 1.0, 2.5, 5.0,
    ),
    registry=default_registry,
)


__all__ = [
    "default_registry",
    "engine_iterations_total",
    "fire_latency_seconds",
    "fires_total",
    "grpc_calls_total",
    "is_leader",
    "per_schedule_fire_latency_seconds",
    "per_schedule_fires_total",
    "schedules_loaded",
    "tick_drift_seconds",
    "watch_stream_reconnects_total",
]
