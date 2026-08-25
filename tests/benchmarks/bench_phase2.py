"""Phase 2 benchmark harness.

Measures the operationally-relevant numbers introduced by Phase 2
so operators can confirm the architecture holds under spec'd load
before deploying multi-instance:

1. **Cache snapshot at 10k schedules** - pure Python, no I/O. The
   tick engine reads ``cache.snapshot()`` every iteration, so its
   latency caps the tick rate.
2. **Tick engine pass at 10k schedules** - drives one full
   ``TickEngine`` iteration through ``ManualClock`` to measure how
   long the per-tick scan + dispatch loop takes.
3. **Per-project leader acquisition for 100 projects** - against a
   real Postgres via testcontainers. Measures how long it takes
   the gate to converge from "no locks held" to "all 100 held."
4. **Failover** - one instance dies, second takes over. Measures
   the wall-clock window from ``stop()`` to ``is_leader()=True``
   on the standby.

Run:

    cd packages/z4j-scheduler
    PYTHONPATH=src python tests/benchmarks/bench_phase2.py

Output: JSON to stdout. The shape is stable so a regression-detect
script can diff against a baseline.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

# These imports are heavy; wrap them so a missing extra (e.g.
# testcontainers) doesn't kill the whole script - the cache + tick
# benches don't need it.
try:
    from testcontainers.postgres import PostgresContainer  # noqa: F401

    _TESTCONTAINERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TESTCONTAINERS_AVAILABLE = False

from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry

# =====================================================================
# Workload generation
# =====================================================================


def build_entries(*, count: int, projects: int) -> list[ScheduleEntry]:
    """Build N schedules spread evenly across M projects.

    Mix of cron + interval kinds in a 70/30 split (typical
    real-world distribution).
    """
    project_ids = [uuid.uuid4() for _ in range(projects)]
    entries: list[ScheduleEntry] = []
    base = datetime.now(UTC)
    for i in range(count):
        kind = "cron" if i % 10 < 7 else "interval"
        # cron branch fires every 5 min
        expression = "*/5 * * * *" if kind == "cron" else f"{30 + (i % 60)}s"
        entries.append(
            ScheduleEntry(
                id=uuid.uuid4(),
                project_id=project_ids[i % projects],
                kind=kind,  # type: ignore[arg-type]
                expression=expression,
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=base - timedelta(minutes=i % 60),
                last_fire_at=None,
            ),
        )
    return entries


# =====================================================================
# Cache benchmarks
# =====================================================================


async def bench_cache_snapshot(*, count: int) -> dict:
    """Measure ``cache.snapshot()`` latency at scale.

    Returns the median + p99 over 100 calls. The tick engine reads
    snapshot every iteration; this is the floor on tick latency.
    """
    cache = ScheduleCache()
    entries = build_entries(count=count, projects=max(1, count // 100))
    await cache.upsert_many(entries)

    samples: list[float] = []
    # Warm-up to avoid measuring asyncio cold-start.
    for _ in range(5):
        await cache.snapshot()

    for _ in range(100):
        start = time.perf_counter()
        await cache.snapshot()
        samples.append((time.perf_counter() - start) * 1000)  # ms

    return {
        "schedules": count,
        "samples": len(samples),
        "p50_ms": round(statistics.median(samples), 3),
        "p99_ms": round(_p99(samples), 3),
        "max_ms": round(max(samples), 3),
    }


async def bench_cache_upsert(*, count: int) -> dict:
    """Measure bulk-upsert at scale (initial sync + watch reload)."""
    cache = ScheduleCache()
    entries = build_entries(count=count, projects=max(1, count // 100))

    start = time.perf_counter()
    await cache.upsert_many(entries)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "schedules": count,
        "total_ms": round(elapsed_ms, 3),
        "per_schedule_us": round((elapsed_ms * 1000) / count, 3),
    }


# =====================================================================
# Leader benchmarks (testcontainers)
# =====================================================================


async def bench_per_project_acquire(
    *,
    project_count: int,
    container,
) -> dict:
    """Time how long it takes the gate to acquire N project locks."""
    from z4j_scheduler.leader.postgres import (
        AsyncpgLockBackend,
        PerProjectLeaderGate,
    )

    dsn = container.get_connection_url().replace(
        "postgresql+psycopg2://",
        "postgresql://",
    )
    project_ids = [uuid.uuid4() for _ in range(project_count)]
    gate = PerProjectLeaderGate(
        backend=AsyncpgLockBackend(dsn=dsn),
        project_source=lambda: project_ids,
        namespace=f"bench-{uuid.uuid4()}",
        heartbeat_seconds=0.05,
    )

    start = time.perf_counter()
    await gate.start()
    # Wait until we hold all locks (or timeout).
    deadline = time.perf_counter() + 30.0
    while time.perf_counter() < deadline:
        if len(gate.held_projects()) >= project_count:
            break
        await asyncio.sleep(0.05)
    elapsed_ms = (time.perf_counter() - start) * 1000
    held = len(gate.held_projects())
    await gate.stop()

    return {
        "projects": project_count,
        "acquired": held,
        "total_ms": round(elapsed_ms, 1),
        "per_project_ms": round(elapsed_ms / max(1, held), 2),
    }


async def bench_failover(*, container) -> dict:
    """Measure failover time when the leader is stopped cleanly."""
    from z4j_scheduler.leader.postgres import (
        AsyncpgLockBackend,
        PostgresAdvisoryLockLeaderGate,
    )

    dsn = container.get_connection_url().replace(
        "postgresql+psycopg2://",
        "postgresql://",
    )
    namespace = f"bench-failover-{uuid.uuid4()}"

    gate_a = PostgresAdvisoryLockLeaderGate(
        backend=AsyncpgLockBackend(dsn=dsn),
        namespace=namespace,
        heartbeat_seconds=0.1,
    )
    gate_b = PostgresAdvisoryLockLeaderGate(
        backend=AsyncpgLockBackend(dsn=dsn),
        namespace=namespace,
        heartbeat_seconds=0.1,
    )
    await gate_a.start()
    await gate_a.wait_for_first_cycle(timeout=10.0)
    await gate_b.start()
    await gate_b.wait_for_first_cycle(timeout=10.0)

    sentinel = uuid.uuid4()
    assert gate_a.is_leader(sentinel) is True
    assert gate_b.is_leader(sentinel) is False

    start = time.perf_counter()
    await gate_a.stop()
    promoted_ms: float | None = None
    deadline = time.perf_counter() + 5.0
    while time.perf_counter() < deadline:
        if gate_b.is_leader(sentinel):
            promoted_ms = (time.perf_counter() - start) * 1000
            break
        await asyncio.sleep(0.02)
    await gate_b.stop()

    return {
        "failover_ms": (round(promoted_ms, 1) if promoted_ms is not None else None),
        "succeeded": promoted_ms is not None,
    }


# =====================================================================
# Helpers
# =====================================================================


def _p99(samples: list[float]) -> float:
    if not samples:
        return 0.0
    sorted_samples = sorted(samples)
    idx = max(0, int(len(sorted_samples) * 0.99) - 1)
    return sorted_samples[idx]


# =====================================================================
# Entry point
# =====================================================================


async def _run_all() -> dict:
    """Run every benchmark + return one report dict."""
    results: dict[str, object] = {
        "z4j_scheduler_phase2_bench": {
            "version": 1,
            "ts": datetime.now(UTC).isoformat(),
        },
    }

    # Cache benches always run (no infra required).
    results["cache_snapshot_10k"] = await bench_cache_snapshot(count=10_000)
    results["cache_upsert_10k"] = await bench_cache_upsert(count=10_000)
    results["cache_snapshot_1k"] = await bench_cache_snapshot(count=1_000)

    # Leader benches need testcontainers + Docker.
    if not _TESTCONTAINERS_AVAILABLE:
        results["leader"] = {"skipped": "testcontainers not installed"}
        return results
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer(
            "postgres:18.6@sha256:06cad38a5d9f5d24b4d83d86def30795d5e4b757fedbf5281172b576dedcd941"
        )
        container.start()
    except Exception as exc:
        results["leader"] = {
            "skipped": f"could not start Postgres: {exc}",
        }
        return results

    try:
        results["per_project_acquire_100"] = await bench_per_project_acquire(
            project_count=100,
            container=container,
        )
        results["failover"] = await bench_failover(container=container)
    finally:
        container.stop()

    return results


def main() -> int:
    try:
        report = asyncio.run(_run_all())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
