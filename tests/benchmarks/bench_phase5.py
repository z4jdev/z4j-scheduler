"""Phase 5 benchmark harness - GA-readiness numbers.

Bench_phase2 covered the operational primitives (cache snapshot,
leader acquire, failover). Phase 5 adds the §23 performance
targets that need real numbers before tagging v1 GA:

1. **Memory at idle** (target <80MB) - resident set after the
   process has booted but before any schedules are loaded.
2. **Memory with 10k schedules** (target <300MB) - same process
   after pushing 10k entries through the cache.
3. **Startup time** (target <2s) - wall-clock from
   ``SchedulerApp.__init__`` to ``await app.start()`` returning.
4. **Sustained-load tick + dispatch latency** - spin the tick
   engine + a fake dispatcher under a 100 fires/sec target and
   measure p50/p99 tick drift + per-dispatch latency.

Run::

    cd packages/z4j-scheduler
    PYTHONPATH=src python tests/benchmarks/bench_phase5.py

Output: JSON to stdout. Same shape as bench_phase2 so a regression
script can diff both side-by-side.

Memory measurement uses ``resource.getrusage`` (Unix) /
``psutil.Process().memory_info().rss`` (Windows fallback). Both
report RSS in bytes; we normalise to MB.
"""

from __future__ import annotations

import asyncio
import gc
import json
import statistics
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

# These are pure-Python imports - no Postgres, no network. We can
# benchmark in any environment.
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.leader import SingleInstanceLeaderGate
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick.engine import TickEngine


# =====================================================================
# Memory + startup
# =====================================================================


def _rss_mb() -> float:
    """Return process RSS in MB. Falls back across stdlib options.

    Probe order:

    1. ``psutil`` if installed (preferred - well-tested across OS).
    2. ``resource.getrusage`` on Unix (Linux + macOS).
    3. ``/proc/self/status`` parsing on Linux without ``resource``.
    4. ``ctypes`` ``GetProcessMemoryInfo`` on Windows (pure stdlib).
    5. ``-1.0`` sentinel only if every probe fails.

    Pre-fix this helper returned ``-1.0`` on the JFK Trading dev
    Windows box because the bare-stdlib install ships neither
    ``psutil`` nor ``resource``. The §23 memory targets in the
    SCHEDULER.md spec depend on this number, so a silent ``-1.0``
    masquerading as "well under target" is a real correctness bug.
    """
    import sys  # noqa: PLC0415

    # 1. psutil - works everywhere, accurate, tested.
    try:
        import psutil  # noqa: PLC0415

        return psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        pass

    # 2. Unix: resource.getrusage. Not on Windows.
    if sys.platform != "win32":
        try:
            import resource  # noqa: PLC0415

            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Linux reports kilobytes; macOS reports bytes. Heuristic:
            # if the number is implausibly large (>1B), assume bytes.
            return (
                (rss / 1024)
                if rss < 1_000_000_000
                else (rss / (1024 * 1024))
            )
        except ImportError:
            pass

    # 3. Linux fallback if ``resource`` missing for some reason.
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/self/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        # "VmRSS:    12345 kB"
                        kb = int(line.split()[1])
                        return kb / 1024
        except OSError:
            pass

    # 4. Windows: GetProcessMemoryInfo via ctypes. Pure stdlib so the
    # operator can run the bench harness without `pip install psutil`
    # in their sandbox env. The argtypes / restype declarations are
    # load-bearing - without them the 64-bit pseudo-handle from
    # ``GetCurrentProcess()`` gets truncated to 32 bits and the call
    # fails with ERROR_INVALID_HANDLE (6), silently returning 0
    # bytes which would then look like "20 GB headroom" in the
    # report.
    if sys.platform == "win32":
        try:
            import ctypes  # noqa: PLC0415
            from ctypes import wintypes  # noqa: PLC0415

            class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            get_current_process = ctypes.windll.kernel32.GetCurrentProcess
            get_current_process.restype = wintypes.HANDLE

            get_proc_mem_info = ctypes.windll.psapi.GetProcessMemoryInfo
            get_proc_mem_info.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(_PROCESS_MEMORY_COUNTERS),
                wintypes.DWORD,
            ]
            get_proc_mem_info.restype = wintypes.BOOL

            counters = _PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(counters)
            ok = get_proc_mem_info(
                get_current_process(),
                ctypes.byref(counters),
                counters.cb,
            )
            if ok:
                return counters.WorkingSetSize / (1024 * 1024)
        except (OSError, AttributeError):
            pass

    return -1.0


def bench_memory_idle() -> dict:
    """Snapshot RSS with an empty cache."""
    gc.collect()
    return {"rss_mb": round(_rss_mb(), 1)}


async def bench_memory_with_10k() -> dict:
    """Push 10k schedule entries into the cache + snapshot RSS."""
    return await _bench_memory_at(10_000)


async def bench_memory_at_scale() -> dict:
    """Walk 100k → 1M schedule entries to characterise scaling.

    The §23 spec target is 10k schedules per instance; this bench
    explores the headroom beyond that for operators who want to
    consolidate many tenants onto a single scheduler. We measure
    RSS + cache snapshot time at 100k and 1M to surface any
    surprises (allocator fragmentation, dict resize cliffs, GC
    spikes from large young generations) BEFORE an operator hits
    them in production.

    1M schedules at ~800 B/each = ~800 MB RSS. We bound the bench
    work to keep total runtime under ~30s on a typical laptop;
    operators with a real million-schedule deployment should re-
    measure on their own hardware.
    """
    out: dict = {}
    for n in (100_000, 1_000_000):
        out[str(n)] = await _bench_memory_at(n)
    return out


async def _bench_memory_at(n: int) -> dict:
    """Push ``n`` schedule entries into the cache + measure scaling.

    Reports:

    - ``schedules`` - the count fed in
    - ``rss_mb`` - process RSS after upsert + gc
    - ``upsert_seconds`` - wall-clock time for the bulk upsert
    - ``snapshot_p50_us`` / ``snapshot_p99_us`` - per-snapshot
      cost (dashboard reads + tick-engine due-list scans both
      pay this on every iteration; bounded growth is a load-
      bearing property)
    """
    cache = ScheduleCache()
    base = datetime.now(UTC)
    entries = [
        ScheduleEntry(
            id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=base,
            last_fire_at=None,
            name=f"sched-{i}",
        )
        for i in range(n)
    ]
    t0 = time.perf_counter()
    await cache.upsert_many(entries)
    upsert_seconds = round(time.perf_counter() - t0, 3)
    gc.collect()
    rss = round(_rss_mb(), 1)

    # Snapshot cost characterisation. The cache exposes
    # ``snapshot()`` (returns a list copy) which the watch stream
    # uses on every full-sync and the dashboard uses on every read.
    # We sample N times to get p50/p99 - one sample is too noisy.
    sample_count = max(5, min(50, 1_000_000 // n))
    snap_us: list[float] = []
    for _ in range(sample_count):
        ts = time.perf_counter()
        await cache.snapshot()
        snap_us.append((time.perf_counter() - ts) * 1_000_000)
    snap_us.sort()

    def _q(samples: list[float], q: float) -> float:
        if not samples:
            return 0.0
        idx = max(0, min(len(samples) - 1, int(round(q * (len(samples) - 1)))))
        return samples[idx]

    return {
        "schedules": n,
        "rss_mb": rss,
        "upsert_seconds": upsert_seconds,
        "snapshot_samples": sample_count,
        "snapshot_p50_us": round(_q(snap_us, 0.50), 2),
        "snapshot_p99_us": round(_q(snap_us, 0.99), 2),
    }


def bench_startup_components() -> dict:
    """Time the cheap subsystem builds that ``SchedulerApp.start`` runs.

    Excludes the gRPC ``BrainClient.connect`` because that's
    network-dependent (covered by integration tests). The
    pure-Python init time is what the §23 <2s target measures
    against - operators see this as "process supervisor restart
    latency."
    """
    timings: dict[str, float] = {}

    t0 = time.perf_counter()
    cache = ScheduleCache()
    timings["cache_init_ms"] = round((time.perf_counter() - t0) * 1000, 3)

    t0 = time.perf_counter()
    leader = SingleInstanceLeaderGate()
    timings["leader_init_ms"] = round((time.perf_counter() - t0) * 1000, 3)

    t0 = time.perf_counter()
    # Fake dispatcher - no brain connection.
    class _FakeClient:
        async def fire_schedule(self, **_kw):
            return FireResult(
                command_id=uuid.uuid4(),
                error_code=None,
                error_message=None,
                buffered=False,
            )

        async def acknowledge_result(self, **_kw):
            return None

    from z4j_scheduler.settings import Settings

    settings = Settings(
        brain_grpc_url="x:1",
        brain_rest_url="http://x:1",
        tls_cert="/dev/null",
        tls_key="/dev/null",
        tls_ca="/dev/null",
    )
    dispatcher = FireDispatcher(client=_FakeClient(), settings=settings)
    timings["dispatcher_init_ms"] = round(
        (time.perf_counter() - t0) * 1000, 3,
    )

    t0 = time.perf_counter()
    TickEngine(cache=cache, leader_gate=leader, dispatcher=dispatcher)
    timings["tick_engine_init_ms"] = round(
        (time.perf_counter() - t0) * 1000, 3,
    )

    timings["total_ms"] = round(sum(timings.values()), 3)
    return timings


# =====================================================================
# Sustained-load fire + tick latency
# =====================================================================


class _LatencyRecordingDispatcher:
    """In-process dispatcher that just records call timestamps.

    Lets us measure the SCHEDULER's contribution to fire latency
    in isolation - no brain, no agent, no broker. Compare to the
    real-network bench_phase2 numbers to attribute latency to its
    layer.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[uuid.UUID, datetime, float]] = []

    async def dispatch(
        self, *, schedule_id, scheduled_for, schedule_name="",
    ):
        self.calls.append((schedule_id, scheduled_for, time.perf_counter()))


async def bench_sustained_load(
    *,
    schedule_count: int = 100,
    duration_seconds: float = 5.0,
) -> dict:
    """Drive the tick engine + dispatcher under sustained load.

    Sets up ``schedule_count`` interval schedules that fire every
    second and runs for ``duration_seconds``. Records each
    fire's wall-clock latency from ``scheduled_for`` to dispatch
    callback (the tick engine's own latency contribution; the
    network round-trip to brain is a separate cost measured in
    the e2e tests).

    Reports p50 / p99 / max in milliseconds + the achieved
    fires/sec (target: ≥100 fires/sec sustained, per §23's "100
    fires/sec load" implicit assumption).
    """
    cache = ScheduleCache()
    dispatcher = _LatencyRecordingDispatcher()
    leader = SingleInstanceLeaderGate()

    # Build interval schedules that fire every second so we hit
    # the 100 fires/sec load with ``schedule_count`` schedules.
    base = datetime.now(UTC) - timedelta(seconds=1)
    entries = [
        ScheduleEntry(
            id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            kind="interval",
            expression="1s",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=base,
            last_fire_at=None,
            name=f"load-{i}",
        )
        for i in range(schedule_count)
    ]
    await cache.upsert_many(entries)

    engine = TickEngine(cache=cache, leader_gate=leader, dispatcher=dispatcher)
    task = asyncio.create_task(engine.run())
    try:
        await asyncio.sleep(duration_seconds)
    finally:
        await engine.stop()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.TimeoutError:
            task.cancel()

    if not dispatcher.calls:
        return {
            "schedules": schedule_count,
            "duration_s": duration_seconds,
            "fires": 0,
            "skipped": "no fires recorded - tick engine never advanced",
        }

    # Measured latencies - difference between dispatch wall-clock
    # and scheduled_for, in milliseconds. Negative values would
    # indicate clock skew; we clamp at 0.
    base_perf = time.perf_counter()
    base_wall = datetime.now(UTC)
    latencies_ms: list[float] = []
    for _sid, scheduled_for, perf_at in dispatcher.calls:
        # Convert recorded perf_counter back to wall-clock.
        wall_at = base_wall + timedelta(seconds=perf_at - base_perf)
        delta = (wall_at - scheduled_for).total_seconds() * 1000
        latencies_ms.append(max(0.0, delta))

    sorted_lat = sorted(latencies_ms)
    return {
        "schedules": schedule_count,
        "duration_s": duration_seconds,
        "fires": len(dispatcher.calls),
        "fires_per_sec": round(len(dispatcher.calls) / duration_seconds, 1),
        "tick_drift_p50_ms": round(statistics.median(sorted_lat), 2),
        "tick_drift_p99_ms": round(_p99(sorted_lat), 2),
        "tick_drift_max_ms": round(max(sorted_lat), 2),
    }


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
    results: dict[str, object] = {
        "z4j_scheduler_phase5_bench": {
            "version": 1,
            "ts": datetime.now(UTC).isoformat(),
            "targets": {
                "memory_idle_max_mb": 80,
                "memory_10k_max_mb": 300,
                "startup_max_ms": 2000,
                "tick_drift_p99_max_ms": 500,
                "fires_per_sec_min": 100,
            },
        },
    }

    # Memory at idle (before anything's loaded).
    results["memory_idle"] = bench_memory_idle()

    # Startup component timings.
    results["startup"] = bench_startup_components()

    # Memory with 10k schedules in cache (the §23 GA target).
    results["memory_with_10k_schedules"] = await bench_memory_with_10k()

    # Memory + snapshot scaling beyond §23 - the headroom story for
    # operators consolidating many tenants. Off by default because
    # the 1M variant takes ~30s and allocates ~800 MB; opt in via
    # ``--scale`` on the runner.
    if "--scale" in sys.argv:
        results["memory_at_scale"] = await bench_memory_at_scale()

    # Sustained-load tick latency. 100 schedules @ 1s = 100 fires/sec
    # target.
    results["sustained_load"] = await bench_sustained_load(
        schedule_count=100, duration_seconds=5.0,
    )

    return results


def main() -> int:
    try:
        report = asyncio.run(_run_all())
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
