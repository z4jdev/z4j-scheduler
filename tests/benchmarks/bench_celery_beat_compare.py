"""Head-to-head benchmark: z4j-scheduler vs celery-beat.

Phase 0 deliverable per ``docs/SCHEDULER.md §25`` and §29 GA
criterion: *"Benchmarks published comparing tick accuracy + fire
latency vs celery-beat."* The harness measures the metrics where
the two schedulers spend their time on identical workloads:

1. **Next-fire computation cost** - given a cron expression and a
   current moment, how long does each library take to compute the
   next fire time? Run on a typical mix (every-minute / hourly /
   daily / weekly).
2. **Per-tick due-list cost** at 100 / 1k / 10k schedules - how
   long does each scheduler take to figure out which schedules
   would fire at "now"?
3. **Per-schedule memory footprint** - how much RSS does each
   side add for a fixed schedule count?

We deliberately do NOT measure broker latency or worker dispatch
- both stacks share the same downstream agent/celery worker path
once the fire is decided. The comparison's value is the SCHEDULER
cost, not the queue cost.

Running the bench:

    pip install celery amqp                    # if not already
    cd packages/z4j-scheduler
    python -m tests.benchmarks.bench_celery_beat_compare

Output is a JSON report + a printable summary. JSON path is
configurable via ``--out path/to/report.json``; default writes to
``tests/benchmarks/results/celery_beat_compare.json``.

If celery is not installed, the harness still runs the
z4j-scheduler side and prints a note explaining how to enable the
celery side.
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# z4j side - always available (we're running from within the package).
from croniter import croniter

# Optional celery side. We import lazily and skip the celery half
# if missing.
try:
    from celery.schedules import crontab as _celery_crontab
    from celery.schedules import schedule as _celery_schedule  # noqa: F401

    _CELERY_AVAILABLE = True
except Exception:
    _CELERY_AVAILABLE = False

# Reuse the well-tested RSS helper from the phase-5 bench. Single
# source of truth for memory measurement so both benches report
# numbers we trust.
from tests.benchmarks.bench_phase5 import _rss_mb

# =====================================================================
# Workloads - identical schedule sets fed to both sides
# =====================================================================


# Five canonical schedule shapes that operators run in production.
# Mix matters: a benchmark that's all every-minute would
# under-represent the daily / weekly cron cost.
_TYPICAL_CRONS = [
    ("every-minute", "* * * * *"),
    ("every-5-min", "*/5 * * * *"),
    ("hourly", "0 * * * *"),
    ("daily-3am", "0 3 * * *"),
    ("weekly-mon", "0 9 * * 1"),
]


def _make_celery_crontab(expr: str):
    """Translate a 5-field cron string into a celery ``crontab`` object."""
    minute, hour, dom, month, dow = expr.split()
    return _celery_crontab(
        minute=minute,
        hour=hour,
        day_of_month=dom,
        month_of_year=month,
        day_of_week=dow,
    )


# =====================================================================
# Metric 1 - next-fire computation cost
# =====================================================================


def bench_next_fire_cost(iterations: int = 1_000) -> dict[str, Any]:
    """Time each library's "given current moment, when's the next fire?"

    Single-schedule cost. Operators care about this for the
    schedule-create UX latency and for the per-tick cost at scale
    (which is iterations of this function).
    """
    out: dict[str, Any] = {"iterations": iterations, "per_cron": {}}
    now = datetime.now(UTC)

    for name, expr in _TYPICAL_CRONS:
        result: dict[str, Any] = {"expression": expr}

        # z4j-scheduler path - croniter
        z4j_times: list[float] = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            cron = croniter(expr, now)
            cron.get_next(datetime)
            z4j_times.append((time.perf_counter() - t0) * 1_000_000)
        result["z4j_us_p50"] = round(statistics.median(z4j_times), 2)
        result["z4j_us_p99"] = round(_quantile(z4j_times, 0.99), 2)

        # celery-beat path - crontab.remaining_estimate
        if _CELERY_AVAILABLE:
            celery_obj = _make_celery_crontab(expr)
            celery_times: list[float] = []
            for _ in range(iterations):
                t0 = time.perf_counter()
                celery_obj.remaining_estimate(now)
                celery_times.append(
                    (time.perf_counter() - t0) * 1_000_000,
                )
            result["celery_us_p50"] = round(statistics.median(celery_times), 2)
            result["celery_us_p99"] = round(_quantile(celery_times, 0.99), 2)
        out["per_cron"][name] = result
    return out


# =====================================================================
# Metric 2 - per-tick due-list cost at scale
# =====================================================================


def bench_tick_at_scale() -> dict[str, Any]:
    """Time the "what schedules are due right now?" pass at N schedules.

    Both sides iterate every schedule on every tick, so cost is
    ``O(N)`` per tick. The benchmark reports per-tick cost in
    milliseconds at 100 / 1k / 10k schedules so operators can
    judge scaling.
    """
    out: dict[str, Any] = {"per_scale": {}}
    now = datetime.now(UTC)
    last = now - timedelta(seconds=1)  # "1 second ago"

    for n in (100, 1_000, 10_000):
        # z4j path: build a list of croniter instances + one
        # full pass to find which ones would fire on this tick.
        z4j_crons = [croniter("* * * * *", now) for _ in range(n)]
        t0 = time.perf_counter()
        z4j_due = 0
        for c in z4j_crons:
            nxt = c.get_next(datetime)
            if nxt <= now + timedelta(seconds=60):
                z4j_due += 1
        z4j_ms = (time.perf_counter() - t0) * 1000

        result: dict[str, Any] = {
            "schedules": n,
            "z4j_tick_ms": round(z4j_ms, 2),
            "z4j_due_count": z4j_due,
        }

        if _CELERY_AVAILABLE:
            celery_objs = [_make_celery_crontab("* * * * *") for _ in range(n)]
            t0 = time.perf_counter()
            celery_due = 0
            for c in celery_objs:
                state = c.is_due(last)
                if state.is_due:
                    celery_due += 1
            celery_ms = (time.perf_counter() - t0) * 1000
            result["celery_tick_ms"] = round(celery_ms, 2)
            result["celery_due_count"] = celery_due

        out["per_scale"][str(n)] = result
    return out


# =====================================================================
# Metric 3 - per-schedule memory footprint
# =====================================================================


def bench_memory_per_schedule() -> dict[str, Any]:
    """Measure RSS growth as we add 10k schedule objects per side.

    Subtracts a baseline RSS reading taken before construction so
    background process noise doesn't swamp the per-schedule
    number. The reported ``bytes_per_schedule`` is approximate -
    Python's allocator pads, the GC moves things - but it gives a
    defensible order-of-magnitude.
    """
    n = 10_000
    out: dict[str, Any] = {"schedules": n}

    # z4j side - just the croniter objects + minimal wrapper.
    gc.collect()
    baseline_z4j = _rss_mb()
    z4j_holder = [croniter("* * * * *") for _ in range(n)]
    gc.collect()
    after_z4j = _rss_mb()
    z4j_growth_mb = max(0.0, after_z4j - baseline_z4j)
    out["z4j_total_mb"] = round(z4j_growth_mb, 2)
    out["z4j_bytes_per_schedule"] = (
        round(z4j_growth_mb * 1024 * 1024 / n, 1) if z4j_growth_mb > 0 else None
    )
    del z4j_holder
    gc.collect()

    if _CELERY_AVAILABLE:
        baseline_c = _rss_mb()
        c_holder = [_make_celery_crontab("* * * * *") for _ in range(n)]
        gc.collect()
        after_c = _rss_mb()
        c_growth_mb = max(0.0, after_c - baseline_c)
        out["celery_total_mb"] = round(c_growth_mb, 2)
        out["celery_bytes_per_schedule"] = (
            round(c_growth_mb * 1024 * 1024 / n, 1) if c_growth_mb > 0 else None
        )
        del c_holder
        gc.collect()
    return out


# =====================================================================
# Helpers + report
# =====================================================================


def _quantile(samples: list[float], q: float) -> float:
    if not samples:
        return 0.0
    sorted_s = sorted(samples)
    k = max(0, min(len(sorted_s) - 1, round(q * (len(sorted_s) - 1))))
    return sorted_s[k]


def render_summary(report: dict[str, Any]) -> str:
    """Pretty-print the comparison for human consumption."""
    lines = [
        "=" * 70,
        "z4j-scheduler vs celery-beat - head-to-head benchmark",
        "=" * 70,
        f"Generated:    {report['generated']}",
        f"Celery side:  {'enabled' if report['celery_available'] else 'SKIPPED (pip install celery amqp)'}",
        "",
    ]

    # 1. Next-fire computation cost
    lines.append("--- 1. Next-fire computation cost (microseconds, lower is better) ---")
    lines.append(
        f"{'cron':<14} {'z4j p50':>10} {'celery p50':>11} "
        f"{'z4j p99':>10} {'celery p99':>11} {'z4j vs celery':>16}"
    )
    for name, r in report["next_fire_cost"]["per_cron"].items():
        z50 = r["z4j_us_p50"]
        z99 = r["z4j_us_p99"]
        if "celery_us_p50" in r:
            c50 = r["celery_us_p50"]
            c99 = r["celery_us_p99"]
            ratio = z50 / c50 if c50 > 0 else float("inf")
            ratio_str = f"{ratio:>13.2f}x"
            lines.append(
                f"{name:<14} {z50:>10.2f} {c50:>11.2f} {z99:>10.2f} {c99:>11.2f} {ratio_str:>16}"
            )
        else:
            lines.append(f"{name:<14} {z50:>10.2f} {'(skip)':>11} {z99:>10.2f} {'(skip)':>11}")
    lines.append("")

    # 2. Per-tick due-list cost at scale
    lines.append("--- 2. Per-tick due-list cost (milliseconds, lower is better) ---")
    lines.append(f"{'schedules':<12} {'z4j ms':>10} {'celery ms':>11} {'ratio':>10}")
    for n, r in report["tick_at_scale"]["per_scale"].items():
        zms = r["z4j_tick_ms"]
        if "celery_tick_ms" in r:
            cms = r["celery_tick_ms"]
            ratio = zms / cms if cms > 0 else float("inf")
            lines.append(f"{int(n):<12,} {zms:>10.2f} {cms:>11.2f} {ratio:>9.2f}x")
        else:
            lines.append(f"{int(n):<12,} {zms:>10.2f} {'(skip)':>11}")
    lines.append("")

    # 3. Memory per schedule
    lines.append("--- 3. Memory footprint at 10k schedules (lower is better) ---")
    mem = report["memory_per_schedule"]
    z_total = mem["z4j_total_mb"]
    z_per = mem.get("z4j_bytes_per_schedule")
    lines.append(
        f"  z4j-scheduler:   {z_total:>7.2f} MB total, ~{z_per:>6.0f} bytes/schedule"
        if z_per is not None
        else f"  z4j-scheduler:   {z_total:>7.2f} MB total"
    )
    if "celery_total_mb" in mem:
        c_total = mem["celery_total_mb"]
        c_per = mem.get("celery_bytes_per_schedule")
        lines.append(
            f"  celery-beat:     {c_total:>7.2f} MB total, ~{c_per:>6.0f} bytes/schedule"
            if c_per is not None
            else f"  celery-beat:     {c_total:>7.2f} MB total"
        )
    lines.append("")

    # Summary verdict
    lines.append("--- Summary ---")
    if report["celery_available"]:
        # Compute the geometric mean of the z4j/celery ratios across
        # both metrics. < 1 means z4j is faster on average.
        ratios = []
        for r in report["next_fire_cost"]["per_cron"].values():
            if r.get("celery_us_p50"):
                ratios.append(r["z4j_us_p50"] / r["celery_us_p50"])
        for r in report["tick_at_scale"]["per_scale"].values():
            if r.get("celery_tick_ms"):
                ratios.append(r["z4j_tick_ms"] / r["celery_tick_ms"])
        if ratios:
            geomean = pow(
                abs(__import__("math").prod(ratios)),
                1 / len(ratios),
            )
            verdict = (
                f"z4j is {1 / geomean:.2f}x FASTER than celery-beat"
                if geomean < 1
                else f"z4j is {geomean:.2f}x SLOWER than celery-beat"
            )
            lines.append(f"  Geomean across all timing metrics: {verdict}")
    else:
        lines.append(
            "  Re-run with celery installed to see the comparison.",
        )
    lines.append("=" * 70)
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--out",
        default="tests/benchmarks/results/celery_beat_compare.json",
        help="Path to write the JSON report.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1_000,
        help="Iterations per next-fire timing sample (default 1000).",
    )
    args = parser.parse_args()

    print("Running head-to-head benchmark...", file=sys.stderr)
    if not _CELERY_AVAILABLE:
        print(
            "[note] celery is not importable - skipping celery side. "
            "Install with: pip install celery amqp",
            file=sys.stderr,
        )

    report: dict[str, Any] = {
        "generated": datetime.now(UTC).isoformat(),
        "celery_available": _CELERY_AVAILABLE,
        "next_fire_cost": bench_next_fire_cost(iterations=args.iterations),
        "tick_at_scale": bench_tick_at_scale(),
        "memory_per_schedule": bench_memory_per_schedule(),
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\nJSON report: {out_path}", file=sys.stderr)
    print()
    print(render_summary(report))
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())


__all__ = [
    "bench_memory_per_schedule",
    "bench_next_fire_cost",
    "bench_tick_at_scale",
    "main",
    "render_summary",
]
