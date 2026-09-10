"""Reproducible local component evidence; never a production capacity certificate.

Run from packages/z4j-scheduler:
  python -m tests.benchmarks.bench_reliability --out /tmp/reliability.json

The engine uses an in-memory recorder, no Brain/broker/worker. RSS is process
peak on Linux without psutil. The optional PostgreSQL failover probe measures
clean stop at a 100 ms heartbeat; use the integration suite for fault injection.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from tests.benchmarks.bench_phase2 import bench_failover
from tests.benchmarks.bench_phase5 import _bench_memory_at, bench_sustained_load


async def run(duration: float) -> dict:
    report = {
        "generated": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "scope": "development-host component samples; not isolated production hardware or end-to-end delivery",
        "load": [],
        "memory": [],
    }
    for count in (100, 1_000, 10_000):
        result = await bench_sustained_load(schedule_count=count, duration_seconds=duration)
        if not result["fires"] or result["duplicate_slots"]:
            raise RuntimeError(f"component load recorded no work or duplicate slots at {count}")
        report["load"].append(result)
    for count in (10_000, 100_000):
        report["memory"].append(await _bench_memory_at(count))
    dsn = os.environ.get("Z4J_TEST_POSTGRES_URL")
    if dsn:
        probe = SimpleNamespace(get_connection_url=lambda: dsn)
        report["clean_postgres_failover"] = [
            await bench_failover(container=probe) for _ in range(3)
        ]
        if not all(row["succeeded"] for row in report["clean_postgres_failover"]):
            raise RuntimeError("PostgreSQL clean failover failed")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--duration", type=float, default=10)
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error("duration must be positive")
    result = asyncio.run(run(args.duration))
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
