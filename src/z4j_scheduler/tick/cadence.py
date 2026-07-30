"""Scheduler cadence authority used for Brain differential verification."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from functools import lru_cache

from z4j_scheduler.tick import cron, interval, one_shot, solar
from z4j_scheduler.tick._runtime import (
    CADENCE_SEMANTICS_VERSION,
)
from z4j_scheduler.tick._runtime import (
    cadence_runtime_fingerprint as _runtime_fingerprint,
)


def canonical_next_run_at(
    *,
    kind: str,
    expression: str,
    timezone: str,
    last_run_at: datetime | None,
    anchor_at: datetime,
) -> datetime | None:
    """Return the scheduler's canonical UTC successor."""

    if kind == "cron":
        result = cron.next_fire(
            expression,
            timezone,
            last_run_at if last_run_at is not None else anchor_at,
        )
    elif kind == "interval":
        result = interval.next_fire(
            expression,
            last_fire_at=last_run_at,
            anchor_at=anchor_at,
        )
    elif kind in {"clocked", "one_shot"}:
        result = one_shot.next_fire(expression, last_fire_at=last_run_at)
    elif kind == "solar":
        result = solar.next_solar_fire(
            expression,
            after=last_run_at if last_run_at is not None else anchor_at,
        )
    else:
        raise ValueError(f"unknown schedule kind: {kind!r}")
    return result.astimezone(UTC) if result is not None else None


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds") if value else None


@lru_cache(maxsize=1)
def cadence_behavior_vector_digest() -> str:
    """Digest the same fixed edge vectors used by the Brain."""

    anchors = {
        "winter": datetime(2026, 1, 15, 5, 59, 59, tzinfo=UTC),
        "spring": datetime(2026, 3, 8, 6, 59, 59, tzinfo=UTC),
        "interval": datetime(2026, 1, 1, 12, 3, 7, tzinfo=UTC),
        "solar": datetime(2026, 1, 1, tzinfo=UTC),
    }
    vector = [
        _iso(
            canonical_next_run_at(
                kind="cron",
                expression="0 1 * * *",
                timezone="America/New_York",
                last_run_at=anchors["winter"],
                anchor_at=anchors["winter"],
            ),
        ),
        _iso(
            canonical_next_run_at(
                kind="cron",
                expression="30 2 * * *",
                timezone="America/New_York",
                last_run_at=anchors["spring"],
                anchor_at=anchors["spring"],
            ),
        ),
        _iso(
            canonical_next_run_at(
                kind="interval",
                expression="5m",
                timezone="UTC",
                last_run_at=None,
                anchor_at=anchors["interval"],
            ),
        ),
        _iso(
            canonical_next_run_at(
                kind="one_shot",
                expression="2026-02-03T04:05:06+05:30",
                timezone="UTC",
                last_run_at=None,
                anchor_at=anchors["winter"],
            ),
        ),
        _iso(
            canonical_next_run_at(
                kind="one_shot",
                expression="2026-02-03T04:05:06Z",
                timezone="UTC",
                last_run_at=anchors["winter"],
                anchor_at=anchors["winter"],
            ),
        ),
        _iso(
            canonical_next_run_at(
                kind="solar",
                expression="sunrise:0:0",
                timezone="UTC",
                last_run_at=None,
                anchor_at=anchors["solar"],
            ),
        ),
    ]
    payload = json.dumps(vector, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


@lru_cache(maxsize=1)
def cadence_runtime_fingerprint() -> str:
    return _runtime_fingerprint(cadence_behavior_vector_digest())


__all__ = [
    "CADENCE_SEMANTICS_VERSION",
    "cadence_behavior_vector_digest",
    "cadence_runtime_fingerprint",
    "canonical_next_run_at",
]
