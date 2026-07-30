"""Canonical Boundary-D stable-snapshot framing."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from z4j_scheduler.storage._models import ScheduleSnapshot
    from z4j_scheduler.tick._entry import ScheduleEntry

_SNAPSHOT_FORMAT_VERSION = 1


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("stable snapshot timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _row_payload(row: ScheduleEntry) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "project_id": str(row.project_id),
        "kind": row.kind,
        "expression": row.expression,
        "timezone": row.timezone,
        "is_enabled": row.is_enabled,
        "catch_up": row.catch_up,
        "anchor_at": _timestamp(row.anchor_at),
        "last_fire_at": _timestamp(row.last_fire_at),
        "next_fire_at": _timestamp(row.next_fire_at),
        "name": row.name,
        "engine": row.engine,
        "control_token": (str(row.control_token) if row.control_token is not None else None),
        "schedule_revision": row.schedule_revision,
        "definition_digest": row.definition_digest,
        "cadence_semantics_version": row.cadence_semantics_version,
        "cadence_runtime_fingerprint": row.cadence_runtime_fingerprint,
    }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _framed(digest: Any, payload: bytes) -> None:
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def snapshot_digest(snapshot: ScheduleSnapshot) -> str:
    """Return the versioned digest for one completed snapshot."""

    ordered_rows = sorted(snapshot.rows, key=lambda row: row.id.bytes)
    header = _canonical_bytes(
        {
            "format_version": _SNAPSHOT_FORMAT_VERSION,
            "snapshot_id": str(snapshot.snapshot_id),
            "project_id": (str(snapshot.project_id) if snapshot.project_id is not None else ""),
            "watermark": snapshot.watermark,
            "row_count": len(ordered_rows),
        },
    )
    digest = hashlib.sha256()
    _framed(digest, header)
    for row in ordered_rows:
        _framed(digest, _canonical_bytes(_row_payload(row)))
    return digest.hexdigest()


__all__ = ["snapshot_digest"]
