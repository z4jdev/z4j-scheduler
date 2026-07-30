"""Fail-closed assembly of Boundary D stable snapshot wire frames."""

from __future__ import annotations

from uuid import UUID

from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import entry_from_pb
from z4j_scheduler.storage._models import ScheduleSnapshot
from z4j_scheduler.storage._snapshot import snapshot_digest
from z4j_scheduler.tick._entry import ScheduleEntry

SNAPSHOT_FORMAT_VERSION = 1


class SnapshotFrameError(RuntimeError):
    """A snapshot stream ended partial, conflicting, or malformed."""


class SnapshotAssembler:
    """Buffer one versioned snapshot without exposing partial state."""

    def __init__(self, *, expected_project_id: UUID | None) -> None:
        self._expected_project_id = expected_project_id
        self._snapshot_id: UUID | None = None
        self._rows: list[ScheduleEntry] = []
        self._seen_ids: set[UUID] = set()
        self._complete: ScheduleSnapshot | None = None

    def accept(self, frame: pb.ScheduleSnapshotFrame) -> None:
        """Accept one ordered frame or raise before any cache mutation."""

        kind = frame.WhichOneof("frame")
        if self._complete is not None:
            raise SnapshotFrameError("snapshot carried a frame after completion")
        if kind == "header":
            self._accept_header(frame.header)
        elif kind == "row":
            self._accept_row(frame.row)
        elif kind == "complete":
            self._accept_complete(frame.complete)
        else:
            raise SnapshotFrameError("snapshot carried an empty or unknown frame")

    def _accept_header(self, header: pb.ScheduleSnapshotHeader) -> None:
        if self._snapshot_id is not None:
            raise SnapshotFrameError("snapshot carried more than one header")
        if header.format_version != SNAPSHOT_FORMAT_VERSION:
            raise SnapshotFrameError("unsupported snapshot framing version")
        snapshot_id = _uuid(header.snapshot_id, field="snapshot id")
        project_id = _scope(header.project_id, field="snapshot project id")
        if project_id != self._expected_project_id:
            raise SnapshotFrameError("snapshot header escaped the requested project")
        self._snapshot_id = snapshot_id

    def _accept_row(self, row: pb.ScheduleSnapshotRow) -> None:
        if self._snapshot_id is None:
            raise SnapshotFrameError("snapshot row arrived before its header")
        if _uuid(row.snapshot_id, field="row snapshot id") != self._snapshot_id:
            raise SnapshotFrameError("snapshot row changed snapshot id")
        entry = entry_from_pb(row.schedule)
        if self._expected_project_id is not None and entry.project_id != self._expected_project_id:
            raise SnapshotFrameError("snapshot row escaped the requested project")
        if entry.id in self._seen_ids:
            raise SnapshotFrameError("snapshot contains a duplicate schedule id")
        self._seen_ids.add(entry.id)
        self._rows.append(entry)

    def _accept_complete(self, complete: pb.ScheduleSnapshotComplete) -> None:
        if self._snapshot_id is None:
            raise SnapshotFrameError("snapshot completed before its header")
        if complete.format_version != SNAPSHOT_FORMAT_VERSION:
            raise SnapshotFrameError("snapshot completion changed framing version")
        if _uuid(complete.snapshot_id, field="completion snapshot id") != self._snapshot_id:
            raise SnapshotFrameError("snapshot completion changed snapshot id")
        project_id = _scope(complete.project_id, field="completion project id")
        if project_id != self._expected_project_id:
            raise SnapshotFrameError("snapshot completion changed project")
        if complete.watermark < 0:
            raise SnapshotFrameError("snapshot watermark cannot be negative")
        if complete.row_count != len(self._rows):
            raise SnapshotFrameError("snapshot terminal row count does not match")
        candidate = ScheduleSnapshot(
            snapshot_id=self._snapshot_id,
            project_id=project_id,
            watermark=int(complete.watermark),
            rows=tuple(self._rows),
            digest=complete.digest,
        )
        if candidate.digest != snapshot_digest(candidate):
            raise SnapshotFrameError("snapshot terminal digest does not match")
        self._complete = candidate

    def finish(self) -> ScheduleSnapshot:
        """Return the validated snapshot only after its terminal frame."""

        if self._complete is None:
            raise SnapshotFrameError("snapshot stream ended before completion")
        return self._complete


def _uuid(value: str, *, field: str) -> UUID:
    try:
        return UUID(value)
    except (TypeError, ValueError) as exc:
        raise SnapshotFrameError(f"{field} is not a UUID") from exc


def _scope(value: str, *, field: str) -> UUID | None:
    if not value:
        return None
    return _uuid(value, field=field)


__all__ = [
    "SNAPSHOT_FORMAT_VERSION",
    "SnapshotAssembler",
    "SnapshotFrameError",
]
