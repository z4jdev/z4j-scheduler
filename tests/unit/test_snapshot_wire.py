"""Stable snapshot frames are all-or-nothing."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import entry_to_pb
from z4j_scheduler.storage._models import ScheduleSnapshot
from z4j_scheduler.storage._snapshot import snapshot_digest
from z4j_scheduler.storage._snapshot_wire import (
    SNAPSHOT_FORMAT_VERSION,
    SnapshotAssembler,
    SnapshotFrameError,
)
from z4j_scheduler.tick._entry import ScheduleEntry


def _entry(*, project_id, revision: int = 7) -> ScheduleEntry:
    entry = ScheduleEntry(
        id=uuid4(),
        project_id=project_id,
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        control_token=uuid4(),
        schedule_revision=revision,
        definition_digest="d" * 64,
        cadence_semantics_version=1,
        cadence_runtime_fingerprint="f" * 64,
    )
    entry.next_fire_at = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
    return entry


def _frames(*, rows=(), watermark: int = 10):
    project_id = rows[0].project_id if rows else uuid4()
    snapshot_id = uuid4()
    unfinished = ScheduleSnapshot(
        snapshot_id=snapshot_id,
        project_id=project_id,
        watermark=watermark,
        rows=tuple(rows),
        digest="",
    )
    digest = snapshot_digest(unfinished)
    frames = [
        pb.ScheduleSnapshotFrame(
            header=pb.ScheduleSnapshotHeader(
                format_version=SNAPSHOT_FORMAT_VERSION,
                snapshot_id=str(snapshot_id),
                project_id=str(project_id),
            ),
        ),
        *[
            pb.ScheduleSnapshotFrame(
                row=pb.ScheduleSnapshotRow(
                    snapshot_id=str(snapshot_id),
                    schedule=entry_to_pb(row),
                ),
            )
            for row in rows
        ],
        pb.ScheduleSnapshotFrame(
            complete=pb.ScheduleSnapshotComplete(
                format_version=SNAPSHOT_FORMAT_VERSION,
                snapshot_id=str(snapshot_id),
                project_id=str(project_id),
                watermark=watermark,
                row_count=len(rows),
                digest=digest,
            ),
        ),
    ]
    return project_id, frames


def test_fresh_empty_snapshot_accepts_zero_backfill_boundary() -> None:
    project_id, frames = _frames(rows=(), watermark=0)
    assembler = SnapshotAssembler(expected_project_id=project_id)
    for frame in frames:
        assembler.accept(frame)

    assert assembler.finish().watermark == 0


def test_completed_snapshot_is_returned() -> None:
    project_id = uuid4()
    row = _entry(project_id=project_id)
    _, frames = _frames(rows=(row,))
    assembler = SnapshotAssembler(expected_project_id=project_id)
    for frame in frames:
        assembler.accept(frame)
    result = assembler.finish()
    assert result.rows[0].id == row.id
    assert result.watermark == 10


def test_completed_empty_snapshot_is_unambiguous() -> None:
    project_id, frames = _frames()
    assembler = SnapshotAssembler(expected_project_id=project_id)
    for frame in frames:
        assembler.accept(frame)
    assert assembler.finish().rows == ()


def test_all_project_snapshot_accepts_rows_from_multiple_projects() -> None:
    rows = (_entry(project_id=uuid4()), _entry(project_id=uuid4(), revision=8))
    snapshot_id = uuid4()
    unfinished = ScheduleSnapshot(
        snapshot_id=snapshot_id,
        project_id=None,
        watermark=8,
        rows=rows,
        digest="",
    )
    frames = [
        pb.ScheduleSnapshotFrame(
            header=pb.ScheduleSnapshotHeader(
                format_version=SNAPSHOT_FORMAT_VERSION,
                snapshot_id=str(snapshot_id),
                project_id="",
            ),
        ),
        *[
            pb.ScheduleSnapshotFrame(
                row=pb.ScheduleSnapshotRow(
                    snapshot_id=str(snapshot_id),
                    schedule=entry_to_pb(row),
                ),
            )
            for row in rows
        ],
        pb.ScheduleSnapshotFrame(
            complete=pb.ScheduleSnapshotComplete(
                format_version=SNAPSHOT_FORMAT_VERSION,
                snapshot_id=str(snapshot_id),
                project_id="",
                watermark=8,
                row_count=2,
                digest=snapshot_digest(unfinished),
            ),
        ),
    ]
    assembler = SnapshotAssembler(expected_project_id=None)
    for frame in frames:
        assembler.accept(frame)

    result = assembler.finish()
    assert result.project_id is None
    assert {row.project_id for row in result.rows} == {
        rows[0].project_id,
        rows[1].project_id,
    }


def test_partial_stream_never_finishes() -> None:
    project_id, frames = _frames()
    assembler = SnapshotAssembler(expected_project_id=project_id)
    assembler.accept(frames[0])
    with pytest.raises(SnapshotFrameError, match="before completion"):
        assembler.finish()


@pytest.mark.parametrize("mutation", ["count", "digest", "snapshot_id"])
def test_conflicting_terminal_frame_is_rejected(mutation: str) -> None:
    project_id, frames = _frames()
    complete = frames[-1].complete
    if mutation == "count":
        complete.row_count += 1
    elif mutation == "digest":
        complete.digest = "0" * 64
    else:
        complete.snapshot_id = str(uuid4())
    assembler = SnapshotAssembler(expected_project_id=project_id)
    assembler.accept(frames[0])
    with pytest.raises(SnapshotFrameError):
        assembler.accept(frames[-1])


def test_duplicate_row_is_rejected_before_completion() -> None:
    project_id = uuid4()
    row = _entry(project_id=project_id)
    _, frames = _frames(rows=(row,))
    assembler = SnapshotAssembler(expected_project_id=project_id)
    assembler.accept(frames[0])
    assembler.accept(frames[1])
    with pytest.raises(SnapshotFrameError, match="duplicate"):
        assembler.accept(frames[1])
