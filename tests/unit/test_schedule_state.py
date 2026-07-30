"""Per-id state recovery uses explicit revision-bounded absence."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import entry_to_pb, schedule_state_from_pb
from z4j_scheduler.storage.cache import ScheduleCache, ScheduleProtocolError
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _entry(*, project_id, schedule_id=None, revision: int) -> ScheduleEntry:
    entry = ScheduleEntry(
        id=schedule_id or uuid4(),
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


async def test_newer_observed_absence_clears_row_and_overlay() -> None:
    cache = ScheduleCache()
    project_id = uuid4()
    row = _entry(project_id=project_id, revision=10)
    await cache.upsert(row)
    assert await cache.quarantine_locally(
        row.id,
        expected_definition=row,
        code="cadence_definition_invalid",
    )

    assert await cache.apply_observed_absence(
        schedule_id=row.id,
        project_id=project_id,
        observed_revision=12,
    )
    assert await cache.get(row.id) is None
    assert await cache.local_quarantine(row.id) is None
    # Per-id absence must not claim that every project row was observed.
    assert await cache.project_watermark(project_id) == 0


async def test_older_observed_absence_cannot_remove_newer_row() -> None:
    cache = ScheduleCache()
    project_id = uuid4()
    row = _entry(project_id=project_id, revision=12)
    await cache.upsert(row)
    assert not await cache.apply_observed_absence(
        schedule_id=row.id,
        project_id=project_id,
        observed_revision=11,
    )
    assert await cache.get(row.id) is row


async def test_same_revision_row_vs_absence_is_protocol_fault() -> None:
    cache = ScheduleCache()
    project_id = uuid4()
    row = _entry(project_id=project_id, revision=12)
    await cache.upsert(row)
    with pytest.raises(ScheduleProtocolError, match="conflicts"):
        await cache.apply_observed_absence(
            schedule_id=row.id,
            project_id=project_id,
            observed_revision=12,
        )


async def test_state_converter_requires_explicit_absence_and_revision_floor() -> None:
    project_id = uuid4()
    schedule_id = uuid4()
    with pytest.raises(ValueError, match="omitted"):
        schedule_state_from_pb(
            pb.GetScheduleStateResponse(observed_revision=12),
            expected_project_id=project_id,
            expected_schedule_id=schedule_id,
            minimum_observed_revision=10,
        )
    with pytest.raises(ValueError, match="minimum"):
        schedule_state_from_pb(
            pb.GetScheduleStateResponse(
                observed_revision=9,
                absence=pb.ScheduleAbsence(
                    project_id=str(project_id),
                    schedule_id=str(schedule_id),
                ),
            ),
            expected_project_id=project_id,
            expected_schedule_id=schedule_id,
            minimum_observed_revision=10,
        )


async def test_state_converter_accepts_matching_row_and_absence() -> None:
    project_id = uuid4()
    row = _entry(project_id=project_id, revision=11)
    present = schedule_state_from_pb(
        pb.GetScheduleStateResponse(
            observed_revision=12,
            schedule=entry_to_pb(row),
        ),
        expected_project_id=project_id,
        expected_schedule_id=row.id,
        minimum_observed_revision=10,
    )
    assert present.schedule is not None

    absent = schedule_state_from_pb(
        pb.GetScheduleStateResponse(
            observed_revision=13,
            absence=pb.ScheduleAbsence(
                project_id=str(project_id),
                schedule_id=str(row.id),
            ),
        ),
        expected_project_id=project_id,
        expected_schedule_id=row.id,
        minimum_observed_revision=12,
    )
    assert absent.schedule is None
