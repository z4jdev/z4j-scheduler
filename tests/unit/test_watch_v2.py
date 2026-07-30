"""Revision Watch events and filtered checkpoints share one safe cursor."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.storage._models import ScannedThrough, ScheduleChange
from z4j_scheduler.storage._watch_v2 import OrderedWatchApplier
from z4j_scheduler.storage.cache import ScheduleCache, ScheduleProtocolError
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _entry(*, project_id, revision: int) -> ScheduleEntry:
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


async def test_events_and_checkpoints_advance_one_strict_cursor() -> None:
    cache = ScheduleCache()
    project_id = uuid4()
    applier = OrderedWatchApplier(
        cache=cache,
        project_id=project_id,
        after_revision=10,
    )
    row = _entry(project_id=project_id, revision=11)
    await applier.apply(
        ScheduleChange(
            kind="upsert",
            revision=11,
            project_id=project_id,
            schedule=row,
            deleted_id=None,
        ),
    )
    await applier.apply(ScannedThrough(revision=13, server_revision=15))

    assert applier.cursor == 13
    assert await cache.get(row.id) is row
    assert await cache.project_watermark(project_id) == 0

    await applier.apply(
        ScheduleChange(
            kind="tombstone",
            revision=14,
            project_id=project_id,
            schedule=None,
            deleted_id=row.id,
        ),
    )
    assert applier.cursor == 14
    assert await cache.get(row.id) is None


async def test_all_project_scope_accepts_real_project_envelopes() -> None:
    cache = ScheduleCache()
    first_project = uuid4()
    second_project = uuid4()
    first = _entry(project_id=first_project, revision=11)
    second = _entry(project_id=second_project, revision=12)
    applier = OrderedWatchApplier(
        cache=cache,
        project_id=None,
        after_revision=10,
    )

    for row in (first, second):
        await applier.apply(
            ScheduleChange(
                kind="upsert",
                revision=row.schedule_revision,
                project_id=row.project_id,
                schedule=row,
                deleted_id=None,
            ),
        )

    assert applier.cursor == 12
    assert await cache.get(first.id) is first
    assert await cache.get(second.id) is second


async def test_nonincreasing_frame_fails_closed() -> None:
    applier = OrderedWatchApplier(
        cache=ScheduleCache(),
        project_id=uuid4(),
        after_revision=10,
    )
    with pytest.raises(ScheduleProtocolError, match="did not increase"):
        await applier.apply(ScannedThrough(revision=10, server_revision=10))
    assert applier.cursor == 10


async def test_checkpoint_beyond_server_revision_fails_closed() -> None:
    applier = OrderedWatchApplier(
        cache=ScheduleCache(),
        project_id=uuid4(),
        after_revision=10,
    )
    with pytest.raises(ScheduleProtocolError, match="exceeds server"):
        await applier.apply(ScannedThrough(revision=12, server_revision=11))
    assert applier.cursor == 10


async def test_upsert_revision_must_match_envelope() -> None:
    project_id = uuid4()
    applier = OrderedWatchApplier(
        cache=ScheduleCache(),
        project_id=project_id,
        after_revision=10,
    )
    with pytest.raises(ScheduleProtocolError, match="does not match"):
        await applier.apply(
            ScheduleChange(
                kind="upsert",
                revision=12,
                project_id=project_id,
                schedule=_entry(project_id=project_id, revision=11),
                deleted_id=None,
            ),
        )
    assert applier.cursor == 10


async def test_failed_cache_apply_does_not_advance_cursor() -> None:
    project_id = uuid4()
    row = _entry(project_id=project_id, revision=11)
    cache = ScheduleCache()
    await cache.upsert(row)
    conflict = _entry(project_id=project_id, revision=11)
    conflict.id = row.id
    applier = OrderedWatchApplier(
        cache=cache,
        project_id=project_id,
        after_revision=10,
    )
    with pytest.raises(ScheduleProtocolError, match="conflicting"):
        await applier.apply(
            ScheduleChange(
                kind="upsert",
                revision=11,
                project_id=project_id,
                schedule=conflict,
                deleted_id=None,
            ),
        )
    assert applier.cursor == 10
