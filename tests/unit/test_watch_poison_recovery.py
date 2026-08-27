"""A frame the applier rejects must not wedge the stream forever.

``OrderedWatchApplier.apply`` raises without advancing its cursor when a frame
fails validation. If the reconnect resumed from that same cursor, the brain would
re-send the identical frame, the applier would reject it again, and the scheduler
would never fire again for any project. The claim under test is that the
reconnect path re-syncs first, which carries the cursor past the bad revision.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.storage._models import ScheduleChange, ScheduleSnapshot
from z4j_scheduler.storage._snapshot import snapshot_digest
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.watch import WatchStream
from z4j_scheduler.tick._entry import ScheduleEntry


def _row(project_id, revision: int) -> ScheduleEntry:
    row = ScheduleEntry(
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
    row.next_fire_at = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
    return row


def _snapshot(project_id, row: ScheduleEntry, watermark: int) -> ScheduleSnapshot:
    unfinished = ScheduleSnapshot(
        snapshot_id=uuid4(),
        project_id=project_id,
        watermark=watermark,
        rows=(row,),
        digest="",
    )
    return ScheduleSnapshot(
        snapshot_id=unfinished.snapshot_id,
        project_id=project_id,
        watermark=watermark,
        rows=(row,),
        digest=snapshot_digest(unfinished),
    )


@pytest.mark.asyncio
async def test_a_rejected_frame_does_not_wedge_the_stream() -> None:
    project_id = uuid4()
    row = _row(project_id, revision=10)

    class PoisonThenCleanClient:
        """First stream serves a frame whose payload contradicts its envelope."""

        def __init__(self) -> None:
            self.stream_attempts = 0
            self.after_revisions: list[int] = []
            self.watermark = 10

        async def list_schedule_snapshot(self, requested_project_id):
            return _snapshot(requested_project_id, row, self.watermark)

        async def watch_schedules_v2(self, requested_project_id, *, after_revision):
            self.stream_attempts += 1
            self.after_revisions.append(after_revision)
            if self.stream_attempts == 1:
                # envelope says revision 11, payload carries 10: rejected.
                yield ScheduleChange(
                    kind="upsert",
                    revision=11,
                    project_id=requested_project_id,
                    schedule=row,
                    deleted_id=None,
                )
                return
            # The brain has moved on; nothing further to send.
            return
            yield  # pragma: no cover - makes this an async generator

    client = PoisonThenCleanClient()
    cache = ScheduleCache()
    watch = WatchStream(
        client=client,  # type: ignore[arg-type]
        cache=cache,
        project_id=project_id,
        protocol_mode="current",
    )

    # Attempt 1: the poisoned frame takes the stream down.
    with pytest.raises(Exception) as caught:
        await watch._sync_then_watch()
    assert "does not match its envelope" in str(caught.value)
    assert watch._revision_cursor == 10

    # The brain's revision advances past the bad frame, as it does whenever any
    # later change is committed.
    client.watermark = 12

    # Attempt 2 is what the reconnect loop does. It must resume PAST the frame
    # that killed attempt 1, not at it.
    await watch._sync_then_watch()

    assert client.after_revisions == [10, 12], (
        "the reconnect resumed at the poisoned revision instead of past it; "
        f"after_revisions={client.after_revisions}"
    )
    assert watch.is_healthy, "the stream never returned to healthy"
