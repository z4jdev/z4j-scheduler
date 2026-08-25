"""Build a current-protocol fire the way the shipped scheduler builds one.

Every field of a current ``FireSchedule`` request comes off the wire: the
control token, the revision, the definition digest and the cadence tuple are
read from the Brain's snapshot, and the successor cursor is computed by the
scheduler's own cadence authority (the one the negotiated runtime fingerprint
covers). Nothing here reads the Brain's database, because a test that assembled
the request from the Brain's own rows would prove only that the Brain agrees
with itself.

Missing any one of those fields is what silently drops a caller onto the
tokenless legacy wire, so :func:`prepare_current_fire` refuses to hand back an
entry that is not complete current-protocol authority.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from z4j_scheduler.dispatch.fire import derive_fire_id
from z4j_scheduler.storage._protocol import CURRENT_PROTOCOL_EPOCH
from z4j_scheduler.tick._prepared import PreparedFire
from z4j_scheduler.tick.cadence import canonical_next_run_at

if TYPE_CHECKING:  # pragma: no cover - typing only
    from z4j_scheduler.storage._models import FireResult
    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.tick._entry import ScheduleEntry


class LegacyShapeError(RuntimeError):
    """The Brain answered in the tokenless 1.7 shape."""


@dataclass(frozen=True, slots=True)
class CurrentFire:
    """One slot, its prepared successor, and the authority that carries it."""

    entry: ScheduleEntry
    prepared: PreparedFire
    fire_id: uuid.UUID
    slot: datetime


async def prepare_current_fire(
    client: BrainClient,
    *,
    project_id: uuid.UUID,
    schedule_id: uuid.UUID,
) -> CurrentFire:
    """Read the live snapshot and prepare the next due slot from it.

    Re-read per call on purpose: an accepted fire allocates a new revision and
    moves the cursor, so an entry held across two fires is stale authority and
    the Brain would answer the second one with a refresh disposition.
    """

    snapshot = await client.list_schedule_snapshot(project_id)
    matches = [row for row in snapshot.rows if row.id == schedule_id]
    if len(matches) != 1:
        msg = f"snapshot carries {len(matches)} rows for schedule {schedule_id}"
        raise LookupError(msg)
    entry = matches[0]

    if (
        entry.control_token is None
        or entry.schedule_revision <= 0
        or not entry.definition_digest
        or entry.cadence_semantics_version <= 0
        or not entry.cadence_runtime_fingerprint
    ):
        msg = (
            "snapshot entry carries no current-protocol authority "
            f"(control_token={entry.control_token!r} "
            f"revision={entry.schedule_revision!r} "
            f"digest={entry.definition_digest!r} "
            f"cadence={entry.cadence_semantics_version!r}/"
            f"{entry.cadence_runtime_fingerprint!r}); a fire built from it "
            "would travel the tokenless legacy wire"
        )
        raise LegacyShapeError(msg)

    slot = entry.next_fire_at
    if slot is None:
        msg = "snapshot entry carries no authoritative next cursor"
        raise LegacyShapeError(msg)

    successor = canonical_next_run_at(
        kind=entry.kind,
        expression=entry.expression,
        timezone=entry.timezone,
        last_run_at=slot,
        anchor_at=slot,
    )
    return CurrentFire(
        entry=entry,
        prepared=PreparedFire(scheduled_for=slot, next_run_at=successor),
        fire_id=derive_fire_id(schedule_id, slot),
        slot=slot,
    )


async def send_current_fire(
    client: BrainClient,
    fire: CurrentFire,
    *,
    fired_at: datetime | None = None,
) -> FireResult:
    """Send one current fire on the raw client, with no follow-up receipt.

    Byte-for-byte the request ``FireDispatcher.dispatch`` builds, minus what
    the dispatcher does after the answer arrives: on a delivered current fire
    it immediately sends ``AcknowledgeFireResult`` for the completed round
    trip. A test about the acknowledge RPC's own contract has to observe a fire
    that nothing has acknowledged yet, so it fires through here and drives the
    receipt itself.
    """

    from datetime import UTC

    return await client.fire_schedule(
        schedule_id=fire.entry.id,
        fire_id=fire.fire_id,
        scheduled_for=fire.slot,
        fired_at=fired_at or datetime.now(UTC),
        schedule_entry=fire.entry,
        prepared_fire=fire.prepared,
        scheduler_protocol_epoch=CURRENT_PROTOCOL_EPOCH,
    )


__all__ = [
    "CurrentFire",
    "LegacyShapeError",
    "prepare_current_fire",
    "send_current_fire",
]
