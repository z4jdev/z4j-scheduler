"""Current FireSchedule wire is complete, present, and typed."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import make_fire_request, parse_fire_response
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick._prepared import PreparedFire


def _entry() -> ScheduleEntry:
    entry = ScheduleEntry(
        id=uuid4(),
        project_id=uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        control_token=uuid4(),
        schedule_revision=40,
        definition_digest="d" * 64,
        # Deliberately NOT the real values. CADENCE_SEMANTICS_VERSION is 1,
        # so a fixture of 1 made the version assertion below unable to tell an
        # echo from a local computation; 99 makes it load-bearing.
        cadence_semantics_version=99,
        cadence_runtime_fingerprint="f" * 64,
    )
    entry.next_fire_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
    return entry


def test_current_request_carries_complete_nullable_authority() -> None:
    entry = _entry()
    prepared = PreparedFire(
        scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
        next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
    )
    request = make_fire_request(
        schedule_id=entry.id,
        fire_id=uuid4(),
        scheduled_for=prepared.scheduled_for,
        fired_at=prepared.scheduled_for,
        schedule_entry=entry,
        prepared_fire=prepared,
        scheduler_protocol_epoch=1,
    )
    assert request.scheduler_protocol_epoch == 1
    assert request.observed_control_token == str(entry.control_token)
    assert request.expected_schedule_revision == 40
    assert not request.HasField("expected_last_run_at")
    assert request.HasField("expected_next_run_at")
    assert request.HasField("prepared_next_run_at")


def test_partial_current_request_is_rejected() -> None:
    entry = _entry()
    with pytest.raises(ValueError, match="complete protocol authority"):
        make_fire_request(
            schedule_id=entry.id,
            fire_id=uuid4(),
            scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
            fired_at=entry.next_fire_at,  # type: ignore[arg-type]
            schedule_entry=entry,
            prepared_fire=None,
            scheduler_protocol_epoch=1,
        )


def test_zero_disposition_remains_explicit_legacy_shape() -> None:
    result = parse_fire_response(pb.FireScheduleResponse(buffered=True))
    assert result.disposition is None
    assert result.success


def test_unknown_nonzero_disposition_fails_closed() -> None:
    message = pb.FireScheduleResponse()
    message.disposition = 99
    with pytest.raises(ValueError, match="unknown disposition"):
        parse_fire_response(message)


def test_accepted_response_preserves_acceptance_and_live_cursor() -> None:
    token = uuid4()
    message = pb.FireScheduleResponse(
        disposition=pb.FireDisposition.FIRE_ACCEPTED,
        acceptance_revision=41,
        live_control_token=str(token),
        live_revision=41,
    )
    message.accepted_last_run_at.FromDatetime(
        datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
    )
    message.accepted_next_run_at.FromDatetime(
        datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
    )
    message.live_last_run_at.CopyFrom(message.accepted_last_run_at)
    message.live_next_run_at.CopyFrom(message.accepted_next_run_at)

    result = parse_fire_response(message)

    assert result.disposition == "accepted"
    assert result.success
    assert result.acceptance_revision == 41
    assert result.live_control_token == token
    assert result.live_next_run_at == datetime(
        2026,
        4,
        26,
        16,
        0,
        tzinfo=UTC,
    )


class TestCadenceIdentityIsComputedNotEchoed:
    """The wire must carry what THIS process computes, not the row's stamp.

    The Brain's acceptance test is ``submitted != <what the Brain computes>``,
    i.e. "does the submitter compute cadence the way I do". That is an
    agreement check between two processes.

    These fields used to be copied from ``schedule_entry``, which arrives from
    the Brain's own watch stream and is therefore the ROW's stored value. The
    check then compared the Brain against itself and said nothing about the
    scheduler -- and, because nothing re-stamps that column, it also made the
    cadence closure unbumpable: any change to the pinned dependencies, or to
    the running Python version (which is in the fingerprint payload), left
    every pre-existing row carrying the old value, every fire refused as
    ``cadence_semantics_mismatch``, and every schedule locally quarantined.
    Cursor advance refused on the same test, so they could not even skip
    forward.

    The entry fixture here carries a deliberately fake fingerprint, so a
    regression is visible immediately: an echo would put "ffff..." on the wire.
    Note that the whole existing suite passed while the bug was live, because
    nothing asserted what the wire actually carried.
    """

    def test_fire_request_carries_the_locally_computed_identity(self) -> None:
        from z4j_scheduler.tick.cadence import (
            CADENCE_SEMANTICS_VERSION,
            cadence_runtime_fingerprint,
        )

        entry = _entry()
        stale = entry.cadence_runtime_fingerprint
        prepared = PreparedFire(
            scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
            next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        request = make_fire_request(
            schedule_id=entry.id,
            fire_id=uuid4(),
            scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
            fired_at=datetime(2026, 4, 26, 15, 0, 1, tzinfo=UTC),
            schedule_entry=entry,
            prepared_fire=prepared,
            scheduler_protocol_epoch=1,
        )
        assert request.cadence_runtime_fingerprint == cadence_runtime_fingerprint(), (
            "the fire request must carry the fingerprint this process computes"
        )
        assert request.cadence_runtime_fingerprint != stale, (
            "the fire request echoed the row's stored fingerprint; a cadence "
            "closure change would refuse every pre-existing schedule"
        )
        assert request.cadence_semantics_version == CADENCE_SEMANTICS_VERSION

    def test_a_stale_row_stamp_does_not_reach_the_wire(self) -> None:
        # The upgrade case stated directly: a row written by a previous
        # release carries a different fingerprint, and that value must not
        # determine whether the fire is accepted.
        from z4j_scheduler.tick.cadence import cadence_runtime_fingerprint

        entry = _entry()
        entry.cadence_runtime_fingerprint = "0" * 64  # as a prior release left it
        prepared = PreparedFire(
            scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
            next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        request = make_fire_request(
            schedule_id=entry.id,
            fire_id=uuid4(),
            scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
            fired_at=datetime(2026, 4, 26, 15, 0, 1, tzinfo=UTC),
            schedule_entry=entry,
            prepared_fire=prepared,
            scheduler_protocol_epoch=1,
        )
        assert request.cadence_runtime_fingerprint == cadence_runtime_fingerprint()


class TestCursorAdvanceAlsoComputesLocally:
    """``advance_cursor`` is the second submission site, and had no test.

    Mutation-proven gap: reverting ``dispatch/fire.py`` alone to the echoing
    version left the entire scheduler suite, the contract suite and the brain
    protocol suites green. Only ``make_fire_request`` was pinned, so half the
    fix was unfalsifiable -- and the pyprojects asserted in shipped comments
    that this file guarded it.

    It matters as much as the fire path: the Brain refuses a cursor advance on
    the same cadence test, so an echoed fingerprint there means schedules
    cannot even skip forward past a missed slot.
    """

    @pytest.mark.asyncio
    async def test_advance_cursor_submits_the_computed_identity(self) -> None:
        from z4j_scheduler.dispatch.fire import FireDispatcher
        from z4j_scheduler.tick._prepared import PreparedFire
        from z4j_scheduler.tick.cadence import (
            CADENCE_SEMANTICS_VERSION,
            cadence_runtime_fingerprint,
        )

        captured: dict[str, object] = {}

        class _CapturingClient:
            async def advance_schedule_cursor(self, **kwargs: object) -> object:
                captured.update(kwargs)
                return object()

        entry = _entry()
        stale_fingerprint = entry.cadence_runtime_fingerprint
        stale_version = entry.cadence_semantics_version
        dispatcher = FireDispatcher.__new__(FireDispatcher)
        dispatcher._client = _CapturingClient()  # type: ignore[attr-defined]

        await dispatcher.advance_cursor(
            entry=entry,
            prepared=PreparedFire(
                scheduled_for=entry.next_fire_at,  # type: ignore[arg-type]
                next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
            ),
        )

        assert captured["cadence_runtime_fingerprint"] == cadence_runtime_fingerprint(), (
            "advance_cursor must submit the fingerprint this process computes"
        )
        assert captured["cadence_runtime_fingerprint"] != stale_fingerprint, (
            "advance_cursor echoed the row's stored fingerprint; a cadence "
            "closure change would refuse every catch-up for every schedule"
        )
        assert captured["cadence_semantics_version"] == CADENCE_SEMANTICS_VERSION
        assert captured["cadence_semantics_version"] != stale_version
