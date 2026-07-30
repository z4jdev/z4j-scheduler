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
        cadence_semantics_version=1,
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
