"""Durable no-work cursor wire carries complete nullable assertions."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import (
    make_advance_cursor_request,
    parse_advance_cursor_response,
    ts_to_datetime,
)


def test_request_preserves_nullable_presence() -> None:
    prior_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
    skipped = prior_next
    prepared_next = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
    request = make_advance_cursor_request(
        project_id=uuid4(),
        schedule_id=uuid4(),
        observed_control_token=uuid4(),
        definition_digest="d" * 64,
        expected_schedule_revision=40,
        expected_last_run_at=None,
        expected_next_run_at=prior_next,
        skipped_through=skipped,
        prepared_next_run_at=prepared_next,
        scheduler_protocol_epoch=1,
        cadence_semantics_version=1,
        cadence_runtime_fingerprint="f" * 64,
    )
    assert not request.HasField("expected_last_run_at")
    assert request.HasField("expected_next_run_at")
    assert ts_to_datetime(request.expected_next_run_at) == prior_next
    assert ts_to_datetime(request.prepared_next_run_at) == prepared_next


def test_success_response_requires_nonzero_disposition_and_revision() -> None:
    with pytest.raises(ValueError, match="unspecified"):
        parse_advance_cursor_response(
            pb.AdvanceScheduleCursorResponse(
                live_control_token=str(uuid4()),
                live_revision=41,
            ),
        )
    with pytest.raises(ValueError, match="committed revision"):
        parse_advance_cursor_response(
            pb.AdvanceScheduleCursorResponse(
                disposition=pb.CursorTransitionDisposition.CURSOR_APPLIED,
                live_control_token=str(uuid4()),
                live_revision=41,
            ),
        )


def test_applied_response_parses_complete_cursors() -> None:
    token = uuid4()
    response = pb.AdvanceScheduleCursorResponse(
        disposition=pb.CursorTransitionDisposition.CURSOR_APPLIED,
        committed_revision=41,
        live_control_token=str(token),
        live_revision=41,
    )
    response.committed_last_run_at.FromDatetime(
        datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
    )
    response.committed_next_run_at.FromDatetime(
        datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
    )
    response.live_last_run_at.CopyFrom(response.committed_last_run_at)
    response.live_next_run_at.CopyFrom(response.committed_next_run_at)

    result = parse_advance_cursor_response(response)

    assert result.disposition == "applied"
    assert result.committed_revision == 41
    assert result.live_control_token == token
    assert result.committed_next_run_at == datetime(
        2026,
        4,
        26,
        16,
        0,
        tzinfo=UTC,
    )
