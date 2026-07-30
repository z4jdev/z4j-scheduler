"""Tests for :mod:`z4j_scheduler.storage._convert`.

Pure protobuf <-> domain conversions. No I/O, no gRPC. Round-trip
tests pin the contract that future proto changes must preserve.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._convert import (
    datetime_to_ts,
    entry_from_pb,
    entry_to_pb,
    event_from_pb,
    make_ack_request,
    make_fire_request,
    make_quarantine_request,
    parse_fire_response,
    parse_ping_response,
    parse_quarantine_response,
    ts_to_datetime,
    watch_frame_from_pb,
)
from z4j_scheduler.storage._models import ScheduleChange
from z4j_scheduler.tick._entry import ScheduleEntry


class TestTimestampHelpers:
    def test_ts_to_datetime_round_trip(self) -> None:
        original = datetime(2026, 4, 26, 15, 0, 0, 123456, tzinfo=UTC)
        ts = datetime_to_ts(original)
        result = ts_to_datetime(ts)
        # Allow microsecond precision via nanos.
        assert result == original

    def test_ts_to_datetime_unset_returns_none(self) -> None:
        assert ts_to_datetime(Timestamp()) is None

    def test_ts_to_datetime_none_returns_none(self) -> None:
        assert ts_to_datetime(None) is None

    def test_datetime_to_ts_naive_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive"):
            datetime_to_ts(datetime(2026, 4, 26))  # naive

    def test_datetime_to_ts_none_returns_unset(self) -> None:
        ts = datetime_to_ts(None)
        assert ts.seconds == 0 and ts.nanos == 0


class TestEntryFromPb:
    def _make_pb(self, **overrides) -> pb.Schedule:
        defaults = {
            "id": str(uuid4()),
            "project_id": str(uuid4()),
            "kind": "cron",
            "expression": "0 * * * *",
            "timezone": "UTC",
            "is_enabled": True,
            "catch_up": "skip",
        }
        defaults.update(overrides)
        if defaults.get("control_token"):
            defaults.setdefault("definition_digest", "d" * 64)
            defaults.setdefault("cadence_semantics_version", 1)
            defaults.setdefault("cadence_runtime_fingerprint", "f" * 64)
        return pb.Schedule(**defaults)

    def test_basic_cron(self) -> None:
        message = self._make_pb()
        entry = entry_from_pb(message)
        assert entry.kind == "cron"
        assert entry.expression == "0 * * * *"
        assert entry.timezone == "UTC"
        assert entry.is_enabled is True
        assert entry.catch_up == "skip"

    def test_with_last_run_at_used_as_anchor(self) -> None:
        last_run = datetime(2026, 4, 26, 14, 0, tzinfo=UTC)
        message = self._make_pb()
        message.last_run_at.FromDatetime(last_run)
        entry = entry_from_pb(message)
        assert entry.last_fire_at == last_run
        assert entry.anchor_at == last_run

    def test_default_timezone_when_empty(self) -> None:
        message = self._make_pb(timezone="")
        entry = entry_from_pb(message)
        assert entry.timezone == "UTC"

    def test_default_catch_up_when_empty(self) -> None:
        message = self._make_pb(catch_up="")
        entry = entry_from_pb(message)
        assert entry.catch_up == "skip"

    def test_unknown_kind_raises(self) -> None:
        message = self._make_pb(kind="bogus")
        with pytest.raises(ValueError, match="unknown schedule kind"):
            entry_from_pb(message)

    def test_unknown_catch_up_raises(self) -> None:
        message = self._make_pb(catch_up="burn_em_all")
        with pytest.raises(ValueError, match="unknown catch_up"):
            entry_from_pb(message)

    def test_current_control_generation_is_converted(self) -> None:
        token = uuid4()
        message = self._make_pb(
            control_token=str(token),
            schedule_revision=41,
        )
        message.next_run_at.FromDatetime(
            datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        entry = entry_from_pb(message)
        assert entry.control_token == token
        assert entry.schedule_revision == 41

    def test_malformed_control_token_raises(self) -> None:
        message = self._make_pb(
            control_token="not-a-uuid",
            schedule_revision=41,
        )
        with pytest.raises(ValueError, match="badly formed hexadecimal UUID"):
            entry_from_pb(message)

    def test_current_repeating_schedule_requires_brain_cursor(self) -> None:
        message = self._make_pb(
            control_token=str(uuid4()),
            schedule_revision=41,
        )
        with pytest.raises(ValueError, match="authoritative next cursor"):
            entry_from_pb(message)

    def test_current_disabled_schedule_uses_stable_non_clock_anchor(self) -> None:
        message = self._make_pb(
            is_enabled=False,
            control_token=str(uuid4()),
            schedule_revision=41,
        )
        entry = entry_from_pb(message)
        assert entry.anchor_at == datetime(2000, 1, 1, tzinfo=UTC)

    def test_current_exhausted_clocked_schedule_uses_last_cursor(self) -> None:
        last_run = datetime(2026, 4, 26, 14, 0, tzinfo=UTC)
        message = self._make_pb(
            kind="clocked",
            expression="2026-04-26T14:00:00+00:00",
            control_token=str(uuid4()),
            schedule_revision=41,
        )
        message.last_run_at.FromDatetime(last_run)
        entry = entry_from_pb(message)
        assert entry.anchor_at == last_run
        assert entry.next_fire_at is None

    def test_partial_control_shape_raises_during_conversion(self) -> None:
        message = self._make_pb(schedule_revision=41)
        with pytest.raises(ValueError, match="must appear together"):
            entry_from_pb(message)

    def test_current_shape_missing_runtime_semantics_raises(self) -> None:
        message = self._make_pb(
            control_token=str(uuid4()),
            schedule_revision=41,
            cadence_runtime_fingerprint="",
        )
        message.next_run_at.FromDatetime(
            datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="cadence semantics"):
            entry_from_pb(message)


class TestEntryRoundTrip:
    def test_round_trip_preserves_fields(self) -> None:
        sid = uuid4()
        pid = uuid4()
        last = datetime(2026, 4, 26, 14, 0, tzinfo=UTC)
        next_at = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        control_token = uuid4()

        original = ScheduleEntry(
            id=sid,
            project_id=pid,
            kind="interval",
            expression="5m",
            timezone="America/New_York",
            is_enabled=True,
            catch_up="fire_one_missed",
            anchor_at=last,
            last_fire_at=last,
            control_token=control_token,
            schedule_revision=41,
            definition_digest="d" * 64,
            cadence_semantics_version=1,
            cadence_runtime_fingerprint="f" * 64,
        )
        original.next_fire_at = next_at

        pb_msg = entry_to_pb(original)
        reconstituted = entry_from_pb(pb_msg)
        assert reconstituted.id == sid
        assert reconstituted.project_id == pid
        assert reconstituted.kind == "interval"
        assert reconstituted.expression == "5m"
        assert reconstituted.timezone == "America/New_York"
        assert reconstituted.catch_up == "fire_one_missed"
        assert reconstituted.last_fire_at == last
        assert reconstituted.next_fire_at == next_at
        assert reconstituted.control_token == control_token
        assert reconstituted.schedule_revision == 41
        assert reconstituted.definition_digest == "d" * 64
        assert reconstituted.cadence_semantics_version == 1
        assert reconstituted.cadence_runtime_fingerprint == "f" * 64


class TestEventFromPb:
    def _entry_msg(self) -> pb.Schedule:
        return pb.Schedule(
            id=str(uuid4()),
            project_id=str(uuid4()),
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
        )

    # ``resume_token`` is the gRPC stream resume cursor, not a
    # password. ruff's S105/S106 default ruleset matches the literal
    # ``token`` substring; the noqa-with-rationale below silences it.
    def test_created_event(self) -> None:
        ev = pb.ScheduleEvent(
            kind=pb.ScheduleEvent.Kind.CREATED,
            schedule=self._entry_msg(),
            resume_token="token-1",
        )
        result = event_from_pb(ev)
        assert result.kind == "created"
        assert result.schedule is not None
        assert result.deleted_id is None
        assert result.resume_token == "token-1"

    def test_updated_event(self) -> None:
        ev = pb.ScheduleEvent(
            kind=pb.ScheduleEvent.Kind.UPDATED,
            schedule=self._entry_msg(),
            resume_token="token-2",
        )
        result = event_from_pb(ev)
        assert result.kind == "updated"
        assert result.schedule is not None

    def test_deleted_event(self) -> None:
        deleted_id = uuid4()
        ev = pb.ScheduleEvent(
            kind=pb.ScheduleEvent.Kind.DELETED,
            deleted_id=str(deleted_id),
            resume_token="token-3",
        )
        result = event_from_pb(ev)
        assert result.kind == "deleted"
        assert result.schedule is None
        assert result.deleted_id == deleted_id


class TestWatchV2Conversion:
    def _current_schedule(self, *, revision: int) -> pb.Schedule:
        message = pb.Schedule(
            id=str(uuid4()),
            project_id=str(uuid4()),
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            control_token=str(uuid4()),
            schedule_revision=revision,
            definition_digest="d" * 64,
            cadence_semantics_version=1,
            cadence_runtime_fingerprint="f" * 64,
        )
        message.next_run_at.FromDatetime(
            datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        return message

    def test_upsert_frame(self) -> None:
        schedule = self._current_schedule(revision=11)
        result = watch_frame_from_pb(
            pb.ScheduleWatchFrame(
                format_version=1,
                change=pb.ScheduleChange(
                    kind=pb.ScheduleChange.Kind.UPSERT,
                    revision=11,
                    project_id=schedule.project_id,
                    schedule=schedule,
                ),
            ),
        )
        assert result.revision == 11
        assert isinstance(result, ScheduleChange)
        assert result.schedule is not None

    def test_checkpoint_frame(self) -> None:
        result = watch_frame_from_pb(
            pb.ScheduleWatchFrame(
                format_version=1,
                scanned_through=pb.ScannedThrough(
                    scanned_through_revision=13,
                    server_revision=15,
                ),
            ),
        )
        assert result.revision == 13

    def test_zero_change_kind_fails_closed(self) -> None:
        schedule = self._current_schedule(revision=11)
        with pytest.raises(ValueError, match="unspecified"):
            watch_frame_from_pb(
                pb.ScheduleWatchFrame(
                    format_version=1,
                    change=pb.ScheduleChange(
                        kind=pb.ScheduleChange.Kind.CHANGE_UNSPECIFIED,
                        revision=11,
                        project_id=schedule.project_id,
                        schedule=schedule,
                    ),
                ),
            )


class TestFireRequestResponse:
    def test_fire_request_round_trip(self) -> None:
        sid = uuid4()
        fid = uuid4()
        scheduled_for = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        fired_at = datetime(2026, 4, 26, 15, 0, 0, 5000, tzinfo=UTC)

        request = make_fire_request(
            schedule_id=sid,
            fire_id=fid,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
        )
        assert UUID(request.schedule_id) == sid
        assert UUID(request.fire_id) == fid
        assert ts_to_datetime(request.scheduled_for) == scheduled_for

    def test_fire_response_success(self) -> None:
        cid = uuid4()
        message = pb.FireScheduleResponse(command_id=str(cid))
        result = parse_fire_response(message)
        assert result.command_id == cid
        assert result.error_code is None
        assert result.success is True

    def test_fire_response_failure(self) -> None:
        message = pb.FireScheduleResponse(
            error_code="agent_offline",
            error_message="no agent for project",
        )
        result = parse_fire_response(message)
        assert result.command_id is None
        assert result.error_code == "agent_offline"
        assert result.success is False

    def test_fire_response_buffered(self) -> None:
        message = pb.FireScheduleResponse(buffered=True)
        result = parse_fire_response(message)
        assert result.buffered is True
        assert result.success is True


class TestAck:
    def test_make_ack_with_command_id(self) -> None:
        fid = uuid4()
        cid = uuid4()
        request = make_ack_request(
            fire_id=fid,
            command_id=cid,
            status="success",
            new_task_id="abc",
            error=None,
        )
        assert UUID(request.fire_id) == fid
        assert UUID(request.command_id) == cid
        assert request.status == "success"
        assert request.new_task_id == "abc"
        assert request.error == ""  # None -> "" on the wire

    def test_make_ack_without_command_id(self) -> None:
        # Failure path - no command was assigned brain-side.
        request = make_ack_request(
            fire_id=uuid4(),
            command_id=None,
            status="failed",
            new_task_id=None,
            error="agent_offline",
        )
        assert request.command_id == ""
        assert request.error == "agent_offline"


class TestQuarantineWire:
    def test_request_carries_exact_generation(self) -> None:
        project_id = uuid4()
        schedule_id = uuid4()
        token = uuid4()
        request = make_quarantine_request(
            project_id=project_id,
            schedule_id=schedule_id,
            observed_control_token=token,
            reason_code="cadence_definition_invalid",
            detail="bad cron",
            scheduler_protocol_epoch=1,
        )
        assert UUID(request.project_id) == project_id
        assert UUID(request.schedule_id) == schedule_id
        assert UUID(request.observed_control_token) == token

    def test_nonzero_outcome_is_required(self) -> None:
        with pytest.raises(ValueError, match="unspecified"):
            parse_quarantine_response(
                pb.QuarantineScheduleResponse(
                    outcome=pb.QuarantineOutcome.QUARANTINE_OUTCOME_UNSPECIFIED,
                    observed_revision=11,
                ),
            )

    def test_applied_outcome_parses(self) -> None:
        result = parse_quarantine_response(
            pb.QuarantineScheduleResponse(
                outcome=pb.QuarantineOutcome.QUARANTINE_APPLIED,
                observed_revision=11,
            ),
        )
        assert result.outcome == "applied"
        assert result.observed_revision == 11


class TestPing:
    def test_parse_ping_response(self) -> None:
        ts = Timestamp()
        ts.FromDatetime(datetime(2026, 4, 26, 15, 0, tzinfo=UTC))
        message = pb.PingResponse(
            brain_version="1.2.0",
            brain_time=ts,
            scheduler_protocol_epoch=1,
        )
        result = parse_ping_response(message)
        assert result.brain_version == "1.2.0"
        assert result.brain_time == datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        assert result.scheduler_protocol_epoch == 1
