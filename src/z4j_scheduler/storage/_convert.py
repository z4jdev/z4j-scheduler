"""Translate between protobuf wire messages and our domain dataclasses.

This module is the only place in the codebase that imports
``scheduler_pb2``. Everything else operates on
:class:`~z4j_scheduler.tick._entry.ScheduleEntry`,
:class:`~z4j_scheduler.storage._models.ScheduleEvent`, etc.

Why a dedicated conversion module:

1. **Single boundary** - one place to fix when the proto changes
2. **Testable in isolation** - no gRPC, no network, just message
   construction and field copying
3. **Type-safe at the boundary** - we control which fields flow
   from protobuf into the rest of the system

Field mapping rules (per ``docs/SCHEDULER.md §10`` + §11):

- protobuf ``string`` UUIDs -> Python ``UUID`` objects
- protobuf ``Timestamp`` -> Python ``datetime`` (UTC, tz-aware)
- protobuf ``bytes args_json`` / ``kwargs_json`` -> raw bytes are
  passed through as-is to brain; the scheduler does not interpret
  them. The tick engine never reads them.
- Empty / unset fields use Python ``None`` rather than empty string
  so consumers do not have to special-case sentinel values
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from google.protobuf.timestamp_pb2 import Timestamp

from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.storage._models import (
    CursorTransitionResult,
    FireResult,
    PingInfo,
    ProtocolCapabilities,
    QuarantineResult,
    ScannedThrough,
    ScheduleChange,
    ScheduleEvent,
    ScheduleEventKind,
    ScheduleStateObservation,
)
from z4j_scheduler.tick._entry import ScheduleEntry

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.tick._prepared import PreparedFire


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------


def ts_to_datetime(ts: Timestamp | None) -> datetime | None:
    """Convert a protobuf Timestamp to a tz-aware UTC datetime.

    Treats the zero/unset Timestamp (seconds=0, nanos=0) as ``None`` -
    the proto3 default for an unset Timestamp - so callers do not have
    to disambiguate between "1970-01-01" and "absent".
    """
    if ts is None or (ts.seconds == 0 and ts.nanos == 0):
        return None
    return datetime.fromtimestamp(
        ts.seconds + ts.nanos / 1_000_000_000,
        tz=UTC,
    )


def datetime_to_ts(dt: datetime | None) -> Timestamp:
    """Convert a tz-aware datetime to a protobuf Timestamp.

    Naive datetimes are rejected because they would silently encode
    as the local-time interpretation, which is wrong for any
    cross-process protocol.

    A ``None`` input maps to the unset Timestamp (zero seconds + nanos).
    """
    out = Timestamp()
    if dt is None:
        return out
    if dt.tzinfo is None:
        raise ValueError(
            "datetime_to_ts requires a tz-aware datetime; got naive",
        )
    out.FromDatetime(dt.astimezone(UTC))
    return out


# ---------------------------------------------------------------------------
# Schedule entry
# ---------------------------------------------------------------------------


_VALID_KINDS: frozenset[str] = frozenset(
    {"cron", "interval", "clocked", "solar", "one_shot"},
)
# Map any legacy alias seen in older protobuf payloads to the
# canonical name from z4j_core.models.event.ScheduleKind. The brain
# emits ``clocked`` since the 1.5 vocab alignment; ``one_shot``
# remains accepted for backwards compatibility with any pre-1.5
# scheduler-protocol fixtures or test rigs that have not yet
# regenerated their protobuf payloads.
_KIND_ALIASES: dict[str, str] = {"one_shot": "clocked"}
_VALID_CATCH_UP: frozenset[str] = frozenset(
    {"skip", "fire_one_missed", "fire_all_missed"},
)


def entry_from_pb(message: pb.Schedule) -> ScheduleEntry:
    """Convert a protobuf ``Schedule`` into a :class:`ScheduleEntry`.

    Raises :class:`ValueError` if ``kind`` or ``catch_up`` is unknown -
    these are validated by brain before sending, so an unknown value
    here means a brain/scheduler version skew that should fail loudly.
    """
    if message.kind not in _VALID_KINDS:
        raise ValueError(
            f"unknown schedule kind {message.kind!r} from brain "
            f"(expected one of {sorted(_VALID_KINDS)})",
        )
    # Normalise legacy aliases so the rest of the scheduler operates
    # on the canonical name from z4j_core.models.event.ScheduleKind.
    kind = _KIND_ALIASES.get(message.kind, message.kind)
    if message.catch_up and message.catch_up not in _VALID_CATCH_UP:
        raise ValueError(
            f"unknown catch_up policy {message.catch_up!r} from brain "
            f"(expected one of {sorted(_VALID_CATCH_UP)})",
        )

    control_token = UUID(message.control_token) if message.control_token else None
    schedule_revision = int(message.schedule_revision)
    current_protocol = control_token is not None and schedule_revision > 0
    current_semantics = (
        bool(message.definition_digest)
        and message.cadence_semantics_version > 0
        and bool(message.cadence_runtime_fingerprint)
    )
    legacy_semantics = (
        not message.definition_digest
        and message.cadence_semantics_version == 0
        and not message.cadence_runtime_fingerprint
    )
    current_protocol = current_protocol and current_semantics
    legacy_protocol = control_token is None and schedule_revision == 0 and legacy_semantics
    if not (current_protocol or legacy_protocol):
        raise ValueError(
            "current schedule control, revision, definition, and cadence "
            "semantics fields must appear together",
        )

    last_run_at = ts_to_datetime(message.last_run_at)
    # Anchor for interval first-fire alignment in explicitly negotiated legacy
    # mode. Current protocol requires Brain's canonical cursor below.
    #
    # Selection rules:
    #   1. ``last_run_at`` if the schedule has fired before -
    #      defines the cadence going forward.
    #   2. ``next_run_at`` if brain pre-computed one - defines a
    #      future fire boundary the operator may have aligned to.
    #   3. ``datetime.now(UTC)`` as a last resort for legacy fresh
    #      schedules with no fire history. This makes a brand-new
    #      interval schedule fire at "next boundary AFTER now",
    #      not "next boundary after the year-2000 sentinel" - the
    #      old far-past sentinel produced a hot fire loop because
    #      interval.next_fire(last_fire_at + interval) stayed in
    #      the past forever, with the schedule re-evaluating as
    #      "due" on every tick. Pinned by
    #      ``tests/unit/test_convert_anchor_at.py``.
    #
    # Current disabled rows use ``_DEFAULT_ANCHOR`` only as an inert dataclass
    # placeholder; it never participates in current-protocol cadence planning.
    next_run_at = ts_to_datetime(message.next_run_at)
    if current_protocol:
        is_exhausted_one_shot = kind == "clocked" and last_run_at is not None
        if message.is_enabled and next_run_at is None and not is_exhausted_one_shot:
            raise ValueError(
                "enabled current-protocol schedule lacks authoritative next cursor",
            )
        # Disabled current rows may park a NULL cursor. They never tick, so the
        # stable sentinel is only a dataclass placeholder; it cannot create
        # process-clock-dependent work. An exhausted one-shot uses last_run_at.
        anchor_at = last_run_at or next_run_at or _DEFAULT_ANCHOR
    else:
        anchor_at = last_run_at or next_run_at or _utcnow()

    entry = ScheduleEntry(
        id=UUID(message.id),
        project_id=UUID(message.project_id),
        kind=kind,  # type: ignore[arg-type]
        expression=message.expression,
        timezone=message.timezone or "UTC",
        is_enabled=message.is_enabled,
        catch_up=(message.catch_up or "skip"),  # type: ignore[arg-type]
        anchor_at=anchor_at,
        last_fire_at=last_run_at,
        name=message.name or "",
        engine=message.engine or "",
        control_token=control_token,
        schedule_revision=schedule_revision,
        definition_digest=message.definition_digest,
        cadence_semantics_version=int(message.cadence_semantics_version),
        cadence_runtime_fingerprint=message.cadence_runtime_fingerprint,
    )
    # The protobuf may carry a precomputed next_run_at; honour it as
    # an initial value so the tick engine doesn't recompute on first
    # iteration. The engine WILL recompute on every cache miss anyway
    # so this is purely a startup-warmth optimisation.
    entry.next_fire_at = ts_to_datetime(message.next_run_at)
    return entry


def entry_to_pb(entry: ScheduleEntry) -> pb.Schedule:
    """Convert a :class:`ScheduleEntry` back to a protobuf ``Schedule``.

    Used primarily by tests for round-trip verification. Production
    code only consumes from brain; the brain owns canonical writes.
    """
    return pb.Schedule(
        id=str(entry.id),
        project_id=str(entry.project_id),
        kind=entry.kind,
        expression=entry.expression,
        timezone=entry.timezone,
        is_enabled=entry.is_enabled,
        catch_up=entry.catch_up,
        last_run_at=datetime_to_ts(entry.last_fire_at),
        next_run_at=datetime_to_ts(entry.next_fire_at),
        control_token=str(entry.control_token) if entry.control_token is not None else "",
        schedule_revision=entry.schedule_revision,
        definition_digest=entry.definition_digest,
        cadence_semantics_version=entry.cadence_semantics_version,
        cadence_runtime_fingerprint=entry.cadence_runtime_fingerprint,
    )


# ---------------------------------------------------------------------------
# Schedule events
# ---------------------------------------------------------------------------


_PB_EVENT_KIND_TO_DOMAIN: dict[int, ScheduleEventKind] = {
    pb.ScheduleEvent.Kind.CREATED: "created",
    pb.ScheduleEvent.Kind.UPDATED: "updated",
    pb.ScheduleEvent.Kind.DELETED: "deleted",
}


def event_from_pb(message: pb.ScheduleEvent) -> ScheduleEvent:
    """Convert a protobuf ``ScheduleEvent`` to the domain dataclass."""
    try:
        kind = _PB_EVENT_KIND_TO_DOMAIN[message.kind]
    except KeyError as exc:
        raise ValueError(
            f"unknown ScheduleEvent.Kind value {message.kind!r}",
        ) from exc

    if kind == "deleted":
        return ScheduleEvent(
            kind="deleted",
            schedule=None,
            deleted_id=UUID(message.deleted_id),
            resume_token=message.resume_token,
        )
    # CREATED / UPDATED carry the full Schedule.
    return ScheduleEvent(
        kind=kind,
        schedule=entry_from_pb(message.schedule),
        deleted_id=None,
        resume_token=message.resume_token,
    )


def watch_frame_from_pb(
    message: pb.ScheduleWatchFrame,
) -> ScheduleChange | ScannedThrough:
    """Decode one versioned V2 Watch frame and reject empty enum success."""

    from z4j_scheduler.storage._watch_v2 import WATCH_FORMAT_VERSION

    if message.format_version != WATCH_FORMAT_VERSION:
        raise ValueError("unsupported schedule Watch framing version")
    kind = message.WhichOneof("frame")
    if kind == "scanned_through":
        checkpoint = message.scanned_through
        return ScannedThrough(
            revision=int(checkpoint.scanned_through_revision),
            server_revision=int(checkpoint.server_revision),
        )
    if kind != "change":
        raise ValueError("schedule Watch frame has no change or checkpoint")
    change = message.change
    if change.kind == pb.ScheduleChange.Kind.UPSERT:
        schedule = entry_from_pb(change.schedule)
        return ScheduleChange(
            kind="upsert",
            revision=int(change.revision),
            project_id=UUID(change.project_id),
            schedule=schedule,
            deleted_id=None,
        )
    if change.kind == pb.ScheduleChange.Kind.TOMBSTONE:
        return ScheduleChange(
            kind="tombstone",
            revision=int(change.revision),
            project_id=UUID(change.project_id),
            schedule=None,
            deleted_id=UUID(change.deleted_id),
        )
    raise ValueError("schedule Watch change has unspecified or unknown kind")


def schedule_state_from_pb(
    message: pb.GetScheduleStateResponse,
    *,
    expected_project_id: UUID,
    expected_schedule_id: UUID,
    minimum_observed_revision: int,
) -> ScheduleStateObservation:
    """Decode an explicit revision-bounded row/absence state response."""

    observed_revision = int(message.observed_revision)
    if observed_revision < minimum_observed_revision or observed_revision <= 0:
        raise ValueError("schedule state response did not meet minimum revision")
    kind = message.WhichOneof("state")
    if kind == "schedule":
        schedule = entry_from_pb(message.schedule)
        if schedule.project_id != expected_project_id or schedule.id != expected_schedule_id:
            raise ValueError("schedule state row escaped the requested identity")
        return ScheduleStateObservation(
            project_id=expected_project_id,
            schedule_id=expected_schedule_id,
            observed_revision=observed_revision,
            schedule=schedule,
        )
    if kind == "absence":
        try:
            project_id = UUID(message.absence.project_id)
            schedule_id = UUID(message.absence.schedule_id)
        except ValueError as exc:
            raise ValueError("schedule absence identity is not a UUID") from exc
        if project_id != expected_project_id or schedule_id != expected_schedule_id:
            raise ValueError("schedule absence escaped the requested identity")
        return ScheduleStateObservation(
            project_id=project_id,
            schedule_id=schedule_id,
            observed_revision=observed_revision,
            schedule=None,
        )
    raise ValueError("schedule state response omitted row and explicit absence")


_QUARANTINE_OUTCOMES = {
    pb.QuarantineOutcome.QUARANTINE_APPLIED: "applied",
    pb.QuarantineOutcome.QUARANTINE_ALREADY_APPLIED: "already_applied",
    pb.QuarantineOutcome.QUARANTINE_STALE_CONTROL: "stale_control",
    pb.QuarantineOutcome.QUARANTINE_NOT_FOUND: "not_found",
}


def make_quarantine_request(
    *,
    project_id: UUID,
    schedule_id: UUID,
    observed_control_token: UUID,
    reason_code: str,
    detail: str,
    scheduler_protocol_epoch: int,
) -> pb.QuarantineScheduleRequest:
    """Encode one bounded current-generation quarantine report."""

    if scheduler_protocol_epoch <= 0:
        raise ValueError("quarantine report requires current protocol epoch")
    code = reason_code.strip()
    if not code or len(code) > 64:
        raise ValueError("quarantine reason code must contain 1..64 characters")
    if len(detail) > 500:
        raise ValueError("quarantine detail exceeds 500 characters")
    return pb.QuarantineScheduleRequest(
        project_id=str(project_id),
        schedule_id=str(schedule_id),
        observed_control_token=str(observed_control_token),
        reason_code=code,
        detail=detail,
        scheduler_protocol_epoch=scheduler_protocol_epoch,
    )


def parse_quarantine_response(
    message: pb.QuarantineScheduleResponse,
) -> QuarantineResult:
    """Decode a nonzero typed outcome and transactional observation revision."""

    try:
        outcome = _QUARANTINE_OUTCOMES[message.outcome]
    except KeyError as exc:
        raise ValueError("quarantine response has unspecified or unknown outcome") from exc
    if message.observed_revision <= 0:
        raise ValueError("quarantine response lacks a positive observed revision")
    return QuarantineResult(
        outcome=outcome,  # type: ignore[arg-type]
        observed_revision=int(message.observed_revision),
    )


_CURSOR_DISPOSITIONS = {
    pb.CursorTransitionDisposition.CURSOR_APPLIED: "applied",
    pb.CursorTransitionDisposition.CURSOR_IDEMPOTENT: "idempotent",
    pb.CursorTransitionDisposition.CURSOR_SLOT_RESOLVED_REFRESH: ("slot_resolved_refresh"),
    pb.CursorTransitionDisposition.CURSOR_STALE_CONTROL_REFRESH: ("stale_control_refresh"),
    pb.CursorTransitionDisposition.CURSOR_CADENCE_SEMANTICS_MISMATCH: (
        "cadence_semantics_mismatch"
    ),
}


def make_advance_cursor_request(
    *,
    project_id: UUID,
    schedule_id: UUID,
    observed_control_token: UUID,
    definition_digest: str,
    expected_schedule_revision: int,
    expected_last_run_at: datetime | None,
    expected_next_run_at: datetime | None,
    skipped_through: datetime,
    prepared_next_run_at: datetime | None,
    scheduler_protocol_epoch: int,
    cadence_semantics_version: int,
    cadence_runtime_fingerprint: str,
) -> pb.AdvanceScheduleCursorRequest:
    """Encode the complete immutable zero-work cursor assertion."""

    if (
        expected_schedule_revision <= 0
        or scheduler_protocol_epoch <= 0
        or cadence_semantics_version <= 0
        or not definition_digest
        or not cadence_runtime_fingerprint
    ):
        raise ValueError("advance cursor request lacks current protocol authority")
    request = pb.AdvanceScheduleCursorRequest(
        project_id=str(project_id),
        schedule_id=str(schedule_id),
        observed_control_token=str(observed_control_token),
        definition_digest=definition_digest,
        expected_schedule_revision=expected_schedule_revision,
        skipped_through=datetime_to_ts(skipped_through),
        scheduler_protocol_epoch=scheduler_protocol_epoch,
        cadence_semantics_version=cadence_semantics_version,
        cadence_runtime_fingerprint=cadence_runtime_fingerprint,
    )
    if expected_last_run_at is not None:
        request.expected_last_run_at.CopyFrom(
            datetime_to_ts(expected_last_run_at),
        )
    if expected_next_run_at is not None:
        request.expected_next_run_at.CopyFrom(
            datetime_to_ts(expected_next_run_at),
        )
    if prepared_next_run_at is not None:
        request.prepared_next_run_at.CopyFrom(
            datetime_to_ts(prepared_next_run_at),
        )
    return request


def parse_advance_cursor_response(
    message: pb.AdvanceScheduleCursorResponse,
) -> CursorTransitionResult:
    """Decode nonzero disposition plus the complete live Brain cursor."""

    try:
        disposition = _CURSOR_DISPOSITIONS[message.disposition]
    except KeyError as exc:
        raise ValueError(
            "advance cursor response has unspecified or unknown disposition",
        ) from exc
    if not message.live_control_token or message.live_revision <= 0:
        raise ValueError("advance cursor response lacks live control state")
    try:
        live_control_token = UUID(message.live_control_token)
    except ValueError as exc:
        raise ValueError("advance cursor response token is not a UUID") from exc
    committed_revision = int(message.committed_revision)
    if disposition in {"applied", "idempotent"} and committed_revision <= 0:
        raise ValueError("successful cursor response lacks committed revision")
    return CursorTransitionResult(
        disposition=disposition,  # type: ignore[arg-type]
        committed_revision=committed_revision,
        committed_last_run_at=(
            ts_to_datetime(message.committed_last_run_at)
            if message.HasField("committed_last_run_at")
            else None
        ),
        committed_next_run_at=(
            ts_to_datetime(message.committed_next_run_at)
            if message.HasField("committed_next_run_at")
            else None
        ),
        live_control_token=live_control_token,
        live_revision=int(message.live_revision),
        live_last_run_at=(
            ts_to_datetime(message.live_last_run_at)
            if message.HasField("live_last_run_at")
            else None
        ),
        live_next_run_at=(
            ts_to_datetime(message.live_next_run_at)
            if message.HasField("live_next_run_at")
            else None
        ),
        error_code=message.error_code or None,
        error_message=message.error_message or None,
    )


# ---------------------------------------------------------------------------
# Fire request / response
# ---------------------------------------------------------------------------


def make_fire_request(
    *,
    schedule_id: UUID,
    fire_id: UUID,
    scheduled_for: datetime,
    fired_at: datetime,
    triggered_by_user_id: str = "",
    schedule_entry: ScheduleEntry | None = None,
    prepared_fire: PreparedFire | None = None,
    scheduler_protocol_epoch: int = 0,
) -> pb.FireScheduleRequest:
    """Build a protobuf ``FireScheduleRequest`` from typed inputs.

    ``triggered_by_user_id`` is set only for operator-triggered fires
    (TriggerSchedule); empty for scheduler-driven cadence fires.
    """
    request = pb.FireScheduleRequest(
        schedule_id=str(schedule_id),
        fire_id=str(fire_id),
        scheduled_for=datetime_to_ts(scheduled_for),
        fired_at=datetime_to_ts(fired_at),
        triggered_by_user_id=triggered_by_user_id,
    )
    if schedule_entry is None and prepared_fire is None:
        return request
    if (
        schedule_entry is None
        or prepared_fire is None
        or schedule_entry.control_token is None
        or schedule_entry.schedule_revision <= 0
        or scheduler_protocol_epoch <= 0
        or not schedule_entry.definition_digest
        or schedule_entry.cadence_semantics_version <= 0
        or not schedule_entry.cadence_runtime_fingerprint
    ):
        raise ValueError("current fire request lacks complete protocol authority")
    if prepared_fire.scheduled_for != scheduled_for:
        raise ValueError("prepared fire slot does not match scheduled_for")
    request.scheduler_protocol_epoch = scheduler_protocol_epoch
    request.observed_control_token = str(schedule_entry.control_token)
    request.definition_digest = schedule_entry.definition_digest
    request.expected_schedule_revision = schedule_entry.schedule_revision
    # Submit what THIS PROCESS computes, not what the row was stamped with.
    #
    # The Brain's check is `submitted != <what the Brain computes>`, i.e. "does
    # the submitter compute cadence the way I do". That is an agreement check
    # between two processes. Echoing schedule_entry.* -- which arrives from the
    # Brain's own watch stream and is therefore the ROW's stored value -- turned
    # it into a staleness check on the row, comparing the Brain against itself
    # and telling us nothing about the scheduler.
    #
    # It also made the closure unbumpable. Nothing re-stamps the column
    # (create_current and the external writers are its only writers), so any
    # change to the cadence dependencies -- or to the running Python version,
    # which is in the fingerprint payload -- left every pre-existing row
    # carrying the old value, every fire refused as cadence_semantics_mismatch,
    # and the schedule locally quarantined. Cursor advance refused on the same
    # test, so they could not even skip forward: a total scheduling stall on
    # upgrade.
    #
    # With this, a scheduler genuinely running different cadence code is still
    # refused, which is the fail-closed behaviour the check exists for.
    from z4j_scheduler.tick.cadence import (
        CADENCE_SEMANTICS_VERSION,
        cadence_runtime_fingerprint,
    )

    request.cadence_semantics_version = CADENCE_SEMANTICS_VERSION
    request.cadence_runtime_fingerprint = cadence_runtime_fingerprint()
    if schedule_entry.last_fire_at is not None:
        request.expected_last_run_at.CopyFrom(
            datetime_to_ts(schedule_entry.last_fire_at),
        )
    if schedule_entry.next_fire_at is not None:
        request.expected_next_run_at.CopyFrom(
            datetime_to_ts(schedule_entry.next_fire_at),
        )
    if prepared_fire.next_run_at is not None:
        request.prepared_next_run_at.CopyFrom(
            datetime_to_ts(prepared_fire.next_run_at),
        )
    return request


_FIRE_DISPOSITIONS = {
    pb.FireDisposition.FIRE_ACCEPTED: "accepted",
    pb.FireDisposition.FIRE_RETRYABLE_OR_AMBIGUOUS: "retryable_or_ambiguous",
    pb.FireDisposition.FIRE_TERMINAL_QUARANTINED: "terminal_quarantined",
    pb.FireDisposition.FIRE_SLOT_RESOLVED_REFRESH: "slot_resolved_refresh",
    pb.FireDisposition.FIRE_STALE_CONTROL_REFRESH: "stale_control_refresh",
    pb.FireDisposition.FIRE_LEGACY_UPGRADE_REQUIRED: "legacy_upgrade_required",
    pb.FireDisposition.FIRE_CADENCE_SEMANTICS_MISMATCH: ("cadence_semantics_mismatch"),
}


def parse_fire_response(message: pb.FireScheduleResponse) -> FireResult:
    """Convert a protobuf ``FireScheduleResponse`` to :class:`FireResult`."""
    disposition = None
    if message.disposition:
        try:
            disposition = _FIRE_DISPOSITIONS[message.disposition]
        except KeyError as exc:
            raise ValueError("fire response has unknown disposition") from exc
    live_control_token = None
    if message.live_control_token:
        try:
            live_control_token = UUID(message.live_control_token)
        except ValueError as exc:
            raise ValueError("fire response live token is not a UUID") from exc
    return FireResult(
        command_id=UUID(message.command_id) if message.command_id else None,
        error_code=message.error_code or None,
        error_message=message.error_message or None,
        buffered=bool(message.buffered),
        disposition=disposition,  # type: ignore[arg-type]
        acceptance_revision=int(message.acceptance_revision),
        accepted_last_run_at=(
            ts_to_datetime(message.accepted_last_run_at)
            if message.HasField("accepted_last_run_at")
            else None
        ),
        accepted_next_run_at=(
            ts_to_datetime(message.accepted_next_run_at)
            if message.HasField("accepted_next_run_at")
            else None
        ),
        live_control_token=live_control_token,
        live_revision=int(message.live_revision),
        live_last_run_at=(
            ts_to_datetime(message.live_last_run_at)
            if message.HasField("live_last_run_at")
            else None
        ),
        live_next_run_at=(
            ts_to_datetime(message.live_next_run_at)
            if message.HasField("live_next_run_at")
            else None
        ),
    )


# ---------------------------------------------------------------------------
# Acknowledge
# ---------------------------------------------------------------------------


def make_ack_request(
    *,
    fire_id: UUID,
    command_id: UUID | None,
    status: str,
    new_task_id: str | None,
    error: str | None,
) -> pb.AcknowledgeFireResultRequest:
    """Build a protobuf ``AcknowledgeFireResultRequest``.

    ``command_id`` is None when the fire failed before brain assigned
    one (e.g. agent_offline) - we still acknowledge so the brain's
    schedule row gets ``last_run_at`` updated either way.
    """
    return pb.AcknowledgeFireResultRequest(
        fire_id=str(fire_id),
        command_id=str(command_id) if command_id is not None else "",
        status=status,
        new_task_id=new_task_id or "",
        error=error or "",
    )


# ---------------------------------------------------------------------------
# Ping
# ---------------------------------------------------------------------------


def parse_ping_response(message: pb.PingResponse) -> PingInfo:
    """Convert a ``PingResponse`` to :class:`PingInfo`."""
    return PingInfo(
        brain_version=message.brain_version,
        brain_time=ts_to_datetime(message.brain_time) or datetime.fromtimestamp(0, tz=UTC),
        scheduler_protocol_epoch=int(message.scheduler_protocol_epoch),
    )


def capabilities_to_pb(
    capabilities: ProtocolCapabilities,
) -> pb.SchedulerProtocolCapabilities:
    """Encode one offered/selected protocol tuple."""

    return pb.SchedulerProtocolCapabilities(
        protocol_epoch=capabilities.protocol_epoch,
        fire_response_version=capabilities.fire_response_version,
        cursor_transition_version=capabilities.cursor_transition_version,
        stable_snapshot_version=capabilities.stable_snapshot_version,
        revision_watch_version=capabilities.revision_watch_version,
        per_id_state_version=capabilities.per_id_state_version,
        quarantine_version=capabilities.quarantine_version,
        cadence_semantics_version=capabilities.cadence_semantics_version,
        cadence_runtime_fingerprint=capabilities.cadence_runtime_fingerprint,
    )


def capabilities_from_pb(
    message: pb.SchedulerProtocolCapabilities,
) -> ProtocolCapabilities:
    """Decode a protocol tuple without treating defaults as success."""

    return ProtocolCapabilities(
        protocol_epoch=int(message.protocol_epoch),
        fire_response_version=int(message.fire_response_version),
        cursor_transition_version=int(message.cursor_transition_version),
        stable_snapshot_version=int(message.stable_snapshot_version),
        revision_watch_version=int(message.revision_watch_version),
        per_id_state_version=int(message.per_id_state_version),
        quarantine_version=int(message.quarantine_version),
        cadence_semantics_version=int(message.cadence_semantics_version),
        cadence_runtime_fingerprint=message.cadence_runtime_fingerprint,
    )


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

#: Sentinel anchor for interval schedules whose brain payload didn't
#: carry a last_run_at. The tick engine recomputes on first cache
#: read, so the precise value is not load-bearing - only that it is
#: tz-aware so the interval module doesn't reject it as naive.
_DEFAULT_ANCHOR = datetime(2000, 1, 1, tzinfo=UTC)


def _utcnow() -> datetime:
    """Return current UTC time. Indirected for test injection."""
    return datetime.now(UTC)


__all__ = [
    "capabilities_from_pb",
    "capabilities_to_pb",
    "datetime_to_ts",
    "entry_from_pb",
    "entry_to_pb",
    "event_from_pb",
    "make_ack_request",
    "make_advance_cursor_request",
    "make_fire_request",
    "make_quarantine_request",
    "parse_advance_cursor_response",
    "parse_fire_response",
    "parse_ping_response",
    "parse_quarantine_response",
    "schedule_state_from_pb",
    "ts_to_datetime",
    "watch_frame_from_pb",
]
