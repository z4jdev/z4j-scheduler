"""Domain dataclasses returned by the brain gRPC client.

These are NOT protobuf messages - the
:mod:`~z4j_scheduler.storage._convert` module translates between
the two at the gRPC boundary so the rest of the codebase never
imports ``scheduler_pb2``. This keeps tests fast (no protobuf
construction) and makes the wire shape an internal detail of the
storage layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from z4j_scheduler.tick._entry import ScheduleEntry

#: Kinds of events the WatchSchedules stream emits.
ScheduleEventKind = Literal["created", "updated", "deleted"]
ScheduleChangeKind = Literal["upsert", "tombstone"]
QuarantineOutcome = Literal[
    "applied",
    "already_applied",
    "stale_control",
    "not_found",
]
CursorTransitionDisposition = Literal[
    "applied",
    "idempotent",
    "slot_resolved_refresh",
    "stale_control_refresh",
    "cadence_semantics_mismatch",
]
FireDisposition = Literal[
    "accepted",
    "retryable_or_ambiguous",
    "terminal_quarantined",
    "slot_resolved_refresh",
    "stale_control_refresh",
    "legacy_upgrade_required",
    "cadence_semantics_mismatch",
]


@dataclass(slots=True, frozen=True)
class ScheduleEvent:
    """A schedule create/update/delete notification from brain.

    Attributes:
        kind: ``"created"`` / ``"updated"`` / ``"deleted"``.
        schedule: The new or updated schedule. Populated for
            ``created`` and ``updated``; ``None`` for ``deleted``.
        deleted_id: The id of the deleted schedule. Populated only
            for ``kind == "deleted"``.
        resume_token: Opaque token to pass back on stream reconnect
            to resume from this point. The cache stashes the latest
            value and the watch consumer presents it on reconnect.
    """

    kind: ScheduleEventKind
    schedule: ScheduleEntry | None
    deleted_id: UUID | None
    resume_token: str


@dataclass(slots=True, frozen=True)
class ScheduleSnapshot:
    """A fully received current-protocol snapshot terminal frame."""

    snapshot_id: UUID
    project_id: UUID | None
    watermark: int
    rows: tuple[ScheduleEntry, ...]
    digest: str


@dataclass(slots=True, frozen=True)
class ScheduleChange:
    """One immutable relevant V2 change-log envelope."""

    kind: ScheduleChangeKind
    revision: int
    project_id: UUID
    schedule: ScheduleEntry | None
    deleted_id: UUID | None


@dataclass(slots=True, frozen=True)
class ScannedThrough:
    """Filtered Watch progress that conveys no schedule absence."""

    revision: int
    server_revision: int


@dataclass(slots=True, frozen=True)
class ScheduleStateObservation:
    """Authenticated row or absence at a transactional global revision."""

    project_id: UUID
    schedule_id: UUID
    observed_revision: int
    schedule: ScheduleEntry | None


@dataclass(slots=True, frozen=True)
class FireResult:
    """Result of a ``FireSchedule`` gRPC call.

    Attributes:
        command_id: Brain-assigned UUID of the Command row created
            for this fire. Populated on success; ``None`` on failure.
        error_code: Machine-readable failure code on error
            (e.g. ``"agent_offline"``). ``None`` on success.
        error_message: Human-readable failure message on error.
        buffered: True if brain stored the fire in its
            ``pending_fires`` buffer because no agent was online and
            the schedule's ``catch_up`` policy supports buffering.
            ``False`` on direct delivery, ``False`` on hard failure.
    """

    command_id: UUID | None
    error_code: str | None
    error_message: str | None
    buffered: bool
    disposition: FireDisposition | None = None
    acceptance_revision: int = 0
    accepted_last_run_at: datetime | None = None
    accepted_next_run_at: datetime | None = None
    live_control_token: UUID | None = None
    live_revision: int = 0
    live_last_run_at: datetime | None = None
    live_next_run_at: datetime | None = None

    @property
    def success(self) -> bool:
        """True if brain accepted the fire (delivered or buffered)."""
        if self.disposition is not None:
            return self.disposition == "accepted"
        return self.command_id is not None or self.buffered


@dataclass(slots=True, frozen=True)
class PingInfo:
    """Brain liveness probe response."""

    brain_version: str
    brain_time: datetime
    scheduler_protocol_epoch: int = 0


@dataclass(slots=True, frozen=True)
class ProtocolCapabilities:
    """Exact independently deployed Brain/scheduler contract tuple."""

    protocol_epoch: int
    fire_response_version: int
    cursor_transition_version: int
    stable_snapshot_version: int
    revision_watch_version: int
    per_id_state_version: int
    quarantine_version: int
    cadence_semantics_version: int
    cadence_runtime_fingerprint: str


@dataclass(slots=True, frozen=True)
class QuarantineResult:
    """Outcome of one token-CAS durable quarantine report."""

    outcome: QuarantineOutcome
    observed_revision: int


@dataclass(slots=True, frozen=True)
class CursorTransitionResult:
    """Typed durable no-work cursor transition and live Brain state."""

    disposition: CursorTransitionDisposition
    committed_revision: int
    committed_last_run_at: datetime | None
    committed_next_run_at: datetime | None
    live_control_token: UUID
    live_revision: int
    live_last_run_at: datetime | None
    live_next_run_at: datetime | None
    error_code: str | None
    error_message: str | None


__all__ = [
    "CursorTransitionDisposition",
    "CursorTransitionResult",
    "FireDisposition",
    "FireResult",
    "PingInfo",
    "ProtocolCapabilities",
    "QuarantineOutcome",
    "QuarantineResult",
    "ScannedThrough",
    "ScheduleChange",
    "ScheduleChangeKind",
    "ScheduleEvent",
    "ScheduleEventKind",
    "ScheduleSnapshot",
    "ScheduleStateObservation",
]
