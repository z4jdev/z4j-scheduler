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

    @property
    def success(self) -> bool:
        """True if brain accepted the fire (delivered or buffered)."""
        return self.command_id is not None or self.buffered


@dataclass(slots=True, frozen=True)
class PingInfo:
    """Brain liveness probe response."""

    brain_version: str
    brain_time: datetime


__all__ = ["FireResult", "PingInfo", "ScheduleEvent", "ScheduleEventKind"]
