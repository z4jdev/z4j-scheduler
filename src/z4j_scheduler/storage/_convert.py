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
    FireResult,
    PingInfo,
    ScheduleEvent,
    ScheduleEventKind,
)
from z4j_scheduler.tick._entry import ScheduleEntry

if TYPE_CHECKING:  # pragma: no cover
    pass


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


_VALID_KINDS: frozenset[str] = frozenset({"cron", "interval", "one_shot"})
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
    if message.catch_up and message.catch_up not in _VALID_CATCH_UP:
        raise ValueError(
            f"unknown catch_up policy {message.catch_up!r} from brain "
            f"(expected one of {sorted(_VALID_CATCH_UP)})",
        )

    last_run_at = ts_to_datetime(message.last_run_at)
    # Anchor for interval first-fire alignment.
    #
    # Selection rules:
    #   1. ``last_run_at`` if the schedule has fired before -
    #      defines the cadence going forward.
    #   2. ``next_run_at`` if brain pre-computed one - defines a
    #      future fire boundary the operator may have aligned to.
    #   3. ``datetime.now(UTC)`` as a last resort for fresh
    #      schedules with no fire history. This makes a brand-new
    #      interval schedule fire at "next boundary AFTER now",
    #      not "next boundary after the year-2000 sentinel" - the
    #      old far-past sentinel produced a hot fire loop because
    #      interval.next_fire(last_fire_at + interval) stayed in
    #      the past forever, with the schedule re-evaluating as
    #      "due" on every tick. Pinned by
    #      ``tests/unit/test_convert_anchor_at.py``.
    #
    # The ``_DEFAULT_ANCHOR`` constant is kept for callers that
    # explicitly want the year-2000 sentinel (none today, but the
    # symbol stays exported for back-compat).
    next_run_at = ts_to_datetime(message.next_run_at)
    anchor_at = last_run_at or next_run_at or _utcnow()

    entry = ScheduleEntry(
        id=UUID(message.id),
        project_id=UUID(message.project_id),
        kind=message.kind,  # type: ignore[arg-type]
        expression=message.expression,
        timezone=message.timezone or "UTC",
        is_enabled=message.is_enabled,
        catch_up=(message.catch_up or "skip"),  # type: ignore[arg-type]
        anchor_at=anchor_at,
        last_fire_at=last_run_at,
        name=message.name or "",
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


# ---------------------------------------------------------------------------
# Fire request / response
# ---------------------------------------------------------------------------


def make_fire_request(
    *,
    schedule_id: UUID,
    fire_id: UUID,
    scheduled_for: datetime,
    fired_at: datetime,
) -> pb.FireScheduleRequest:
    """Build a protobuf ``FireScheduleRequest`` from typed inputs."""
    return pb.FireScheduleRequest(
        schedule_id=str(schedule_id),
        fire_id=str(fire_id),
        scheduled_for=datetime_to_ts(scheduled_for),
        fired_at=datetime_to_ts(fired_at),
    )


def parse_fire_response(message: pb.FireScheduleResponse) -> FireResult:
    """Convert a protobuf ``FireScheduleResponse`` to :class:`FireResult`."""
    return FireResult(
        command_id=UUID(message.command_id) if message.command_id else None,
        error_code=message.error_code or None,
        error_message=message.error_message or None,
        buffered=bool(message.buffered),
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
        brain_time=ts_to_datetime(message.brain_time)
        or datetime.fromtimestamp(0, tz=UTC),
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
    "datetime_to_ts",
    "entry_from_pb",
    "entry_to_pb",
    "event_from_pb",
    "make_ack_request",
    "make_fire_request",
    "parse_fire_response",
    "parse_ping_response",
    "ts_to_datetime",
]
