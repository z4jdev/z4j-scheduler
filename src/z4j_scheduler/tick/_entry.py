"""Slim schedule shape used internally by the tick engine.

The tick engine does NOT need every field of
:class:`z4j_core.models.Schedule` - args, kwargs, queue, task_name
all flow brain-side via ``FireSchedule(schedule_id)`` and brain
looks them up. The engine only needs the timing-related fields.

Keeping a slim internal representation:

- Decouples the engine from z4j-core schema additions
- Makes test fixtures trivial to construct
- Lets the cache consume from gRPC ``Schedule`` messages directly
  without round-tripping through the heavier Pydantic model
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

#: Schedule kinds the tick engine knows how to compute next-fire
#: for. Matches the brain ``schedule_kind`` enum.
ScheduleKind = Literal["cron", "interval", "clocked", "one_shot", "solar"]

#: Catch-up policies. Matches :data:`z4j_scheduler.tick.catch_up.VALID_POLICIES`.
CatchUpPolicy = Literal["skip", "fire_one_missed", "fire_all_missed"]


@dataclass(slots=True)
class ScheduleEntry:
    """Internal tick-engine representation of a schedule.

    Fields needed for next-fire computation only. Everything else
    (task_name, args, kwargs, queue, engine routing, etc.) lives in
    brain's ``schedules`` row and is fetched by brain when it
    materialises the FireSchedule into a Command.

    Attributes:
        id: Stable identifier - the ``schedules.id`` UUID from brain.
        project_id: For the leader gate ("am I leader for this
            project?").
        kind: ``"cron"`` / ``"interval"`` / ``"one_shot"``.
        expression: The kind-specific expression
            (cron string / interval like ``"5m"`` / ISO timestamp).
        timezone: IANA tz name. Used by the cron path; ignored by
            interval and one_shot.
        is_enabled: Disabled schedules sit in the cache but never tick.
        catch_up: Policy when fires were missed.
        anchor_at: Used by interval first-fire alignment - typically
            the schedule's ``created_at``.
        last_fire_at: Most-recent successful fire. None if never
            fired.
        next_fire_at: Cached next-fire time, recomputed by
            :meth:`recompute_next_fire`.
    """

    id: UUID
    project_id: UUID
    kind: ScheduleKind
    expression: str
    timezone: str
    is_enabled: bool
    catch_up: CatchUpPolicy
    anchor_at: datetime
    last_fire_at: datetime | None = None
    # Phase 4: human-readable label for per-schedule observability
    # (Prometheus, dashboard charts, log lines). Optional so older
    # WatchSchedules events that pre-date the field still convert.
    name: str = ""
    # A3: engine label for the fire-variance histogram. Sourced from the
    # gRPC Schedule.engine field (the tick engine does not route by it --
    # it is carried only for observability). Optional/defaulted so a
    # pre-field WatchSchedules event still converts.
    engine: str = ""
    next_fire_at: datetime | None = field(default=None, init=False)
    # Boundary D current-protocol control generation. ``None`` is the
    # explicitly negotiated 1.7 compatibility shape; random UUID equality is
    # authority, never an ordering relation.
    control_token: UUID | None = None
    # Globally ordered Brain transport revision. Zero is legacy/unknown.
    schedule_revision: int = 0
    # Brain-computed digest over the complete execution/cadence definition.
    # The slim scheduler entry cannot safely reconstruct this from its subset.
    definition_digest: str = ""
    # Exact cadence implementation contract selected during channel
    # negotiation. Zero/empty is the explicit legacy shape.
    cadence_semantics_version: int = 0
    cadence_runtime_fingerprint: str = ""


def schedule_definition_changed(a: ScheduleEntry, b: ScheduleEntry) -> bool:
    """True iff the schedule's DEFINITION (its cadence, not enabled/timestamps)
    differs between two snapshots.

    A catch-up plan or a next-fire time computed from ``a`` is stale and must
    not be dispatched or persisted against ``b``'s new cadence. Lives next to
    :class:`ScheduleEntry` so the compared field list cannot drift away from
    the model; imported by both the tick engine (mid-drain abort) and the
    cache (compare-and-set fire-state write).
    """
    # H7: anchor_at is NOT a cadence field for this comparison. A fire ACK
    # advances brain's last_run_at, and entry_from_pb derives anchor_at from
    # last_run_at, so the fire-ack watch echo changes anchor_at WITHOUT any
    # cadence edit. Including it here made the mid-drain catch-up guard abort a
    # fire_all_missed drain on the first ACK and DROP the remaining missed slots;
    # and it made the fire-state compare-and-set reject the engine's own advance.
    # A genuine start-time-only edit no longer aborts a mid-drain, but such an
    # edit resets next_fire_at and is recomputed next tick, so it is safe.
    return schedule_cadence_identity(a) != schedule_cadence_identity(b)


def schedule_cadence_identity(entry: ScheduleEntry) -> tuple[object, ...]:
    """A hashable identity for the schedule's CADENCE.

    Same field list as :func:`schedule_definition_changed`, expressed as a value
    that can be STORED alongside a remembered slot. State that means "this exact
    occurrence of this exact cadence" has to carry both: a slot timestamp alone
    survives an edit that changes the cadence while leaving ``next_fire_at``
    untouched, and the remembered state then applies to an occurrence that is no
    longer on the schedule at all.
    """
    return (entry.kind, entry.expression, entry.timezone, entry.catch_up)


def schedule_control_identity(entry: ScheduleEntry) -> tuple[object, ...]:
    """Identity used by the scheduler's local safety overlay.

    A current Brain supplies an unguessable control token. The legacy fallback
    deliberately binds to the canonical cadence identity and therefore remains
    safety-biased across a same-id/same-definition recreate.
    """

    if entry.control_token is not None:
        return ("control-token", entry.control_token)
    return ("legacy-definition", *schedule_cadence_identity(entry))


def schedule_brain_payload(entry: ScheduleEntry) -> tuple[object, ...]:
    """Canonical received fields for same-revision conflict detection.

    Scheduler-local bookkeeping is deliberately absent. The Brain's complete
    cursor and effective state are included, so reusing one revision with a
    different token, definition, cursor, or enabled value is a protocol fault.
    """

    return (
        entry.id,
        entry.project_id,
        entry.kind,
        entry.expression,
        entry.timezone,
        entry.is_enabled,
        entry.catch_up,
        entry.anchor_at,
        entry.last_fire_at,
        entry.next_fire_at,
        entry.name,
        entry.engine,
        entry.control_token,
        entry.schedule_revision,
        entry.definition_digest,
        entry.cadence_semantics_version,
        entry.cadence_runtime_fingerprint,
    )


__all__ = [
    "CatchUpPolicy",
    "ScheduleEntry",
    "ScheduleKind",
    "schedule_brain_payload",
    "schedule_cadence_identity",
    "schedule_control_identity",
    "schedule_definition_changed",
]
