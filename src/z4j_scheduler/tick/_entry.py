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
ScheduleKind = Literal["cron", "interval", "one_shot"]

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
    next_fire_at: datetime | None = field(default=None, init=False)


__all__ = ["CatchUpPolicy", "ScheduleEntry", "ScheduleKind"]
