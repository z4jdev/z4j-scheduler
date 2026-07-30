"""Immutable cadence transition prepared before a schedule dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PreparedFire:
    """One logical slot and its already-computed successor.

    Boundary D forbids cadence parsing or successor computation after a task
    may have been sent. The engine therefore constructs this value before the
    dispatcher is called and uses the same successor for both the durable
    request and the local post-acceptance mirror.
    """

    scheduled_for: datetime
    next_run_at: datetime | None


__all__ = ["PreparedFire"]
