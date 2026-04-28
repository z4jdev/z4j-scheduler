"""Shared runtime state for the FastAPI operational endpoints.

Constructed once at startup by the SchedulerApp lifespan and stuck
on ``app.state.scheduler_state``. Each endpoint reads fields from
it via FastAPI ``Depends()`` to render /health, /ready, /info.

Kept deliberately simple - just a dataclass. The endpoints only
read, never write. State mutations happen elsewhere (the cache
size tracks itself; the leader gate's projects flip in the gate's
own loop; reconnect counts come from Prometheus).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.storage.cache import ScheduleCache


@dataclass(slots=True)
class SchedulerState:
    """Per-process runtime state for /health, /ready, /info endpoints.

    Mutable by the SchedulerApp lifespan as subsystems come online.
    The endpoints take a snapshot at request time - no locking,
    because the only writer is the lifespan (single coroutine) and
    readers are the FastAPI handlers (also asyncio coroutines).
    """

    settings: Settings
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    #: Set once :meth:`BrainClient.connect` returns. Used by /ready
    #: to refuse traffic until the client is up.
    brain_client_connected: bool = False

    #: Set once the watch stream has completed its first full sync.
    #: /ready refuses until then because the cache is empty.
    cache_initial_sync_complete: bool = False

    #: Set once the leader gate has at least one project resolved
    #: (or in single-instance mode, immediately after startup).
    leader_gate_initialised: bool = False

    #: References to the live subsystems for /info to query.
    cache: ScheduleCache | None = None
    client: BrainClient | None = None

    @property
    def ready(self) -> bool:
        """True if every subsystem has reached a serving state."""
        return (
            self.brain_client_connected
            and self.cache_initial_sync_complete
            and self.leader_gate_initialised
        )

    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()


__all__ = ["SchedulerState"]
