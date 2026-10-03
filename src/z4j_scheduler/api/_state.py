"""Shared runtime state for the FastAPI operational endpoints.

Constructed once at startup by the SchedulerApp lifespan and stuck
on ``app.state.scheduler_state``. Each endpoint reads fields from
it via FastAPI ``Depends()`` to render /health, /ready, /info.

Kept deliberately simple - just a dataclass. The endpoints only
read, never write. State mutations happen elsewhere (the cache
size tracks itself; the leader gate's projects flip in the gate's
own loop; reconnect counts come from Prometheus; the watch stream
keeps its own health and outage clock, read through :attr:`watch`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.storage.cache import ScheduleCache
    from z4j_scheduler.storage.watch import WatchStream


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

    #: The watch stream, read by /ready and /info for its health. Left
    #: ``None`` by tests that do not exercise the watch gate.
    watch: WatchStream | None = None

    @property
    def ready(self) -> bool:
        """True if every subsystem is serving and the watch is not in a sustained outage."""
        return (
            self.brain_client_connected
            and self.cache_initial_sync_complete
            and self.leader_gate_initialised
            and not self.watch_unhealthy_past_grace()
        )

    @property
    def watch_healthy(self) -> bool:
        """True while the attached watch stream is connected; False with none attached."""
        return self.watch is not None and self.watch.is_healthy

    def watch_unhealthy_seconds(self) -> float | None:
        """How long the watch stream has been continuously unhealthy, or ``None``."""
        if self.watch is None:
            return None
        return self.watch.unhealthy_for_seconds()

    def watch_unhealthy_past_grace(self) -> bool:
        """The watch has been down for longer than ``on_time_grace_seconds``.

        A reconnect inside the grace is a blip the engine rides out: it
        refuses to dispatch meanwhile and catch-up covers the gap. Past the
        grace, fires are being missed, so the instance must stop reporting
        ready for a probe to act on it. Clears as soon as the stream is back.
        """
        seconds = self.watch_unhealthy_seconds()
        return seconds is not None and seconds > self.settings.on_time_grace_seconds

    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()


__all__ = ["SchedulerState"]
