"""FastAPI app factory for the operational endpoints.

The scheduler is primarily a worker process; the HTTP surface is
small (4 endpoints) and exists for ops use only - probes,
metrics scrape, status snapshot. The actual scheduling work
happens in the asyncio tasks the SchedulerApp lifespan starts.

This factory builds the FastAPI application with:

- :mod:`~z4j_scheduler.api.health` - /health + /ready
- :mod:`~z4j_scheduler.api.metrics` - /metrics
- :mod:`~z4j_scheduler.api.info` - /info

The :class:`~z4j_scheduler.api._state.SchedulerState` instance is
attached to ``app.state.scheduler_state`` at construction time.
The endpoint handlers read from it via ``request.app.state``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI

from z4j_scheduler import __version__
from z4j_scheduler.api import health as health_mod
from z4j_scheduler.api import info as info_mod
from z4j_scheduler.api import metrics as metrics_mod

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.api._state import SchedulerState


def create_app(state: SchedulerState) -> FastAPI:
    """Build the FastAPI app with the operational endpoints mounted.

    Args:
        state: The per-process runtime state. Owned by the
            SchedulerApp lifespan; the app keeps a reference for
            handlers to read.

    Returns:
        A configured :class:`fastapi.FastAPI` instance ready to be
        served by uvicorn.
    """
    app = FastAPI(
        title="z4j-scheduler",
        description="Operational endpoints for the z4j scheduler.",
        version=__version__,
        docs_url=None,  # ops surface only - no public swagger
        redoc_url=None,
        openapi_url=None,
    )
    app.state.scheduler_state = state
    app.include_router(health_mod.router)
    app.include_router(metrics_mod.router)
    app.include_router(info_mod.router)
    return app


__all__ = ["create_app"]
