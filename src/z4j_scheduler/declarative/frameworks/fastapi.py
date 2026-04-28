"""FastAPI integration for declarative schedule reconciliation.

Returns a lifespan context manager the operator wires into their
FastAPI app::

    from contextlib import asynccontextmanager
    from fastapi import FastAPI
    from z4j_scheduler.declarative import ScheduleSpec
    from z4j_scheduler.declarative.frameworks.fastapi import (
        z4j_lifespan,
    )

    SCHEDULES = [
        ScheduleSpec(
            name="hourly-cleanup",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.cleanup",
        ),
    ]

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with z4j_lifespan(
            schedules=SCHEDULES,
            project="acme-prod",
            brain_url="https://brain.example.com",
            api_token=os.environ["Z4J_API_TOKEN"],
        ):
            yield

    app = FastAPI(lifespan=lifespan)

The reconcile fires on app startup; failures log but don't crash
the FastAPI app (same forgiving behaviour as the Django helper).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from z4j_scheduler.declarative._reconciler import (
    ScheduleSpec,
    reconcile,
)

logger = logging.getLogger("z4j.scheduler.declarative.fastapi")


@asynccontextmanager
async def z4j_lifespan(
    *,
    schedules: list[ScheduleSpec] | dict[str, ScheduleSpec],
    project: str,
    source: str = "declarative_fastapi",
    brain_url: str = "http://brain:7700",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
) -> AsyncIterator[None]:
    """FastAPI lifespan helper that reconciles schedules on startup.

    Async context manager - swaps in for the operator's own
    lifespan or composes with one via ``contextlib.AsyncExitStack``.

    The reconcile fires inside ``__aenter__`` so the schedules are
    in brain by the time the first request arrives. Failure to
    reconcile is logged but doesn't prevent the app from serving -
    a brain outage shouldn't take the whole web app down.
    """
    try:
        summary = await reconcile(
            schedules=schedules,
            project=project,
            source=source,
            brain_url=brain_url,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )
        logger.info(
            "z4j.scheduler.declarative.fastapi: reconcile complete %r",
            summary,
        )
    except Exception:  # noqa: BLE001
        # Never crash app startup. The next deploy will retry.
        logger.exception(
            "z4j.scheduler.declarative.fastapi: reconcile failed; "
            "app will continue",
        )
    try:
        yield
    finally:
        # Nothing to clean up - reconcile is fire-and-forget.
        # Adding shutdown logic later (e.g. drain pending updates)
        # is the natural extension point.
        pass


__all__ = ["z4j_lifespan"]
