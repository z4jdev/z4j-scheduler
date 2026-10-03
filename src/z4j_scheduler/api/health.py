"""Liveness + readiness endpoints.

- ``GET /health`` - liveness. 200 if the process is alive. The
  endpoint trivially returns OK; a process that can't serve this
  is not running. Suitable for k8s liveness probes and process
  supervisors.
- ``GET /ready`` - readiness. 200 only if every subsystem is up
  (gRPC client connected, schedule cache populated, leader gate
  initialised) and the watch stream has not been unhealthy for
  longer than ``on_time_grace_seconds``. Returns 503 with a JSON
  body naming the missing subsystem when not ready; a sustained
  watch outage is named ``watch_unhealthy`` with its duration, and
  the endpoint returns 200 again once the stream reconnects.
  Suitable for k8s readiness probes and load-balancer health checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.api._state import SchedulerState

router = APIRouter(tags=["operational"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness - 200 if the process is alive."""
    return {"status": "alive"}


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    """Readiness - 200 if every subsystem is up, 503 otherwise.

    The 503 body lists the missing subsystems so operators can fix
    one issue at a time without guessing.
    """
    state: SchedulerState = request.app.state.scheduler_state
    if state.ready:
        return JSONResponse({"status": "ready"})
    missing = []
    if not state.brain_client_connected:
        missing.append("brain_client")
    if not state.cache_initial_sync_complete:
        missing.append("cache_initial_sync")
    if not state.leader_gate_initialised:
        missing.append("leader_gate")
    body: dict[str, Any] = {"status": "not_ready", "missing": missing}
    if state.watch_unhealthy_past_grace():
        missing.append("watch_unhealthy")
        seconds = state.watch_unhealthy_seconds()
        if seconds is not None:
            body["watch_unhealthy_seconds"] = round(seconds, 2)
    return JSONResponse(
        body,
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    )


__all__ = ["router"]
