"""GET /info - human-readable runtime status snapshot.

Useful for operator debugging and the dashboard's Schedulers page,
which can poll across all enrolled scheduler instances to render a
per-instance status grid.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request

from z4j_scheduler import __version__

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.api._state import SchedulerState

router = APIRouter(tags=["operational"])


@router.get("/info")
async def info(request: Request) -> dict[str, Any]:
    """Snapshot of current runtime state.

    Audit fix S003 (1.4.0): payload is intentionally minimal and
    does NOT include topology fields (``brain_grpc_url``,
    ``projects``) that previously leaked the upstream brain URL
    and per-instance project bindings to any unauthenticated
    caller able to reach the scheduler's HTTP port.

    Operators see those values in their own deployment config;
    attackers don't get a free topology dump from the dashboard
    polling endpoint. The dashboard's Schedulers admin page
    already handles missing fields with ``?? "-"`` fallbacks, so
    redaction is not a behavior change for legitimate users.

    Excludes anything secret-shaped (TLS cert paths and the
    metrics auth token were never echoed even pre-fix).
    """
    state: SchedulerState = request.app.state.scheduler_state
    schedule_count = len(state.cache) if state.cache is not None else 0
    return {
        "version": __version__,
        "instance_id": state.settings.instance_id,
        "uptime_seconds": round(state.uptime_seconds(), 2),
        "started_at": state.started_at.isoformat(),
        "ready": state.ready,
        "subsystems": {
            "brain_client_connected": state.brain_client_connected,
            "cache_initial_sync_complete": state.cache_initial_sync_complete,
            "leader_gate_initialised": state.leader_gate_initialised,
        },
        "schedules_loaded": schedule_count,
    }


__all__ = ["router"]
