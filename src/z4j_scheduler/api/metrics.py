"""Prometheus /metrics endpoint.

Exposes the metrics defined in :mod:`z4j_scheduler.observability.metrics`
in the standard Prometheus exposition format. Optional bearer auth
via ``Z4J_SCHEDULER_METRICS_AUTH_TOKEN`` mirrors brain's behaviour.
"""

from __future__ import annotations

import hmac

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from z4j_scheduler.observability.metrics import default_registry

router = APIRouter(tags=["operational"])


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    """Prometheus exposition. Optional bearer-token gated."""
    settings = request.app.state.scheduler_state.settings
    expected_token = settings.metrics_auth_token

    if expected_token is not None:
        # Bearer-token mode - constant-time comparison so the endpoint
        # cannot be timing-attacked.
        provided = (request.headers.get("authorization") or "").removeprefix(
            "Bearer ",
        )
        if not provided or not hmac.compare_digest(
            expected_token.get_secret_value(),
            provided,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid or missing bearer token",
            )

    payload = generate_latest(default_registry)
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
