"""Operational HTTP endpoints (FastAPI mount).

Submodules:

- :mod:`~z4j_scheduler.api.health` - GET /health, GET /ready
- :mod:`~z4j_scheduler.api.metrics` - GET /metrics (Prometheus)
- :mod:`~z4j_scheduler.api.info` - GET /info (version + status)

Mounted under uvicorn at ``Z4J_SCHEDULER_BIND_HOST:Z4J_SCHEDULER_BIND_PORT``
(default ``0.0.0.0:7800``).

No auth on these endpoints in v1 - typical ops-network practice.
Operators who need auth use a reverse proxy or mTLS at the network
layer. The /metrics endpoint optionally accepts
``Authorization: Bearer <token>`` if ``Z4J_SCHEDULER_METRICS_AUTH_TOKEN``
is set, matching brain's /metrics behavior.
"""
