"""Observability primitives - structured logging + Prometheus metrics.

Submodules:

- :mod:`~z4j_scheduler.observability.logging` - structlog config
- :mod:`~z4j_scheduler.observability.metrics` - prometheus-client
  metric definitions

Same structlog + prometheus-client patterns as z4j-brain so
operators see one consistent observability story across the stack.
Logs are JSON by default (``Z4J_SCHEDULER_LOG_JSON=true``) with
bound contextual fields for cross-component tracing.
"""
