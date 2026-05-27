"""Shared pytest fixtures for z4j-scheduler tests.

The fixtures here mirror the patterns used by z4j-brain's test
suite - same Settings shape, same testcontainers Postgres helper,
same asyncio mode.

Two fixture scopes:

- **Unit tests** (``tests/unit/``) get fakes for the gRPC client,
  cache, and leader gate. No external services needed.
- **Integration tests** (``tests/integration/``) bring up real
  Postgres (via testcontainers OR ``Z4J_TEST_POSTGRES_URL``) and
  a fake brain gRPC server stubbed with a few canned responses.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True, scope="session")
def _default_dev_environment() -> Iterator[None]:
    """Default ``Z4J_SCHEDULER_ENVIRONMENT=dev`` for the whole test
    session.

    The 1.6.5 production fail-safe in
    :class:`~z4j_scheduler.settings.Settings` refuses to start when
    ``environment=production`` + non-loopback ``bind_host`` +
    ``metrics_enabled=true`` + no ``metrics_auth_token``. Pydantic
    defaults give exactly that combination, so any test that
    constructs ``Settings`` without explicitly overriding those
    fields would trip the validator. The audit scenario *should*
    be a test failure for production code paths, but unit tests
    that happen to instantiate Settings to exercise unrelated
    fields should not have to opt out of the fail-safe.

    Tests that specifically test the production fail-safe set
    ``Z4J_SCHEDULER_ENVIRONMENT=production`` themselves (see
    ``test_api.py::TestR3L1ProductionMetricsFailSafe``).
    """
    previous = os.environ.get("Z4J_SCHEDULER_ENVIRONMENT")
    os.environ["Z4J_SCHEDULER_ENVIRONMENT"] = "dev"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("Z4J_SCHEDULER_ENVIRONMENT", None)
        else:
            os.environ["Z4J_SCHEDULER_ENVIRONMENT"] = previous
