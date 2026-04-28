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

# Phase 1 implementation: Settings factory fixture, FakeBrainClient
# fixture, FakeCache fixture, FakeLeaderGate fixture, asyncio
# event_loop fixture (matching z4j-brain pattern).
