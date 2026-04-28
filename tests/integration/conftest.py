"""Integration test fixtures.

Brings up a real Postgres (via testcontainers OR via the existing
``Z4J_TEST_POSTGRES_URL`` env var pattern z4j-brain established) +
a fake brain gRPC server stubbed with canned responses.

Phase 1 implementation: testcontainers fixture, fake brain server
fixture, end-to-end tick + dispatch + ack round-trip test.
"""

from __future__ import annotations

# Stub - real implementation lands in Phase 1.
