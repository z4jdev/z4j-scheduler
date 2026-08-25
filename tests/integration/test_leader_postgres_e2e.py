"""Integration test: Postgres advisory-lock leader gate with real PG.

Runs two :class:`PostgresAdvisoryLockLeaderGate` instances against a shared or
disposable real PostgreSQL and verifies:

1. Exactly one of the two becomes leader on startup.
2. Killing the leader (closing its connection) lets the standby
   take over within a few heartbeat cycles.
3. The same namespace key stays stable across processes - both
   instances target the same lock.

Uses ``Z4J_TEST_POSTGRES_URL`` when supplied and otherwise starts a disposable
PostgreSQL through the shared integration fixture.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from z4j_scheduler.leader.postgres import (
    AsyncpgLockBackend,
    PostgresAdvisoryLockLeaderGate,
    _namespace_to_key,
)


@pytest.fixture
def asyncpg_dsn(postgres_url: str) -> str:
    """Expose the shared fixture's bare asyncpg-compatible URL."""
    return postgres_url


# =====================================================================
# Tests
# =====================================================================


class TestNamespaceKeyStability:
    def test_same_namespace_has_same_key_in_separate_processes(self) -> None:
        namespace = "z4j-scheduler-cross-process-control"
        script = (
            "from z4j_scheduler.leader.postgres import _namespace_to_key; "
            f"print(_namespace_to_key({namespace!r}))"
        )
        python_path = os.pathsep.join(entry or str(Path.cwd()) for entry in sys.path)
        observed: list[int] = []
        for hash_seed in ("1", "8675309"):
            env = os.environ.copy()
            env["PYTHONHASHSEED"] = hash_seed
            env["PYTHONPATH"] = python_path
            completed = subprocess.run(
                [sys.executable, "-c", script],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
            observed.append(int(completed.stdout.strip()))

        expected = _namespace_to_key(namespace)
        assert observed == [expected, expected]


class TestSingleInstance:
    @pytest.mark.asyncio
    async def test_one_instance_becomes_leader(
        self,
        asyncpg_dsn: str,
    ) -> None:
        namespace = f"test-single-{uuid.uuid4()}"
        gate = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=namespace,
            heartbeat_seconds=0.5,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=10.0)
            assert gate.is_leader(uuid.uuid4()) is True
        finally:
            await gate.stop()


class TestTwoInstancesRace:
    @pytest.mark.asyncio
    async def test_only_one_becomes_leader(
        self,
        asyncpg_dsn: str,
    ) -> None:
        namespace = f"test-race-{uuid.uuid4()}"
        gate_a = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=namespace,
            heartbeat_seconds=0.3,
        )
        gate_b = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=namespace,
            heartbeat_seconds=0.3,
        )
        await gate_a.start()
        await gate_b.start()
        try:
            # Both gates should complete their first acquire pass
            # within a few seconds.
            await gate_a.wait_for_first_cycle(timeout=10.0)
            await gate_b.wait_for_first_cycle(timeout=10.0)

            # Exactly one is leader, the other is standby.
            leaders = [g.is_leader(uuid.uuid4()) for g in (gate_a, gate_b)]
            assert sum(leaders) == 1, f"expected one leader, got {leaders}"
        finally:
            await gate_a.stop()
            await gate_b.stop()

    @pytest.mark.asyncio
    async def test_standby_takes_over_when_leader_stops(
        self,
        asyncpg_dsn: str,
    ) -> None:
        """The classic failover scenario.

        Gate A wins the initial race. We stop it (clean shutdown:
        releases the lock + closes the connection). Gate B, which
        was polling on its heartbeat, picks up the lock on its next
        cycle.

        Failover time bound = heartbeat_seconds (0.3) + jitter.
        """
        namespace = f"test-failover-{uuid.uuid4()}"
        gate_a = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=namespace,
            heartbeat_seconds=0.3,
        )
        gate_b = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=namespace,
            heartbeat_seconds=0.3,
        )
        # Start gate A first; give it a head start so it wins the
        # race deterministically. Otherwise the test relies on
        # arbitrary scheduling order.
        await gate_a.start()
        await gate_a.wait_for_first_cycle(timeout=10.0)
        assert gate_a.is_leader(uuid.uuid4()) is True

        await gate_b.start()
        await gate_b.wait_for_first_cycle(timeout=10.0)
        assert gate_b.is_leader(uuid.uuid4()) is False

        try:
            # Kill the leader cleanly. The lock is released as part
            # of stop() AND when the connection closes - either path
            # makes the lock available to gate_b on its next poll.
            await gate_a.stop()

            # Wait for gate_b to promote. Bound: heartbeat * a few
            # cycles. We poll up to 5s before declaring failure.
            promoted = False
            for _ in range(50):
                await asyncio.sleep(0.1)
                if gate_b.is_leader(uuid.uuid4()):
                    promoted = True
                    break
            assert promoted, "gate_b did not become leader after gate_a stopped"
        finally:
            await gate_b.stop()


class TestNamespaceIsolation:
    @pytest.mark.asyncio
    async def test_distinct_namespaces_dont_interfere(
        self,
        asyncpg_dsn: str,
    ) -> None:
        """Two clusters using different namespaces both get leaders.

        The whole point of the namespace knob: an operator running
        a staging scheduler cluster and a prod cluster against the
        same Postgres should not have them block each other.
        """
        gate_prod = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=f"test-ns-prod-{uuid.uuid4()}",
            heartbeat_seconds=0.5,
        )
        gate_staging = PostgresAdvisoryLockLeaderGate(
            backend=AsyncpgLockBackend(dsn=asyncpg_dsn),
            namespace=f"test-ns-staging-{uuid.uuid4()}",
            heartbeat_seconds=0.5,
        )
        await gate_prod.start()
        await gate_staging.start()
        try:
            await gate_prod.wait_for_first_cycle(timeout=10.0)
            await gate_staging.wait_for_first_cycle(timeout=10.0)
            # Both win - they're contending for different locks.
            assert gate_prod.is_leader(uuid.uuid4()) is True
            assert gate_staging.is_leader(uuid.uuid4()) is True
        finally:
            await gate_prod.stop()
            await gate_staging.stop()
