"""Tests for the Postgres-advisory-lock leader gate.

These exercise the state-machine logic via a :class:`FakeBackend`
that lets the test script the lock outcomes (granted / denied,
healthy / dead). The actual asyncpg path is covered by the
integration test in ``tests/integration/`` which spins up a real
Postgres via testcontainers.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from z4j_scheduler.leader.postgres import (
    PostgresAdvisoryLockLeaderGate,
    _namespace_to_key,
)


# =====================================================================
# FakeBackend - in-memory test double
# =====================================================================


class FakeBackend:
    """Scriptable :class:`LockBackend` for state-machine tests.

    Defaults: lock is grantable, connection is healthy. Tests flip
    these flags to drive the gate through every transition path.

    Records call counts so tests can assert "how many acquire
    attempts happened in 0.5s" - useful for tuning the heartbeat.
    """

    def __init__(self) -> None:
        self.grant_lock: bool = True
        self.healthy: bool = True
        self.acquire_calls: int = 0
        self.health_calls: int = 0
        self.release_calls: int = 0
        self.close_calls: int = 0
        self._holds_lock: bool = False

    async def acquire(self, key: int) -> bool:
        self.acquire_calls += 1
        if self.grant_lock:
            self._holds_lock = True
            return True
        return False

    async def health_check(self) -> None:
        self.health_calls += 1
        if not self.healthy:
            raise ConnectionError("backend simulated dead")

    async def release(self, key: int) -> None:
        self.release_calls += 1
        self._holds_lock = False

    async def close(self) -> None:
        self.close_calls += 1
        self._holds_lock = False


# =====================================================================
# Helpers
# =====================================================================


PROJECT_ID = uuid.uuid4()


# =====================================================================
# Namespace key derivation
# =====================================================================


class TestNamespaceKey:
    def test_deterministic(self) -> None:
        assert _namespace_to_key("foo") == _namespace_to_key("foo")

    def test_distinct_namespaces_distinct_keys(self) -> None:
        assert _namespace_to_key("a") != _namespace_to_key("b")

    def test_key_fits_in_signed_bigint(self) -> None:
        # Postgres bigint is signed 64-bit; we mask to 63 bits to
        # stay within the positive range. Verify we never exceed it.
        for ns in ("foo", "z4j-scheduler-global", "x" * 200):
            key = _namespace_to_key(ns)
            assert 0 <= key < (1 << 63)


# =====================================================================
# Lifecycle
# =====================================================================


class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_idempotent(self) -> None:
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.start()  # noop
        # Wait for the (single) task to complete its first cycle so
        # we can prove acquisition happened. If two tasks were
        # spawned the FakeBackend would record 2 acquire calls;
        # we'd see test_stop_releases_lock fail (double release).
        await gate.wait_for_first_cycle(timeout=2.0)
        await gate.stop()
        assert backend.acquire_calls >= 1

    @pytest.mark.asyncio
    async def test_stop_releases_lock_when_leader(self) -> None:
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert gate.is_leader(PROJECT_ID) is True
        await gate.stop()
        # Stop calls release exactly once when we held the lock.
        assert backend.release_calls == 1
        # And closes the connection on shutdown.
        assert backend.close_calls >= 1

    @pytest.mark.asyncio
    async def test_stop_does_not_release_when_not_leader(self) -> None:
        backend = FakeBackend()
        backend.grant_lock = False
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert gate.is_leader(PROJECT_ID) is False
        await gate.stop()
        # Standby never held the lock - skip the release call to
        # avoid noise in pg_locks for a no-op unlock.
        assert backend.release_calls == 0


# =====================================================================
# Leader transitions
# =====================================================================


class TestBecomesLeader:
    @pytest.mark.asyncio
    async def test_becomes_leader_when_lock_granted(self) -> None:
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(PROJECT_ID) is True
        finally:
            await gate.stop()


class TestStandby:
    @pytest.mark.asyncio
    async def test_remains_standby_when_lock_denied(self) -> None:
        backend = FakeBackend()
        backend.grant_lock = False
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(PROJECT_ID) is False
            # Several more retry cycles; still standby.
            await asyncio.sleep(0.2)
            assert gate.is_leader(PROJECT_ID) is False
        finally:
            await gate.stop()

    @pytest.mark.asyncio
    async def test_promotes_when_lock_becomes_available(self) -> None:
        backend = FakeBackend()
        backend.grant_lock = False
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(PROJECT_ID) is False
            # Simulate the leader dying - lock now available.
            backend.grant_lock = True
            # Wait enough cycles for the promotion to happen.
            for _ in range(20):
                await asyncio.sleep(0.06)
                if gate.is_leader(PROJECT_ID):
                    break
            assert gate.is_leader(PROJECT_ID) is True
        finally:
            await gate.stop()


# =====================================================================
# Failure handling
# =====================================================================


class TestConnectionDeath:
    @pytest.mark.asyncio
    async def test_loses_leadership_when_health_check_fails(self) -> None:
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(PROJECT_ID) is True

            # Simulate the connection dying. The next health-check
            # raises; the gate should demote within one cycle.
            backend.healthy = False
            for _ in range(20):
                await asyncio.sleep(0.06)
                if not gate.is_leader(PROJECT_ID):
                    break
            assert gate.is_leader(PROJECT_ID) is False
            # And the gate forced the connection closed so the next
            # cycle opens a fresh one.
            assert backend.close_calls >= 1
        finally:
            await gate.stop()

    @pytest.mark.asyncio
    async def test_recovers_when_backend_heals(self) -> None:
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(PROJECT_ID) is True

            # Drop. The gate force-closes. Next cycle opens a fresh
            # connection on FakeBackend - which is still ``grant=True``
            # by default.
            backend.healthy = False
            for _ in range(20):
                await asyncio.sleep(0.06)
                if not gate.is_leader(PROJECT_ID):
                    break
            assert gate.is_leader(PROJECT_ID) is False

            # Heal the backend. Re-acquisition should succeed.
            backend.healthy = True
            for _ in range(20):
                await asyncio.sleep(0.06)
                if gate.is_leader(PROJECT_ID):
                    break
            assert gate.is_leader(PROJECT_ID) is True
        finally:
            await gate.stop()


# =====================================================================
# Read surface
# =====================================================================


class TestIsLeaderProjectArgIgnored:
    @pytest.mark.asyncio
    async def test_global_gate_ignores_project_id(self) -> None:
        # Different project ids return the same is_leader value in
        # global mode. Per-project gating lives in step 4.
        backend = FakeBackend()
        gate = PostgresAdvisoryLockLeaderGate(
            backend=backend, heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(uuid.uuid4()) is True
            assert gate.is_leader(uuid.uuid4()) is True
        finally:
            await gate.stop()
