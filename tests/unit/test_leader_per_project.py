"""Tests for the PER-PROJECT advisory-lock leader gate.

Drives the state machine via a scriptable :class:`FakeBackend`
(same shape as in ``test_leader_postgres.py``) plus a configurable
``project_source`` that returns the project list each cycle.

Real-Postgres failover is covered by the integration test in
``tests/integration/test_leader_postgres_e2e.py``; the per-project
mode reuses the same backend so we don't repeat that coverage here.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from z4j_scheduler.leader.postgres import (
    PerProjectLeaderGate,
    _project_key,
)


class FakeBackend:
    """Scriptable :class:`LockBackend` for state-machine tests.

    Tracks which keys are 'held' globally so two FakeBackend
    instances pointing at the same shared dict simulate two
    scheduler instances racing for the same per-project locks.
    """

    def __init__(
        self,
        *,
        shared_held: set[int] | None = None,
        always_grant: bool = True,
    ) -> None:
        self.shared_held = shared_held if shared_held is not None else set()
        self.always_grant = always_grant
        self.healthy = True
        self.acquire_calls = 0
        self.release_calls = 0
        self.close_calls = 0
        self._my_keys: set[int] = set()

    async def acquire(self, key: int) -> bool:
        self.acquire_calls += 1
        if not self.always_grant:
            return False
        if key in self.shared_held and key not in self._my_keys:
            return False
        self.shared_held.add(key)
        self._my_keys.add(key)
        return True

    async def health_check(self) -> None:
        if not self.healthy:
            raise ConnectionError("backend simulated dead")

    async def release(self, key: int) -> None:
        self.release_calls += 1
        self.shared_held.discard(key)
        self._my_keys.discard(key)

    async def close(self) -> None:
        self.close_calls += 1
        for key in list(self._my_keys):
            self.shared_held.discard(key)
        self._my_keys.clear()


class TestStaticProjectSet:
    @pytest.mark.asyncio
    async def test_acquires_every_project_in_source(self) -> None:
        projects = [uuid.uuid4() for _ in range(3)]
        backend = FakeBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            for pid in projects:
                assert gate.is_leader(pid) is True
            assert gate.held_projects() == set(projects)
        finally:
            await gate.stop()

    @pytest.mark.asyncio
    async def test_releases_projects_dropped_from_source(self) -> None:
        projects = [uuid.uuid4() for _ in range(3)]
        backend = FakeBackend()

        def src():
            return list(projects)

        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=src,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert len(gate.held_projects()) == 3

            # Drop the middle project from the source.
            dropped = projects.pop(1)
            for _ in range(20):
                await asyncio.sleep(0.06)
                if dropped not in gate.held_projects():
                    break
            assert dropped not in gate.held_projects()
            assert gate.is_leader(dropped) is False
            # Backend saw a release for that key.
            assert backend.release_calls >= 1
        finally:
            await gate.stop()


class TestTwoInstancesShareWork:
    @pytest.mark.asyncio
    async def test_each_instance_leads_some_projects(self) -> None:
        # Both instances share the same lock state via shared_held.
        # FakeBackend's grant logic returns False for keys already
        # held by another instance, so the two gates split the
        # project set roughly in half (whoever polls first wins).
        shared: set[int] = set()
        projects = [uuid.uuid4() for _ in range(6)]

        backend_a = FakeBackend(shared_held=shared)
        backend_b = FakeBackend(shared_held=shared)
        gate_a = PerProjectLeaderGate(
            backend=backend_a,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        gate_b = PerProjectLeaderGate(
            backend=backend_b,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate_a.start()
        await gate_b.start()
        try:
            await gate_a.wait_for_first_cycle(timeout=2.0)
            await gate_b.wait_for_first_cycle(timeout=2.0)

            held_a = gate_a.held_projects()
            held_b = gate_b.held_projects()
            # No project led by both - core invariant.
            assert not (held_a & held_b), (
                f"projects double-held: {held_a & held_b}"
            )
            # Together they cover everything (both ran first cycle).
            assert held_a | held_b == set(projects)
        finally:
            await gate_a.stop()
            await gate_b.stop()


class TestConnectionDeath:
    @pytest.mark.asyncio
    async def test_loses_all_locks_on_health_failure(self) -> None:
        projects = [uuid.uuid4() for _ in range(3)]
        backend = FakeBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert len(gate.held_projects()) == 3

            # Connection dies on the next health probe.
            backend.healthy = False
            for _ in range(30):
                await asyncio.sleep(0.06)
                if not gate.held_projects():
                    break
            assert gate.held_projects() == set()

            # Heal the backend - the gate re-acquires.
            backend.healthy = True
            for _ in range(30):
                await asyncio.sleep(0.06)
                if len(gate.held_projects()) == 3:
                    break
            assert len(gate.held_projects()) == 3
        finally:
            await gate.stop()


class TestProjectKeyDerivation:
    def test_namespace_separates_clusters(self) -> None:
        pid = uuid.uuid4()
        assert _project_key("prod", pid) != _project_key("staging", pid)

    def test_distinct_projects_distinct_keys(self) -> None:
        a, b = uuid.uuid4(), uuid.uuid4()
        assert _project_key("ns", a) != _project_key("ns", b)

    def test_deterministic(self) -> None:
        pid = uuid.uuid4()
        assert _project_key("ns", pid) == _project_key("ns", pid)


class TestAsyncProjectSource:
    @pytest.mark.asyncio
    async def test_async_source_works(self) -> None:
        # Cache.snapshot() is async; the gate must accept that.
        projects = [uuid.uuid4()]

        async def asrc():
            return projects

        backend = FakeBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=asrc,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        try:
            await gate.wait_for_first_cycle(timeout=2.0)
            assert gate.is_leader(projects[0]) is True
        finally:
            await gate.stop()
