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

    @pytest.mark.asyncio
    async def test_release_failure_on_stop_does_not_abort_cleanup(self) -> None:
        # B17: a per-project release RAISING during stop() must not abort the
        # release loop before held.clear() + backend.close(). The pre-fix
        # code logged the failure with a structlog ``key=`` kwarg on a stdlib
        # logger, which itself raised TypeError inside the except handler,
        # aborting cleanup -> advisory locks + the connection leak and a
        # standby cannot promote (split-brain window).
        projects = [uuid.uuid4() for _ in range(3)]

        class FailingReleaseBackend(FakeBackend):
            async def release(self, key: int) -> None:
                self.release_calls += 1
                raise RuntimeError("simulated release failure")

        backend = FailingReleaseBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert len(gate.held_projects()) == 3

        # Must complete cleanly despite every release failing.
        await gate.stop()

        assert backend.release_calls >= 3  # every held key was attempted
        assert backend.close_calls >= 1  # close REACHED (loop not aborted)
        assert gate.held_projects() == set()  # held state cleared


class TestCancelledStop:
    @pytest.mark.asyncio
    async def test_cancel_during_stop_still_completes_cleanup(self) -> None:
        # R3-M5: cancelling stop() mid-release must not abandon the
        # cleanup. asyncio.shield alone only protects the inner
        # coroutine - the outer await still raises CancelledError
        # immediately, so the first release raises, the remaining
        # per-project releases are skipped, and held.clear() +
        # close() never run (advisory locks stay held -> standby
        # cannot promote, split-brain window, plus a leaked
        # connection). The fix runs the whole cleanup in a dedicated
        # task and defers the caller's cancellation until it
        # finishes.
        projects = [uuid.uuid4() for _ in range(3)]

        class BlockingReleaseBackend(FakeBackend):
            def __init__(self) -> None:
                super().__init__()
                self.release_starts = 0
                self.first_release_started = asyncio.Event()
                self.unblock_first_release = asyncio.Event()

            async def release(self, key: int) -> None:
                self.release_starts += 1
                if self.release_starts == 1:
                    self.first_release_started.set()
                    await self.unblock_first_release.wait()
                await super().release(key)

        backend = BlockingReleaseBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert len(gate.held_projects()) == 3

        stop_task = asyncio.create_task(gate.stop())
        await asyncio.wait_for(
            backend.first_release_started.wait(),
            timeout=2.0,
        )
        # Cancel the caller while the FIRST release is in flight,
        # then let it proceed.
        stop_task.cancel()
        backend.unblock_first_release.set()

        # stop() re-raises the cancellation AFTER cleanup finished.
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        assert backend.release_calls == 3  # every held key released
        assert backend.close_calls >= 1  # close still reached
        assert gate.held_projects() == set()  # held state cleared

    @pytest.mark.asyncio
    async def test_hung_release_is_bounded_on_stop(self, monkeypatch) -> None:
        # A release that never returns must not stall shutdown: each
        # cleanup call is bounded by the stop-cleanup timeout, the
        # remaining keys still get their release attempt, and
        # close() is still reached afterwards.
        from z4j_scheduler.leader import postgres as postgres_mod

        monkeypatch.setattr(
            postgres_mod,
            "_STOP_CLEANUP_OP_TIMEOUT_SECONDS",
            0.1,
        )

        class HangingReleaseBackend(FakeBackend):
            async def release(self, key: int) -> None:
                self.release_calls += 1
                await asyncio.Event().wait()  # never set - hangs forever

        projects = [uuid.uuid4() for _ in range(3)]
        backend = HangingReleaseBackend()
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert len(gate.held_projects()) == 3

        stop_task = asyncio.create_task(gate.stop())
        done, _pending = await asyncio.wait({stop_task}, timeout=5.0)
        assert stop_task in done, "stop() did not finish within the bound"
        assert backend.release_calls == 3  # every held key attempted
        assert backend.close_calls >= 1  # close still reached
        assert gate.held_projects() == set()


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
            assert not (held_a & held_b), f"projects double-held: {held_a & held_b}"
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


class TestCancelDuringLoopJoin:
    @pytest.mark.asyncio
    async def test_cancel_while_joining_loop_still_releases_all(self) -> None:
        # R4-M4 (per-project gate): a cancel during stop()'s loop
        # join, before R3-M5's cleanup task existed, previously left
        # every held project lock dangling and the backend open. The
        # whole stop sequence is now the cancellation-immune task.
        class BlockingHealthBackend(FakeBackend):
            # The per-project loop health-checks INSIDE its first
            # cycle, so blocking immediately would deadlock
            # wait_for_first_cycle. Let the first call through and
            # park the loop on a later beat.
            def __init__(self) -> None:
                super().__init__()
                self.health_started = asyncio.Event()
                self.unblock_health = asyncio.Event()
                self._calls = 0

            async def health_check(self) -> None:
                self._calls += 1
                if self._calls >= 2:
                    self.health_started.set()
                    await self.unblock_health.wait()
                await super().health_check()

        backend = BlockingHealthBackend()
        projects = [uuid.uuid4() for _ in range(3)]
        gate = PerProjectLeaderGate(
            backend=backend,
            project_source=lambda: projects,
            heartbeat_seconds=0.05,
        )
        await gate.start()
        await gate.wait_for_first_cycle(timeout=2.0)
        assert gate.held_projects() == set(projects)

        await asyncio.wait_for(backend.health_started.wait(), timeout=2.0)
        stop_task = asyncio.create_task(gate.stop())
        await asyncio.sleep(0.05)
        stop_task.cancel()
        backend.unblock_health.set()

        with pytest.raises(asyncio.CancelledError):
            await stop_task

        assert backend.release_calls == 3
        assert backend.close_calls >= 1
        assert gate.held_projects() == set()
