"""A silent database stall must demote local leadership and allow recovery."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from z4j_scheduler.leader import postgres

from .test_leader_postgres import FakeBackend


@pytest.mark.parametrize("mode", ["global", "per_project"])
@pytest.mark.parametrize("operation", ["acquire", "health_check"])
async def test_hung_election_call_closes_connection_and_recovers(monkeypatch, mode, operation):
    monkeypatch.setattr(postgres, "_BACKEND_OP_TIMEOUT_SECONDS", 0.03)
    backend = FakeBackend()
    project = uuid4()
    started, cancelled, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = getattr(backend, operation)

    async def stalled(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def close():
        await FakeBackend.close(backend)
        closed.set()

    setattr(backend, operation, stalled)
    backend.close = close
    kwargs = {"backend": backend, "heartbeat_seconds": 0.01}
    gate = (
        postgres.PostgresAdvisoryLockLeaderGate(**kwargs)
        if mode == "global"
        else postgres.PerProjectLeaderGate(**kwargs, project_source=lambda: [project])
    )
    await gate.start()
    try:
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.wait_for(closed.wait(), 1)
        assert cancelled.is_set()
        assert not gate.is_leader(project)
        # Service recovers without restarting the scheduler.
        setattr(backend, operation, original)
        async with asyncio.timeout(1):
            while not gate.is_leader(project):  # noqa: ASYNC110 - poll the public gate state
                await asyncio.sleep(0.005)
    finally:
        await gate.stop()


async def test_hung_project_release_drops_all_local_leadership(monkeypatch):
    monkeypatch.setattr(postgres, "_BACKEND_OP_TIMEOUT_SECONDS", 0.03)
    backend = FakeBackend()
    projects = [uuid4(), uuid4()]
    initial = list(projects)
    closed = asyncio.Event()

    async def release(_key):
        await asyncio.Event().wait()

    async def close():
        await FakeBackend.close(backend)
        closed.set()

    backend.release = release
    backend.close = close
    gate = postgres.PerProjectLeaderGate(
        backend=backend, heartbeat_seconds=0.05, project_source=lambda: projects
    )
    await gate.start()
    try:
        await gate.wait_for_first_cycle()
        assert gate.held_projects() == set(initial)
        projects.pop()
        await asyncio.wait_for(closed.wait(), 1)
        assert gate.held_projects() == set()
    finally:
        # Avoid testing the independent stop-cleanup timeout here.
        backend.release = FakeBackend.release.__get__(backend)
        await gate.stop()
