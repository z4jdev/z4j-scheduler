"""Tests for :class:`z4j_scheduler.main.SchedulerApp`.

The full ``run()`` loop is hard to test directly because it owns
uvicorn. We focus on:

- Construction + ``start()`` opens subsystems in the right order
- ``start()`` is idempotent
- ``stop()`` is idempotent and tears down without raising
- ``run()`` raising ``RuntimeError`` if start was never called
- The state object reflects subsystem readiness as start() progresses

We inject a fake brain client by overriding ``_build_brain_client``
on a subclass - the real BrainClient would need a real network /
real certs.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from z4j_scheduler.main import SchedulerApp
from z4j_scheduler.settings import Settings

pytestmark = pytest.mark.asyncio


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    cert = tmp_path / "scheduler.crt"
    key = tmp_path / "scheduler.key"
    ca = tmp_path / "brain-ca.crt"
    for p in (cert, key, ca):
        p.write_bytes(
            b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
        )
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))
    monkeypatch.setenv("Z4J_SCHEDULER_INSTANCE_ID", "test-instance")
    return Settings(_env_file=None)  # type: ignore[call-arg]


class _FakeBrainClient:
    """Minimal fake. Only ``connect`` and ``close`` are exercised in
    these tests; the full RPC surface is tested in test_brain_client
    + test_dispatch."""

    def __init__(self) -> None:
        self.connect = AsyncMock(return_value=None)
        self.close = AsyncMock(return_value=None)


class _AppWithFakeClient(SchedulerApp):
    """Subclass that injects ``_FakeBrainClient`` instead of the real one."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.fake_client = _FakeBrainClient()

    def _build_brain_client(self):  # type: ignore[no-untyped-def, override]
        return self.fake_client


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    async def test_construct_does_no_io(self, settings: Settings) -> None:
        app = SchedulerApp(settings)
        # No subsystems built yet - construction is cheap.
        assert app._client is None
        assert app._cache is None
        assert app._tick_engine is None
        assert app._watch is None
        assert app._dispatcher is None
        assert app._state is None
        assert app._uvicorn_server is None
        assert app._started is False


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


class TestStart:
    async def test_start_opens_all_subsystems(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()

        assert app._client is not None
        assert app._cache is not None
        assert app._leader_gate is not None
        assert app._dispatcher is not None
        assert app._tick_engine is not None
        assert app._watch is not None
        assert app._state is not None
        assert app._uvicorn_server is not None
        assert app._started is True

    async def test_start_connects_brain_client(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        assert app.fake_client.connect.await_count == 1

    async def test_start_marks_state_subsystems_up(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        assert app._state is not None
        # Brain + leader gate up immediately after start.
        assert app._state.brain_client_connected is True
        assert app._state.leader_gate_initialised is True
        # Cache sync flag flips only AFTER the watch task runs its
        # first sync - not yet.
        assert app._state.cache_initial_sync_complete is False
        # Until cache sync completes, the state is NOT ready.
        assert app._state.ready is False

    async def test_start_is_idempotent(self, settings: Settings) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        await app.start()  # second call is a no-op
        # Brain client only connected once.
        assert app.fake_client.connect.await_count == 1


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


class TestStop:
    async def test_stop_before_start_does_not_raise(
        self,
        settings: Settings,
    ) -> None:
        app = SchedulerApp(settings)
        await app.stop()  # nothing to tear down; no exception

    async def test_stop_after_start_closes_brain_client(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        await app.stop()
        assert app.fake_client.close.await_count == 1

    async def test_stop_is_idempotent(self, settings: Settings) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        await app.stop()
        await app.stop()
        # close() may run twice; both should be safe (the real client
        # is also idempotent on close).
        assert app.fake_client.close.await_count >= 1

    async def test_stop_signals_subsystems(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()
        # uvicorn server should not exit until stop.
        assert app._uvicorn_server is not None
        assert app._uvicorn_server.should_exit is False
        await app.stop()
        # After stop, uvicorn is told to exit.
        assert app._uvicorn_server.should_exit is True


# ---------------------------------------------------------------------------
# run() guard
# ---------------------------------------------------------------------------


class TestRunGuard:
    async def test_run_before_start_raises(self, settings: Settings) -> None:
        app = SchedulerApp(settings)
        with pytest.raises(RuntimeError, match="start"):
            await app.run()
