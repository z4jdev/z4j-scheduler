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

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import grpc
import pytest
from z4j_scheduler.main import SchedulerApp
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage._models import FireResult, PingInfo
from z4j_scheduler.storage._protocol import ProtocolNegotiationError
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick.cadence import cadence_runtime_fingerprint

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
        self.ping = AsyncMock(
            return_value=PingInfo(
                brain_version="1.8.0",
                brain_time=datetime.now(UTC),
                scheduler_protocol_epoch=1,
            ),
        )
        self.negotiate_protocol = AsyncMock(side_effect=lambda offered: offered)


class _RpcError(grpc.RpcError):
    def __init__(self, status: grpc.StatusCode) -> None:
        self._status = status

    def code(self) -> grpc.StatusCode:
        return self._status


class _AppWithFakeClient(SchedulerApp):
    """Subclass that injects ``_FakeBrainClient`` instead of the real one."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.fake_client = _FakeBrainClient()

    def _build_brain_client(self):  # type: ignore[no-untyped-def, override]
        return self.fake_client


class _SlowFireBrainClient:
    """A brain whose one FireSchedule answer waits for the test to release it.

    Records, in order, every event the shutdown sequence is supposed to keep
    in order: the fire going out, its acknowledgement, the leader gate's
    release (appended by the gate), and the channel close.
    """

    def __init__(self) -> None:
        self.events: list[str] = []
        self.fire_started = asyncio.Event()
        self.release = asyncio.Event()

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.events.append("close")

    async def ping(self) -> PingInfo:
        return PingInfo(
            brain_version="1.11.0",
            brain_time=datetime.now(UTC),
            scheduler_protocol_epoch=1,
        )

    async def negotiate_protocol(self, offered: Any) -> Any:
        return offered

    async def fire_schedule(
        self,
        *,
        schedule_entry: ScheduleEntry,
        prepared_fire: Any,
        **_kwargs: Any,
    ) -> FireResult:
        self.events.append("fire")
        self.fire_started.set()
        await self.release.wait()
        revision = schedule_entry.schedule_revision + 1
        return FireResult(
            command_id=uuid4(),
            error_code=None,
            error_message=None,
            buffered=False,
            disposition="accepted",
            acceptance_revision=revision,
            accepted_last_run_at=prepared_fire.scheduled_for,
            accepted_next_run_at=prepared_fire.next_run_at,
            live_control_token=schedule_entry.control_token,
            live_revision=revision,
            live_last_run_at=prepared_fire.scheduled_for,
            live_next_run_at=prepared_fire.next_run_at,
        )

    async def acknowledge_result(self, **_kwargs: Any) -> None:
        self.events.append("ack")


class _RecordingLeaderGate:
    """Always leader; records when the application releases it."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    def is_leader(self, project_id: UUID) -> bool:
        return True

    async def stop(self) -> None:
        self._events.append("gate_release")


class _AppWithSlowFire(SchedulerApp):
    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.brain = _SlowFireBrainClient()
        self.gate = _RecordingLeaderGate(self.brain.events)

    def _build_brain_client(self):  # type: ignore[no-untyped-def, override]
        return self.brain

    async def _build_leader_gate(self):  # type: ignore[no-untyped-def, override]
        return self.gate


def _due_current_entry() -> ScheduleEntry:
    now = datetime.now(UTC)
    entry = ScheduleEntry(
        id=uuid4(),
        project_id=uuid4(),
        kind="interval",
        expression="5m",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=now - timedelta(minutes=5),
        last_fire_at=now - timedelta(minutes=5),
        control_token=uuid4(),
        schedule_revision=40,
        definition_digest="d" * 64,
        cadence_semantics_version=1,
        cadence_runtime_fingerprint="f" * 64,
    )
    entry.next_fire_at = now
    return entry


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

    async def test_start_negotiates_and_selects_current_protocol(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        await app.start()

        assert app.fake_client.ping.await_count == 1
        assert app.fake_client.negotiate_protocol.await_count == 1
        (offered,), _ = app.fake_client.negotiate_protocol.await_args
        assert offered.cadence_runtime_fingerprint == cadence_runtime_fingerprint()
        assert app._watch is not None
        assert app._watch._protocol_mode == "current"
        assert app._watch._project_id is None
        assert app._watch._protocol_selector is not None
        assert app._quarantine_reporter is not None
        assert app._tick_engine is not None
        assert app._tick_engine._quarantine_reporter is app._quarantine_reporter

    async def test_start_selects_legacy_only_from_exact_legacy_pair(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        app.fake_client.ping.return_value = PingInfo(
            brain_version="1.7.0",
            brain_time=datetime.now(UTC),
            scheduler_protocol_epoch=0,
        )
        app.fake_client.negotiate_protocol.side_effect = _RpcError(
            grpc.StatusCode.UNIMPLEMENTED,
        )

        await app.start()

        assert app._watch is not None
        assert app._watch._protocol_mode == "legacy"
        assert app._quarantine_reporter is None

    async def test_start_fails_closed_on_contradictory_negotiation(
        self,
        settings: Settings,
    ) -> None:
        app = _AppWithFakeClient(settings)
        app.fake_client.negotiate_protocol.side_effect = _RpcError(
            grpc.StatusCode.UNIMPLEMENTED,
        )

        with pytest.raises(ProtocolNegotiationError, match="exact legacy"):
            await app.start()

        assert app._started is False
        assert app.fake_client.close.await_count == 1

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

    async def test_start_warns_once_when_single_leader_backend_is_networked(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """``single`` is not an election. A networked, non-dev process that
        relies on it is told so once at startup; the same process on a
        loopback bind, or in dev, is not."""
        _ = settings  # the fixture's env (TLS bundle, brain urls) is what we build on
        monkeypatch.setenv("Z4J_SCHEDULER_ENVIRONMENT", "production")
        monkeypatch.setenv("Z4J_SCHEDULER_METRICS_AUTH_TOKEN", "t" * 32)
        monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "0.0.0.0")

        def _warnings() -> list[logging.LogRecord]:
            return [
                r
                for r in caplog.records
                if r.levelno == logging.WARNING and "not an election" in r.getMessage()
            ]

        with caplog.at_level(logging.WARNING, logger="z4j.scheduler.main"):
            networked = _AppWithFakeClient(Settings(_env_file=None))  # type: ignore[call-arg]
            assert networked.settings.leader_backend == "single"
            await networked.start()
            assert len(_warnings()) == 1
            message = _warnings()[0].getMessage()
            assert "double-dispatch" in message
            assert "Z4J_SCHEDULER_LEADER_BACKEND=postgres" in message
            await networked.stop()

            caplog.clear()
            monkeypatch.setenv("Z4J_SCHEDULER_BIND_HOST", "127.0.0.1")
            loopback = _AppWithFakeClient(Settings(_env_file=None))  # type: ignore[call-arg]
            await loopback.start()
            assert _warnings() == []
            await loopback.stop()


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

    async def test_stop_drains_an_in_flight_fire_before_releasing_the_leader_gate(
        self,
        settings: Settings,
    ) -> None:
        """What the module docstring promises: a fire already sent to the
        brain is acknowledged before the leader gate is released and the
        channel closed, and stop() does not return until then."""
        app = _AppWithSlowFire(settings)
        await app.start()
        assert app._watch is not None
        assert app._cache is not None
        assert app._tick_engine is not None
        # The stream itself is not run here; the engine dispatches only
        # behind a healthy one.
        app._watch._is_healthy = True
        entry = _due_current_entry()
        await app._cache.upsert(entry)

        engine_task = asyncio.create_task(app._tick_engine.run())
        await asyncio.wait_for(app.brain.fire_started.wait(), timeout=5)
        assert app._tick_engine.in_flight_count == 1

        stop_task = asyncio.create_task(app.stop())
        await asyncio.sleep(0.05)
        # The fire is still out; nothing downstream of it has been torn down.
        assert not stop_task.done()
        assert app.brain.events == ["fire"]

        app.brain.release.set()
        await asyncio.wait_for(stop_task, timeout=5)
        await asyncio.wait_for(engine_task, timeout=5)

        assert app.brain.events == ["fire", "ack", "gate_release", "close"]
        assert app._tick_engine.in_flight_count == 0
        live = await app._cache.get(entry.id)
        assert live is not None
        assert live.last_fire_at == entry.next_fire_at or live.schedule_revision == 41

    async def test_stop_abandons_a_fire_that_outlives_the_fire_timeout(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Negative control: the drain is bounded by fire_timeout_seconds, so
        a brain that never answers cannot hold the process open."""
        monkeypatch.setenv("Z4J_SCHEDULER_FIRE_TIMEOUT_SECONDS", "1")
        app = _AppWithSlowFire(Settings(_env_file=None))  # type: ignore[call-arg]
        await app.start()
        assert app._watch is not None
        assert app._cache is not None
        assert app._tick_engine is not None
        app._watch._is_healthy = True
        await app._cache.upsert(_due_current_entry())

        engine_task = asyncio.create_task(app._tick_engine.run())
        await asyncio.wait_for(app.brain.fire_started.wait(), timeout=5)
        with caplog.at_level(logging.WARNING, logger="z4j.scheduler.main"):
            await asyncio.wait_for(app.stop(), timeout=5)
        assert any("still in flight" in r.getMessage() for r in caplog.records)
        assert app.brain.events == ["fire", "gate_release", "close"]

        # Let the stranded fire finish so the engine task exits cleanly.
        app.brain.release.set()
        await asyncio.wait_for(engine_task, timeout=5)


# ---------------------------------------------------------------------------
# run() guard
# ---------------------------------------------------------------------------


class TestRunGuard:
    async def test_run_before_start_raises(self, settings: Settings) -> None:
        app = SchedulerApp(settings)
        with pytest.raises(RuntimeError, match="start"):
            await app.run()
