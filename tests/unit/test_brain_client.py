"""Tests for :class:`z4j_scheduler.storage.brain_client.BrainClient`.

The actual gRPC calls require a real server - those live in the
integration test suite. This module covers:

- Construction without I/O (no certs read, no channel opened)
- :meth:`connect` opens the channel exactly once (idempotent)
- :meth:`close` is idempotent
- Calling RPC methods before :meth:`connect` raises a clear error
- Every RPC counts under ``z4j_scheduler_grpc_calls_total`` by its
  gRPC status name
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import grpc
import pytest
from z4j_scheduler.observability import metrics as m
from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage.brain_client import BrainClient


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings with synthetic mTLS files that exist on disk."""
    cert = tmp_path / "scheduler.crt"
    key = tmp_path / "scheduler.key"
    ca = tmp_path / "brain-ca.crt"
    # Real-shaped PEM blocks so grpc.ssl_channel_credentials does not
    # reject them as malformed when connect() runs.
    cert.write_bytes(b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    key.write_bytes(b"-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n")
    ca.write_bytes(b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))
    return Settings(_env_file=None)  # type: ignore[call-arg]


class TestConstruction:
    def test_construct_does_no_io(self, settings: Settings) -> None:
        # No file reads, no channel open. Pure attribute setup.
        client = BrainClient(settings)
        assert client._channel is None
        assert client._stub is None


class TestConnectClose:
    @pytest.mark.asyncio
    async def test_connect_opens_channel_idempotently(
        self,
        settings: Settings,
    ) -> None:
        client = BrainClient(settings)
        # Patch the channel constructor so we don't actually open a
        # network connection; we just want to verify the lifecycle.
        with patch(
            "z4j_scheduler.storage.brain_client.grpc.aio.secure_channel",
        ) as mock_ch:
            await client.connect()
            assert client._channel is mock_ch.return_value
            assert client._stub is not None
            assert mock_ch.call_count == 1
            # Second connect is a no-op.
            await client.connect()
            assert mock_ch.call_count == 1

    @pytest.mark.asyncio
    async def test_close_before_connect_is_noop(
        self,
        settings: Settings,
    ) -> None:
        client = BrainClient(settings)
        await client.close()  # no error, no exception
        assert client._channel is None

    @pytest.mark.asyncio
    async def test_close_after_connect_clears_state(
        self,
        settings: Settings,
    ) -> None:
        client = BrainClient(settings)
        with patch(
            "z4j_scheduler.storage.brain_client.grpc.aio.secure_channel",
        ) as mock_ch:
            mock_ch.return_value.close = _AsyncNoop()
            await client.connect()
            await client.close()
            assert client._channel is None
            assert client._stub is None
            # Second close is a no-op.
            await client.close()


class TestRpcRequiresConnect:
    @pytest.mark.asyncio
    async def test_ping_before_connect_raises(self, settings: Settings) -> None:
        client = BrainClient(settings)
        with pytest.raises(RuntimeError, match="connect"):
            await client.ping()


class TestConfiguredDeadlines:
    @pytest.mark.asyncio
    async def test_fire_uses_configured_timeout(self, settings: Settings) -> None:
        configured = settings.model_copy(update={"fire_timeout_seconds": 37})
        client = BrainClient(configured)

        class _Stub:
            timeout: float | None = None

            async def FireSchedule(  # noqa: N802 - mirrors generated gRPC stub
                self,
                request: object,
                *,
                timeout: float,  # noqa: ASYNC109 - generated stub API
            ) -> object:
                self.timeout = timeout
                return pb.FireScheduleResponse(buffered=True)

        stub = _Stub()
        client._stub = stub  # type: ignore[assignment]

        await client.fire_schedule(
            schedule_id=uuid4(),
            fire_id=uuid4(),
            scheduled_for=datetime.now(UTC),
            fired_at=datetime.now(UTC),
        )

        assert stub.timeout == 37.0


class _UnavailableError(grpc.aio.AioRpcError):
    """Constructible AioRpcError; the real initialiser needs metadata objects."""

    def __init__(self) -> None:
        pass

    def code(self) -> grpc.StatusCode:  # type: ignore[override]
        return grpc.StatusCode.UNAVAILABLE


def _calls(method: str, status: str) -> float:
    return (
        m.default_registry.get_sample_value(
            "z4j_scheduler_grpc_calls_total",
            {"method": method, "status": status},
        )
        or 0.0
    )


class TestGrpcCallCounter:
    @pytest.mark.asyncio
    async def test_unary_success_counts_ok(self, settings: Settings) -> None:
        class _Stub:
            async def Ping(  # noqa: N802 - mirrors generated gRPC stub
                self,
                request: object,
                *,
                timeout: float,  # noqa: ASYNC109 - generated stub API
            ) -> object:
                return pb.PingResponse(brain_version="test")

        client = BrainClient(settings)
        client._stub = _Stub()  # type: ignore[assignment]
        before = _calls("Ping", "OK")

        info = await client.ping()

        assert info.brain_version == "test"
        assert _calls("Ping", "OK") == before + 1

    @pytest.mark.asyncio
    async def test_unary_failure_counts_the_grpc_status_name(self, settings: Settings) -> None:
        class _Stub:
            async def Ping(  # noqa: N802 - mirrors generated gRPC stub
                self,
                request: object,
                *,
                timeout: float,  # noqa: ASYNC109 - generated stub API
            ) -> object:
                raise _UnavailableError

        client = BrainClient(settings)
        client._stub = _Stub()  # type: ignore[assignment]
        before = _calls("Ping", "UNAVAILABLE")
        before_ok = _calls("Ping", "OK")

        with pytest.raises(grpc.aio.AioRpcError):
            await client.ping()

        assert _calls("Ping", "UNAVAILABLE") == before + 1
        assert _calls("Ping", "OK") == before_ok

    @pytest.mark.asyncio
    async def test_non_grpc_failure_counts_as_error(self, settings: Settings) -> None:
        class _Stub:
            async def Ping(  # noqa: N802 - mirrors generated gRPC stub
                self,
                request: object,
                *,
                timeout: float,  # noqa: ASYNC109 - generated stub API
            ) -> object:
                raise ValueError("not a transport failure")

        client = BrainClient(settings)
        client._stub = _Stub()  # type: ignore[assignment]
        before = _calls("Ping", "ERROR")

        with pytest.raises(ValueError, match="transport"):
            await client.ping()

        assert _calls("Ping", "ERROR") == before + 1

    @pytest.mark.asyncio
    async def test_stream_counts_once_when_it_ends(self, settings: Settings) -> None:
        class _Stub:
            def ListSchedules(  # noqa: N802 - mirrors generated gRPC stub
                self,
                request: object,
                *,
                timeout: float,
            ) -> object:
                async def empty() -> object:
                    return
                    yield  # pragma: no cover - makes this an async generator

                return empty()

        client = BrainClient(settings)
        client._stub = _Stub()  # type: ignore[assignment]
        before = _calls("ListSchedules", "OK")

        entries = [entry async for entry in client.list_schedules()]

        assert entries == []
        assert _calls("ListSchedules", "OK") == before + 1


class _AsyncNoop:
    """Minimal awaitable that absorbs any args. Used to stub
    ``channel.close(grace=...)`` in tests."""

    async def __call__(self, *_args: object, **_kwargs: object) -> None:
        return None
