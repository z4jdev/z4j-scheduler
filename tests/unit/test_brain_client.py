"""Tests for :class:`z4j_scheduler.storage.brain_client.BrainClient`.

The actual gRPC calls require a real server - those live in the
integration test suite. This module covers:

- Construction without I/O (no certs read, no channel opened)
- :meth:`connect` opens the channel exactly once (idempotent)
- :meth:`close` is idempotent
- Calling RPC methods before :meth:`connect` raises a clear error
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
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
        self, settings: Settings,
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
        self, settings: Settings,
    ) -> None:
        client = BrainClient(settings)
        await client.close()  # no error, no exception
        assert client._channel is None

    @pytest.mark.asyncio
    async def test_close_after_connect_clears_state(
        self, settings: Settings,
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


class _AsyncNoop:
    """Minimal awaitable that absorbs any args. Used to stub
    ``channel.close(grace=...)`` in tests."""

    async def __call__(self, *_args: object, **_kwargs: object) -> None:
        return None
