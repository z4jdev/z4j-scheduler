"""Tests for :mod:`z4j_scheduler.dispatch.fire`.

Tests the production :class:`FireDispatcher` with a fake brain
client. No real gRPC, no real network. Covers:

- Idempotent fire_id derivation
- Success path (delivered + buffered)
- Failure path (FireDispatchError raised)
- Retry on transient gRPC errors
- Permanent errors not retried
- Best-effort acknowledge swallows ack failures
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from z4j_scheduler.dispatch.fire import (
    FireDispatcher,
    FireDispatchError,
    derive_fire_id,
)
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage._models import FireResult

# NOTE: no module-level ``pytestmark = pytest.mark.asyncio`` because
# the ``TestDeriveFireId`` class below contains sync tests of the
# pure helper. The async test classes use the marker explicitly.


# ---------------------------------------------------------------------------
# Settings fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Construct a Settings with synthetic mTLS paths.

    The dispatcher does not read the cert files itself; only the
    BrainClient does, and we inject a fake client. So the cert paths
    just need to exist on disk to satisfy the Path validator.
    """
    cert = tmp_path / "scheduler.crt"
    key = tmp_path / "scheduler.key"
    ca = tmp_path / "brain-ca.crt"
    for p in (cert, key, ca):
        p.write_text("dummy")

    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))
    # Tight retry tuning to keep tests fast.
    monkeypatch.setenv("Z4J_SCHEDULER_FIRE_RETRY_MAX", "2")
    monkeypatch.setenv("Z4J_SCHEDULER_FIRE_RETRY_BACKOFF_SECONDS", "0.001")
    return Settings(_env_file=None)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Fake brain client
# ---------------------------------------------------------------------------


class _FakeAioRpcError(grpc.aio.AioRpcError):
    """Constructible AioRpcError for tests.

    The real one's ``__init__`` is awkward to call. We subclass to
    expose a stable code() return.
    """

    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:  # type: ignore[override]
        return self._code

    def details(self) -> str:  # type: ignore[override]
        return f"fake error {self._code.name}"

    def initial_metadata(self) -> grpc.aio.Metadata:  # type: ignore[override]
        return grpc.aio.Metadata()

    def trailing_metadata(self) -> grpc.aio.Metadata:  # type: ignore[override]
        return grpc.aio.Metadata()

    def debug_error_string(self) -> str:  # type: ignore[override]
        return ""


@dataclass
class _RecordedAck:
    fire_id: UUID
    command_id: UUID | None
    status: str
    error: str | None


@dataclass
class FakeBrainClient:
    """Captures fire_schedule + acknowledge_result calls.

    Configurable failure scripts:
    - ``fire_responses`` - a queue of FireResult OR Exception. Each
      fire_schedule call pops the head.
    - ``ack_should_raise`` - if True, every acknowledge_result raises.
    """

    fire_responses: list[FireResult | Exception] = field(default_factory=list)
    ack_should_raise: bool = False

    fire_calls: list[dict] = field(default_factory=list)
    ack_calls: list[_RecordedAck] = field(default_factory=list)

    async def fire_schedule(
        self,
        *,
        schedule_id: UUID,
        fire_id: UUID,
        scheduled_for: datetime,
        fired_at: datetime,
    ) -> FireResult:
        self.fire_calls.append(
            {
                "schedule_id": schedule_id,
                "fire_id": fire_id,
                "scheduled_for": scheduled_for,
                "fired_at": fired_at,
            },
        )
        if not self.fire_responses:
            return FireResult(
                command_id=uuid4(), error_code=None,
                error_message=None, buffered=False,
            )
        head = self.fire_responses.pop(0)
        if isinstance(head, Exception):
            raise head
        return head

    async def acknowledge_result(
        self,
        *,
        fire_id: UUID,
        command_id: UUID | None,
        status: str,
        new_task_id: str | None = None,
        error: str | None = None,
    ) -> None:
        self.ack_calls.append(
            _RecordedAck(
                fire_id=fire_id, command_id=command_id,
                status=status, error=error,
            ),
        )
        _ = new_task_id  # not asserted in current tests; preserved for future
        if self.ack_should_raise:
            raise RuntimeError("simulated ack failure")


# ---------------------------------------------------------------------------
# Tests - derive_fire_id
# ---------------------------------------------------------------------------


class TestDeriveFireId:
    def test_deterministic(self) -> None:
        sid = uuid4()
        when = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        a = derive_fire_id(sid, when)
        b = derive_fire_id(sid, when)
        assert a == b

    def test_different_schedule_different_id(self) -> None:
        when = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        a = derive_fire_id(uuid4(), when)
        b = derive_fire_id(uuid4(), when)
        assert a != b

    def test_different_time_different_id(self) -> None:
        sid = uuid4()
        a = derive_fire_id(sid, datetime(2026, 4, 26, 15, 0, tzinfo=UTC))
        b = derive_fire_id(sid, datetime(2026, 4, 26, 16, 0, tzinfo=UTC))
        assert a != b

    def test_naive_rejected(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            derive_fire_id(uuid4(), datetime(2026, 4, 26, 15, 0))


# ---------------------------------------------------------------------------
# Tests - FireDispatcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDispatchSuccess:
    async def test_delivered_path(self, settings: Settings) -> None:
        cid = uuid4()
        client = FakeBrainClient(
            fire_responses=[
                FireResult(
                    command_id=cid, error_code=None,
                    error_message=None, buffered=False,
                ),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]

        sid = uuid4()
        when = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        await dispatcher.dispatch(schedule_id=sid, scheduled_for=when)

        assert len(client.fire_calls) == 1
        assert client.fire_calls[0]["schedule_id"] == sid
        # Fire id is deterministic.
        expected_fire_id = derive_fire_id(sid, when)
        assert client.fire_calls[0]["fire_id"] == expected_fire_id

        # Best-effort ack happened with status=success.
        assert len(client.ack_calls) == 1
        assert client.ack_calls[0].status == "success"
        assert client.ack_calls[0].command_id == cid

    async def test_buffered_path(self, settings: Settings) -> None:
        client = FakeBrainClient(
            fire_responses=[
                FireResult(
                    command_id=None, error_code=None,
                    error_message=None, buffered=True,
                ),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        await dispatcher.dispatch(
            schedule_id=uuid4(),
            scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        # Buffered counts as success - ack as success.
        assert client.ack_calls[0].status == "success"


@pytest.mark.asyncio
class TestDispatchFailure:
    async def test_brain_error_raises_fire_dispatch_error(
        self, settings: Settings,
    ) -> None:
        client = FakeBrainClient(
            fire_responses=[
                FireResult(
                    command_id=None, error_code="agent_offline",
                    error_message="no agent online", buffered=False,
                ),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        with pytest.raises(FireDispatchError, match="agent_offline"):
            await dispatcher.dispatch(
                schedule_id=uuid4(),
                scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
            )
        # Ack with status=failed still happens.
        assert client.ack_calls[0].status == "failed"
        assert client.ack_calls[0].command_id is None


@pytest.mark.asyncio
class TestDispatchRetry:
    async def test_transient_error_retried_then_succeeds(
        self, settings: Settings,
    ) -> None:
        client = FakeBrainClient(
            fire_responses=[
                _FakeAioRpcError(grpc.StatusCode.UNAVAILABLE),
                FireResult(
                    command_id=uuid4(), error_code=None,
                    error_message=None, buffered=False,
                ),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        await dispatcher.dispatch(
            schedule_id=uuid4(),
            scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        # Two fire calls (one failed transient, one succeeded).
        assert len(client.fire_calls) == 2
        # Same fire_id on both (idempotent retry).
        assert client.fire_calls[0]["fire_id"] == client.fire_calls[1]["fire_id"]

    async def test_transient_error_giving_up_after_max(
        self, settings: Settings,
    ) -> None:
        # fire_retry_max=2 means 1 + 2 = 3 attempts max.
        client = FakeBrainClient(
            fire_responses=[
                _FakeAioRpcError(grpc.StatusCode.UNAVAILABLE),
                _FakeAioRpcError(grpc.StatusCode.UNAVAILABLE),
                _FakeAioRpcError(grpc.StatusCode.UNAVAILABLE),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        with pytest.raises(grpc.aio.AioRpcError):
            await dispatcher.dispatch(
                schedule_id=uuid4(),
                scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
            )
        # 3 attempts total.
        assert len(client.fire_calls) == 3

    async def test_permanent_error_not_retried(
        self, settings: Settings,
    ) -> None:
        client = FakeBrainClient(
            fire_responses=[
                _FakeAioRpcError(grpc.StatusCode.PERMISSION_DENIED),
            ],
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        with pytest.raises(grpc.aio.AioRpcError):
            await dispatcher.dispatch(
                schedule_id=uuid4(),
                scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
            )
        # No retry on PERMISSION_DENIED - only 1 attempt.
        assert len(client.fire_calls) == 1


@pytest.mark.asyncio
class TestAckBestEffort:
    async def test_ack_failure_does_not_raise(
        self, settings: Settings,
    ) -> None:
        """If brain.acknowledge_result raises, the dispatcher swallows
        and the engine continues."""
        client = FakeBrainClient(
            fire_responses=[
                FireResult(
                    command_id=uuid4(), error_code=None,
                    error_message=None, buffered=False,
                ),
            ],
            ack_should_raise=True,
        )
        dispatcher = FireDispatcher(client=client, settings=settings)  # type: ignore[arg-type]
        # No exception raised even though ack fails.
        await dispatcher.dispatch(
            schedule_id=uuid4(),
            scheduled_for=datetime(2026, 4, 26, 15, 0, tzinfo=UTC),
        )
        assert len(client.ack_calls) == 1
