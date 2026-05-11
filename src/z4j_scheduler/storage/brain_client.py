"""gRPC client to z4j-brain's SchedulerService.

Owns one long-lived ``grpc.aio.Channel`` per scheduler instance with:

- mTLS authentication (client cert from settings)
- Keepalive pings (default 30s)
- Per-method deadlines

Exposes typed methods that return :mod:`._models` dataclasses, NOT
raw protobuf - the conversion happens at this boundary so the rest
of the codebase never imports ``scheduler_pb2``.

The client is the seam between the scheduler's pure-Python core
and the gRPC wire. Tests for the rest of the system inject fakes
that satisfy the same method shape.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import grpc

from z4j_scheduler.proto import scheduler_pb2 as pb
from z4j_scheduler.proto import scheduler_pb2_grpc as pb_grpc
from z4j_scheduler.storage._convert import (
    entry_from_pb,
    event_from_pb,
    make_ack_request,
    make_fire_request,
    parse_fire_response,
    parse_ping_response,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator
    from datetime import datetime
    from uuid import UUID

    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage._models import (
        FireResult,
        PingInfo,
        ScheduleEvent,
    )
    from z4j_scheduler.tick._entry import ScheduleEntry

logger = logging.getLogger("z4j.scheduler.brain_client")

#: Per-call deadlines. The Watch stream has no deadline - it's
#: long-lived. Other RPCs get fast deadlines so a hung brain does
#: not block the tick engine indefinitely.
_LIST_TIMEOUT_SECONDS = 30.0
_FIRE_TIMEOUT_SECONDS = 10.0  # the dispatcher applies its own outer retry
_ACK_TIMEOUT_SECONDS = 5.0
_PING_TIMEOUT_SECONDS = 3.0


def _build_credentials(settings: Settings) -> grpc.ChannelCredentials:
    """Load mTLS credentials from the configured cert paths.

    Reads the three PEM files from disk synchronously - cheap one-shot
    at construction time, kept off the asyncio event loop. The certs
    are operator-provisioned and rotation requires a scheduler
    restart.

    Raises ``RuntimeError`` if any of the three cert paths is None;
    the Settings model_validator should have caught this before, but
    we re-check here to fail loud if something slipped through.
    """
    if (
        settings.tls_ca is None
        or settings.tls_cert is None
        or settings.tls_key is None
    ):
        raise RuntimeError(
            "z4j-scheduler: _build_credentials called without a "
            "complete TLS bundle - this is a Settings validator bug",
        )
    ca_pem = settings.tls_ca.read_bytes()
    cert_pem = settings.tls_cert.read_bytes()
    key_pem = settings.tls_key.read_bytes()
    return grpc.ssl_channel_credentials(
        root_certificates=ca_pem,
        private_key=key_pem,
        certificate_chain=cert_pem,
    )


class BrainClient:
    """Async gRPC client for the brain SchedulerService.

    Construction does NOT open the connection - call :meth:`connect`
    in the lifespan to do that. This lets tests construct the client
    cheaply with synthetic settings and inspect attributes without
    touching the network.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._channel: grpc.aio.Channel | None = None
        self._stub: pb_grpc.SchedulerServiceStub | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the gRPC channel and construct the stub.

        Idempotent - calling twice is a no-op. Configures keepalive +
        max-message-size + DNS resolution options that match what
        production deployments need.
        """
        if self._channel is not None:
            return

        options = [
            ("grpc.keepalive_time_ms", self._settings.grpc_keepalive_seconds * 1000),
            ("grpc.keepalive_timeout_ms", 5_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
            # Schedule payloads are tiny (<1 KiB typical). Cap at 1
            # MiB defensively against malformed brain responses.
            ("grpc.max_receive_message_length", 1024 * 1024),
            ("grpc.max_send_message_length", 1024 * 1024),
        ]

        if self._settings.insecure_grpc:
            # Insecure-channel path. The Settings model_validator
            # enforces this is only allowed when environment is not
            # 'production'. Loud log line so operators see the
            # security trade-off in their startup output.
            logger.warning(
                "z4j.scheduler.brain_client: using INSECURE gRPC "
                "channel to %s (insecure_grpc=true). Use only on "
                "trusted loopback or container networks.",
                self._settings.brain_grpc_url,
            )
            self._channel = grpc.aio.insecure_channel(
                self._settings.brain_grpc_url,
                options=options,
            )
        else:
            credentials = _build_credentials(self._settings)
            self._channel = grpc.aio.secure_channel(
                self._settings.brain_grpc_url,
                credentials,
                options=options,
            )
        self._stub = pb_grpc.SchedulerServiceStub(self._channel)
        logger.info(
            "z4j.scheduler.brain_client: connected to %s",
            self._settings.brain_grpc_url,
        )

    async def close(self) -> None:
        """Close the channel cleanly. Idempotent.

        Shields the underlying ``self._channel.close()`` so a
        lifespan cancel mid-close doesn't leave the gRPC sockets
        open while ``self._channel = None`` clears the only
        reference; that combination would leak file descriptors
        across restarts.
        """
        if self._channel is None:
            return
        channel = self._channel
        # Clear the references BEFORE awaiting close so a re-entrant
        # close() during the await is a no-op.
        self._channel = None
        self._stub = None
        try:
            await asyncio.shield(channel.close(grace=2.0))
        except Exception:  # noqa: BLE001
            logger.debug(
                "z4j.scheduler.brain_client: shielded close raised",
                exc_info=True,
            )
        logger.info("z4j.scheduler.brain_client: channel closed")

    # ------------------------------------------------------------------
    # RPC methods
    # ------------------------------------------------------------------

    async def list_schedules(
        self,
        project_id: UUID | None = None,
        *,
        page_size: int = 100,
    ) -> AsyncIterator[ScheduleEntry]:
        """Stream every schedule the scheduler should tick.

        Server-streaming - yields one :class:`ScheduleEntry` at a
        time so the cache populates incrementally even for large
        deployments.
        """
        stub = self._require_stub()
        request = pb.ListSchedulesRequest(
            project_id=str(project_id) if project_id is not None else "",
            page_size=page_size,
        )
        async for message in stub.ListSchedules(
            request, timeout=_LIST_TIMEOUT_SECONDS,
        ):
            yield entry_from_pb(message)

    async def watch_schedules(
        self,
        project_id: UUID | None = None,
        *,
        resume_token: str = "",
    ) -> AsyncIterator[ScheduleEvent]:
        """Subscribe to schedule changes. Server-streaming, long-lived.

        No deadline - the stream stays open until brain or the
        network drops it. Reconnect logic lives in
        :mod:`z4j_scheduler.storage.watch`.
        """
        stub = self._require_stub()
        request = pb.WatchSchedulesRequest(
            project_id=str(project_id) if project_id is not None else "",
            resume_token=resume_token,
        )
        async for message in stub.WatchSchedules(request):
            yield event_from_pb(message)

    async def fire_schedule(
        self,
        *,
        schedule_id: UUID,
        fire_id: UUID,
        scheduled_for: datetime,
        fired_at: datetime,
    ) -> FireResult:
        """Tell brain to fire a schedule. Single short-deadline RPC."""
        stub = self._require_stub()
        request = make_fire_request(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=scheduled_for,
            fired_at=fired_at,
        )
        response = await stub.FireSchedule(
            request, timeout=_FIRE_TIMEOUT_SECONDS,
        )
        return parse_fire_response(response)

    async def acknowledge_result(
        self,
        *,
        fire_id: UUID,
        command_id: UUID | None,
        status: str,
        new_task_id: str | None = None,
        error: str | None = None,
    ) -> None:
        """Acknowledge a fire's terminal result back to brain."""
        stub = self._require_stub()
        request = make_ack_request(
            fire_id=fire_id,
            command_id=command_id,
            status=status,
            new_task_id=new_task_id,
            error=error,
        )
        await stub.AcknowledgeFireResult(
            request, timeout=_ACK_TIMEOUT_SECONDS,
        )

    async def ping(self) -> PingInfo:
        """Liveness check. Returns brain version + clock."""
        stub = self._require_stub()
        response = await stub.Ping(
            pb.PingRequest(), timeout=_PING_TIMEOUT_SECONDS,
        )
        return parse_ping_response(response)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _require_stub(self) -> pb_grpc.SchedulerServiceStub:
        if self._stub is None:
            raise RuntimeError(
                "BrainClient.connect() must be called before any RPC method",
            )
        return self._stub


__all__ = ["BrainClient"]
