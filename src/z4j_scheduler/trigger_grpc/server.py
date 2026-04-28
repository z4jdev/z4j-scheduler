"""gRPC server lifecycle for the scheduler-side TriggerSchedule RPC.

Started by :class:`SchedulerApp` when
``Z4J_SCHEDULER_TRIGGER_GRPC_ENABLED`` is true. Bound to
``Z4J_SCHEDULER_TRIGGER_GRPC_BIND_HOST`` :
``Z4J_SCHEDULER_TRIGGER_GRPC_BIND_PORT`` (default ``0.0.0.0:7802``,
distinct from the FastAPI port at 7800 and the brain-side
SchedulerService port at 7701 so all three can run on one host).

Cleanly stops on scheduler shutdown - drains in-flight RPCs within
``trigger_grpc_grace_seconds`` before tearing the runtime down.

Mirrors the structure of :class:`z4j_brain.scheduler_grpc.server.
SchedulerGrpcServer`; differences are limited to which servicer is
registered + which interceptor is wired.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import grpc

from z4j_scheduler.proto import scheduler_pb2_grpc as pb_grpc
from z4j_scheduler.trigger_grpc.auth import TriggerAllowlistInterceptor
from z4j_scheduler.trigger_grpc.handlers import TriggerScheduleServicer

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.dispatch.fire import FireDispatcher
    from z4j_scheduler.settings import Settings
    from z4j_scheduler.storage.cache import ScheduleCache

logger = logging.getLogger("z4j.scheduler.trigger_grpc.server")


class TriggerGrpcServer:
    """Owns the lifecycle of the scheduler-side TriggerSchedule server.

    Construction is cheap. :meth:`start` opens the port + binds
    handlers. :meth:`stop` is idempotent.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        cache: ScheduleCache,
        dispatcher: FireDispatcher,
        leader_gate: object,
    ) -> None:
        self._settings = settings
        self._cache = cache
        self._dispatcher = dispatcher
        self._leader_gate = leader_gate
        self._server: grpc.aio.Server | None = None
        self._bound_port: int = 0

    @property
    def bound_port(self) -> int:
        """Port the gRPC server is currently listening on.

        Captured from ``add_secure_port`` after :meth:`start`. 0
        before start or when the server is disabled. Useful for
        integration tests passing port=0.
        """
        return self._bound_port

    async def start(self) -> None:
        """Bind the gRPC port and start serving.

        Returns immediately if ``trigger_grpc_enabled`` is false.
        Otherwise raises on missing TLS material - we don't
        silently fall back to insecure mode for a privileged
        operator-facing surface.
        """
        if not self._settings.trigger_grpc_enabled:
            logger.info(
                "z4j.scheduler.trigger_grpc: disabled via settings; "
                "not starting server",
            )
            return

        creds = _build_server_credentials(self._settings)
        interceptors = (
            TriggerAllowlistInterceptor(
                allowed_cns=tuple(self._settings.trigger_grpc_allowed_cns),
            ),
        )
        # Round-9 audit fix R9-Sched-H4 (Apr 2026): keepalive opts.
        # Pre-fix the scheduler-side TriggerGrpc server had no
        # keepalive — a half-open NAT/proxy could silently wedge
        # the brain's persistent ``TriggerScheduleClient`` channel
        # for hours before the 10s deadline tripped, manifesting
        # as "fire now" buttons that hang for the full timeout
        # before the client retries. Mirrors the brain-side
        # SchedulerService server keepalive policy.
        _keepalive_opts = (
            # Server pings the client every 30s if the connection
            # has been idle (no data either direction).
            ("grpc.keepalive_time_ms", 30_000),
            # Wait 10s for a ping ack; closer if it's not received.
            ("grpc.keepalive_timeout_ms", 10_000),
            # Permit pings even when no calls are in flight.
            ("grpc.keepalive_permit_without_calls", 1),
            # Don't penalise a client that pings frequently — the
            # default 0 ban-strikes setting closes a connection
            # after just two too-frequent pings; kept lax here.
            ("grpc.http2.max_ping_strikes", 0),
            ("grpc.http2.min_recv_ping_interval_without_data_ms", 5_000),
            # Cap the lifetime of an idle channel; forces a fresh
            # TLS handshake every 30 min so a leaked session key
            # has bounded value.
            ("grpc.max_connection_idle_ms", 30 * 60 * 1000),
        )
        server = grpc.aio.server(
            interceptors=interceptors,
            options=_keepalive_opts,
        )
        servicer = TriggerScheduleServicer(
            cache=self._cache,
            dispatcher=self._dispatcher,
            leader_gate=self._leader_gate,
        )
        pb_grpc.add_SchedulerServiceServicer_to_server(servicer, server)

        bind_addr = (
            f"{self._settings.trigger_grpc_bind_host}"
            f":{self._settings.trigger_grpc_bind_port}"
        )
        self._bound_port = server.add_secure_port(bind_addr, creds)
        await server.start()
        self._server = server
        logger.info(
            "z4j.scheduler.trigger_grpc: serving on %s (mTLS, allow-list=%s)",
            bind_addr,
            tuple(self._settings.trigger_grpc_allowed_cns) or "(open CA)",
        )

    async def stop(self) -> None:
        """Stop + drain. Idempotent."""
        if self._server is None:
            return
        try:
            await self._server.stop(
                grace=float(self._settings.trigger_grpc_grace_seconds),
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "z4j.scheduler.trigger_grpc: server.stop crashed",
            )
        self._server = None
        logger.info("z4j.scheduler.trigger_grpc: stopped")


def _build_server_credentials(settings: Settings) -> grpc.ServerCredentials:
    cert = _read_required_pem(
        settings.trigger_grpc_tls_cert,
        "Z4J_SCHEDULER_TRIGGER_GRPC_TLS_CERT",
    )
    key = _read_required_pem(
        settings.trigger_grpc_tls_key,
        "Z4J_SCHEDULER_TRIGGER_GRPC_TLS_KEY",
    )
    ca = _read_required_pem(
        settings.trigger_grpc_tls_ca,
        "Z4J_SCHEDULER_TRIGGER_GRPC_TLS_CA",
    )
    return grpc.ssl_server_credentials(
        private_key_certificate_chain_pairs=[(key, cert)],
        root_certificates=ca,
        require_client_auth=True,
    )


def _read_required_pem(path_obj: Path | str | None, env_var: str) -> bytes:
    if path_obj is None:
        raise RuntimeError(
            f"trigger_grpc enabled but {env_var} is not set",
        )
    path = Path(path_obj)
    if not path.is_file():
        raise RuntimeError(
            f"{env_var} points at {path!s} which does not exist",
        )
    data = path.read_bytes()
    if not data.strip():
        raise RuntimeError(f"{env_var} file at {path!s} is empty")
    return data


__all__ = ["TriggerGrpcServer"]
