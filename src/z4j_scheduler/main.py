"""Application factory + lifespan for the scheduler process.

Composes every subsystem into a single asyncio-driven process:

1. Loads :class:`~z4j_scheduler.settings.Settings`
2. Configures structured logging
3. Builds + connects the :class:`BrainClient` (gRPC to brain)
4. Builds the :class:`ScheduleCache`
5. Builds the leader gate (single-instance default; HA in Phase 3)
6. Builds the :class:`TickEngine`
7. Builds the :class:`WatchStream` consumer
8. Builds the :class:`FireDispatcher`
9. Builds the :class:`SchedulerState` + FastAPI app
10. Starts uvicorn for the HTTP surface
11. Starts the watch stream + tick engine in an asyncio.TaskGroup
12. Wires SIGTERM / SIGINT for graceful shutdown
13. On stop: drains in-flight fires, closes streams, closes the gRPC
    channel, stops uvicorn

Stop order is the inverse of start order so dependencies don't
break mid-shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress
from typing import TYPE_CHECKING, Literal

import grpc
import uvicorn

from z4j_scheduler.api._state import SchedulerState
from z4j_scheduler.api.app import create_app
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.leader import SingleInstanceLeaderGate
from z4j_scheduler.observability.logging import configure_logging
from z4j_scheduler.storage._protocol import (
    ProtocolNegotiationError,
    current_capabilities,
    require_exact_current,
)
from z4j_scheduler.storage.brain_client import BrainClient
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.quarantine import QuarantineReporter
from z4j_scheduler.storage.watch import WatchStream
from z4j_scheduler.tick.cadence import cadence_runtime_fingerprint
from z4j_scheduler.tick.engine import TickEngine

if TYPE_CHECKING:  # pragma: no cover
    from z4j_scheduler.settings import Settings

logger = logging.getLogger("z4j.scheduler.main")


async def _select_protocol_mode(
    client: BrainClient,
) -> Literal["legacy", "current"]:
    """Select exactly one authenticated Brain/scheduler protocol.

    Legacy is accepted only when both independent legacy facts agree:
    Ping advertises epoch zero and negotiation is not implemented. Every
    partial, future, or contradictory response fails startup closed.
    """

    ping = await client.ping()
    offered = current_capabilities(
        cadence_runtime_fingerprint=cadence_runtime_fingerprint(),
    )
    try:
        selected = await client.negotiate_protocol(offered)
    except grpc.RpcError as exc:
        if ping.scheduler_protocol_epoch == 0 and exc.code() is grpc.StatusCode.UNIMPLEMENTED:
            return "legacy"
        raise ProtocolNegotiationError(
            "Brain protocol negotiation failed without an exact legacy pair",
        ) from exc
    require_exact_current(
        selected=selected,
        expected=offered,
        ping_protocol_epoch=ping.scheduler_protocol_epoch,
    )
    return "current"


class SchedulerApp:
    """The scheduler process lifecycle.

    Construction is cheap and synchronous - the real work happens
    in :meth:`start` / :meth:`run` / :meth:`stop`.

    Typical use::

        settings = Settings()
        app = SchedulerApp(settings)
        await app.start()
        await app.run()  # blocks until SIGTERM / SIGINT
        await app.stop()

    The CLI's ``serve`` command wraps this; tests construct directly
    with a synthetic Settings + injected fakes.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        # Subsystem references - populated in start(), torn down in stop().
        self._client: BrainClient | None = None
        self._cache: ScheduleCache | None = None
        # The leader gate may be either implementation; the
        # ``stop()`` path checks for an async ``stop`` method to
        # decide whether to await teardown.
        self._leader_gate: object | None = None
        self._tick_engine: TickEngine | None = None
        self._watch: WatchStream | None = None
        self._quarantine_reporter: QuarantineReporter | None = None
        self._dispatcher: FireDispatcher | None = None
        self._state: SchedulerState | None = None
        self._uvicorn_server: uvicorn.Server | None = None
        # TriggerSchedule reverse-direction gRPC server. Constructed
        # in start(), torn down in stop(). Stays None when disabled.
        self._trigger_server: object | None = None
        self._stop_event = asyncio.Event()
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open every subsystem in dependency order. Idempotent."""
        if self._started:
            return

        configure_logging(self.settings)
        logger.info(
            "z4j.scheduler.main: starting (instance_id=%s)",
            self.settings.instance_id,
        )

        # --- Subsystems (order matters for dependency direction) ---

        # 1. Brain client first - everything else depends on the gRPC
        #    connection being open.
        client = self._build_brain_client()
        self._client = client
        await client.connect()
        try:
            protocol_mode = await _select_protocol_mode(client)
        except BaseException:
            # start() has not reached the normal teardown lifecycle yet.
            await client.close()
            raise

        # 2. Cache - in-memory, instant.
        self._cache = ScheduleCache()
        if protocol_mode == "current":
            self._quarantine_reporter = QuarantineReporter(
                client=client,
                cache=self._cache,
            )

        # 3. Leader gate - backend selected by settings. ``single``
        #    is the v1 default (no infra deps); ``postgres`` is the
        #    HA backend that replaces the always-true gate with a
        #    real advisory-lock race across multiple instances.
        self._leader_gate = await self._build_leader_gate()
        # Teach the cache which projects this instance leads, so the
        # watch echo-merge preserves engine fire-state ONLY on the leader and a
        # follower adopts the leader's brain-advanced state (converges instead of
        # busy-spinning / replaying after failover).
        self._cache.is_leader = self._leader_gate.is_leader

        # 4. Dispatcher - uses brain client for fire delivery.
        self._dispatcher = FireDispatcher(
            client=client,
            settings=self.settings,
        )

        # 5. Tick engine - reads cache, calls dispatcher when leader.
        # Wrap the gate so every is_leader() call updates the
        # ``z4j_scheduler_is_leader`` Prometheus gauge - one cheap
        # branch per tick keeps the metric current without an extra
        # background task. The gauge is the operator's primary
        # signal during failover ("which instance is hot right now").
        # 6. Watch stream - keeps the cache hot. Construct BEFORE
        # the engine so we can pass its is_healthy probe in.
        # ``reconcile_interval_seconds`` drives the defensive
        # periodic full re-sync alongside the live event stream
        # (see ``storage/watch.py``).
        self._watch = WatchStream(
            client=client,
            cache=self._cache,
            full_resync_interval_seconds=float(
                self.settings.reconcile_interval_seconds,
            ),
            protocol_mode=protocol_mode,
            protocol_selector=lambda: _select_protocol_mode(client),
        )

        # 5b. Tick engine. The engine receives the watch's
        # ``is_healthy`` probe so it refuses to fire while the
        # cache is stale (stream-down window). The engine never
        # holds a reference to the watch stream itself - just to
        # its bool getter - so test fixtures can plug in their
        # own.
        self._tick_engine = TickEngine(
            cache=self._cache,
            leader_gate=_GaugePublishingLeaderGate(self._leader_gate),
            dispatcher=self._dispatcher,
            watch_healthy=lambda: self._watch.is_healthy if self._watch else True,
            # The promotion-scoped grace must cover THIS deployment's
            # failover latency. Feed it the configured leader heartbeat so a slow
            # heartbeat (supported up to 60s) does not make a promoted leader
            # classify the slot it parked as a follower as "missed" and drop it.
            leader_heartbeat_seconds=getattr(self.settings, "leader_heartbeat_seconds", 2.0),
            quarantine_reporter=self._quarantine_reporter,
        )

        # 6.5 TriggerSchedule gRPC server (Phase 2). Off by default.
        # Constructed unconditionally so the symmetric stop call in
        # teardown is simple; ``.start()`` short-circuits when
        # disabled. Importing the gRPC runtime is deferred to start()
        # so installs without the trigger server pay no cost.
        self._trigger_server = await self._build_trigger_grpc_server()

        # 7. Shared state for /health, /ready, /info endpoints.
        self._state = SchedulerState(
            settings=self.settings,
            cache=self._cache,
            client=self._client,
        )
        # Mark subsystems up - the watch stream will flip
        # cache_initial_sync_complete when its first sync finishes.
        self._state.brain_client_connected = True
        # Leader gate is "ready" the moment it exists. For the
        # postgres backend, that means the background task is
        # running; the actual leader/standby state is observable
        # via :meth:`is_leader` once the first acquisition cycle
        # completes (typically <1s).
        self._state.leader_gate_initialised = True

        # 8. uvicorn server for the HTTP surface.
        self._uvicorn_server = self._build_uvicorn_server(self._state)

        self._started = True
        logger.info("z4j.scheduler.main: subsystems initialised")

    async def run(self) -> None:
        """Run all background tasks until :meth:`stop` is called.

        Blocks the caller. Inside, uses :class:`asyncio.TaskGroup` so
        cancellation propagates cleanly to every task.
        """
        if not self._started:
            raise RuntimeError("call start() before run()")
        assert self._tick_engine is not None
        assert self._watch is not None
        assert self._uvicorn_server is not None
        assert self._state is not None

        logger.info("z4j.scheduler.main: running")
        try:
            async with asyncio.TaskGroup() as tg:
                # The watch stream is what populates the cache. Mark
                # cache_initial_sync_complete after the first sync
                # by wrapping the watch task.
                tg.create_task(
                    self._watch_with_ready_signal(),
                    name="watch",
                )
                tg.create_task(self._tick_engine.run(), name="tick")
                if self._quarantine_reporter is not None:
                    tg.create_task(
                        self._quarantine_reporter.run(),
                        name="quarantine_reporter",
                    )
                tg.create_task(
                    self._uvicorn_server.serve(),
                    name="uvicorn",
                )
                tg.create_task(
                    self._await_stop_then_cancel(),
                    name="shutdown_watcher",
                )
        except* asyncio.CancelledError:
            # Expected during shutdown - the TaskGroup raises this
            # when one of its tasks is cancelled.
            logger.info("z4j.scheduler.main: tasks cancelled")
        logger.info("z4j.scheduler.main: run() returning")

    async def stop(self) -> None:
        """Trigger graceful shutdown + tear down subsystems.

        Idempotent. Safe to call from any coroutine, from a signal
        handler, or from a process supervisor.

        Stop order (inverse of start):
        1. Signal stop_event - wakes the run() loop's TaskGroup
           shutdown watcher
        2. Tell uvicorn to stop accepting new requests
        3. Stop the tick engine (no new dispatches)
        4. Stop the watch stream (no new cache mutations)
        5. Close the brain client gRPC channel
        """
        self._stop_event.set()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True
        # TriggerSchedule server first - stops accepting brain calls
        # before we tear down the dispatcher and cache it depends on.
        if self._trigger_server is not None:
            with suppress(Exception):
                await self._trigger_server.stop()  # type: ignore[attr-defined]
        if self._tick_engine is not None:
            await self._tick_engine.stop()
        if self._quarantine_reporter is not None:
            await self._quarantine_reporter.stop()
        if self._watch is not None:
            await self._watch.stop()
        # Leader gate: only the postgres backend has an async stop;
        # the single-instance gate is stateless. Detect by attribute
        # so the same teardown path works for both backends.
        gate_stop = getattr(self._leader_gate, "stop", None)
        if callable(gate_stop):
            with suppress(Exception):
                await gate_stop()
        if self._client is not None:
            with suppress(Exception):
                await self._client.close()
        logger.info("z4j.scheduler.main: stopped")

    # ------------------------------------------------------------------
    # Internals (overridable in tests)
    # ------------------------------------------------------------------

    def _build_brain_client(self) -> BrainClient:
        """Construct the brain client. Override in tests to inject a fake."""
        return BrainClient(self.settings)

    async def _build_leader_gate(self) -> object:
        """Construct + start the leader gate per ``leader_backend`` setting.

        - ``single``: always-true gate. No async work, no infra.
        - ``postgres``: global gate. One advisory lock per cluster;
          one instance leads everything.
        - ``postgres_per_project``: per-project gate. One advisory
          lock per project_id; the cluster naturally load-balances.
          Requires the ScheduleCache to be available so the gate can
          discover which projects to compete for.

        Override in tests to inject a fake.
        """
        backend = self.settings.leader_backend
        if backend == "single":
            return SingleInstanceLeaderGate()
        if backend in ("postgres", "postgres_per_project"):
            if self.settings.leader_pg_dsn is None:
                raise RuntimeError(
                    f"leader_backend={backend!r} requires Z4J_SCHEDULER_LEADER_PG_DSN to be set",
                )
            from z4j_scheduler.leader.postgres import (
                AsyncpgLockBackend,
                PerProjectLeaderGate,
                PostgresAdvisoryLockLeaderGate,
            )

            lock_backend = AsyncpgLockBackend(
                dsn=self.settings.leader_pg_dsn.get_secret_value(),
            )
            if backend == "postgres":
                gate = PostgresAdvisoryLockLeaderGate(
                    backend=lock_backend,
                    namespace=self.settings.leader_namespace,
                    heartbeat_seconds=(self.settings.leader_heartbeat_seconds),
                )
            else:
                # Per-project: project_source pulls the unique
                # project_ids out of the cache snapshot. Cache may
                # be empty during the boot window (before the first
                # WatchSchedules sync); the gate handles that
                # gracefully (no projects → no locks held).
                assert self._cache is not None

                async def _project_source() -> list:
                    snap = await self._cache.snapshot()  # type: ignore[union-attr]
                    return list({entry.project_id for entry in snap})

                gate = PerProjectLeaderGate(
                    backend=lock_backend,
                    project_source=_project_source,
                    namespace=self.settings.leader_namespace,
                    heartbeat_seconds=(self.settings.leader_heartbeat_seconds),
                )
            await gate.start()
            return gate
        raise RuntimeError(f"unknown leader_backend {backend!r}")

    async def _build_trigger_grpc_server(self) -> object | None:
        """Construct + start the TriggerSchedule gRPC server.

        Returns ``None`` when ``trigger_grpc_enabled=False`` so the
        teardown path can short-circuit cleanly. Starts the gRPC
        runtime when enabled - failure to bind raises so the
        operator sees the misconfiguration loudly at boot, rather
        than discovering missing certs the first time someone
        clicks "fire now."

        Override in tests to inject a fake.
        """
        if not self.settings.trigger_grpc_enabled:
            return None
        from z4j_scheduler.trigger_grpc.server import (
            TriggerGrpcServer,
        )

        assert self._cache is not None
        assert self._dispatcher is not None
        # Pass the leader gate so the TriggerScheduleServicer can
        # reject standby-side trigger calls. Single-instance
        # deployments use the always-True
        # SingleInstanceLeaderGate so behavior is unchanged; HA
        # deployments use PostgresAdvisoryLockLeaderGate which
        # correctly returns False on standbys.
        assert self._leader_gate is not None
        server = TriggerGrpcServer(
            settings=self.settings,
            cache=self._cache,
            dispatcher=self._dispatcher,
            leader_gate=self._leader_gate,
        )
        await server.start()
        return server

    async def _watch_with_ready_signal(self) -> None:
        """Run the watch stream + flip cache_initial_sync_complete.

        Wraps :meth:`WatchStream._full_sync` so the FIRST successful
        sync flips the readiness flag - subsequent re-syncs (after
        reconnect) don't reset it.
        """
        assert self._watch is not None
        assert self._state is not None

        original_full_sync = self._watch._full_sync
        ready_already = False

        async def patched_full_sync() -> None:
            nonlocal ready_already
            await original_full_sync()
            if not ready_already:
                self._state.cache_initial_sync_complete = True  # type: ignore[union-attr]
                ready_already = True
                logger.info(
                    "z4j.scheduler.main: cache initial sync complete",
                )

        self._watch._full_sync = patched_full_sync  # type: ignore[method-assign]
        await self._watch.run()

    async def _await_stop_then_cancel(self) -> None:
        """Wait for stop_event, then cancel sibling tasks.

        TaskGroup catches CancelledError raised here and unwinds the
        whole group cleanly.
        """
        await self._stop_event.wait()
        logger.info("z4j.scheduler.main: stop signalled, cancelling tasks")
        # Tell each subsystem to wind down gracefully.
        if self._tick_engine is not None:
            await self._tick_engine.stop()
        if self._watch is not None:
            await self._watch.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True
        # And raise to unwind the TaskGroup.
        raise asyncio.CancelledError("z4j-scheduler graceful shutdown")

    def _build_uvicorn_server(
        self,
        state: SchedulerState,
    ) -> uvicorn.Server:
        """Construct the uvicorn Server for the HTTP surface."""
        app = create_app(state)
        # Log the bind interface at INFO so
        # operators see in their boot logs exactly what surface
        # they're exposing (``0.0.0.0`` = all interfaces; a
        # specific IP = that NIC; ``127.0.0.1`` = loopback only).
        # The ``/info`` endpoint is now redacted of topology data,
        # but this transparency matters if a future change adds
        # any non-trivial response to /health, /ready, or /metrics.
        logger.info(
            "z4j.scheduler.api: binding HTTP server to %s:%d (/info, /health, /ready, /metrics)",
            self.settings.bind_host,
            self.settings.bind_port,
        )
        config = uvicorn.Config(
            app,
            host=self.settings.bind_host,
            port=self.settings.bind_port,
            # We already configured structlog; tell uvicorn to leave
            # logging alone (otherwise it overrides with its own
            # access-log format).
            log_config=None,
            access_log=False,
        )
        return uvicorn.Server(config)


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------


#: Module-level set holds strong references to in-flight stop()
#: tasks spawned by signal handlers. Without keeping a reference,
#: ``loop.create_task(app.stop())`` lets the asyncio loop GC the
#: task mid-shutdown, CPython surfaces this as the noisy "Task
#: was destroyed while pending" warning, and on a slow drain
#: (Postgres advisory locks, gRPC graceful close) the cleanup
#: never completes and standby promotion stalls.
_SIGNAL_STOP_TASKS: set[asyncio.Task[None]] = set()


def install_signal_handlers(
    app: SchedulerApp,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Register SIGTERM + SIGINT to call :meth:`SchedulerApp.stop`.

    Best-effort - on platforms (Windows) where ``loop.add_signal_handler``
    is not supported, the operator relies on Ctrl-C raising
    KeyboardInterrupt which propagates through the event loop normally.
    """
    target_loop = loop or asyncio.get_event_loop()

    def _handler(signum: int) -> None:
        logger.info(
            "z4j.scheduler.main: received signal %d, requesting stop",
            signum,
        )
        # Retain a strong reference until the stop() task completes.
        task = target_loop.create_task(
            app.stop(),
            name=f"z4j-scheduler-stop-sig{signum}",
        )
        _SIGNAL_STOP_TASKS.add(task)
        task.add_done_callback(_SIGNAL_STOP_TASKS.discard)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            target_loop.add_signal_handler(sig, _handler, sig)
        except NotImplementedError:
            # Windows asyncio loop does not support add_signal_handler
            # for SIGTERM. Operators on Windows rely on Ctrl-C.
            logger.debug(
                "z4j.scheduler.main: signal %d not installable on this "
                "platform - relying on KeyboardInterrupt",
                sig,
            )


# ---------------------------------------------------------------------------
# Entry point - awaitable that wires settings -> app -> signals -> run
# ---------------------------------------------------------------------------


async def run_from_settings(settings: Settings) -> int:
    """Run the scheduler from a fully-resolved Settings.

    The CLI's ``serve`` subcommand calls this after constructing
    Settings. Returns the process exit code (0 on clean shutdown,
    nonzero on startup failure).
    """
    app = SchedulerApp(settings)
    install_signal_handlers(app)
    try:
        await app.start()
    except Exception:
        logger.exception("z4j.scheduler.main: startup failed")
        with suppress(Exception):
            await app.stop()
        return 1
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("z4j.scheduler.main: interrupted")
    finally:
        await app.stop()
    return 0


class _GaugePublishingLeaderGate:
    """Wrap a LeaderGate so each ``is_leader`` call updates the gauge.

    Sits between the tick engine and the real gate. The tick engine
    calls ``is_leader(project_id)`` once per project per tick; this
    wrapper publishes the result to ``z4j_scheduler_is_leader`` so
    operators can graph "who is leader right now" across the cluster
    in Prometheus.

    The wrapper is intentionally trivial - keeping the side effect
    out of the real gate means the gate stays test-isolated and the
    Prometheus dep doesn't leak into the leader module.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def is_leader(self, project_id: object) -> bool:
        from z4j_scheduler.observability.metrics import (
            is_leader as _gauge,
        )

        result = bool(self._inner.is_leader(project_id))  # type: ignore[attr-defined]
        # One label value per project so the gauge is queryable at
        # the same granularity the gate is. Global-mode gates pass
        # the same value for every project_id, which is fine - the
        # gauge just shows N project labels with identical values.
        # Metrics must never break ticks. Swallow.
        with suppress(Exception):
            _gauge.labels(project=str(project_id)).set(1.0 if result else 0.0)
        return result


__all__ = ["SchedulerApp", "install_signal_handlers", "run_from_settings"]
