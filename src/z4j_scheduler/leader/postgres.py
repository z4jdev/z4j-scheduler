"""Postgres advisory-lock-backed leader election.

Replaces v1's :class:`SingleInstanceLeaderGate` with real
HA-capable election. Two or more scheduler instances point at the
same Postgres database (typically the brain's own DB); each tries
to ``pg_try_advisory_lock`` a deterministic namespace key. The one
that gets the lock is leader; the others stand by.

Failover model
--------------

Postgres advisory locks held via ``pg_advisory_lock`` are
session-scoped: when the holder's connection drops (TCP close,
process kill, network partition with TCP keepalive triggering),
Postgres automatically releases the lock. The standby's next
``pg_try_advisory_lock`` poll then succeeds and it becomes the
new leader.

Failover time is bounded by:

1. Postgres TCP keepalive timeout - kernel-level setting on the
   server, typically 30-60s on Linux defaults. Operators tune
   ``tcp_keepalive_*`` on the Postgres host or use shorter
   ``Z4J_SCHEDULER_LEADER_HEARTBEAT_SECONDS`` to drive client-side
   liveness checks that close the connection sooner.
2. The standby's poll interval (``leader_acquire_retry_seconds``).

A typical deployment sees failover within 1-3 seconds when the
leader process is killed cleanly (TCP RST is immediate) and within
30-60s on a hard network partition (waits for keepalive).

API
---

The gate satisfies the ``is_leader(project_id) -> bool`` Protocol
that :class:`~z4j_scheduler.tick.engine.TickEngine` consumes. The
``project_id`` argument is currently ignored - this is the GLOBAL
leader gate (one leader per scheduler cluster). The per-project
gate lands in step 4 of Phase 2 and uses one advisory lock per
project_id.

Lifecycle (``start`` / ``stop``) is async and matches the
``BrainClient`` / ``WatchStream`` pattern: the gate holds a
background task that refreshes the lock + standby retry. The tick
engine sees state changes within one heartbeat cycle.

Test seam
---------

The actual asyncpg calls live behind :class:`LockBackend` so unit
tests can drive the state machine without a real Postgres. The
integration test in ``tests/integration/`` uses
:class:`AsyncpgLockBackend` against a testcontainers Postgres.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # pragma: no cover
    from uuid import UUID

logger = logging.getLogger("z4j.scheduler.leader.postgres")


# =====================================================================
# Backend Protocol (test seam)
# =====================================================================


class LockBackend(Protocol):
    """Abstraction over the Postgres lock acquisition surface.

    Implementations open + own one network connection. The leader
    gate calls ``acquire`` on each cycle until it succeeds, then
    ``health_check`` on each subsequent cycle to detect connection
    death. ``release`` is best-effort (the connection close already
    drops the lock).
    """

    async def acquire(self, key: int) -> bool:
        """Try to acquire the advisory lock. Non-blocking.

        Returns True iff this connection holds the lock after the
        call. Multiple invocations against an already-held lock
        return True (idempotent at the connection level).
        """
        ...

    async def health_check(self) -> None:
        """Verify the connection is alive. Raises on failure.

        Must round-trip a query to the server. Returning normally
        does NOT guarantee the lock is still held by this
        connection - it's a liveness probe only. The lock-holder
        invariant is ensured by Postgres itself: the lock is
        bound to the connection lifetime.
        """
        ...

    async def release(self, key: int) -> None:
        """Best-effort lock release. Failures are swallowed."""
        ...

    async def close(self) -> None:
        """Tear down the connection. Idempotent."""
        ...


# =====================================================================
# Asyncpg backend (production)
# =====================================================================


class AsyncpgLockBackend:
    """Production :class:`LockBackend` backed by an asyncpg connection.

    One dedicated connection per gate instance - we deliberately do
    NOT share a pool because the lock is bound to the connection.
    Pool checkout could give us a different connection on the next
    call and we'd lose the lock.

    Connection setup:

    - ``application_name`` is set to ``z4j-scheduler-leader`` so
      operators can identify the connection in
      ``pg_stat_activity``.
    - TCP keepalive is enabled (server-side ``tcp_keepalives_*``
      drives the timing - we can't override from the client side
      without OS-specific socket options).
    """

    def __init__(self, *, dsn: str) -> None:
        self._dsn = dsn
        self._conn: object | None = None  # asyncpg.Connection at runtime

    async def _ensure_conn(self) -> object:
        if self._conn is not None:
            return self._conn
        try:
            import asyncpg
        except ImportError as exc:  # pragma: no cover - dep is required
            raise RuntimeError(
                "PostgresAdvisoryLockLeaderGate requires `asyncpg`",
            ) from exc
        self._conn = await asyncpg.connect(
            self._dsn,
            server_settings={"application_name": "z4j-scheduler-leader"},
        )
        return self._conn

    async def acquire(self, key: int) -> bool:
        conn = await self._ensure_conn()
        # ``pg_try_advisory_lock`` returns True iff the lock was
        # newly acquired OR was already held by this session
        # (Postgres reference-counts within a session, so a re-call
        # is a true no-op from the standby's perspective).
        granted = await conn.fetchval(  # type: ignore[attr-defined]
            "SELECT pg_try_advisory_lock($1::bigint)",
            key,
        )
        return bool(granted)

    async def health_check(self) -> None:
        conn = await self._ensure_conn()
        await conn.fetchval("SELECT 1")  # type: ignore[attr-defined]

    async def release(self, key: int) -> None:
        if self._conn is None:
            return
        try:
            await self._conn.fetchval(  # type: ignore[attr-defined]
                "SELECT pg_advisory_unlock($1::bigint)",
                key,
            )
        except Exception:
            # Connection may already be dead; the lock is gone
            # either way. Don't surface the error - release is
            # best-effort by contract.
            logger.debug(
                "z4j.scheduler.leader.postgres: release ignored error",
                exc_info=True,
            )

    async def close(self) -> None:
        if self._conn is None:
            return
        with contextlib.suppress(Exception):
            await self._conn.close()  # type: ignore[attr-defined]
        self._conn = None


# =====================================================================
# Shutdown-cleanup machinery (shared by both gates)
# =====================================================================

# Bound for each backend call made during stop() cleanup (release,
# close). Mirrors the 5s task-join bound in stop() so a hung
# connection cannot stall shutdown indefinitely: cleanup runs a
# finite number of calls and each is individually bounded, so every
# held lock still gets its release attempt even if an earlier one
# timed out.
_STOP_CLEANUP_OP_TIMEOUT_SECONDS = 5.0


async def _await_despite_cancel(task: asyncio.Task[None]) -> None:
    """Await ``task`` to completion, deferring caller cancellation.

    A bare ``asyncio.shield`` is NOT enough for shutdown cleanup:
    it protects the inner coroutine from cancellation, but when the
    ENCLOSING task is cancelled the outer ``await`` of the shield
    still raises CancelledError immediately, abandoning whatever
    cleanup steps remained (remaining releases, held.clear(),
    close()). This helper absorbs the cancel, keeps awaiting the
    SAME task until it finishes, then re-raises the CancelledError
    so the caller still observes its cancellation. R3-M5.
    """
    cancelled: asyncio.CancelledError | None = None
    while True:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if task.cancelled():
                # The cleanup task itself was cancelled (nothing is
                # left to wait for) - don't spin, propagate.
                raise
            # The CALLER was cancelled mid-cleanup. Remember it and
            # keep awaiting the same task; cleanup keeps running.
            cancelled = exc
            continue
        break
    if cancelled is not None:
        raise cancelled


# =====================================================================
# Leader gate
# =====================================================================


class PostgresAdvisoryLockLeaderGate:
    """Postgres advisory-lock-backed global leader gate.

    Construction is cheap (no I/O). :meth:`start` opens the
    connection and spawns the background acquisition / heartbeat
    loop. :meth:`is_leader` is synchronous and reads the cached
    state - safe to call from the tick engine's hot path.

    Args:
        backend: The :class:`LockBackend` implementation. Production
            wires :class:`AsyncpgLockBackend`; tests inject a fake.
        namespace: A free-form string identifying the scheduler
            cluster. Hashed to a bigint key so two clusters running
            against the same Postgres don't collide.
        heartbeat_seconds: Cadence for the leader's liveness probe
            and the standby's acquire retry. Lower = faster
            failover, higher = lower DB load.
    """

    def __init__(
        self,
        *,
        backend: LockBackend,
        namespace: str = "z4j-scheduler-global",
        heartbeat_seconds: float = 2.0,
    ) -> None:
        self._backend = backend
        self._key = _namespace_to_key(namespace)
        self._heartbeat_seconds = heartbeat_seconds
        self._is_leader = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # Set after the first acquisition cycle finishes so callers
        # can ``await gate.wait_for_first_cycle()`` to deflake tests.
        self._first_cycle = asyncio.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the background loop. Idempotent."""
        if self._task is not None:
            return
        self._stop_event.clear()
        self._first_cycle.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="z4j-scheduler-leader-gate",
        )

    async def stop(self) -> None:
        """Stop the loop, release the lock, close the connection.

        The ENTIRE sequence (loop join + release + close) runs inside
        one dedicated task guarded by :func:`_await_despite_cancel`.
        R3-M5 moved only the release+close tail into a task, which
        left a gap round 4 reproduced: a cancel landing during the
        loop JOIN (before the cleanup task existed) escaped stop()
        with zero releases, the lock still held, and the backend
        open. Creating the task FIRST makes every phase
        cancellation-immune; the caller's cancellation is re-raised
        after cleanup completes. R4-M4.
        """
        self._stop_event.set()
        stopper = asyncio.create_task(
            self._stop_impl(),
            name="z4j-scheduler-leader-gate-stop",
        )
        await _await_despite_cancel(stopper)

    async def _stop_impl(self) -> None:
        """Join the loop, then release + close (see stop()).

        A cancel mid-release would otherwise leave the lock held
        until the asyncpg session timeout, blocking standby
        instances from leadership for tens of seconds and opening a
        HA split-brain gap on rolling redeploy. R3-M5.
        """
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except TimeoutError:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._task
            self._task = None
        await self._release_and_close()

    async def _release_and_close(self) -> None:
        """Full stop() cleanup; runs inside its own task (see stop()).

        Each backend call is individually bounded so a hung
        connection can't stall shutdown, and failures are logged and
        swallowed so ``close`` is always reached.
        """
        if self._is_leader:
            try:
                await asyncio.wait_for(
                    self._backend.release(self._key),
                    timeout=_STOP_CLEANUP_OP_TIMEOUT_SECONDS,
                )
            except Exception:
                logger.exception(
                    "z4j leader: release failed during stop",
                )
        self._is_leader = False
        try:
            await asyncio.wait_for(
                self._backend.close(),
                timeout=_STOP_CLEANUP_OP_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.exception(
                "z4j leader: backend close failed during stop",
            )

    async def wait_for_first_cycle(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109  public API timeout param delegated to asyncio.wait_for
        """Block until the loop has done at least one acquisition pass."""
        await asyncio.wait_for(self._first_cycle.wait(), timeout=timeout)

    # ------------------------------------------------------------------
    # Read surface (Protocol-conforming)
    # ------------------------------------------------------------------

    def is_leader(self, project_id: UUID) -> bool:
        """Synchronous leader check. Project id ignored in global mode."""
        return self._is_leader

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        """Acquire + refresh loop.

        Two states:

        - **Standby** (not leader): try to acquire. On success,
          transition to leader and log it.
        - **Leader**: probe the connection. Any error means we lost
          the lock; transition back to standby and start trying to
          re-acquire on the next cycle.

        Both paths handle backend exceptions identically - drop the
        connection, mark non-leader, retry next cycle.
        """
        try:
            while not self._stop_event.is_set():
                try:
                    if not self._is_leader:
                        granted = await self._backend.acquire(self._key)
                        if granted:
                            self._is_leader = True
                            logger.info(
                                "z4j.scheduler.leader.postgres: became LEADER (key=%d)",
                                self._key,
                            )
                    else:
                        # Stay-alive probe. If the connection died
                        # silently (network partition, server
                        # restart) this raises and the except branch
                        # demotes us to standby.
                        await self._backend.health_check()
                except Exception:
                    if self._is_leader:
                        logger.warning(
                            "z4j.scheduler.leader.postgres: "
                            "LOST LEADER status (key=%d) - reason: %s",
                            self._key,
                            "backend error",
                        )
                    self._is_leader = False
                    # Force the connection closed so the next cycle
                    # opens a fresh one (the existing one is in an
                    # unknown state).
                    with contextlib.suppress(Exception):
                        await self._backend.close()

                # Notify first-cycle waiters AFTER state has settled.
                self._first_cycle.set()

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._heartbeat_seconds,
                    )
                    # stop_event fired: exit cleanly.
                    return
                except TimeoutError:
                    continue
        except asyncio.CancelledError:  # noqa: TRY203  explicit CancelledError propagation
            raise


# =====================================================================
# Helpers
# =====================================================================


def _namespace_to_key(namespace: str) -> int:
    """Hash a namespace string to a 64-bit signed int.

    Postgres advisory-lock keys are bigint (signed 64-bit). We
    derive a deterministic key from a free-form namespace so two
    operators running independent scheduler clusters against the
    same Postgres can pick distinct namespaces and not collide.

    SHA-256 truncated to 8 bytes, interpreted as signed bigint
    (modulo 2^63 to stay positive on Python's int side - Postgres
    doesn't care about sign for advisory locks).
    """
    digest = hashlib.sha256(namespace.encode()).digest()
    raw = int.from_bytes(digest[:8], "big", signed=False)
    # Postgres bigint range is [-2^63, 2^63-1]; mask to fit.
    return raw & ((1 << 63) - 1)


# =====================================================================
# Per-project leader gate
# =====================================================================


class PerProjectLeaderGate:
    """Postgres advisory-lock-backed PER-PROJECT leader gate.

    Different from :class:`PostgresAdvisoryLockLeaderGate` which is
    a single global lock: this one races for one advisory lock per
    project_id, so the cluster naturally load-balances work. A
    cluster of N scheduler instances + M projects sees roughly M/N
    projects led by each instance once steady state is reached.

    Acquisition strategy
    --------------------

    The gate periodically asks ``project_source`` for the current
    set of projects in scope, then on each cycle:

    1. For each project NOT currently held: try ``pg_try_advisory_lock``.
       Successful → mark held; failed (another instance holds it) →
       try again next cycle.
    2. For each project currently held but no longer in source:
       release the lock (the schedule was deleted, or the operator
       restricted scope).
    3. Health-check the connection. On error, drop ALL held locks
       (they were all bound to that one connection, Postgres
       released them when the connection died) and let the next
       cycle re-establish.

    One asyncpg connection holds every lock - Postgres advisory
    locks are per-session, and one session can hold many. This
    keeps the connection footprint constant regardless of how many
    projects we lead.

    The synchronous :meth:`is_leader` check reads from a cached
    ``set`` so the tick engine's hot path stays cheap.
    """

    def __init__(
        self,
        *,
        backend: LockBackend,
        project_source,
        namespace: str = "z4j-scheduler-projects",
        heartbeat_seconds: float = 2.0,
    ) -> None:
        self._backend = backend
        self._project_source = project_source
        self._namespace = namespace
        self._heartbeat_seconds = heartbeat_seconds
        self._held: dict[object, int] = {}  # project_id → lock_key
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._first_cycle = asyncio.Event()

    async def start(self) -> None:
        """Spawn the background loop. Idempotent."""
        if self._task is not None:
            return
        self._stop_event.clear()
        self._first_cycle.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="z4j-scheduler-per-project-gate",
        )

    async def stop(self) -> None:
        """Stop loop, release locks, close connection.

        The ENTIRE sequence (loop join + every release + held.clear()
        + close) runs inside one dedicated task guarded by
        :func:`_await_despite_cancel`. R3-M5 protected only the
        release tail; round 4 reproduced a cancel during the loop
        JOIN (before the cleanup task existed) escaping with all
        locks still held and the backend open. Creating the task
        FIRST makes every phase cancellation-immune; the caller's
        cancellation is re-raised after cleanup completes. R4-M4.
        """
        self._stop_event.set()
        stopper = asyncio.create_task(
            self._stop_impl(),
            name="z4j-scheduler-per-project-gate-stop",
        )
        await _await_despite_cancel(stopper)

    async def _stop_impl(self) -> None:
        """Join the loop, then release-all + close (see stop()).

        A cancel mid-release would otherwise leave per-project
        advisory locks dangling and block standby promotion. R3-M5.
        """
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except TimeoutError:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._task
            self._task = None
        await self._release_all_and_close()

    async def _release_all_and_close(self) -> None:
        """Full stop() cleanup; runs inside its own task (see stop()).

        Each backend call is individually bounded so a hung
        connection can't stall shutdown, and failures are logged and
        swallowed so every held key gets its release attempt and
        ``close`` is always reached.
        """
        for key in list(self._held.values()):
            try:
                await asyncio.wait_for(
                    self._backend.release(key),
                    timeout=_STOP_CLEANUP_OP_TIMEOUT_SECONDS,
                )
            except Exception:
                # stdlib logger: %-format, NOT a structlog ``key=`` kwarg.
                # A TypeError HERE (inside the except) would abort the
                # release loop before self._held.clear() + backend.close(),
                # leaving per-project advisory locks HELD + the connection
                # open -> a standby cannot promote (split-brain window). B17.
                logger.exception(
                    "z4j leader: per-project release failed for key=%s",
                    key,
                )
        self._held.clear()
        try:
            await asyncio.wait_for(
                self._backend.close(),
                timeout=_STOP_CLEANUP_OP_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.exception(
                "z4j leader: per-project backend close failed",
            )

    async def wait_for_first_cycle(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109  public API timeout param delegated to asyncio.wait_for
        await asyncio.wait_for(self._first_cycle.wait(), timeout=timeout)

    def is_leader(self, project_id) -> bool:
        """Synchronous read of whether we currently hold this project's lock."""
        return project_id in self._held

    def held_projects(self) -> set:
        """Snapshot of project_ids this instance currently leads.

        Used by observability / dashboard to show "instance X leads
        N projects." Returns a copy so callers can safely iterate.
        """
        return set(self._held.keys())

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    projects = await _resolve_project_source(
                        self._project_source,
                    )
                    desired: dict[object, int] = {
                        pid: _project_key(self._namespace, pid) for pid in projects
                    }

                    # Acquire missing.
                    for pid, key in desired.items():
                        if pid in self._held:
                            continue
                        try:
                            granted = await self._backend.acquire(key)
                        except Exception:  # noqa: TRY203  documents bail to outer handler
                            # Connection died mid-acquisition. Bail
                            # out of this cycle; the outer except
                            # handles cleanup.
                            raise
                        if granted:
                            self._held[pid] = key
                            logger.info(
                                "z4j.scheduler.leader.postgres: acquired project=%s (key=%d)",
                                pid,
                                key,
                            )

                    # Release dropped.
                    for pid in list(self._held):
                        if pid not in desired:
                            with contextlib.suppress(Exception):
                                await self._backend.release(self._held[pid])
                            del self._held[pid]
                            logger.info(
                                "z4j.scheduler.leader.postgres: "
                                "released project=%s (no longer in scope)",
                                pid,
                            )

                    # Liveness probe.
                    await self._backend.health_check()
                except Exception:
                    if self._held:
                        logger.warning(
                            "z4j.scheduler.leader.postgres: "
                            "LOST %d project lock(s) due to backend error",
                            len(self._held),
                        )
                    self._held.clear()
                    with contextlib.suppress(Exception):
                        await self._backend.close()

                self._first_cycle.set()

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._heartbeat_seconds,
                    )
                    return
                except TimeoutError:
                    continue
        except asyncio.CancelledError:  # noqa: TRY203  explicit CancelledError propagation
            raise


def _project_key(namespace: str, project_id) -> int:
    """Derive a 63-bit lock key from (namespace, project_id).

    Includes the namespace so a single Postgres instance can host
    multiple scheduler clusters (prod + staging) without colliding
    on the same project_id across clusters.
    """
    seed = f"{namespace}::{project_id}"
    return _namespace_to_key(seed)


async def _resolve_project_source(source) -> list:
    """Call ``project_source`` accepting both sync and async callables.

    The cache exposes ``snapshot()`` as a coroutine; tests prefer
    plain functions returning a list. Adapter at the seam keeps the
    contract permissive.
    """
    result = source()
    if asyncio.iscoroutine(result):
        result = await result
    return list(result)


__all__ = [
    "AsyncpgLockBackend",
    "LockBackend",
    "PerProjectLeaderGate",
    "PostgresAdvisoryLockLeaderGate",
]
