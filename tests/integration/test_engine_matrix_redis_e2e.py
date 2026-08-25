"""Redis-backed live e2e for engines that need a real broker.

Runs when EITHER:

- ``REDIS_URL`` env var is set (operator points at a real Redis
  via docker-compose, testcontainers, or local install), OR
- ``fakeredis`` is installed (we fall back to in-process fakeredis
  which most engines accept as a drop-in replacement).

This file delivers live-execution e2e for the 4 engines that
``test_engine_matrix_live_e2e.py`` had to skip:

| Engine | Real broker required | Test status |
|--------|----------------------|-------------|
| celery | redis broker         | runs if REDIS_URL set |
| rq     | redis connection     | runs if fakeredis OR REDIS_URL |
| arq    | redis pool           | runs if fakeredis OR REDIS_URL |

(Taskiq is the odd one out: InMemoryBroker doesn't auto-execute,
so its live test would need a Redis-backed taskiq broker. Skipped
from this file; covered by docker-compose harness if needed.)

What this proves
================

When run with REDIS_URL pointing at a real Redis (or fakeredis),
the dispatcher → adapter → broker → worker → task body chain is
exercised end-to-end. The task's sentinel side-effect proves the
worker actually invoked the function.

This closes the "Live broker e2e" backlog item from the audit
report for celery / rq / arq, in addition to the dramatiq +
huey coverage in ``test_engine_matrix_live_e2e.py``.

Set ``Z4J_REQUIRE_REAL_BROKER_E2E=1`` in a release lane.  In that
mode a missing ``REDIS_URL`` or missing engine dependency is a
failure, never a skip or fakeredis fallback.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import importlib
import json
import os
from pathlib import Path

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

from tests.integration.helpers import live_tasks

_REDIS_URL = os.environ.get("REDIS_URL")
_REQUIRE_REAL_BROKER = os.environ.get("Z4J_REQUIRE_REAL_BROKER_E2E", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


# NOTE on resource hygiene: the suite-wide ``filterwarnings = error``
# policy turns any ResourceWarning raised inside a destructor into an
# unraisable exception, which pytest pins on whichever test happens to
# be running when the GC fires - often a test in a LATER module. Every
# broker client created in this module must therefore be closed
# deterministically before the test returns (fixture finalizers and
# try/finally below). Do not add ``filterwarnings`` ignores here; a
# leak that only shows up at GC time is still a leak.


@pytest.fixture(autouse=True)
def _collect_cycles_eagerly():
    """Run the cycle collector after each test in this module.

    The broker clients built here (celery app, kombu channels, RQ
    worker) are cyclic object graphs, so they die in the generational
    collector, not by refcount. Collecting right away means that if a
    test DOES leak an open resource, the resulting unraisable error
    lands on the guilty test instead of an innocent later one. This is
    attribution, not suppression - the explicit close calls in the
    fixtures/teardowns are what actually prevent the warnings.
    """
    yield
    gc.collect()


def _have_fakeredis() -> bool:
    try:
        import fakeredis  # noqa: F401

        return True
    except ImportError:
        return False


def _require_engine_module(name: str) -> None:
    """Import an engine dependency, failing closed in the requested lane."""

    try:
        importlib.import_module(name)
    except ImportError as exc:
        message = f"real-broker lane requires importable engine dependency {name!r}: {exc}"
        if _REQUIRE_REAL_BROKER:
            pytest.fail(message)
        pytest.skip(message)


@pytest.fixture(scope="module", autouse=True)
def _requested_lane_has_real_redis() -> None:
    if _REQUIRE_REAL_BROKER and not _REDIS_URL:
        pytest.fail(
            "Z4J_REQUIRE_REAL_BROKER_E2E is set but REDIS_URL is missing; "
            "the requested release lane may not fall back to fakeredis or skip",
        )


def _make_redis_connection():
    """Return (connection, mode) where mode is 'real' or 'fake'."""
    if _REDIS_URL:
        import redis

        return redis.from_url(_REDIS_URL), "real"
    if _have_fakeredis():
        import fakeredis

        return fakeredis.FakeStrictRedis(), "fake"
    pytest.skip(
        "no Redis available: set REDIS_URL=redis://host:port or "
        "`pip install fakeredis` to enable live-broker e2e",
    )
    return None, ""


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


def _close_sync_redis(conn) -> None:
    """Close a sync redis client AND its connection pool.

    ``Redis.close()`` alone is not always enough: connections checked
    out (or parked) in the pool keep their sockets open until GC,
    where ``socket.__del__`` raises ResourceWarning under the strict
    warning policy. ``disconnect()`` closes every pooled socket now.
    Works for fakeredis too (same client API).
    """
    with contextlib.suppress(Exception):
        conn.close()
    pool = getattr(conn, "connection_pool", None)
    if pool is not None:
        with contextlib.suppress(Exception):
            pool.disconnect()


@pytest.fixture
def redis_conn():
    """(connection, mode) pair with a finalizer that closes both the
    client and every socket in its pool."""
    conn, mode = _make_redis_connection()
    yield conn, mode
    _close_sync_redis(conn)


# =====================================================================
# RQ
# =====================================================================


@pytest.mark.asyncio
async def test_rq_live_e2e_through_dispatcher(
    buf: BufferStore,
    redis_conn,
) -> None:
    """schedule.fire → RQ adapter → real(ish) Redis queue → worker
    drains → task body runs (counter increments)."""
    _require_engine_module("rq")
    from rq import Queue
    from z4j_rq.engine import RqEngineAdapter

    conn, mode = redis_conn
    live_tasks.reset_counter()

    class _RqApp:
        """Minimal RQ-app shape z4j-rq expects."""

        def __init__(self) -> None:
            self._queues: dict[str, Queue] = {}

        def queue_for_name(self, name: str) -> Queue:
            if name not in self._queues:
                self._queues[name] = Queue(
                    name=name,
                    connection=conn,
                )
            return self._queues[name]

    rq_app = _RqApp()
    adapter = RqEngineAdapter(rq_app=rq_app)
    dispatcher = CommandDispatcher(
        engines={"rq": adapter},
        schedulers={},
        buffer=buf,
    )

    # The RQ adapter's submit_task calls Queue.enqueue with the
    # task NAME, but vanilla RQ requires a callable. The adapter
    # actually does ``queue.enqueue(name, *args, **kwargs)`` which
    # RQ resolves as a string callable path. Use the importable
    # path of our live task.
    task_path = "tests.integration.helpers.live_tasks.rq_live_task"

    frame = CommandFrame(
        id="cmd_rq_live",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "task_name": task_path,
                "engine": "rq",
                "queue": "default",
                "args": [1],
                "kwargs": {},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )
    await dispatcher.handle(frame)

    # Drain via SimpleWorker.work(burst=True). Must run in main
    # thread (RQ installs signal handlers, which require main
    # thread). The work_burst with disabled signal handlers is the
    # supported path for in-process testing.
    queue = rq_app.queue_for_name("default")
    _drain_rq(queue, conn)

    # Universal: success frame.
    results = [e for e in buf.drain(20) if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"rq dispatch failed - {parsed['payload'].get('error')!r}"
    )

    # Sentinel: the task body actually ran.
    assert live_tasks.get_counter() == 1, (
        f"rq task body should have incremented counter to 1 "
        f"(redis mode={mode}); got {live_tasks.get_counter()}"
    )


def _drain_rq(queue, conn) -> None:
    """Burst-mode worker that drains and exits.

    Disables RQ's default signal handlers via ``_install_signal_
    handlers = lambda *a, **kw: None`` (a method override on the
    instance) so the worker can run in a non-main asyncio context
    without ``signal.signal only works in main thread`` errors.
    """
    from rq import SimpleWorker

    worker = SimpleWorker([queue], connection=conn)
    # Patch signal-handler installation to a no-op for in-process
    # test use. Production RQ workers run in their own process and
    # need the real signal handlers; we don't.
    worker._install_signal_handlers = lambda *a, **kw: None  # type: ignore[method-assign]
    worker.work(burst=True, with_scheduler=False)


# =====================================================================
# arq
# =====================================================================


@pytest.mark.asyncio
async def test_arq_live_e2e_through_dispatcher(buf: BufferStore) -> None:
    """schedule.fire → arq adapter → real(ish) Redis pool → arq
    Worker drains → task body runs."""
    _require_engine_module("arq")
    if not _REDIS_URL:
        pytest.skip(
            "arq requires a real (not fake) Redis - fakeredis "
            "doesn't implement all the BLPOP / BRPOPLPUSH ops arq "
            "uses. Set REDIS_URL=redis://host:port to enable.",
        )

    from arq.connections import RedisSettings
    from arq.worker import Worker
    from z4j_arq.engine import ArqEngineAdapter

    live_tasks.reset_counter()

    settings = RedisSettings.from_dsn(_REDIS_URL)

    # Use arq's DEFAULT queue name (``arq:queue``) so the
    # in-process Worker (also default) drains what the adapter
    # enqueues. Production deployments with custom queue names
    # set ``queue_name`` on both sides; we keep this test on
    # the default to avoid the cross-side wiring noise.
    arq_default_queue = "arq:queue"

    adapter = ArqEngineAdapter(
        redis_settings=settings,
        function_names=["arq_live_task"],
        queue_name=arq_default_queue,
    )
    dispatcher = CommandDispatcher(
        engines={"arq": adapter},
        schedulers={},
        buffer=buf,
    )

    frame = CommandFrame(
        id="cmd_arq_live",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "task_name": "arq_live_task",
                "engine": "arq",
                # Match arq's worker-side default so the in-process
                # Worker actually picks up what we enqueue.
                "queue": arq_default_queue,
                "args": [1],
                "kwargs": {},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )
    await dispatcher.handle(frame)

    # Drain via in-process arq worker on the same default queue.
    worker = Worker(
        functions=[live_tasks.arq_live_task],
        redis_settings=settings,
        burst=True,
        queue_name=arq_default_queue,
    )
    try:
        await worker.async_run()
    finally:
        # Close BOTH redis pools (the adapter's and the worker's)
        # with the modern ``aclose()`` API. Do NOT use
        # ``worker.close()``: it goes through redis-py's deprecated
        # ``pool.close(close_connection_pool=True)`` alias, whose
        # DeprecationWarning the suite-wide ``filterwarnings =
        # error`` policy raises as an exception part-way through
        # cleanup. That is exactly how the worker pool used to leak:
        # a blanket suppress() swallowed the error, the pool never
        # disconnected, and its connections + transports hit the GC
        # unraisable path during a later test.
        worker_tasks = [t for t in getattr(worker, "tasks", {}).values() if not t.done()]
        for t in worker_tasks:
            t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        worker_pool = getattr(worker, "_pool", None)
        if worker_pool is not None:
            await worker_pool.aclose(close_connection_pool=True)
            worker._pool = None
        adapter_pool = getattr(adapter, "_pool", None)
        if adapter_pool is not None:
            await adapter_pool.aclose()

    results = [e for e in buf.drain(20) if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"arq dispatch failed - {parsed['payload'].get('error')!r}"
    )
    assert live_tasks.get_counter() == 1


# =====================================================================
# Celery
# =====================================================================


def _close_celery_app(app) -> None:
    """Deterministically close every connection a test-local Celery
    app opened, so no open socket survives the test.

    ``Celery.close()`` alone is NOT enough - it only drops the pool
    reference without disconnecting anything. Left open, the sockets
    below sit inside the app's cyclic object graph until the
    generational GC finally collects it (often during a LATER test),
    where each ``socket.__del__`` raises ResourceWarning under the
    suite-wide ``filterwarnings = error`` policy and errors out an
    innocent test ("multiple unraisable exception warnings").

    Three connections leak from a single ``send_task`` round-trip
    against a redis broker+backend (verified via gc referrer census):

    1. the result-consumer pub/sub connection (``on_task_call``
       subscribes to the result channel at publish time),
    2. the redis result-backend client's pooled connection(s),
    3. the kombu broker (producer pool) connection.

    Reads celery's lazily-created attributes (``_backend``, cached
    ``amqp``, ``_pool``) without going through the creating
    properties, so teardown never CREATES an object that did not
    already exist.
    """
    backend = getattr(app, "_backend", None)
    if backend is not None:
        consumer = getattr(backend, "result_consumer", None)
        if consumer is not None:
            with contextlib.suppress(Exception):
                consumer.stop()  # closes the pub/sub connection
        client = backend.__dict__.get("client")
        if client is not None:
            with contextlib.suppress(Exception):
                client.connection_pool.disconnect()
    amqp = app.__dict__.get("amqp")
    if amqp is not None and amqp._producer_pool is not None:
        with contextlib.suppress(Exception):
            amqp._producer_pool.force_close_all()
    if app._pool is not None:
        with contextlib.suppress(Exception):
            app._pool.force_close_all()
    with contextlib.suppress(Exception):
        app.close()


@pytest.mark.asyncio
async def test_celery_live_e2e_through_dispatcher(buf: BufferStore) -> None:
    """schedule.fire → celery adapter → real Redis broker → celery
    worker drains → task body runs.

    Uses the same shared ``live_tasks`` module so the celery worker
    can import the task by name.
    """
    _require_engine_module("celery")
    if not _REDIS_URL:
        pytest.skip(
            "celery live e2e requires a real Redis broker. Set "
            "REDIS_URL=redis://host:port (or use docker-compose).",
        )

    from celery import Celery
    from z4j_celery.engine import CeleryEngineAdapter

    live_tasks.reset_counter()

    # Flush the Redis broker so leftover messages from a prior
    # test run (or another celery app sharing the broker) don't
    # cause spurious extra invocations of our task.
    import redis as _redis_sync

    _conn = _redis_sync.from_url(_REDIS_URL)
    try:
        _conn.flushdb()
    finally:
        _close_sync_redis(_conn)

    app = Celery(
        "z4j-live-test",
        broker=_REDIS_URL,
        backend=_REDIS_URL,
    )

    @app.task(name="z4j.test.live.celery_redis_task")
    def _live(value: int = 1) -> int:
        live_tasks._state["n"] += value
        return live_tasks._state["n"]

    adapter = CeleryEngineAdapter(celery_app=app)
    dispatcher = CommandDispatcher(
        engines={"celery": adapter},
        schedulers={},
        buffer=buf,
    )

    # celery.contrib.testing.worker.start_worker runs a real
    # celery worker in-process (a thread) and joins on context
    # exit. Lets us avoid a sibling ``celery -A app worker``
    # subprocess while still exercising the real Redis broker
    # path. The ``perform_ping_check=False`` skips celery's
    # built-in startup ping which requires the worker to be on
    # the network; we don't need it for a single-task test.
    from celery.contrib.testing.worker import start_worker

    try:
        with start_worker(
            app,
            perform_ping_check=False,
            loglevel="ERROR",
        ):
            frame = CommandFrame(
                id="cmd_celery_live",
                payload=CommandPayload(
                    action="schedule.fire",
                    target={},
                    parameters={
                        "task_name": "z4j.test.live.celery_redis_task",
                        "engine": "celery",
                        "queue": "celery",
                        "args": [1],
                        "kwargs": {},
                        "fire_id": "f1",
                    },
                ),
                hmac="deadbeef" * 8,
            )
            await dispatcher.handle(frame)

            # Wait up to 10s for the worker thread to drain.
            deadline = asyncio.get_event_loop().time() + 10.0
            while live_tasks.get_counter() == 0:
                if asyncio.get_event_loop().time() > deadline:
                    break
                await asyncio.sleep(0.1)
    finally:
        _close_celery_app(app)

    results = [e for e in buf.drain(20) if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert live_tasks.get_counter() == 1, "celery worker should have run the task body within 10s"
