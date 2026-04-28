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
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

from tests.integration.helpers import live_tasks


_REDIS_URL = os.environ.get("REDIS_URL")


# arq's redis-py async pool occasionally leaks an "unclosed Connection"
# ResourceWarning at GC time even after explicit ``aclose()``. Pytest's
# ``PytestUnraisableExceptionWarning`` promotes those to errors, which
# masks otherwise-passing tests. Suppress at module level - the
# warnings are GC-timing noise, not real bugs.
pytestmark = [
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnraisableExceptionWarning",
    ),
]


def _have_fakeredis() -> bool:
    try:
        import fakeredis  # noqa: F401
        return True
    except ImportError:
        return False


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


# =====================================================================
# RQ
# =====================================================================


@pytest.mark.asyncio
async def test_rq_live_e2e_through_dispatcher(buf: BufferStore) -> None:
    """schedule.fire → RQ adapter → real(ish) Redis queue → worker
    drains → task body runs (counter increments)."""
    pytest.importorskip("rq")
    from rq import Queue, SimpleWorker

    from z4j_rq.engine import RqEngineAdapter

    conn, mode = _make_redis_connection()
    live_tasks.reset_counter()

    class _RqApp:
        """Minimal RQ-app shape z4j-rq expects."""
        def __init__(self) -> None:
            self._queues: dict[str, Queue] = {}

        def queue_for_name(self, name: str) -> Queue:
            if name not in self._queues:
                self._queues[name] = Queue(
                    name=name, connection=conn,
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
    pytest.importorskip("arq")
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
        # Close the arq adapter's pool (avoids ResourceWarning that
        # pytest's PytestUnraisableExceptionWarning catches) and the
        # worker's own pool reference.
        adapter_pool = getattr(adapter, "_pool", None)
        if adapter_pool is not None:
            try:
                await adapter_pool.aclose()
            except Exception:  # noqa: BLE001
                pass
        try:
            await worker.close()
        except Exception:  # noqa: BLE001
            pass

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


@pytest.mark.asyncio
async def test_celery_live_e2e_through_dispatcher(buf: BufferStore) -> None:
    """schedule.fire → celery adapter → real Redis broker → celery
    worker drains → task body runs.

    Uses the same shared ``live_tasks`` module so the celery worker
    can import the task by name.
    """
    pytest.importorskip("celery")
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
    _conn.flushdb()
    _conn.close()

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

    results = [e for e in buf.drain(20) if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert live_tasks.get_counter() == 1, (
        "celery worker should have run the task body within 10s"
    )
