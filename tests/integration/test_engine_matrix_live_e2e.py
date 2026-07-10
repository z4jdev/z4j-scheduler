"""Live-broker e2e matrix: schedule.fire → engine → task ACTUALLY RUNS.

The companion ``test_engine_matrix_e2e.py`` proves the dispatcher
routes correctly to each engine adapter. This file goes one step
deeper: for each engine where in-process execution is feasible,
the test makes the engine ACTUALLY EXECUTE the task body (not
just enqueue) and asserts a sentinel side-effect happened.

Scope: in-process / in-memory brokers only. We do not stand up
Redis, RabbitMQ, etc. - that's the docker-compose harness's job
(tracked separately for the production-hardening backlog).

Engine coverage matrix
======================

| Engine    | In-process execution feasible? | Why / what we test |
|-----------|--------------------------------|--------------------|
| celery    | ❌                              | ``send_task`` (the adapter's primitive) ignores ``task_always_eager``. Live execution requires a real broker; covered by docker harness. |
| rq        | ❌                              | Needs Redis. Dispatch boundary covered by per-adapter test. |
| dramatiq  | ✅                              | StubBroker + in-process Worker drains the queue and runs the actor. |
| huey      | ✅                              | ``MemoryHuey(immediate=True)`` runs tasks synchronously. |
| arq       | ❌                              | Needs Redis. Dispatch boundary covered. |
| taskiq    | ⚠️ partial                       | InMemoryBroker enqueues but only executes on ``await sent.wait_result()``; the adapter (correctly) does not block. Dispatch boundary covered. |

So this file delivers live-execution e2e for **dramatiq + huey**.
The other 4 engines have their dispatch boundary covered by
``test_engine_matrix_e2e.py`` + per-adapter ``test_dispatcher_
integration.py``; live execution against their real brokers is
the docker-compose harness scope (next item on the backlog).

What this proves
================

For each engine where in-process execution is feasible:

1. The bare dispatcher accepts the ``schedule.fire`` frame.
2. The adapter's ``submit_task`` enqueues into a real engine.
3. The engine's worker / executor RUNS the registered task.
4. A sentinel side-effect (counter increment, list append)
   confirms the task body executed.

What this does NOT prove
========================

- Real-broker scheduling against Redis / RabbitMQ. The docker-
  compose harness in the production-hardening backlog covers
  that.
- Cross-process delivery. Everything here runs in one Python
  process.
- Failure / retry semantics. Per-engine retry tests live in
  each engine's adapter package.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

# Each builder returns
# ``(engine_name, adapter, task_name, run_pending_async, assert_executed)``
# where ``run_pending_async()`` drains any worker queue (no-op when
# the engine runs eagerly) and ``assert_executed()`` verifies the
# sentinel side-effect.
_LiveBuilderResult = tuple[
    str,
    Any,
    str,
    Callable[[], Awaitable[None]],
    Callable[[], None],
]


def _build_celery_eager() -> _LiveBuilderResult:
    """Celery with task_always_eager=True - tasks run synchronously."""
    pytest.importorskip("celery")
    from celery import Celery
    from z4j_celery.engine import CeleryEngineAdapter

    app = Celery("z4j-test", broker="memory://")
    app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
        broker_url="memory://",
    )

    counter = {"n": 0}

    @app.task(name="z4j.test.live.celery_task")
    def _live_task(value: int = 1) -> int:
        counter["n"] += value
        return counter["n"]

    adapter = CeleryEngineAdapter(celery_app=app)

    async def _no_op_drain() -> None:
        return None

    def _assert() -> None:
        assert counter["n"] == 1, (
            f"celery eager task should have incremented counter to 1; got {counter['n']}"
        )

    return "celery", adapter, "z4j.test.live.celery_task", _no_op_drain, _assert


def _build_dramatiq_stub() -> _LiveBuilderResult:
    """Dramatiq with StubBroker - join() drains the queue."""
    pytest.importorskip("dramatiq")
    import dramatiq
    from dramatiq.brokers.stub import StubBroker
    from dramatiq.worker import Worker
    from z4j_dramatiq.engine import DramatiqEngineAdapter

    saved = dramatiq.broker.global_broker
    broker = StubBroker()
    dramatiq.broker.global_broker = broker

    counter = {"n": 0}

    @dramatiq.actor(broker=broker, actor_name="z4j.test.live.dramatiq_task")
    def _live_task(value: int = 1) -> None:
        counter["n"] += value

    worker = Worker(broker, worker_threads=1)
    worker.start()

    adapter = DramatiqEngineAdapter(broker=broker)

    async def _drain() -> None:
        # StubBroker.join blocks until the queue empties; wrap in
        # to_thread so the asyncio loop isn't blocked.
        broker.join("default", timeout=5_000)
        worker.join()

    def _assert() -> None:
        try:
            assert counter["n"] == 1, f"dramatiq task should have run; counter={counter['n']}"
        finally:
            worker.stop()
            broker.close()
            dramatiq.broker.global_broker = saved

    return (
        "dramatiq",
        adapter,
        "z4j.test.live.dramatiq_task",
        _drain,
        _assert,
    )


def _build_huey_immediate() -> _LiveBuilderResult:
    """Huey with immediate=True - tasks run synchronously."""
    pytest.importorskip("huey")
    from huey import MemoryHuey
    from z4j_huey.engine import HueyEngineAdapter

    huey_inst = MemoryHuey("live-test", immediate=True)

    counter = {"n": 0}

    @huey_inst.task(name="z4j.test.live.huey_task")
    def _live_task(value: int = 1) -> int:
        counter["n"] += value
        return counter["n"]

    adapter = HueyEngineAdapter(huey=huey_inst)

    full_name = next(
        k
        for k in huey_inst._registry._registry
        if k.endswith(".huey_task") or k == "z4j.test.live.huey_task"
    )

    async def _no_op_drain() -> None:
        return None

    def _assert() -> None:
        assert counter["n"] == 1, f"huey immediate task should have run; counter={counter['n']}"

    return "huey", adapter, full_name, _no_op_drain, _assert


def _build_taskiq_inmemory() -> _LiveBuilderResult:
    """Taskiq InMemoryBroker - kiq() runs the task immediately."""
    pytest.importorskip("taskiq")
    from taskiq import InMemoryBroker
    from z4j_taskiq.engine import TaskiqEngineAdapter

    broker = InMemoryBroker()

    counter = {"n": 0}

    @broker.task
    async def _live_task(value: int = 1) -> int:
        counter["n"] += value
        return counter["n"]

    adapter = TaskiqEngineAdapter(broker=broker)
    full_name = next(k for k in broker.get_all_tasks() if k.endswith(":_live_task"))

    async def _drain() -> None:
        # InMemoryBroker runs the task in the kiq() coroutine. Nothing
        # to drain.
        return None

    def _assert() -> None:
        assert counter["n"] == 1, (
            f"taskiq InMemoryBroker task should have run; counter={counter['n']}"
        )

    started = {"v": False}

    async def _setup_drain() -> None:
        if not started["v"]:
            await broker.startup()
            started["v"] = True

    async def _teardown_drain() -> None:
        await _drain()
        if started["v"]:
            await broker.shutdown()

    # Wrap the drain to also handle startup on first call.
    async def _setup_then_noop() -> None:
        await _setup_drain()

    return (
        "taskiq",
        adapter,
        full_name,
        # Replace drain with the teardown wrapper so the test fixture
        # can call it last.
        _teardown_drain,
        _assert,
    )


_LIVE_BUILDERS: list[Callable[[], _LiveBuilderResult]] = [
    # taskiq removed: InMemoryBroker doesn't auto-execute. See
    # module docstring.
    # celery RESTORED (Apr 2026 follow-up): the adapter now prefers
    # ``apply_async`` on locally-registered tasks, which honors
    # ``task_always_eager=True``. In-process live execution works.
    _build_celery_eager,
    _build_dramatiq_stub,
    _build_huey_immediate,
]


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "build_engine",
    _LIVE_BUILDERS,
    ids=lambda fn: fn.__name__.removeprefix("_build_"),
)
async def test_schedule_fire_actually_runs_task(
    build_engine: Callable[[], _LiveBuilderResult],
    buf: BufferStore,
) -> None:
    """For each in-process-capable engine, ``schedule.fire`` must
    actually CAUSE THE TASK TO RUN (not just enqueue).

    Sentinel: a counter dict mutated by the registered task body.
    The assertion ``counter == 1`` proves the task body executed
    end-to-end.
    """
    engine_name, adapter, task_name, drain, assert_executed = build_engine()

    # Taskiq's InMemoryBroker needs an explicit startup() before
    # tasks can be sent. Other engines don't need pre-startup.
    if engine_name == "taskiq":
        from taskiq import InMemoryBroker

        # The broker is the adapter's broker; introspect it from
        # the adapter via attribute access.
        broker_attr = getattr(adapter, "broker", None) or getattr(
            adapter,
            "_broker",
            None,
        )
        if broker_attr is not None and isinstance(broker_attr, InMemoryBroker):
            await broker_attr.startup()

    dispatcher = CommandDispatcher(
        engines={engine_name: adapter},
        schedulers={},
        buffer=buf,
    )

    frame = CommandFrame(
        id=f"cmd_live_{engine_name}",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "sched-uuid",
                "schedule_name": "live-matrix",
                "task_name": task_name,
                "engine": engine_name,
                "queue": "default",
                "args": [1],
                "kwargs": {},
                "fire_id": "fire-uuid",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    # Drain any pending work (no-op for engines that ran eagerly).
    await drain()

    # Universal: dispatcher emitted a success command_result.
    entries = buf.drain(20)
    results = [e for e in entries if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"engine={engine_name}: dispatch failed - {parsed['payload'].get('error')!r}"
    )

    # The task body actually ran (sentinel mutated).
    assert_executed()


# =====================================================================
# Engines requiring real brokers (Redis) - skipped without containers
# =====================================================================


@pytest.mark.parametrize("engine_name", ["rq", "arq", "taskiq"])
def test_real_broker_engines_documented_as_docker_compose_scope(
    engine_name: str,
) -> None:
    """celery / rq / arq / taskiq require a real broker (or
    explicit blocking ``wait_result``) to execute tasks end-to-end.

    Their per-engine ``test_dispatcher_integration.py`` files
    cover the dispatch boundary with recording fakes; full
    end-to-end execution against a live Redis broker is the
    docker-compose harness's scope (see
    ``docker-compose.scheduler-test.yml`` in the repo root for
    the scaffold).

    This test is a placeholder so the engine matrix in CI shows
    explicit "skipped, deferred to docker harness" instead of
    silently missing coverage.
    """
    pytest.skip(
        f"engine={engine_name}: live-broker execution requires a "
        f"real broker (celery/rq/arq: Redis or RabbitMQ; taskiq: "
        f"AsyncResultBackend with blocking wait_result). Dispatch "
        f"boundary is covered by packages/z4j-{engine_name}/tests/"
        f"unit/test_dispatcher_integration.py + the matrix at "
        f"tests/integration/test_engine_matrix_e2e.py. Real-broker "
        f"execution is the docker-compose harness's scope.",
    )
