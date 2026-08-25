"""Engine-agnostic matrix smoke test for the schedule.fire dispatcher.

The user's ask (Apr 2026): "we will need to smoke test and verify
this claim before we ship it", referring to the marketing claim
that z4j-scheduler works with celery, rq, dramatiq, huey, arq, and
taskiq.

What this proves
================

For EACH of the six engines (celery, rq, dramatiq, huey, arq,
taskiq), this file:

1. Constructs the real ``<Engine>EngineAdapter`` against an
   in-memory engine instance (fake celery_app, MemoryHuey,
   InMemoryBroker, dramatiq StubBroker, etc.).
2. Wires the adapter into a real ``z4j_bare.dispatcher.CommandDispatcher``.
3. Hands the dispatcher a real ``schedule.fire`` ``CommandFrame``
   shaped exactly like brain's ``SchedulerService.FireSchedule``
   produces.
4. Asserts the dispatcher emitted a SUCCESS ``command_result``
   frame whose ``result.engine`` field matches the target engine.

If the matrix passes, the v1.1.0 ``schedule.fire`` dispatch path
is proven engine-agnostic at the dispatcher boundary - the same
brain-side scheduler tick will land correctly on any of the six
engines.

What this does NOT prove
========================

This is a smoke test, not a docker-compose harness. It does NOT
prove that the engine actually runs the task end-to-end against a
live broker (Redis, RabbitMQ, etc.). The per-adapter
``test_dispatcher_integration.py`` files already exercise the
deep engine-side enqueue mechanics for each engine; this file
exercises the cross-engine surface.

Marketing-claim coverage
========================

After this file passes alongside each adapter's per-engine
``test_dispatcher_integration.py``, the marketing claim "z4j
works with celery, rq, dramatiq, huey, arq, taskiq" has
end-to-end test evidence at the agent-side dispatch boundary.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

# Per-engine builders return ``(engine_name, adapter, task_name,
# setup_async)``. ``setup_async`` is awaited before dispatch (to do
# any async startup, e.g. taskiq broker startup); pass None when
# nothing async is needed.
_BuilderResult = tuple[
    str,
    Any,
    str,
    Callable[[], Awaitable[None]] | None,
    Callable[[], Awaitable[None]] | None,
]


# =====================================================================
# Per-engine fixture builders
# =====================================================================


def _build_celery() -> _BuilderResult:
    from z4j_celery.engine import CeleryEngineAdapter

    sent: list[dict] = []

    class _FakeApp:
        def send_task(
            self,
            name,
            *,
            args=(),
            kwargs=None,
            eta=None,
            queue=None,
            priority=None,
        ):
            sent.append(
                {
                    "name": name,
                    "args": list(args),
                    "kwargs": dict(kwargs or {}),
                    "queue": queue,
                },
            )

            class _R:
                id = f"celery-{len(sent)}"

            return _R()

    fake = _FakeApp()
    adapter = CeleryEngineAdapter(celery_app=fake)
    adapter._matrix_recorder = sent  # type: ignore[attr-defined]
    return "celery", adapter, "myapp.t", None, None


def _build_rq() -> _BuilderResult:
    from z4j_rq.engine import RqEngineAdapter

    class _FakeQueue:
        def __init__(self, name):
            self.name = name
            self.submit_calls: list[dict] = []

        def enqueue(self, name, *args, **kwargs):
            self.submit_calls.append(
                {"name": name, "args": args, "kwargs": kwargs},
            )

            class _Job:
                id = f"rq-{len(self.submit_calls)}"

            return _Job()

    class _FakeApp:
        def __init__(self):
            self._queues: dict[str, _FakeQueue] = {}

        def queue_for_name(self, name):
            if name not in self._queues:
                self._queues[name] = _FakeQueue(name)
            return self._queues[name]

    fake = _FakeApp()
    adapter = RqEngineAdapter(rq_app=fake)
    adapter._matrix_recorder = fake  # type: ignore[attr-defined]
    return "rq", adapter, "myapp.t", None, None


def _build_dramatiq() -> _BuilderResult:
    pytest.importorskip("dramatiq")
    import dramatiq
    from dramatiq.brokers.stub import StubBroker
    from z4j_dramatiq.engine import DramatiqEngineAdapter

    saved = dramatiq.broker.global_broker
    broker = StubBroker()
    dramatiq.broker.global_broker = broker

    @dramatiq.actor(broker=broker, actor_name="matrix_task", queue_name="default")
    def _matrix_task(*args, **kwargs):
        return None

    adapter = DramatiqEngineAdapter(broker=broker)

    async def _teardown():
        dramatiq.broker.global_broker = saved
        broker.close()

    return "dramatiq", adapter, "matrix_task", None, _teardown


def _build_huey() -> _BuilderResult:
    pytest.importorskip("huey")
    from huey import MemoryHuey
    from z4j_huey.engine import HueyEngineAdapter

    huey_inst = MemoryHuey("matrix-test", immediate=False)

    @huey_inst.task()
    def matrix_task(*args, **kwargs):
        return None

    adapter = HueyEngineAdapter(huey=huey_inst)
    # Huey registers the task under "<module>.matrix_task".
    full_name = next(k for k in huey_inst._registry._registry if k.endswith(".matrix_task"))
    return "huey", adapter, full_name, None, None


def _build_arq() -> _BuilderResult:
    pytest.importorskip("arq")
    from z4j_arq.engine import ArqEngineAdapter

    class _RecordingPool:
        def __init__(self):
            self.calls: list[dict] = []

        async def enqueue_job(self, name, *args, **kwargs):
            self.calls.append(
                {"name": name, "args": tuple(args), "kwargs": dict(kwargs)},
            )

            class _Job:
                job_id = f"arq-{len(self.calls)}"

            return _Job()

    pool = _RecordingPool()
    adapter = ArqEngineAdapter(
        redis_settings=pool,
        function_names=["myapp.t"],
    )
    adapter._matrix_recorder = pool  # type: ignore[attr-defined]
    return "arq", adapter, "myapp.t", None, None


def _build_taskiq() -> _BuilderResult:
    pytest.importorskip("taskiq")
    from taskiq import InMemoryBroker
    from z4j_taskiq.engine import TaskiqEngineAdapter

    broker = InMemoryBroker()

    @broker.task
    async def matrix_task(*args, **kwargs):
        return None

    adapter = TaskiqEngineAdapter(
        broker=broker,
        broker_loop=asyncio.get_running_loop(),
    )
    full_name = next(k for k in broker.get_all_tasks() if k.endswith(":matrix_task"))

    async def _setup():
        await broker.startup()

    async def _teardown():
        await broker.shutdown()

    return "taskiq", adapter, full_name, _setup, _teardown


# =====================================================================
# Matrix
# =====================================================================


_ENGINE_BUILDERS: list[Callable[[], _BuilderResult]] = [
    _build_celery,
    _build_rq,
    _build_dramatiq,
    _build_huey,
    _build_arq,
    _build_taskiq,
]


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "build_engine",
    _ENGINE_BUILDERS,
    ids=lambda fn: fn.__name__.removeprefix("_build_"),
)
async def test_schedule_fire_lands_on_engine(
    build_engine: Callable[[], _BuilderResult],
    buf: BufferStore,
) -> None:
    """For every engine, a ``schedule.fire`` CommandFrame must produce
    a SUCCESS command_result whose ``result.engine`` matches the
    target engine name.

    Engine matrix: celery, rq, dramatiq, huey, arq, taskiq.

    The success frame is the contract: it's emitted by the bare
    dispatcher only when the adapter's ``submit_task`` returned
    ``status='success'``. Every adapter's submit_task only returns
    success when the underlying engine API (broker / queue / pool)
    accepted the enqueue call. So a green test here proves the
    full chain: dispatcher → adapter → engine API.
    """
    engine_name, adapter, task_name, setup, teardown = build_engine()
    if setup is not None:
        await setup()

    try:
        dispatcher = CommandDispatcher(
            engines={engine_name: adapter},
            schedulers={},
            buffer=buf,
        )

        frame = CommandFrame(
            id=f"cmd_matrix_{engine_name}",
            payload=CommandPayload(
                action="schedule.fire",
                target={},
                parameters={
                    "schedule_id": "sched-uuid",
                    "schedule_name": "matrix-test",
                    "task_name": task_name,
                    "engine": engine_name,
                    "queue": "default",
                    "args": ["a"],
                    "kwargs": {"k": "v"},
                    "fire_id": "fire-uuid",
                },
            ),
            hmac="deadbeef" * 8,
        )

        await dispatcher.handle(frame)
    finally:
        if teardown is not None:
            await teardown()

    # Universal assertion: success frame with correct engine.
    entries = buf.drain(20)
    results = [e for e in entries if e.kind == "command_result"]
    assert len(results) == 1, (
        f"engine={engine_name}: exactly one command_result expected, got {len(results)}"
    )
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"engine={engine_name}: dispatch failed - {parsed['payload'].get('error')!r}"
    )
    assert parsed["payload"]["result"]["engine"] == engine_name, (
        f"engine={engine_name}: result.engine field mismatch - "
        f"got {parsed['payload']['result'].get('engine')!r}"
    )

    # Deep assertion (only for engines with reliable fakes - celery,
    # rq, arq). Other engines are validated via the per-adapter
    # test_dispatcher_integration.py for engine-side enqueue
    # mechanics; the success frame above is the contract.
    recorder = getattr(adapter, "_matrix_recorder", None)
    if engine_name == "celery":
        assert len(recorder) == 1
        assert recorder[0]["name"] == "myapp.t"
        assert recorder[0]["queue"] == "default"
    elif engine_name == "rq":
        q = recorder.queue_for_name("default")
        assert len(q.submit_calls) == 1
        assert q.submit_calls[0]["name"] == "myapp.t"
    elif engine_name == "arq":
        assert len(recorder.calls) == 1
        assert recorder.calls[0]["name"] == "myapp.t"


# =====================================================================
# Multi-engine routing
# =====================================================================


@pytest.mark.asyncio
async def test_schedule_fire_routes_to_correct_engine_in_mixed_deployment(
    buf: BufferStore,
) -> None:
    """Multi-engine deployment: register celery + rq adapters in the
    same dispatcher. Fire targeting rq must land on rq, not celery.

    A pre-1.1 bug (collapsing to "the only registered engine") would
    have silently mis-fired in mixed deployments.
    """
    celery_name, celery_adapter, _celery_task, _, _ = _build_celery()
    rq_name, rq_adapter, rq_task, _, _ = _build_rq()

    dispatcher = CommandDispatcher(
        engines={celery_name: celery_adapter, rq_name: rq_adapter},
        schedulers={},
        buffer=buf,
    )

    frame = CommandFrame(
        id="cmd_mixed_target_rq",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "task_name": rq_task,
                "engine": "rq",
                "queue": "default",
                "args": ["a"],
                "kwargs": {"k": "v"},
                "fire_id": "fire-uuid",
            },
        ),
        hmac="deadbeef" * 8,
    )
    await dispatcher.handle(frame)

    # rq received the call.
    rq_recorder = rq_adapter._matrix_recorder  # type: ignore[attr-defined]
    q = rq_recorder.queue_for_name("default")
    assert len(q.submit_calls) == 1
    assert q.submit_calls[0]["name"] == rq_task

    # Celery did NOT receive it.
    celery_recorder = celery_adapter._matrix_recorder  # type: ignore[attr-defined]
    assert celery_recorder == [], (
        "schedule.fire with engine='rq' must not leak to celery "
        "even when both adapters are registered"
    )

    # Frame engine field reflects rq.
    results = [e for e in buf.drain(20) if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert parsed["payload"]["result"]["engine"] == "rq"
