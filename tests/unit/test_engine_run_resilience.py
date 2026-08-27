"""The tick loop must outlive a single bad iteration.

``run()`` is the scheduler's supervisor loop and it is started inside an
``asyncio.TaskGroup`` in main. A TaskGroup cancels every sibling when one child
raises, so an exception escaping one iteration does not merely stop scheduling:
it tears down the watch stream, the metrics server and the whole process, for
every project the deployment serves.

The paths known to raise are each guarded individually (the fire path
quarantines, the next-fire computation quarantines). The loop itself had no
catch-all, so any path NOT yet known to raise had that blast radius.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick.engine import TickEngine

from .test_engine import AlwaysLeader, ManualClock, RecordingDispatcher


def _engine(**kwargs: object) -> TickEngine:
    return TickEngine(
        cache=ScheduleCache(),
        leader_gate=AlwaysLeader(),
        dispatcher=RecordingDispatcher(),
        clock=ManualClock(datetime(2026, 4, 26, 12, 0, tzinfo=UTC)),
        max_sleep_seconds=0.01,
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_one_bad_iteration_does_not_kill_the_loop() -> None:
    """A single unexpected exception must not end scheduling."""

    engine = _engine(iteration_error_backoff_seconds=0.01)
    calls = 0
    real_iteration = engine._iteration

    async def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("unexpected failure inside one iteration")
        if calls >= 3:
            await engine.stop()
        await real_iteration()

    engine._iteration = flaky  # type: ignore[method-assign]

    await asyncio.wait_for(engine.run(), timeout=10)

    assert calls >= 3, (
        f"the loop stopped after the first failure (calls={calls}); one bad "
        "iteration took down the whole scheduler task"
    )


@pytest.mark.asyncio
async def test_an_endlessly_failing_loop_still_gives_up() -> None:
    """Resilience must not become an infinite silent spin.

    Swallowing every exception forever would replace a loud crash with a
    scheduler that looks alive and never fires. After a run of consecutive
    failures the loop has to surface the fault.
    """

    engine = _engine(
        max_consecutive_iteration_errors=4,
        iteration_error_backoff_seconds=0.01,
    )
    calls = 0

    async def always_fails() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("permanently broken iteration")

    engine._iteration = always_fails  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="permanently broken"):
        await asyncio.wait_for(engine.run(), timeout=30)

    assert calls == 4, f"the loop should have tried exactly the configured ceiling; calls={calls}"


@pytest.mark.asyncio
async def test_cancellation_is_never_swallowed() -> None:
    """Shutdown must still work: CancelledError is control flow, not an error."""

    engine = _engine()

    async def hangs() -> None:
        await asyncio.sleep(3600)

    engine._iteration = hangs  # type: ignore[method-assign]

    task = asyncio.create_task(engine.run())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
