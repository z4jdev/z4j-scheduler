"""The reconnect penalty escalates and is not cleared. That is deliberate.

``_reconnect_attempts`` only increments, so a burst of failures drives the delay
to its 30-second ceiling and it stays there for the life of the process. The
cost is real: a later drop can leave the tick engine refusing to dispatch for
30 seconds, and a missed fire is skipped rather than deferred.

It is accepted because every attempt to clear it was worse. Clearing before the
``async for`` clears before the RPC has started, and the brain rejects Watch at
a per-certificate and a global cap; that rejection arrives before any frame, so
the penalty cleared every cycle and the scheduler re-synced two or three times a
second against a brain that was reachable and deliberately shedding load.
Clearing inside the loop never fires at all on an idle brain, which yields no
frames. The full reasoning is on the block comment where the reset used to live.

These tests pin the accepted behaviour so a fourth attempt has to argue with
them rather than quietly reintroduce a busy-loop.
"""

from __future__ import annotations

import pytest
from z4j_scheduler.storage import watch as watch_module
from z4j_scheduler.storage.cache import ScheduleCache


def _watch() -> watch_module.WatchStream:
    return watch_module.WatchStream(
        client=object(),  # type: ignore[arg-type]
        cache=ScheduleCache(),
        full_resync_interval_seconds=0,
    )


@pytest.mark.asyncio
async def test_the_delay_escalates_to_a_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sustained failure has to cost the brain less over time, not the same."""
    delays: list[float] = []

    async def capture_wait(
        awaitable,
        *,
        timeout: float,  # noqa: ASYNC109 - mirrors asyncio.wait_for
    ) -> None:
        awaitable.close()
        delays.append(timeout)
        raise TimeoutError

    monkeypatch.setattr(watch_module.random, "uniform", lambda *_args: 0.0)
    monkeypatch.setattr(watch_module.asyncio, "wait_for", capture_wait)

    stream = _watch()
    for attempts in (0, 3, 6, 50):
        stream._reconnect_attempts = attempts
        await stream._backoff_or_stop()

    assert delays == [0.5, 4.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_capacity_rejections_keep_escalating_the_penalty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard against a fourth attempt at the same mistake.

    A reset placed anywhere that runs before the first frame clears the penalty
    for a Watch the brain may have rejected at its capacity cap, which turns
    back-off into a retry storm aimed at an already-overloaded brain.

    If a reset is reintroduced, it must be driven by the client signalling that
    the RPC actually opened.
    """
    delays: list[float] = []
    stream = _watch()

    async def capacity_rejection() -> None:
        raise RuntimeError("brain Watch capacity exhausted")

    async def capture_wait(
        awaitable,
        *,
        timeout: float,  # noqa: ASYNC109 - mirrors asyncio.wait_for
    ) -> None:
        awaitable.close()
        delays.append(timeout)
        if len(delays) == 3:
            stream._stop_event.set()
            return
        raise TimeoutError

    monkeypatch.setattr(stream, "_sync_then_watch", capacity_rejection)
    monkeypatch.setattr(watch_module.random, "uniform", lambda *_args: 0.0)
    monkeypatch.setattr(watch_module.asyncio, "wait_for", capture_wait)

    await stream._watch_loop()

    assert delays == [0.5, 1.0, 2.0]
    assert stream._reconnect_attempts == 3


def test_new_streams_start_with_independent_penalties() -> None:
    first = _watch()
    second = _watch()

    first._reconnect_attempts = 7

    assert first._reconnect_attempts == 7
    assert second._reconnect_attempts == 0
