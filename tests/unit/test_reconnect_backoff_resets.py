"""The reconnect penalty escalates across drops and clears after a healthy stretch.

``_reconnect_attempts`` grows with every drop, so a burst of failures drives the
delay to its 30-second ceiling. It clears only once the stream has stayed
healthy for one full re-sync interval (``reconcile_interval_seconds``), and
``_mark_unhealthy`` is where the reset is applied, because that is the moment
the length of the healthy stretch is known.

The placement is the point. Clearing before the ``async for`` cleared before
the RPC had started, and the brain rejects Watch at a per-certificate and a
global cap; that rejection arrives before any frame, so the penalty cleared
every cycle and the scheduler re-synced two or three times a second against a
brain that was reachable and deliberately shedding load. Clearing inside the
loop never fired on an idle brain, which yields no frames. A stretch measured
on the clock tells the two apart: a rejected stream is healthy for
milliseconds and keeps its penalty, an idle stream still ages.

These tests pin both halves so neither a busy-loop nor a permanent penalty can
come back quietly: a flap shorter than the interval keeps the penalty, a
healthy stretch of one interval clears it, and the first reconnect after a
reset is fast.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from z4j_scheduler.storage import watch as watch_module
from z4j_scheduler.storage.cache import ScheduleCache

#: The full re-sync cadence the streams under test run on. The penalty clears
#: after the stream has been healthy for this long.
_INTERVAL = 900.0


def _watch(
    clock: Callable[[], float] | None = None,
    *,
    full_resync_interval_seconds: float = _INTERVAL,
) -> watch_module.WatchStream:
    return watch_module.WatchStream(
        client=object(),  # type: ignore[arg-type]
        cache=ScheduleCache(),
        full_resync_interval_seconds=full_resync_interval_seconds,
        clock=clock,
    )


def _capture_backoff(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record each backoff delay instead of sleeping it; jitter is pinned to zero."""
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
    return delays


@pytest.mark.asyncio
async def test_the_delay_escalates_to_a_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sustained failure has to cost the brain less over time, not the same."""
    delays = _capture_backoff(monkeypatch)

    stream = _watch()
    for attempts in (0, 3, 6, 50):
        stream._reconnect_attempts = attempts
        await stream._backoff_or_stop()

    assert delays == [0.5, 4.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_capacity_rejections_keep_escalating_the_penalty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Watch the brain rejects at its capacity cap never earns a reset.

    The rejection arrives before the stream is marked healthy, so no healthy
    stretch accrues and back-off keeps growing instead of turning into a retry
    storm aimed at an already-overloaded brain.
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


@pytest.mark.asyncio
async def test_a_flap_shorter_than_the_interval_keeps_the_penalty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Brief reconnects must not clear the penalty, however many of them land."""
    delays = _capture_backoff(monkeypatch)
    now = [1_000.0]
    stream = _watch(lambda: now[0])
    stream._reconnect_attempts = 6

    # A stream that opens and drops just short of the interval, three times over.
    for _ in range(3):
        stream._mark_healthy()
        now[0] += _INTERVAL - 1.0
        stream._mark_unhealthy()
        await stream._backoff_or_stop()

    assert delays == [30.0, 30.0, 30.0]
    assert stream._reconnect_attempts == 9


def test_a_healthy_stretch_of_one_interval_clears_the_penalty() -> None:
    now = [1_000.0]
    stream = _watch(lambda: now[0])
    stream._reconnect_attempts = 6

    stream._mark_healthy()
    now[0] += _INTERVAL
    stream._mark_unhealthy()

    assert stream._reconnect_attempts == 0


@pytest.mark.asyncio
async def test_the_first_reconnect_after_a_reset_is_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a reset the next attempt is the initial delay and escalation restarts."""
    delays = _capture_backoff(monkeypatch)
    now = [1_000.0]
    stream = _watch(lambda: now[0])
    stream._reconnect_attempts = 50

    stream._mark_healthy()
    now[0] += _INTERVAL
    stream._mark_unhealthy()
    await stream._backoff_or_stop()

    # The stream reopens and flaps again inside the interval: the penalty rebuilds.
    stream._mark_healthy()
    now[0] += 5.0
    stream._mark_unhealthy()
    await stream._backoff_or_stop()

    assert delays == [0.5, 1.0]
    assert stream._reconnect_attempts == 2


def test_a_disabled_periodic_resync_does_not_clear_on_every_reconnect() -> None:
    """Interval 0 disables the timer, not the penalty; the default cadence applies."""
    now = [1_000.0]
    stream = _watch(lambda: now[0], full_resync_interval_seconds=0)
    stream._reconnect_attempts = 3

    stream._mark_healthy()
    now[0] += 100.0
    stream._mark_unhealthy()
    assert stream._reconnect_attempts == 3

    stream._mark_healthy()
    now[0] += watch_module._DEFAULT_FULL_RESYNC_INTERVAL_SECONDS
    stream._mark_unhealthy()
    assert stream._reconnect_attempts == 0


def test_new_streams_start_with_independent_penalties() -> None:
    first = _watch()
    second = _watch()

    first._reconnect_attempts = 7

    assert first._reconnect_attempts == 7
    assert second._reconnect_attempts == 0
