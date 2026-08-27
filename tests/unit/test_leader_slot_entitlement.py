"""A slot seen due while leading must not be dropped by our own latency.

Froze the entitlement for a follower-handoff slot, and its comment states
the reasoning in full: the classification is recomputed on every attempt against
a moving clock, so a slot judged on-time at the first attempt can exceed the
grace by the retry a second later, and under catch_up="skip" the retry then
produces an empty plan, advances past the slot and consumes it without ever
dispatching.

Everything in that sentence is equally true of a slot a LEADER observed as due.
The freeze was applied to one path and not the other."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick.engine import TickEngine

from .test_engine import (
    AlwaysLeader,
    ManualClock,
    RecordingDispatcher,
    make_cron_entry,
)

SLOT = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_a_leader_slot_survives_our_own_retry_latency() -> None:
    """The engine's own lateness must not reclassify a slot it already owned."""

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=1))
    cache = ScheduleCache()
    await cache.upsert(entry)

    # Fail the first two dispatches, exactly as a brief brain outage would.
    dispatcher = RecordingDispatcher(raise_count=2)
    clock = ManualClock(SLOT)

    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=dispatcher,
        clock=clock,
        max_sleep_seconds=0.01,
    )

    # Attempt one: on time, fails.
    await engine._iteration()
    # The retries land past the 5s grace, which the engine's own backoff
    # makes ordinary at the shipped defaults.
    clock.advance(timedelta(seconds=7))
    await engine._iteration()
    clock.advance(timedelta(seconds=7))
    await engine._iteration()

    fired = [f.scheduled_for for f in dispatcher.fires]
    assert SLOT in fired, (
        "the 12:00 slot was observed due while leading and was never dispatched; "
        f"fires={fired} last_fire_at={entry.last_fire_at} "
        f"next_fire_at={entry.next_fire_at}"
    )


@pytest.mark.asyncio
async def test_a_genuinely_missed_slot_still_obeys_skip() -> None:
    """The freeze must not quietly turn skip into fire_one_missed.

    A slot that elapsed while this engine was NOT running is exactly what
    catch_up governs, and skip must still discard it. Without this control the
    fix above could be "make everything fire", which is worse than the bug.
    """

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=1))
    cache = ScheduleCache()
    await cache.upsert(entry)
    dispatcher = RecordingDispatcher()

    # First observation is already an hour late: the engine was down for it.
    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=dispatcher,
        clock=ManualClock(SLOT + timedelta(hours=1)),
        max_sleep_seconds=0.01,
    )
    await engine._iteration()

    assert [f.scheduled_for for f in dispatcher.fires] == [], (
        "skip must still discard a slot that elapsed while the engine was not "
        "running; the entitlement freeze applies only to slots we observed due"
    )


@pytest.mark.asyncio
async def test_a_discarded_slot_is_logged_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A skip discard must leave a trace an operator can find.

    The discard itself is correct. Its invisibility was the defect: a schedule
    could stop producing work and neither the log nor the metrics said so.
    """
    from z4j_scheduler.observability import metrics as m

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=3))
    entry.name = "nightly-rollup"
    cache = ScheduleCache()
    await cache.upsert(entry)

    def _count() -> float:
        return m.slots_discarded_total.labels(catch_up="skip")._value.get()

    before = _count()
    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=RecordingDispatcher(),
        clock=ManualClock(SLOT),
        max_sleep_seconds=0.01,
    )
    with caplog.at_level("WARNING", logger="z4j.scheduler.tick"):
        await engine._iteration()

    assert _count() > before, "the discard was not counted"
    rendered = [r.getMessage() for r in caplog.records]
    assert any("without firing" in line for line in rendered), (
        f"no WARNING named the dropped slots; records={rendered}"
    )
    assert any("nightly-rollup" in line for line in rendered), (
        "the WARNING must name the schedule so it can be acted on"
    )


@pytest.mark.asyncio
async def test_the_on_time_grace_is_deployment_configurable() -> None:
    """A deployment whose dispatch latency exceeds 5s must be able to say so."""

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=1))
    cache = ScheduleCache()
    await cache.upsert(entry)
    dispatcher = RecordingDispatcher()

    # 30s late: missed at the default grace, on-time at a 60s grace.
    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=dispatcher,
        clock=ManualClock(SLOT + timedelta(seconds=30)),
        max_sleep_seconds=0.01,
        on_time_grace_seconds=60.0,
    )
    await engine._iteration()

    assert [f.scheduled_for for f in dispatcher.fires] == [SLOT], (
        "a 30s-late slot must count as on-time under an explicit 60s grace"
    )


@pytest.mark.asyncio
async def test_an_entitlement_expires_so_a_real_outage_is_still_an_outage() -> None:
    """The freeze must not turn a long outage into an on-time fire.

    Freezing the on-time judgement is right for the seconds a retry takes. It is
    wrong for hours. Without a ceiling, a slot whose first dispatch failed keeps
    its entitlement for as long as the schedule is live and next_fire_at has not
    moved, and a dispatch failure moves neither. A nightly 03:00 job whose brain
    was down could then run at 09:00 under catch_up="skip", which is the exact
    outcome that policy exists to prevent, and the exact "force on-time
    regardless of age" that removed for handoff slots.
    """

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=1))
    cache = ScheduleCache()
    await cache.upsert(entry)

    # The first dispatch is on time and fails, which grants the entitlement.
    dispatcher = RecordingDispatcher(raise_count=1)
    clock = ManualClock(SLOT)
    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=dispatcher,
        clock=clock,
        max_sleep_seconds=0.01,
    )
    await engine._iteration()
    assert dispatcher.fires == [], "the first attempt was supposed to fail"

    # The brain stays unreachable well past any plausible retry sequence.
    clock.advance(timedelta(minutes=40))
    await engine._iteration()

    assert [f.scheduled_for for f in dispatcher.fires] == [], (
        "a 40-minute-stale slot was dispatched under catch_up='skip'; the "
        "entitlement freeze has no age ceiling, so an outage is being treated "
        "as this engine's own dispatch latency"
    )


@pytest.mark.asyncio
async def test_the_entitlement_still_covers_an_ordinary_retry_sequence() -> None:
    """The ceiling must not undo the fix it bounds.

    Dispatch backoff caps at 300s per attempt, so a legitimate retry sequence
    can take minutes. Those must still be covered.
    """

    entry = make_cron_entry(catch_up="skip", last_fire_at=SLOT - timedelta(hours=1))
    cache = ScheduleCache()
    await cache.upsert(entry)

    dispatcher = RecordingDispatcher(raise_count=1)
    clock = ManualClock(SLOT)
    engine = TickEngine(
        cache=cache,
        leader_gate=AlwaysLeader(),
        dispatcher=dispatcher,
        clock=clock,
        max_sleep_seconds=0.01,
    )
    await engine._iteration()
    clock.advance(timedelta(minutes=5))
    await engine._iteration()

    assert [f.scheduled_for for f in dispatcher.fires] == [SLOT], (
        "a slot retried five minutes later is still this engine's own latency "
        "and must keep its on-time entitlement"
    )
