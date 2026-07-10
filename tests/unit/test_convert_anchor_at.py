"""Regression test for the v1.1.0 tick-engine fairness bug.

Symptom: 8 enabled interval schedules in the cache, only ONE fires
(repeatedly, in a hot loop) while the other 7 stay at total_runs=0.

Root cause: ``entry_from_pb`` set ``anchor_at`` to a year-2000
sentinel when the brain payload had no ``last_run_at``. The
:func:`interval.next_fire` first-fire calculation then returned a
year-2000 timestamp. The tick engine saw ``next_fire_at`` in the
past, fired the schedule, advanced ``last_fire_at`` by the interval
(still in the past), and immediately re-evaluated as due, a hot
loop on the first schedule the dispatcher could reach.

Fix: anchor at ``next_run_at`` if brain pre-computed one, else at
``datetime.now(UTC)``. A fresh interval-15 schedule now first fires
at the next 15s boundary AFTER its arrival, and its subsequent
``last_fire_at + interval`` walks forward in time normally, the
tick engine round-robins all due schedules at their respective
cadences.

Pinned this regression so a future refactor can't reintroduce the
year-2000 sentinel.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from z4j_scheduler.storage._convert import _DEFAULT_ANCHOR, entry_from_pb
from z4j_scheduler.tick.interval import next_fire as interval_next_fire


def _build_schedule_message(
    *,
    last_run_at_seconds: int = 0,
    next_run_at_seconds: int = 0,
    kind: str = "interval",
    expression: str = "15",
):
    """Construct a minimal protobuf-shaped namespace for entry_from_pb."""
    from google.protobuf.timestamp_pb2 import Timestamp
    from z4j_scheduler.proto import scheduler_pb2 as pb

    msg = pb.Schedule(
        id="00000000-0000-0000-0000-000000000001",
        project_id="00000000-0000-0000-0000-000000000002",
        engine="celery",
        name="test-interval-15s",
        task_name="myapp.tasks.ping",
        kind=kind,
        expression=expression,
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
    )
    if last_run_at_seconds:
        ts = Timestamp()
        ts.seconds = last_run_at_seconds
        msg.last_run_at.CopyFrom(ts)
    if next_run_at_seconds:
        ts = Timestamp()
        ts.seconds = next_run_at_seconds
        msg.next_run_at.CopyFrom(ts)
    return msg


class TestAnchorAtSelectsRecentTime:
    """``anchor_at`` MUST default to a current-or-future time, not the
    year-2000 sentinel, when brain has no fire history.
    """

    def test_fresh_interval_schedule_anchors_at_now(self):
        msg = _build_schedule_message()
        before = datetime.now(UTC)
        entry = entry_from_pb(msg)
        after = datetime.now(UTC)

        # anchor_at must be inside the wall-clock window of the call,
        # NOT the year-2000 sentinel.
        assert _DEFAULT_ANCHOR.year == 2000  # sanity
        assert entry.anchor_at.year >= before.year
        assert before - timedelta(seconds=1) <= entry.anchor_at <= after + timedelta(seconds=1)

    def test_fresh_interval_first_fire_lands_in_future(self):
        """End-to-end check: a fresh interval schedule's first
        computed next_fire MUST be within a small window of now,
        not in the year 2000.
        """
        msg = _build_schedule_message(expression="15")
        entry = entry_from_pb(msg)

        nxt = interval_next_fire(
            entry.expression,
            last_fire_at=None,  # fresh schedule
            anchor_at=entry.anchor_at,
        )
        # Next fire must be within a 15-second future window -
        # the next 15s boundary after now.
        now = datetime.now(UTC)
        assert nxt > now - timedelta(seconds=2), (
            f"first-fire {nxt} is in the past; year-2000 sentinel regression?"
        )
        assert nxt < now + timedelta(seconds=20)

    def test_anchor_uses_brain_next_run_at_when_present(self):
        """If brain pre-computed a ``next_run_at``, it wins over now."""
        future = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
        msg = _build_schedule_message(next_run_at_seconds=future)
        entry = entry_from_pb(msg)

        # anchor_at should be the brain-supplied next_run_at, not now.
        assert entry.anchor_at == datetime.fromtimestamp(future, tz=UTC)

    def test_anchor_uses_last_run_at_when_present(self):
        """If brain has fire history, ``last_run_at`` wins over both
        ``next_run_at`` and now (defines the cadence going forward).
        """
        last = int((datetime.now(UTC) - timedelta(minutes=5)).timestamp())
        future = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
        msg = _build_schedule_message(
            last_run_at_seconds=last,
            next_run_at_seconds=future,
        )
        entry = entry_from_pb(msg)

        assert entry.anchor_at == datetime.fromtimestamp(last, tz=UTC)
        assert entry.last_fire_at == datetime.fromtimestamp(last, tz=UTC)


class TestNoHotLoopAfterFirstFire:
    """After the first fire, ``last_fire_at + interval`` must walk
    forward in wall-clock time, not stay frozen in the past.
    """

    def test_advance_after_fire_walks_forward(self):
        """Simulate the engine's advance logic: set last_fire_at to
        scheduled_for, then compute next_fire. The result must be in
        the future relative to now (proving no hot-loop).
        """
        msg = _build_schedule_message(expression="15")
        entry = entry_from_pb(msg)

        # First fire: scheduled_for is the engine's first-pass
        # next_fire computation against the (now-anchored) entry.
        first_fire = interval_next_fire(
            entry.expression,
            last_fire_at=None,
            anchor_at=entry.anchor_at,
        )
        # Engine advance: stamp last_fire_at to first_fire, recompute.
        second_fire = interval_next_fire(
            entry.expression,
            last_fire_at=first_fire,
            anchor_at=entry.anchor_at,
        )
        # The second fire is exactly one interval after the first.
        assert second_fire == first_fire + timedelta(seconds=15)
        # And it is within the next 30 seconds of wall-clock now -
        # NOT 26 years in the past (the pre-fix bug).
        now = datetime.now(UTC)
        assert second_fire > now - timedelta(seconds=2)
        assert second_fire < now + timedelta(seconds=35)
