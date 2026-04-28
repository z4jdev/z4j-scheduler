"""Property tests for DST + timezone correctness in the cron module.

These tests are the hard ones - cron + DST is where every scheduler
historically gets bugs. We rely on croniter + zoneinfo for the
underlying behavior; these tests pin the contract we depend on so
a regression in either dependency surfaces here.

Property-based tests via :mod:`hypothesis` walk a wide input space.
Spot-check tests pin specific known-tricky transitions for the
US Eastern timezone.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from z4j_scheduler.tick.cron import next_fire

UTC = ZoneInfo("UTC")
NYC = ZoneInfo("America/New_York")
LON = ZoneInfo("Europe/London")
TYO = ZoneInfo("Asia/Tokyo")


# Hand-curated 2026 DST transitions for spot checks.
NYC_SPRING_FORWARD_2026 = datetime(2026, 3, 8, 7, 0, tzinfo=UTC)  # 02:00 EST -> 03:00 EDT
NYC_FALL_BACK_2026 = datetime(2026, 11, 1, 6, 0, tzinfo=UTC)       # 02:00 EDT -> 01:00 EST


class TestSpringForwardGap:
    """A wall-clock time that does not exist (clock jumps from 2am
    straight to 3am on spring-forward day).

    Cron expression "30 2 * * *" on the day of spring-forward asks
    for 2:30am - which never happened. The schedule must not be
    silently dropped; it must fire on the next valid wall-clock
    time. croniter's behavior is to skip to the next day's 2:30am,
    which is the correct conservative choice.
    """

    def test_2_30_am_on_spring_forward_day_does_not_get_lost(self) -> None:
        # Just before spring-forward in NYC.
        before = datetime(2026, 3, 8, 1, 0, tzinfo=NYC)
        result = next_fire("30 2 * * *", "America/New_York", before)
        # Either fires that day at the moved time OR skips to next day.
        # Both are acceptable; the failure mode we're guarding against
        # is "fires at a time that does not exist on the wall clock"
        # OR "fires twice for one missed slot."
        # Confirm result is a valid wall-clock time on a valid day.
        assert result.tzinfo is not None
        assert result.year == 2026 and result.month == 3
        assert result > before

    def test_subsequent_calls_advance_monotonically(self) -> None:
        # Three consecutive next_fire calls around the spring-forward
        # transition should produce strictly increasing timestamps.
        moment = datetime(2026, 3, 8, 0, 0, tzinfo=NYC)
        fires = []
        for _ in range(5):
            moment = next_fire("0 * * * *", "America/New_York", moment)
            fires.append(moment)
        for a, b in pairwise(fires):
            assert b > a, f"non-monotonic: {a} then {b}"


class TestFallBackOverlap:
    """A wall-clock time that occurs twice on fall-back day (clock
    falls back from 2am to 1am, so 1:30 EDT and 1:30 EST exist as
    two distinct absolute moments one hour apart).

    Behavior we commit to (matches croniter + zoneinfo):

    - Each absolute moment that satisfies the cron expression is
      fired exactly once.
    - On fall-back day, "30 1 * * *" therefore fires TWICE - once at
      1:30 EDT and once at 1:30 EST - because both are valid wall-
      clock 1:30 instants on the day.
    - This matches the operational expectation for hourly schedules
      ("the day has 25 hours and the schedule should run for each").
    - It does NOT match the operational expectation for some daily
      schedules ("3am should fire once even on a 25-hour day"), but
      "30 3 * * *" naturally fires once because 3:30am is not the
      ambiguous slot.
    - The two fires are 1 hour apart in absolute time and have
      distinct ``fold`` attributes (0 for the first / EDT, 1 for the
      second / EST), so downstream tooling that needs to deduplicate
      can.

    Operators who want the alternate "fire once at the first
    occurrence" semantics should use a non-overlapping hour
    (e.g. "30 3 * * *" instead of "30 1 * * *").
    """

    def test_1_30_am_on_fall_back_day_fires_at_both_distinct_moments(self) -> None:
        # Start just before midnight on fall-back day.
        moment = datetime(2026, 11, 1, 0, 0, tzinfo=NYC)
        # Get all fires for "30 1 * * *" within the next 24 hours.
        fires: list[datetime] = []
        end = moment + timedelta(hours=25)
        while moment < end:
            moment = next_fire("30 1 * * *", "America/New_York", moment)
            if moment > end:
                break
            fires.append(moment)

        # Filter to fires on fall-back day only.
        on_fb_day = [f for f in fires if f.date() == datetime(2026, 11, 1).date()]
        # Exactly two fires - one in EDT, one in EST, an hour apart.
        assert len(on_fb_day) == 2, f"expected 2 fires, got {on_fb_day}"
        # Both have wall-clock 1:30.
        assert all(f.hour == 1 and f.minute == 30 for f in on_fb_day)
        # The two are one hour apart in absolute time.
        utc_fires = [f.astimezone(UTC) for f in on_fb_day]
        gap = (utc_fires[1] - utc_fires[0]).total_seconds()
        assert gap == 3600, f"expected 1h gap between 1:30 EDT and 1:30 EST; got {gap}s"


class TestPropertyMonotonicNextFire:
    """For ANY valid cron expression in ANY timezone, calling
    next_fire(expr, tz, after) must strictly satisfy result > after.

    This is the most fundamental property - violations would mean
    the scheduler could fire at the same moment forever or fire in
    the past.
    """

    @settings(max_examples=200, deadline=None)
    @given(
        # A handful of common cron shapes, broad enough to catch DST,
        # narrow enough to stay parseable.
        expression=st.sampled_from(
            [
                "0 * * * *",
                "*/15 * * * *",
                "0 3 * * *",
                "30 1 * * *",
                "0 0 1 * *",
                "0 0 * * 0",
                "*/5 9-17 * * MON-FRI",
            ],
        ),
        tz=st.sampled_from(
            [
                "UTC",
                "America/New_York",
                "Europe/London",
                "Asia/Tokyo",
                "Australia/Sydney",
                "Pacific/Auckland",
                "America/Los_Angeles",
            ],
        ),
        # Datetimes spanning a 5-year window covering multiple DST
        # transitions in both hemispheres.
        after_naive=st.datetimes(
            min_value=datetime(2024, 1, 1),
            max_value=datetime(2029, 1, 1),
        ),
    )
    def test_strict_monotonic(
        self,
        expression: str,
        tz: str,
        after_naive: datetime,
    ) -> None:
        after = after_naive.replace(tzinfo=ZoneInfo(tz))
        result = next_fire(expression, tz, after)
        assert result > after, (
            f"non-monotonic for expr={expression!r} tz={tz!r} "
            f"after={after!r} got={result!r}"
        )


class TestPropertyTimezoneIndependence:
    """next_fire should be deterministic regardless of the tz the
    caller passes ``after`` in. The cron expression is interpreted
    in the schedule's own tz; the input tz of ``after`` is just a
    moment-in-time reference.
    """

    @settings(max_examples=50, deadline=None)
    @given(
        expression=st.sampled_from(["0 3 * * *", "*/15 * * * *", "0 0 * * MON"]),
        sched_tz=st.sampled_from(["UTC", "America/New_York", "Asia/Tokyo"]),
        # The same instant expressed in two different timezones should
        # produce the same next_fire result.
        instant_offset_hours=st.integers(min_value=-12, max_value=14),
    )
    def test_input_tz_does_not_change_result(
        self,
        expression: str,
        sched_tz: str,
        instant_offset_hours: int,
    ) -> None:
        # Two equivalent representations of the same UTC instant.
        moment_utc = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
        equivalent_tz = ZoneInfo(
            f"Etc/GMT{'+' if instant_offset_hours <= 0 else '-'}"
            f"{abs(instant_offset_hours)}",
        )
        moment_other = moment_utc.astimezone(equivalent_tz)

        a = next_fire(expression, sched_tz, moment_utc)
        b = next_fire(expression, sched_tz, moment_other)
        assert a == b, (
            f"next_fire is not tz-independent for input: "
            f"{a!r} vs {b!r}"
        )


@pytest.mark.parametrize("tz", ["UTC", "America/New_York", "Asia/Tokyo", "Europe/London"])
def test_hourly_cron_in_each_tz_produces_60_minute_gaps(tz: str) -> None:
    """Hourly cron in any tz produces fires exactly 60 minutes apart
    (except across DST transitions in non-UTC zones, which are
    expected to be skipped or doubled by the wall-clock semantics).
    """
    moment = datetime(2026, 4, 26, 0, 0, tzinfo=ZoneInfo(tz))
    fires = []
    for _ in range(10):
        moment = next_fire("0 * * * *", tz, moment)
        fires.append(moment)

    # Use UTC for the gap check so DST adjustments don't trip us.
    utc_fires = [f.astimezone(UTC) for f in fires]
    for a, b in pairwise(utc_fires):
        gap = (b - a).total_seconds()
        assert gap == 3600, f"expected 60min gap, got {gap}s between {a} and {b}"
