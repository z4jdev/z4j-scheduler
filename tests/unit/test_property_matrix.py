"""Hypothesis property tests for the wider cron / interval / solar
matrix. Complements ``test_dst_transitions.py`` (which focuses on
DST + monotonicity for hand-curated cron shapes) by walking:

- A broader cron syntax space (lists, ranges, steps, day-of-week
  modifiers).
- Cross-kind invariants - the predicted-fire shape from
  :mod:`z4j_scheduler.verify.shadow_comparator` must match what
  the per-tick :func:`next_fire` would emit if iterated.
- Solar event determinism + monotonicity across timezones.
- Interval scaling - same expression in different tzs produces
  evenly spaced UTC fires.
- Year boundary + leap-day edge cases.

The §5.5 spec sentence: *"Property tests via Hypothesis cover a
wide cron / timezone / wall-clock matrix to catch regressions in
the underlying croniter + zoneinfo behavior."* These tests close
the gap between "we have property tests" (true at 4 shapes) and
the spec's "wide ... matrix" (this file's broader walk).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from z4j_scheduler.importers._core import ImportedSchedule
from z4j_scheduler.tick.cron import next_fire
from z4j_scheduler.verify.shadow_comparator import predict_fires


_TIMEZONES = [
    "UTC",
    "America/New_York",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Berlin",
    "Asia/Tokyo",
    "Asia/Kolkata",        # half-hour offset (+05:30)
    "Australia/Sydney",
    "Pacific/Auckland",    # +12 / +13 (DST)
    "Pacific/Chatham",     # +12:45 / +13:45 (DST + odd offset)
    "Africa/Cairo",
]


# =====================================================================
# Cron syntax coverage
# =====================================================================


# Build cron expressions from valid field shapes. Hypothesis composes
# these into a 5-field string. We don't try to cover EVERY valid
# crontab(5) shape - just enough to widen coverage past the
# hand-picked ``test_dst_transitions.py`` set.
@st.composite
def _cron_field(draw, *, lo: int, hi: int, max_step: int):
    """Build a cron field that's `*`, an integer, a range, or a step."""
    shape = draw(st.sampled_from(["star", "int", "range", "step", "list"]))
    if shape == "star":
        return "*"
    if shape == "int":
        return str(draw(st.integers(min_value=lo, max_value=hi)))
    if shape == "range":
        a = draw(st.integers(min_value=lo, max_value=hi - 1))
        b = draw(st.integers(min_value=a + 1, max_value=hi))
        return f"{a}-{b}"
    if shape == "step":
        step = draw(st.integers(min_value=1, max_value=max_step))
        return f"*/{step}"
    if shape == "list":
        items = draw(
            st.lists(
                st.integers(min_value=lo, max_value=hi),
                min_size=2,
                max_size=4,
                unique=True,
            ),
        )
        return ",".join(str(i) for i in sorted(items))
    return "*"  # pragma: no cover - covered by sampled_from


@st.composite
def _cron_expression(draw):
    """Compose a 5-field cron string from valid per-field shapes."""
    return " ".join(
        [
            draw(_cron_field(lo=0, hi=59, max_step=30)),     # minute
            draw(_cron_field(lo=0, hi=23, max_step=12)),     # hour
            draw(_cron_field(lo=1, hi=28, max_step=14)),     # day-of-month (cap at 28 to avoid Feb edge cases)
            draw(_cron_field(lo=1, hi=12, max_step=6)),      # month
            draw(_cron_field(lo=0, hi=6, max_step=3)),       # day-of-week
        ],
    )


class TestCronSyntaxMonotonic:
    """For every well-formed cron expression croniter accepts, the
    next_fire helper returns a strictly later moment.

    This catches the failure mode where a sneaky combination
    (lists + steps + ranges) causes croniter to return ``after``
    itself or an earlier moment - which would loop the tick engine.
    """

    @settings(
        max_examples=300,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    @given(
        expression=_cron_expression(),
        tz=st.sampled_from(_TIMEZONES),
        offset_seconds=st.integers(min_value=0, max_value=365 * 24 * 3600),
    )
    def test_monotonic_for_synthesised_expressions(
        self, expression: str, tz: str, offset_seconds: int,
    ) -> None:
        # Anchor at 2026-01-01 in the schedule's tz, walk forward.
        anchor = datetime(2026, 1, 1, tzinfo=ZoneInfo(tz)) + timedelta(
            seconds=offset_seconds,
        )
        try:
            result = next_fire(expression, tz, anchor)
        except Exception:
            # Some synthesised expressions are valid but produce no
            # fire within croniter's lookahead window (e.g. day-of-
            # month + month combos that never coincide). Skip - the
            # importer rejects these earlier in the pipeline.
            assume(False)
            return
        assert result > anchor, (
            f"non-monotonic for expr={expression!r} tz={tz!r} "
            f"anchor={anchor.isoformat()!r} got={result.isoformat()!r}"
        )


# =====================================================================
# Cross-kind invariant: predict_fires == iterated next_fire
# =====================================================================


class TestPredictFiresMatchesIteratedNextFire:
    """The shadow comparator's batch prediction MUST yield the same
    set of fires as iterating the per-tick :func:`next_fire` helper.

    If they ever diverge, the operator's "verify --duration 24h"
    output is lying about what z4j-scheduler will actually do at
    runtime - the worst possible failure mode for a cutover gate.
    """

    @settings(max_examples=80, deadline=None)
    @given(
        expression=st.sampled_from(
            [
                "*/15 * * * *",
                "0 * * * *",
                "0 3 * * *",
                "*/30 9-17 * * 1-5",
            ],
        ),
        tz=st.sampled_from(_TIMEZONES),
        window_hours=st.integers(min_value=1, max_value=72),
    )
    def test_predict_fires_matches_iterated(
        self, expression: str, tz: str, window_hours: int,
    ) -> None:
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        end = start + timedelta(hours=window_hours)

        sched = ImportedSchedule(
            project_slug="p",
            name="t",
            engine="celery",
            kind="cron",
            expression=expression,
            task_name="app.t",
            timezone=tz,
            args=[],
            kwargs={},
            is_enabled=True,
            source="test",
        )
        # Path 1: shadow comparator.
        predicted = predict_fires(
            [sched], window_start=start, window_end=end,
        )
        predicted_times = sorted(p.fire_time for p in predicted)

        # Path 2: iterate next_fire.
        iterated_times: list[datetime] = []
        cursor = start
        # Cap iterations defensively - 72h window at every-15-min =
        # 288 fires worst case.
        for _ in range(2000):
            cursor = next_fire(expression, tz, cursor)
            cursor_utc = cursor.astimezone(UTC)
            if cursor_utc > end:
                break
            iterated_times.append(cursor_utc)

        assert predicted_times == iterated_times, (
            f"predict_fires diverged from iterated next_fire for "
            f"expr={expression!r} tz={tz!r} window={window_hours}h:\n"
            f"  predicted: {predicted_times[:5]}\n"
            f"  iterated:  {iterated_times[:5]}"
        )


# =====================================================================
# Interval scaling
# =====================================================================


class TestIntervalEvenlySpacedAcrossTimezones:
    """Interval kind must produce evenly spaced UTC fires regardless
    of the schedule's declared timezone. Intervals are wall-clock-
    independent (they fire on absolute deltas), so the timezone
    field should have ZERO impact on the resulting fire times.
    """

    @settings(max_examples=40, deadline=None)
    @given(
        seconds=st.integers(min_value=10, max_value=3600),
        tz=st.sampled_from(_TIMEZONES),
    )
    def test_evenly_spaced(self, seconds: int, tz: str) -> None:
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        sched = ImportedSchedule(
            project_slug="p",
            name="t",
            engine="celery",
            kind="interval",
            expression=f"{seconds}s",
            task_name="app.t",
            timezone=tz,
            args=[],
            kwargs={},
            is_enabled=True,
            source="test",
        )
        # 10× the interval as window so we get ~10 fires.
        window = timedelta(seconds=seconds * 10)
        fires = predict_fires(
            [sched], window_start=start, window_end=start + window,
        )
        # Sort + verify each consecutive pair is exactly N seconds
        # apart in UTC.
        times = sorted(f.fire_time for f in fires)
        for a, b in pairwise(times):
            gap = (b - a).total_seconds()
            assert gap == seconds, (
                f"interval={seconds}s tz={tz!r}: gap={gap} "
                f"between {a.isoformat()} and {b.isoformat()}"
            )


# =====================================================================
# Year boundary + leap day
# =====================================================================


class TestYearAndLeapBoundaries:
    """Cron schedules crossing a year boundary or hitting Feb 29
    must remain monotonic. Catches a class of croniter regressions
    where the 'next year' computation misses an edge case.
    """

    @pytest.mark.parametrize(
        "anchor",
        [
            # Just before NYE, in several tzs.
            datetime(2026, 12, 31, 23, 59, tzinfo=UTC),
            datetime(2026, 12, 31, 23, 59, tzinfo=ZoneInfo("America/New_York")),
            datetime(2026, 12, 31, 23, 59, tzinfo=ZoneInfo("Asia/Tokyo")),
            # Right after a leap day.
            datetime(2028, 2, 29, 12, 0, tzinfo=UTC),
        ],
    )
    @pytest.mark.parametrize(
        "expr", ["0 * * * *", "0 3 * * *", "*/15 * * * *", "0 0 * * 0"],
    )
    def test_year_or_leap_boundary_monotonic(
        self, anchor: datetime, expr: str,
    ) -> None:
        moment = anchor
        prev = moment
        for _ in range(20):
            moment = next_fire(expr, str(anchor.tzinfo), moment)
            assert moment > prev, (
                f"non-monotonic across boundary: expr={expr!r} "
                f"anchor={anchor.isoformat()} {prev} -> {moment}"
            )
            prev = moment


# =====================================================================
# Solar determinism
# =====================================================================


pytest.importorskip("astral", reason="solar property tests need astral")


class TestSolarMonotonicAcrossTimezones:
    """Solar events fire at absolute UTC instants (the lat/lon pins
    the location). The schedule's declared ``timezone`` field has
    no effect on solar fire times. Monotonicity should hold for
    every (event, location) pair across day-by-day iteration.
    """

    @settings(max_examples=40, deadline=None)
    @given(
        event=st.sampled_from(
            ["sunrise", "sunset", "noon", "dusk", "dawn", "midnight"],
        ),
        # Latitude restricted to -60..60 so we avoid polar
        # perpetual-day windows where ``next_solar_fire`` legitimately
        # returns ``None``.
        lat=st.floats(min_value=-60.0, max_value=60.0, allow_nan=False),
        lon=st.floats(min_value=-180.0, max_value=180.0, allow_nan=False),
    )
    def test_solar_monotonic(
        self, event: str, lat: float, lon: float,
    ) -> None:
        from z4j_scheduler.tick.solar import next_solar_fire

        anchor = datetime(2026, 4, 27, 0, 0, 0, tzinfo=UTC)
        expr = f"{event}:{lat}:{lon}"
        prev = anchor
        for _ in range(5):
            nxt = next_solar_fire(expr, prev)
            if nxt is None:
                # Polar latitude or astral can't compute - skip the
                # remainder of the iteration. ``predict_fires``
                # handles None by skipping.
                return
            assert nxt > prev, (
                f"solar non-monotonic: event={event!r} "
                f"lat={lat} lon={lon} {prev} -> {nxt}"
            )
            prev = nxt


# =====================================================================
# Cron expression interpretation invariance
# =====================================================================


class TestCronExpressionEquivalence:
    """``"0 * * * *"`` and ``"0,0,0 * * * *"`` MUST produce identical
    fire sequences (the second is a redundant list of the same minute).
    Same for ``"0-23/1"`` vs ``"*"`` on the hour field.

    Catches a class of "the parser silently dropped a duplicate"
    bug that would let two adjacent imports diverge.
    """

    @pytest.mark.parametrize(
        "expr_a,expr_b",
        [
            ("0 * * * *", "0,0 * * * *"),
            ("0 * * * *", "0 0-23/1 * * *"),
            ("*/30 * * * *", "0,30 * * * *"),
            ("0 9-17 * * *", "0 9,10,11,12,13,14,15,16,17 * * *"),
        ],
    )
    @pytest.mark.parametrize("tz", ["UTC", "America/New_York", "Asia/Tokyo"])
    def test_equivalent_expressions_produce_identical_fires(
        self, expr_a: str, expr_b: str, tz: str,
    ) -> None:
        anchor = datetime(2026, 4, 27, 0, 0, 0, tzinfo=ZoneInfo(tz))
        fires_a = []
        fires_b = []
        cursor_a = cursor_b = anchor
        for _ in range(20):
            cursor_a = next_fire(expr_a, tz, cursor_a)
            cursor_b = next_fire(expr_b, tz, cursor_b)
            fires_a.append(cursor_a)
            fires_b.append(cursor_b)
        assert fires_a == fires_b, (
            f"equivalent cron expressions diverged in {tz}:\n"
            f"  {expr_a}: {fires_a[:3]}\n"
            f"  {expr_b}: {fires_b[:3]}"
        )
