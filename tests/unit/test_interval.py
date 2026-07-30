"""Tests for :mod:`z4j_scheduler.tick.interval`."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from z4j_scheduler.tick.interval import (
    IntervalExpressionError,
    fires_between,
    next_fire,
    parse,
)

UTC = ZoneInfo("UTC")


class TestFiresBetween:
    """H4: the full missed-slot backlog for an interval schedule."""

    def test_enumerates_every_slot_in_window(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        until = datetime(2026, 4, 26, 12, 30, tzinfo=UTC)
        slots = fires_between("5m", after=after, until=until)
        assert slots == [
            datetime(2026, 4, 26, 12, 5, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 10, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 15, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 20, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 25, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 30, tzinfo=UTC),
        ]

    def test_half_open_excludes_after_includes_until(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        until = datetime(2026, 4, 26, 12, 10, tzinfo=UTC)
        slots = fires_between("5m", after=after, until=until)
        # ``after`` itself is NOT a slot (strict >); the exact ``until``
        # boundary IS included.
        assert after not in slots
        assert until in slots
        assert slots == [
            datetime(2026, 4, 26, 12, 5, tzinfo=UTC),
            datetime(2026, 4, 26, 12, 10, tzinfo=UTC),
        ]

    def test_empty_window_returns_no_slots(self) -> None:
        t = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        assert fires_between("5m", after=t, until=t) == []

    def test_cap_bounds_a_huge_backlog(self) -> None:
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2027, 1, 1, 0, 0, tzinfo=UTC)  # a year of 1m slots
        slots = fires_between("1m", after=after, until=until, cap=100)
        assert len(slots) == 100

    def test_cap_keeps_the_most_recent_slots_r10_m2(self) -> None:
        # When the cap truncates, keep the MOST-RECENT slots so the last
        # is the true latest (== until here). fire_one_missed coalesces to
        # slots[-1] and the engine advances its anchor there; oldest-first
        # truncation fired the 100th-oldest slot and crept forward one cap-window
        # per tick.
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2027, 1, 1, 0, 0, tzinfo=UTC)  # a year of 1m slots
        slots = fires_between("1m", after=after, until=until, cap=100)
        assert len(slots) == 100
        assert slots[-1] == until
        assert slots[0] == until - timedelta(minutes=99)

    def test_naive_bounds_rejected(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            fires_between(
                "5m", after=datetime(2026, 4, 26, 12, 0), until=datetime(2026, 4, 26, 13, 0)
            )


class TestParse:
    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            ("30s", timedelta(seconds=30)),
            ("5m", timedelta(minutes=5)),
            ("2h", timedelta(hours=2)),
            ("1d", timedelta(days=1)),
            ("60", timedelta(seconds=60)),  # bare int = seconds
            (" 60 ", timedelta(seconds=60)),  # whitespace tolerated
            ("90s", timedelta(seconds=90)),
        ],
    )
    def test_valid_expressions(self, expression: str, expected: timedelta) -> None:
        assert parse(expression) == expected

    @pytest.mark.parametrize(
        "expression",
        ["", "abc", "5x", "-5m", "1.5m", "5 m extra", "5min"],
    )
    def test_invalid_format_raises(self, expression: str) -> None:
        with pytest.raises(IntervalExpressionError):
            parse(expression)

    def test_zero_rejected(self) -> None:
        with pytest.raises(IntervalExpressionError, match="> 0"):
            parse("0s")

    def test_zero_bare_rejected(self) -> None:
        with pytest.raises(IntervalExpressionError, match="> 0"):
            parse("0")


class TestNextFireWithLastFire:
    def test_anchored_to_last_fire(self) -> None:
        last = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        result = next_fire("5m", last_fire_at=last, anchor_at=last)
        assert result == datetime(2026, 4, 26, 12, 5, tzinfo=UTC)

    def test_naive_last_fire_rejected(self) -> None:
        last = datetime(2026, 4, 26, 12, 0)  # naive
        anchor = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="last_fire_at"):
            next_fire("5m", last_fire_at=last, anchor_at=anchor)

    def test_does_not_drift_to_now(self) -> None:
        # Even if last_fire_at is "way in the past", we anchor to it
        # so a behind scheduler catches up at the configured cadence
        # rather than skipping to "now".
        last = datetime(2020, 1, 1, 0, 0, tzinfo=UTC)
        result = next_fire("1h", last_fire_at=last, anchor_at=last)
        assert result == datetime(2020, 1, 1, 1, 0, tzinfo=UTC)


class TestNextFireFirstFire:
    def test_first_fire_aligns_to_interval_boundary(self) -> None:
        # 5-minute schedule created at 12:03 first fires at 12:05.
        anchor = datetime(2026, 4, 26, 12, 3, tzinfo=UTC)
        result = next_fire("5m", last_fire_at=None, anchor_at=anchor)
        assert result == datetime(2026, 4, 26, 12, 5, tzinfo=UTC)

    def test_first_fire_on_boundary_advances(self) -> None:
        # If the anchor is exactly on a boundary, we go to the NEXT
        # boundary (strict > semantics).
        anchor = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        result = next_fire("5m", last_fire_at=None, anchor_at=anchor)
        assert result == datetime(2026, 4, 26, 12, 5, tzinfo=UTC)

    def test_first_fire_hourly(self) -> None:
        anchor = datetime(2026, 4, 26, 12, 33, tzinfo=UTC)
        result = next_fire("1h", last_fire_at=None, anchor_at=anchor)
        assert result == datetime(2026, 4, 26, 13, 0, tzinfo=UTC)

    def test_naive_anchor_rejected(self) -> None:
        anchor = datetime(2026, 4, 26, 12, 0)
        with pytest.raises(ValueError, match="anchor_at"):
            next_fire("5m", last_fire_at=None, anchor_at=anchor)

    def test_returned_tz_matches_anchor_tz(self) -> None:
        nyc = ZoneInfo("America/New_York")
        anchor = datetime(2026, 4, 26, 12, 3, tzinfo=nyc)
        result = next_fire("5m", last_fire_at=None, anchor_at=anchor)
        assert result.tzinfo == nyc
