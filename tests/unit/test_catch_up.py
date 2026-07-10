"""Tests for :mod:`z4j_scheduler.tick.catch_up`."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from z4j_scheduler.tick.catch_up import (
    VALID_POLICIES,
    InvalidPolicyError,
    plan_catch_up,
)

UTC = ZoneInfo("UTC")


def _times(*hours: int) -> list[datetime]:
    """Build chronological list of datetimes at given hours on a fixed day."""
    return [datetime(2026, 4, 26, h, 0, tzinfo=UTC) for h in hours]


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 4, 26, 12, 0, tzinfo=UTC)


class TestSkipPolicy:
    def test_skip_with_no_missed(self, now: datetime) -> None:
        assert plan_catch_up("skip", missed_times=[], now=now) == []

    def test_skip_with_one_missed(self, now: datetime) -> None:
        result = plan_catch_up(
            "skip",
            missed_times=_times(9),
            now=now,
        )
        assert result == []

    def test_skip_with_many_missed(self, now: datetime) -> None:
        result = plan_catch_up(
            "skip",
            missed_times=_times(6, 7, 8, 9, 10, 11),
            now=now,
        )
        assert result == []


class TestFireOneMissedPolicy:
    def test_no_missed_returns_empty(self, now: datetime) -> None:
        assert (
            plan_catch_up(
                "fire_one_missed",
                missed_times=[],
                now=now,
            )
            == []
        )

    def test_one_missed_returns_that_one(self, now: datetime) -> None:
        result = plan_catch_up(
            "fire_one_missed",
            missed_times=_times(9),
            now=now,
        )
        assert result == _times(9)

    def test_many_missed_returns_most_recent(self, now: datetime) -> None:
        # APScheduler's coalesce: collapse to one fire at the latest
        # missed time (operationally, "I missed 6 things, do the
        # latest one once").
        missed = _times(6, 7, 8, 9, 10, 11)
        result = plan_catch_up(
            "fire_one_missed",
            missed_times=missed,
            now=now,
        )
        assert result == _times(11)


class TestFireAllMissedPolicy:
    def test_no_missed_returns_empty(self, now: datetime) -> None:
        assert (
            plan_catch_up(
                "fire_all_missed",
                missed_times=[],
                now=now,
            )
            == []
        )

    def test_all_missed_returned_in_order(self, now: datetime) -> None:
        missed = _times(6, 7, 8, 9, 10, 11)
        result = plan_catch_up(
            "fire_all_missed",
            missed_times=missed,
            now=now,
        )
        assert result == missed

    def test_returns_a_copy_not_the_input(self, now: datetime) -> None:
        # Defensive: caller may mutate the returned list. We must
        # not be aliasing the input list.
        missed = _times(6, 7, 8)
        result = plan_catch_up(
            "fire_all_missed",
            missed_times=missed,
            now=now,
        )
        assert result == missed
        assert result is not missed


class TestErrorPaths:
    def test_unknown_policy_raises(self, now: datetime) -> None:
        with pytest.raises(InvalidPolicyError, match="unknown catch-up policy"):
            plan_catch_up("burn_em_all", missed_times=[], now=now)

    def test_empty_string_policy_raises(self, now: datetime) -> None:
        with pytest.raises(InvalidPolicyError):
            plan_catch_up("", missed_times=[], now=now)


def test_valid_policies_constant_pinned() -> None:
    """Pin the public set of policies to catch accidental additions
    that would also need brain-side enum + dashboard work."""
    assert VALID_POLICIES == ("skip", "fire_one_missed", "fire_all_missed")
