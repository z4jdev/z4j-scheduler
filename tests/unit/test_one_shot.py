"""Tests for :mod:`z4j_scheduler.tick.one_shot`."""

from __future__ import annotations

from datetime import UTC as DT_UTC
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from z4j_scheduler.tick.one_shot import OneShotExpressionError, next_fire

UTC = ZoneInfo("UTC")


class TestNextFireFirstTime:
    def test_iso_z_suffix(self) -> None:
        result = next_fire("2026-04-30T15:00:00Z", last_fire_at=None)
        assert result is not None
        assert result == datetime(2026, 4, 30, 15, 0, tzinfo=DT_UTC)

    def test_iso_explicit_offset(self) -> None:
        result = next_fire(
            "2026-04-30T15:00:00+00:00",
            last_fire_at=None,
        )
        assert result is not None
        assert result.year == 2026 and result.month == 4 and result.day == 30
        assert result.utcoffset() == timedelta(0)

    def test_iso_non_utc_offset(self) -> None:
        # NYC in winter is -05:00.
        result = next_fire(
            "2026-01-15T09:00:00-05:00",
            last_fire_at=None,
        )
        assert result is not None
        # Same instant in UTC = 14:00 UTC.
        as_utc = result.astimezone(UTC)
        assert as_utc == datetime(2026, 1, 15, 14, 0, tzinfo=UTC)

    def test_iso_with_microseconds(self) -> None:
        result = next_fire(
            "2026-04-30T15:00:00.123456Z",
            last_fire_at=None,
        )
        assert result is not None
        assert result.microsecond == 123456


class TestNextFireAfterFire:
    def test_returns_none_after_fire(self) -> None:
        last = datetime(2026, 4, 30, 15, 0, tzinfo=UTC)
        result = next_fire("2026-04-30T15:00:00Z", last_fire_at=last)
        assert result is None

    def test_returns_none_even_if_fire_was_yesterday(self) -> None:
        # One-shot is forever-done after the first fire, regardless
        # of how long ago.
        last = datetime(2020, 1, 1, 0, 0, tzinfo=UTC)
        result = next_fire("2026-04-30T15:00:00Z", last_fire_at=last)
        assert result is None


class TestErrorPaths:
    def test_garbage_expression_rejected(self) -> None:
        with pytest.raises(OneShotExpressionError, match="ISO-8601"):
            next_fire("not a date", last_fire_at=None)

    def test_naive_iso_rejected(self) -> None:
        # ISO without offset = ambiguous timezone. Reject.
        with pytest.raises(OneShotExpressionError, match="naive"):
            next_fire("2026-04-30T15:00:00", last_fire_at=None)

    def test_date_only_rejected(self) -> None:
        # date-only is parseable by fromisoformat but has no time
        # component, which would be ambiguous as "fire when?"
        # Actually datetime.fromisoformat on a date string returns
        # midnight, so let's verify it's rejected because midnight
        # without tz info is naive.
        with pytest.raises(OneShotExpressionError):
            next_fire("2026-04-30", last_fire_at=None)
