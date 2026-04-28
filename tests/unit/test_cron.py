"""Tests for :mod:`z4j_scheduler.tick.cron`.

Covers happy-path next-fire computation, validation, error types,
timezone handling. DST property tests live in
``test_dst_transitions.py`` to keep this file focused on the
straightforward semantics.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from z4j_scheduler.tick.cron import CronExpressionError, is_valid, next_fire

UTC = ZoneInfo("UTC")
NYC = ZoneInfo("America/New_York")


class TestIsValid:
    def test_valid_5_field(self) -> None:
        assert is_valid("0 3 * * *") is True

    def test_valid_complex(self) -> None:
        assert is_valid("*/15 9-17 * * MON-FRI") is True

    def test_invalid_empty(self) -> None:
        assert is_valid("") is False

    def test_invalid_garbage(self) -> None:
        assert is_valid("not a cron") is False

    def test_invalid_too_many_fields(self) -> None:
        assert is_valid("0 0 0 0 0 0 0 0 0") is False


class TestNextFire:
    def test_simple_daily_cron_in_utc(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        result = next_fire("0 3 * * *", "UTC", after)
        # Next 3am after noon today is 3am tomorrow.
        assert result == datetime(2026, 4, 27, 3, 0, tzinfo=UTC)

    def test_evaluates_in_target_tz_not_after_tz(self) -> None:
        # 'after' is in UTC at 8am UTC = 4am NYC. The cron asks for
        # 3am Eastern. Next 3am Eastern is the NEXT day at 3am NYC
        # (since 4am NYC has already passed today).
        after = datetime(2026, 4, 26, 8, 0, tzinfo=UTC)
        result = next_fire("0 3 * * *", "America/New_York", after)
        assert result.tzinfo == NYC
        assert result == datetime(2026, 4, 27, 3, 0, tzinfo=NYC)

    def test_strict_after_semantics(self) -> None:
        # A cron that matches exactly the 'after' moment must NOT
        # return the current moment - it must return the NEXT match.
        moment = datetime(2026, 4, 26, 3, 0, tzinfo=UTC)
        result = next_fire("0 3 * * *", "UTC", moment)
        assert result == datetime(2026, 4, 27, 3, 0, tzinfo=UTC)

    def test_returns_tz_aware_datetime_in_requested_tz(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        result = next_fire("*/5 * * * *", "America/New_York", after)
        assert result.tzinfo == NYC

    def test_naive_after_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive"):
            next_fire("0 3 * * *", "UTC", datetime(2026, 4, 26, 12, 0))

    def test_invalid_cron_raises_typed_error(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        with pytest.raises(CronExpressionError, match="invalid cron"):
            next_fire("not a cron", "UTC", after)

    def test_unknown_tz_raises_typed_error(self) -> None:
        after = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        with pytest.raises(CronExpressionError, match="unknown timezone"):
            next_fire("0 3 * * *", "Mars/Olympus_Mons", after)

    @pytest.mark.parametrize(
        ("expression", "expected_hour"),
        [
            ("0 0 * * *", 0),
            ("0 6 * * *", 6),
            ("0 12 * * *", 12),
            ("0 18 * * *", 18),
            ("0 23 * * *", 23),
        ],
    )
    def test_hour_of_day_variants(self, expression: str, expected_hour: int) -> None:
        # Use 11pm so every "0 H * * *" with H >= 0 produces a fire
        # within the next 24 hours, simplifying the assertion.
        after = datetime(2026, 4, 26, 23, 0, tzinfo=UTC)
        result = next_fire(expression, "UTC", after)
        assert result.hour == expected_hour
