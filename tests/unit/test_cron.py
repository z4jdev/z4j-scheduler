"""Tests for :mod:`z4j_scheduler.tick.cron`.

Covers happy-path next-fire computation, validation, error types,
timezone handling. DST property tests live in
``test_dst_transitions.py`` to keep this file focused on the
straightforward semantics.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from z4j_scheduler.tick import cron as cron_module
from z4j_scheduler.tick.cron import (
    CronExpressionError,
    fires_between,
    is_valid,
    next_fire,
)

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
    def test_uses_release_packaged_timezone_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[str] = []

        def load(key: str) -> ZoneInfo:
            calls.append(key)
            return UTC

        monkeypatch.setattr(cron_module, "packaged_zoneinfo", load)
        next_fire("0 3 * * *", "Release/Pinned", datetime(2026, 4, 26, tzinfo=UTC))

        assert calls == ["Release/Pinned"]

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
        assert getattr(result.tzinfo, "key", None) == NYC.key
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
        assert getattr(result.tzinfo, "key", None) == NYC.key

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


class TestFiresBetween:
    def test_returns_slots_in_open_closed_window(self) -> None:
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        slots = fires_between("* * * * *", "UTC", after=after, until=until)
        # (after, until]: 00:01..00:05, and 00:00 (== after) is excluded.
        assert slots == [datetime(2026, 1, 1, 0, m, tzinfo=UTC) for m in range(1, 6)]

    def test_cap_keeps_the_most_recent_slots_r10_m2(self) -> None:
        # The cap keeps the MOST-RECENT slots (backward walk), so the
        # last is the true latest (== until) that fire_one_missed coalesces to
        # and the engine anchors on -- not the cap-th oldest.
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2026, 1, 1, 5, 0, tzinfo=UTC)  # 300 minute-slots
        slots = fires_between("* * * * *", "UTC", after=after, until=until, cap=10)
        assert len(slots) == 10
        assert slots[-1] == until
        assert slots[0] == until - timedelta(minutes=9)

    def test_fall_back_keeps_both_absolute_moments_r11(self) -> None:
        # The backward walk must NOT lose the repeated wall-clock hour on
        # DST fall-back. America/New_York 2026-11-01: local 01:00 happens twice
        # (EDT at 05:00Z, EST at 06:00Z) and BOTH are real fires -- the module
        # docstring commits to firing at both distinct absolute moments. A
        # fold-blind backward walk returns only one, so fire_one_missed
        # coalesces to an OLDER slot and the engine creeps forward a tick later.
        after = datetime(2026, 11, 1, 4, 30, tzinfo=UTC)
        until = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
        slots = fires_between("0 * * * *", "America/New_York", after=after, until=until)
        assert [s.astimezone(UTC) for s in slots] == [
            datetime(2026, 11, 1, 5, 0, tzinfo=UTC),
            datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
        ]

    def test_window_bounds_are_absolute_across_a_dst_fold_r11(self) -> None:
        # The (after, until] bounds are ABSOLUTE instants. Comparing two
        # aware datetimes that share a tzinfo makes Python compare naive
        # wall-clock fields and ignore fold, which previously let a slot BEYOND
        # `until` be returned (01:00 EST reads as "before" 01:17 EDT even though
        # it is 43 minutes later in real time), and made other in-window slots be
        # dropped. Bounds must be evaluated in UTC.
        after = datetime(2026, 11, 1, 3, 17, tzinfo=UTC)
        until = datetime(2026, 11, 1, 5, 17, tzinfo=UTC)
        slots = [
            s.astimezone(UTC)
            for s in fires_between("0 * * * *", "America/New_York", after=after, until=until)
        ]
        assert slots == [
            datetime(2026, 11, 1, 4, 0, tzinfo=UTC),
            datetime(2026, 11, 1, 5, 0, tzinfo=UTC),
        ]
        # Nothing outside the absolute window, in either direction.
        assert all(after < s <= until for s in slots)

    def test_non_positive_cap_returns_nothing_r12(self) -> None:
        # A non-positive cap must not read as "unbounded". out[-cap:] with
        # cap==0 is out[0:] -- the WHOLE window -- and a negative cap slices from
        # the wrong end. A cap is a safety bound, so the conservative answer is
        # no slots rather than an unbounded backlog.
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        assert fires_between("* * * * *", "UTC", after=after, until=until, cap=0) == []
        assert fires_between("* * * * *", "UTC", after=after, until=until, cap=-1) == []
        # A positive cap still keeps the most-recent slots.
        assert len(fires_between("* * * * *", "UTC", after=after, until=until, cap=3)) == 3

    def test_spring_gap_six_field_slot_is_not_dropped_r12(self) -> None:
        # The two walks are blind to OPPOSITE DST directions, so neither
        # alone is correct. ``get_prev`` reports a spring-GAP occurrence by
        # snapping the nonexistent local time forward (Europe/London has no local
        # 01:00:30 on 2026-03-29; get_prev yields 02:00:30 BST). A forward walk
        # starting just before that instant can never re-report it, because
        # 02:00:30 does not match hour=1 -- so the rewrite dropped it and
        # fire_all_missed skipped that occurrence permanently. next_fire() and
        # the pre- code both return it. The union of both walks does too.
        after = datetime(2026, 3, 28, 1, 0, 30, tzinfo=UTC)
        until = datetime(2026, 3, 30, 0, 5, tzinfo=UTC)
        slots = [
            s.astimezone(UTC)
            for s in fires_between("0 1 * * * 30", "Europe/London", after=after, until=until)
        ]
        assert slots == [
            datetime(2026, 3, 29, 1, 0, 30, tzinfo=UTC),  # the gap-snapped one
            datetime(2026, 3, 30, 0, 0, 30, tzinfo=UTC),
        ]
        # ...and it agrees with walking next_fire, which is the true semantics.
        walk, cur = [], after
        while True:
            nxt = next_fire("0 1 * * * 30", "Europe/London", cur)
            if nxt.astimezone(UTC) > until:
                break
            walk.append(nxt.astimezone(UTC))
            cur = nxt
        assert slots == walk

    def test_fall_back_overlap_keeps_the_first_fold_r13(self) -> None:
        # The corrective forward walk started ONE SECOND
        # before the lower bound. The backward iterator is fold-blind, so that
        # bound can already sit in the SECOND occurrence of a repeated
        # wall-clock span -- one second earlier is still inside the overlap, and
        # the walk then never revisits the FIRST occurrence. Pacific/Chatham is
        # the sharp case because its offset is :45, so the repeated span does not
        # align to the hour. Seven occurrences are due here; four were returned.
        after = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
        until = datetime(2026, 4, 4, 16, 0, tzinfo=UTC)
        slots = [
            s.astimezone(UTC)
            for s in fires_between("*/15 3 * * * 30", "Pacific/Chatham", after=after, until=until)
        ]
        assert [s.time().isoformat() for s in slots] == [
            "13:15:30",  # first fold, +13:45 -- the three that were dropped
            "13:30:30",
            "13:45:30",
            "14:15:30",  # second fold, +12:45
            "14:30:30",
            "14:45:30",
            "15:00:30",
        ]
        assert all(after < s <= until for s in slots)

    def test_fall_back_overlap_five_field_still_correct_r13(self) -> None:
        # The same transition with a five-field expression: both absolute
        # moments of the repeated hour, and nothing outside the window.
        after = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
        until = datetime(2026, 4, 4, 16, 0, tzinfo=UTC)
        slots = [
            s.astimezone(UTC)
            for s in fires_between("0 3 * * *", "Pacific/Chatham", after=after, until=until)
        ]
        assert [s.time().isoformat() for s in slots] == ["13:15:00", "14:15:00"]

    def test_invalid_input_raises_regardless_of_cap_or_window_r12(self) -> None:
        # The cap<=0 guard was placed BEFORE validation, so a bad expression
        # or timezone silently returned [] instead of raising. A config error must
        # not stay hidden until the window or cap happens to be non-degenerate.
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        for cap in (0, -1, 10):
            with pytest.raises(CronExpressionError, match="invalid cron"):
                fires_between("not a cron", "UTC", after=after, until=until, cap=cap)
            with pytest.raises(CronExpressionError, match="unknown timezone"):
                fires_between("0 3 * * *", "Mars/Olympus_Mons", after=after, until=until, cap=cap)
        # A degenerate WINDOW must validate too, not short-circuit.
        with pytest.raises(CronExpressionError, match="invalid cron"):
            fires_between("not a cron", "UTC", after=until, until=after)

    def test_upper_bound_nudge_is_absolute_not_wall_clock_r14(self) -> None:
        # The inclusive-upper-bound nudge used wall-clock arithmetic
        # (until_in_zone + 1us). On a fall-back day that lands one microsecond
        # after a REPEATED wall time rather than after the real instant, so the
        # backward walk starts up to a whole overlap late and spends that
        # difference out of the cap. Antarctica/Troll shifts two hours, which
        # makes it visible: 10,799 seconds are due in this window and the latest
        # 10,000 should come back; 2,800 did.
        after = datetime(2026, 10, 24, 22, 0, tzinfo=UTC)
        until = datetime(2026, 10, 25, 0, 59, 59, 999999, tzinfo=UTC)
        slots = fires_between(
            "* * * * * *", "Antarctica/Troll", after=after, until=until, cap=10_000
        )
        assert len(slots) == 10_000
        utc = [s.astimezone(UTC) for s in slots]
        # The cap keeps the MOST RECENT slots, so the last is the true latest.
        assert utc[-1] == datetime(2026, 10, 25, 0, 59, 59, tzinfo=UTC)
        assert all(utc[i] < utc[i + 1] for i in range(len(utc) - 1))
        assert all(after < s <= until for s in utc)

    def test_cap_is_o_cap_not_o_window(self) -> None:
        # A 1-second cron over a full day is ~86400 slots; the backward walk must
        # return the most-recent `cap` without iterating the whole window.
        after = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        until = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
        slots = fires_between("* * * * * *", "UTC", after=after, until=until, cap=5)
        assert len(slots) == 5
        assert slots[-1] == until
        assert slots[0] == until - timedelta(seconds=4)
