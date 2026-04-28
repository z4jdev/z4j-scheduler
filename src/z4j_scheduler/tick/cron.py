"""Cron next-fire computation - thin wrapper over croniter.

Why a wrapper:

1. Centralises the timezone handling pattern (croniter takes a
   ``base`` datetime that must be tz-aware via stdlib ``zoneinfo``)
2. Validates expressions at parse time with a clear error type
3. Lets us swap the underlying library if croniter regresses
   (only this one module changes - the tick engine never imports
   croniter directly)

Public API:

    next_fire(expression, tz, after) -> datetime
    is_valid(expression) -> bool

DST corner cases - documented behavior we commit to:

- **Spring-forward gap** (a clock time that does not exist on
  spring-forward day) - fires on the next valid wall-clock time.
  No fire is silently dropped.
- **Fall-back overlap** (a wall-clock time that exists twice on
  fall-back day, once in DST and once in standard time) - fires
  at BOTH distinct absolute moments. They are one hour apart in
  absolute time and carry distinct ``fold`` values (0 for the
  first occurrence / DST, 1 for the second / standard). This
  matches the operational expectation for hourly schedules ("the
  day has 25 hours and the schedule should run for each"). For
  daily schedules where a single fire is desired even on the
  ambiguous day, choose an hour outside the overlap window
  (e.g. ``"30 3 * * *"`` instead of ``"30 1 * * *"``).

Property tests in ``tests/unit/test_dst_transitions.py`` walk a
wide cron / timezone / wall-clock matrix to catch regressions in
the underlying croniter + zoneinfo behavior.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, croniter


class CronExpressionError(ValueError):
    """The cron expression or timezone is invalid.

    Distinct subclass so callers can catch this specifically without
    swallowing other ``ValueError`` from datetime parsing.
    """


def is_valid(expression: str) -> bool:
    """Return True if ``expression`` is a valid cron expression.

    Cheap probe used by the schedule-create REST handler before
    persisting. Does not raise.
    """
    try:
        croniter(expression)
    except (CroniterBadCronError, ValueError):
        return False
    return True


def next_fire(
    expression: str,
    tz: str,
    after: datetime,
) -> datetime:
    """Return the next fire time strictly after ``after``.

    Args:
        expression: A standard 5-field cron expression
            (e.g. ``"0 3 * * *"``) OR a 6-field expression with a
            seconds field appended last
            (``"minute hour dom month dow second"``, e.g.
            ``"* * * * * 30"`` fires at the 30th second of every
            minute). The 6-field form is documented in
            ``docs/SCHEDULER.md §5.1`` as "optional 6th seconds
            field for higher resolution where the engine supports
            it." z4j-scheduler still does not target sub-second
            resolution for the wire-protocol fire path
            (§6 acknowledged ~10-30ms decoupling cost) - the 6-
            field form is for operators who need a fire at a
            specific second-within-minute boundary, not for
            high-frequency tickers.
        tz: An IANA timezone name (e.g. ``"America/New_York"``).
            Cron expressions are always evaluated in this timezone,
            independent of ``after``'s tz attribute. Local-time
            interpretation is what operators expect.
        after: A timezone-aware datetime. Naive datetimes are rejected
            because they would silently change behavior across systems
            with different default timezones.

    Returns:
        A timezone-aware datetime in the requested ``tz`` representing
        the next fire after ``after``.

    Raises:
        CronExpressionError: ``expression`` does not parse as cron, or
            ``tz`` is not a recognised IANA zone.
        ValueError: ``after`` is naive (no tzinfo).
    """
    if after.tzinfo is None:
        raise ValueError(
            "next_fire() requires a timezone-aware 'after' datetime; got naive",
        )

    try:
        zone = ZoneInfo(tz)
    except ZoneInfoNotFoundError as exc:
        raise CronExpressionError(f"unknown timezone: {tz!r}") from exc

    # croniter evaluates the expression in the tz of the base
    # datetime. Convert ``after`` to the requested tz so the
    # expression fires "at 3am Eastern" no matter what tz the caller
    # used to express ``after``.
    base = after.astimezone(zone)

    try:
        itr = croniter(expression, base)
    except (CroniterBadCronError, ValueError) as exc:
        raise CronExpressionError(
            f"invalid cron expression: {expression!r}",
        ) from exc

    # ``get_next(datetime)`` returns a tz-aware datetime in the same
    # tz as ``base`` (so, the requested tz). Strict ``>`` semantics:
    # if ``after`` happens to coincide with a fire time, the NEXT
    # fire is returned.
    return itr.get_next(datetime)


def fires_between(
    expression: str,
    tz: str,
    *,
    after: datetime,
    until: datetime,
    cap: int = 10_000,
) -> list[datetime]:
    """Return every fire slot strictly after ``after`` and ≤ ``until``.

    Used by the catch-up planner when the schedule's policy is
    ``fire_all_missed`` and the engine needs to materialise the
    backlog of missed slots between the last successful fire and
    the current tick. Audit fix (Apr 2026 follow-up) for the
    silent-contract violation where ``fire_all_missed`` was
    indistinguishable from ``fire_one_missed``.

    Args:
        expression: cron expression, same form as :func:`next_fire`.
        tz: IANA tz name.
        after: strict lower bound (exclusive). The first slot must
            be > ``after``.
        until: inclusive upper bound. The last slot can equal
            ``until``.
        cap: hard ceiling on the number of slots returned. A
            ``*/1 * * * *`` cron over a 365-day outage produces
            ~525,000 slots - we'd OOM the dispatcher's queue and
            wedge the worker fleet for hours. ``cap`` truncates and
            the caller logs the lossy result. Default 10k = ~7
            days of minute-cron, well past the operational
            "we should have noticed" threshold.

    Raises:
        CronExpressionError: same as :func:`next_fire`.
        ValueError: either ``after`` or ``until`` is naive.
    """
    if after.tzinfo is None or until.tzinfo is None:
        raise ValueError(
            "fires_between() requires timezone-aware datetimes",
        )
    if until <= after:
        return []
    try:
        zone = ZoneInfo(tz)
    except ZoneInfoNotFoundError as exc:
        raise CronExpressionError(f"unknown timezone: {tz!r}") from exc

    base = after.astimezone(zone)
    until_in_zone = until.astimezone(zone)
    try:
        itr = croniter(expression, base)
    except (CroniterBadCronError, ValueError) as exc:
        raise CronExpressionError(
            f"invalid cron expression: {expression!r}",
        ) from exc

    out: list[datetime] = []
    while True:
        nxt = itr.get_next(datetime)
        if nxt > until_in_zone:
            break
        out.append(nxt)
        if len(out) >= cap:
            break
    return out


__all__ = ["CronExpressionError", "fires_between", "is_valid", "next_fire"]
