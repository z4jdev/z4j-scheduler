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

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfoNotFoundError

from croniter import CroniterBadCronError, croniter

from z4j_scheduler.tick._runtime import packaged_zoneinfo

#: How far back to probe for a DST transition when deciding where the corrective
#: forward walk must start. Real-world shifts are 30 minutes to 2 hours; 3 hours
#: covers every current zone with margin, and probing wider costs nothing because
#: it only reads a UTC offset.
_MAX_DST_SHIFT = timedelta(hours=3)


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
        zone = packaged_zoneinfo(tz)
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


def fires_between(  # noqa: PLR0912 -- validation and DST branches are explicit safety gates
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
    # Validate BEFORE any early return, so a bad expression or zone raises
    # regardless of the window or the cap. Returning [] for a degenerate window
    # while silently accepting "not-a-cron" would hide a config error until the
    # window happened to be non-degenerate.
    try:
        zone = packaged_zoneinfo(tz)
    except ZoneInfoNotFoundError as exc:
        raise CronExpressionError(f"unknown timezone: {tz!r}") from exc
    try:
        croniter(expression)
    except (CroniterBadCronError, ValueError) as exc:
        raise CronExpressionError(
            f"invalid cron expression: {expression!r}",
        ) from exc
    if until <= after:
        return []
    # A non-positive cap must not be treated as "unbounded". out[-cap:]
    # with cap==0 is out[0:], i.e. the WHOLE window, and a negative cap slices
    # from the wrong end. A cap is a safety bound, so the conservative answer is
    # no slots rather than an unbounded backlog.
    if cap <= 0:
        return []

    after_in_zone = after.astimezone(zone)
    try:
        # Two-phase, so we get BOTH properties at once:
        #
        # 1. Walk BACKWARD from ``until`` at most ``cap`` slots purely to
        #    establish a LOWER BOUND. This is what keeps the cap truncation on
        #    the MOST-RECENT slots (fire_one_missed must coalesce to the
        #    true latest, and the engine anchors on ``missed_times[-1]``), and it
        #    keeps the whole call O(cap) rather than O(window), so a fine cadence
        #    over a long outage never iterates millions of slots. The +1us nudge
        #    makes an ``until`` that lands exactly on a slot INCLUSIVE (get_prev
        #    is otherwise strictly-before).
        # 2. ENUMERATE FORWARD from that bound.: the forward walk is the one
        #    that reproduces DST fall-back correctly -- a repeated wall-clock
        #    hour yields BOTH distinct absolute moments (fold 0 and fold 1),
        #    which this module explicitly commits to. A pure backward walk is
        #    fold-blind and silently DROPS one of them, which made
        #    fire_one_missed coalesce to an older slot and the engine creep
        #    forward a tick later.
        # Nudge in ABSOLUTE time, then convert. ``until_in_zone + 1us`` is
        # wall-clock arithmetic, so on a fall-back day it lands one microsecond
        # after a repeated wall time rather than after the real instant, and the
        # backward walk starts up to a whole overlap LATE. With a fine cadence
        # that difference is spent out of the cap: for Antarctica/Troll, a
        # per-second cron over 22:00Z..00:59:59.999999Z should return the latest
        # 10,000 of 10,799 due seconds and returned 2,800. Every other bound in
        # this function is already compared in UTC; this one was the exception.
        back = croniter(
            expression,
            (until.astimezone(UTC) + timedelta(microseconds=1)).astimezone(zone),
        )
    except (CroniterBadCronError, ValueError) as exc:
        raise CronExpressionError(
            f"invalid cron expression: {expression!r}",
        ) from exc

    # Every boundary comparison is done in ABSOLUTE (UTC) time. Comparing
    # two aware datetimes that share the SAME tzinfo makes Python compare the
    # naive wall-clock fields and IGNORE ``fold`` (documented behaviour), so on a
    # DST fall-back day an EARLIER instant (01:30 fold=0) compares as "after" a
    # LATER one (01:17 fold=1). That silently admitted an out-of-window slot and
    # broke the backward bound. Comparing in UTC is unambiguous.
    after_utc = after.astimezone(UTC)
    until_utc = until.astimezone(UTC)

    # The two walks are each blind to one DST direction, so neither alone is
    # correct and we UNION them, keyed by absolute instant.
    #
    #   - ``get_prev`` reports a SPRING-GAP occurrence by snapping the
    #     nonexistent local time forward (Europe/London "0 1 * * * 30" on
    #     2026-03-29 has no local 01:00:30, and get_prev yields 02:00:30 BST).
    #     A forward walk starting just before that instant can NEVER re-report
    #     it, because 02:00:30 does not match hour=1 -- so the occurrence was
    #     silently dropped and fire_all_missed skipped it permanently.
    #   - ``get_next`` is the only one that yields BOTH absolute moments of a
    #     repeated FALL-BACK hour (fold 0 and fold 1), which this module commits
    #     to; a backward walk is fold-blind and drops one.
    #
    # Collecting from both and de-duplicating by UTC instant gives each walk's
    # strength without either's blind spot. Both remain bounded by ``cap``.
    found: dict[datetime, datetime] = {}
    lower = after_in_zone
    for _ in range(cap):
        prv = back.get_prev(datetime)
        prv_utc = prv.astimezone(UTC)
        if prv_utc <= after_utc:  # ``after`` is an exclusive bound
            break
        lower = prv
        if prv_utc <= until_utc:
            found[prv_utc] = prv

    # Step back one second in ABSOLUTE time, not wall-clock time. Plain
    # ``lower - timedelta`` on an aware datetime is wall-clock arithmetic, so on
    # a SPRING-FORWARD day it lands inside the hour that does not exist (e.g.
    # 03:00 EDT minus 1s = 02:59:59, a skipped local time) and croniter then
    # walks forward past the first real slot, dropping it. Round-tripping
    # through UTC keeps the instant valid in the zone (01:59:59 EST).
    # Step back far enough to clear the WHOLE overlap, not one second.
    #
    # On a fall-back the backward iterator is fold-blind, so ``lower`` can land
    # on the SECOND occurrence of a repeated wall-clock time. Starting one second
    # before that is still inside the overlap, and the forward walk then never
    # revisits the FIRST occurrence: for Pacific/Chatham (a 45-minute-offset zone)
    # "*/15 3 * * * 30" over the transition returned four slots where seven are
    # due, silently dropping three. Backing off by the overlap's own width puts
    # the start ahead of both folds, and the union plus the cap discard anything
    # extra that produces.
    lower_utc = lower.astimezone(UTC)
    back_off = timedelta(seconds=1)
    offset_here = lower_utc.astimezone(zone).utcoffset()
    offset_before = (lower_utc - _MAX_DST_SHIFT).astimezone(zone).utcoffset()
    if offset_here is not None and offset_before is not None and offset_before != offset_here:
        # Fall-back (the offset DECREASED) repeats a span equal to the shift.
        overlap = offset_before - offset_here
        if overlap > timedelta(0):
            back_off = overlap + timedelta(seconds=1)
    start = (lower_utc - back_off).astimezone(zone)
    if start.astimezone(UTC) < after_utc:
        start = after_in_zone
    fwd = croniter(expression, start)
    while True:
        nxt = fwd.get_next(datetime)
        nxt_utc = nxt.astimezone(UTC)
        if nxt_utc > until_utc:
            break
        if nxt_utc > after_utc:
            found.setdefault(nxt_utc, nxt)
    # Sort by ABSOLUTE instant: the zoned values are not safely comparable across
    # a fold (Python compares naive wall-clock fields when the tzinfo matches).
    out = [found[key] for key in sorted(found)]
    # out[-1] is the latest slot <= until. Either walk can contribute a slot the
    # other misses, so trim to the most-recent ``cap``.
    return out[-cap:] if len(out) > cap else out


__all__ = ["CronExpressionError", "fires_between", "is_valid", "next_fire"]
