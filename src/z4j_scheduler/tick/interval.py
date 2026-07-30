"""Interval next-fire computation.

Schedule kind ``"interval"`` with expression in the form ``"30s"``,
``"5m"``, ``"2h"``, ``"1d"``, or a positive integer (interpreted as
seconds for backwards compatibility with celery-beat-style configs).

Public API:

    parse(expression) -> timedelta
    next_fire(expression, last_fire_at, anchor_at) -> datetime

Semantics:

- If the schedule has fired before (``last_fire_at`` is set), next
  fire = ``last_fire_at + interval``
- If the schedule has never fired (``last_fire_at`` is None), next
  fire is computed from ``anchor_at`` (typically the schedule's
  ``created_at``) rounded up to the next interval boundary, so a
  5-minute interval schedule created at 12:03 first fires at 12:05
- Drift correction is the schedule's responsibility, not ours - we
  always anchor to ``last_fire_at``, never to ``now()``, so a
  scheduler that's behind catches up at the cron-style cadence
  rather than skipping forward
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

# Mapping from suffix character to the timedelta unit it represents.
_UNIT_SECONDS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
}

# "30s", "5m", "2h", "1d" - positive integer + single-char unit.
# Plain integers (no suffix) are accepted as seconds for
# celery-beat-style configs that use `timedelta(seconds=N)`.
_PATTERN = re.compile(r"^\s*(\d+)\s*([smhd]?)\s*$")


class IntervalExpressionError(ValueError):
    """The interval expression is invalid.

    Distinct subclass so callers can catch this specifically.
    """


def parse(expression: str) -> timedelta:
    """Parse an interval expression into a :class:`datetime.timedelta`.

    Examples::

        parse("30s") -> timedelta(seconds=30)
        parse("5m")  -> timedelta(minutes=5)
        parse("2h")  -> timedelta(hours=2)
        parse("1d")  -> timedelta(days=1)
        parse("60")  -> timedelta(seconds=60)  # bare int = seconds

    Raises:
        IntervalExpressionError: malformed expression, zero, or
            negative interval (fail-fast on configurations that would
            cause a tight spin loop in the tick engine).
    """
    match = _PATTERN.match(expression)
    if match is None:
        raise IntervalExpressionError(
            f"interval expression must match '<int>[s|m|h|d]'; got {expression!r}",
        )
    n = int(match.group(1))
    if n <= 0:
        raise IntervalExpressionError(
            f"interval must be > 0; got {expression!r}",
        )
    unit = match.group(2) or "s"
    return timedelta(seconds=n * _UNIT_SECONDS[unit])


def next_fire(
    expression: str,
    *,
    last_fire_at: datetime | None,
    anchor_at: datetime,
) -> datetime:
    """Compute the next fire time for an interval schedule.

    Args:
        expression: An interval expression accepted by :func:`parse`.
        last_fire_at: The most recent fire timestamp. None if the
            schedule has never fired.
        anchor_at: Used only when ``last_fire_at`` is None - typically
            the schedule's ``created_at``. Defines the "boundary" the
            first fire aligns to so a schedule created at 12:03 with
            a 5m interval first fires at 12:05, not 12:08.

    Returns:
        The next fire time, timezone-aware (matches the tzinfo of the
        input). Strict ``>`` semantics relative to ``last_fire_at``
        so a schedule cannot fire twice at the same instant.

    Raises:
        IntervalExpressionError: invalid expression.
        ValueError: ``anchor_at`` (and ``last_fire_at`` when set) must
            be timezone-aware. Naive datetimes are rejected to avoid
            silent timezone-dependent behavior.
    """
    interval = parse(expression)

    if last_fire_at is not None:
        if last_fire_at.tzinfo is None:
            raise ValueError(
                "next_fire() requires tz-aware last_fire_at; got naive",
            )
        return last_fire_at + interval

    if anchor_at.tzinfo is None:
        raise ValueError(
            "next_fire() requires tz-aware anchor_at; got naive",
        )

    # First-fire alignment: round ``anchor_at`` up to the next
    # interval boundary. Implemented as integer math on epoch
    # seconds for determinism.
    interval_s = int(interval.total_seconds())
    anchor_s = int(anchor_at.timestamp())
    # Number of full intervals before anchor; +1 to land on the next.
    next_s = ((anchor_s // interval_s) + 1) * interval_s
    return datetime.fromtimestamp(next_s, tz=anchor_at.tzinfo)


def fires_between(
    expression: str,
    *,
    after: datetime,
    until: datetime,
    cap: int = 10_000,
) -> list[datetime]:
    """Return every interval slot in the half-open window ``(after, until]``.

    Mirrors :func:`z4j_scheduler.tick.cron.fires_between` so the tick engine
    can materialise the FULL missed backlog for an interval schedule on
    recovery (H4). Without this the engine could only produce a single-slot
    missed list for intervals, so ``fire_one_missed`` (and ``skip``)
    behaved like ``fire_all_missed``: the engine advanced one interval,
    re-entered still past-due, and re-fired every missed slot one tick at a
    time -- the same duplicate-fire storm B3 fixed for cron.

    Slots are ``after + k*interval`` for ``k = 1, 2, ...`` while
    ``<= until``, capped at ``cap`` (a very long outage of a short interval
    is otherwise closer to millions of slots and would wedge the dispatcher
    queue). Both bounds must be timezone-aware.
    """
    if after.tzinfo is None or until.tzinfo is None:
        raise ValueError("fires_between() requires tz-aware after/until bounds")
    interval = parse(expression)
    if interval <= timedelta(0):
        # parse() rejects non-positive expressions, but guard against an
        # infinite loop if that ever changes.
        return []
    # Closed-form, keeping the MOST-RECENT ``cap`` slots. Slots are
    # ``after + k*interval`` for k >= 1 with the slot <= until. Computing k
    # directly avoids iterating the whole window (a 1-second interval over a
    # year is ~31M slots) AND makes the cap keep the newest, not the oldest, so
    # ``slots[-1]`` is the true latest missed slot the engine anchors to.
    span = until - after
    if span < interval:
        return []
    k_max = int(span // interval)  # after + k_max*interval <= until
    if k_max < 1:
        return []
    k_start = max(1, k_max - cap + 1)
    return [after + k * interval for k in range(k_start, k_max + 1)]


__all__ = ["IntervalExpressionError", "fires_between", "next_fire", "parse"]
