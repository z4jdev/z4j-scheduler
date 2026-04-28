"""One-shot schedule semantics.

Schedule kind ``"one_shot"`` with expression as an ISO-8601 absolute
timestamp (e.g. ``"2026-04-30T15:00:00Z"`` or
``"2026-04-30T15:00:00+00:00"``). Fires exactly once at that time.

After the fire completes, the brain-side ``AcknowledgeFireResult``
handler flips ``schedules.is_enabled = False`` so the schedule does
not re-fire. The tick engine treats one-shot schedules identically
to cron/interval until that flip happens.

Public API:

    next_fire(expression, last_fire_at) -> datetime | None

Returns:

- The configured timestamp if the schedule has not yet fired
- None if it has already fired (caller treats this as "schedule is
  done; do not re-tick")
"""

from __future__ import annotations

from datetime import datetime


class OneShotExpressionError(ValueError):
    """The one-shot expression is not a valid ISO-8601 timestamp.

    Distinct subclass so callers can catch this specifically.
    """


def next_fire(
    expression: str,
    *,
    last_fire_at: datetime | None,
) -> datetime | None:
    """Compute the next fire time for a one-shot schedule.

    Args:
        expression: ISO-8601 timestamp string. The Python parser
            accepts ``YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM|Z]``.
        last_fire_at: The most recent fire timestamp. None if the
            schedule has never fired.

    Returns:
        The configured timestamp (always tz-aware - we reject naive
        values) if the schedule has not yet fired. ``None`` if
        ``last_fire_at`` is set, meaning the schedule is done.

    Raises:
        OneShotExpressionError: expression does not parse as ISO-8601,
            or parses to a naive (tz-less) datetime. Naive timestamps
            are rejected because their meaning is ambiguous across
            deployments in different timezones.
    """
    if last_fire_at is not None:
        return None

    # Python's fromisoformat accepts a trailing 'Z' as of 3.11+.
    try:
        moment = datetime.fromisoformat(expression)
    except ValueError as exc:
        raise OneShotExpressionError(
            f"one-shot expression must be ISO-8601; got {expression!r}",
        ) from exc

    if moment.tzinfo is None:
        raise OneShotExpressionError(
            f"one-shot expression must include a timezone offset (e.g. "
            f"'...Z' or '...+00:00'); got naive {expression!r}",
        )

    return moment


__all__ = ["OneShotExpressionError", "next_fire"]
