"""Catch-up policy implementations - what to do about missed fires.

Three modes per ``docs/SCHEDULER.md §5.4``:

- ``skip`` - if the scheduler was down when N fires were missed,
  fire none. Default. Safest - no fire storms.
- ``fire_one_missed`` - fire once on recovery, regardless of how
  many were missed. Equivalent to APScheduler's ``coalesce=True``.
- ``fire_all_missed`` - fire every missed schedule. Use only when
  the task is genuinely catch-up-safe (idempotent + history-aware).

Public API:

    Policy = Literal["skip", "fire_one_missed", "fire_all_missed"]

    plan_catch_up(
        policy, *, missed_times, now,
    ) -> list[datetime]

The function returns the list of ``scheduled_for`` timestamps the
caller (the dispatch module) should fire, in chronological order.
The list may be empty (``skip``), have exactly one entry
(``fire_one_missed``), or have ``len(missed_times)`` entries
(``fire_all_missed``).

This is pure logic - no I/O, no async, no datetime parsing. The
caller does the work of computing ``missed_times`` from the
schedule's expression + last_fire_at + now, then asks this module
"given these missed fires, which ones do I dispatch?"
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

#: Type alias for the three valid policy strings. Matches the
#: ``schedules.catch_up`` column's CHECK constraint.
Policy = Literal["skip", "fire_one_missed", "fire_all_missed"]

#: Module-level constant exposing every valid policy as a runtime
#: tuple - useful for validation and for the schema's CHECK
#: constraint mirror.
VALID_POLICIES: tuple[Policy, ...] = (
    "skip",
    "fire_one_missed",
    "fire_all_missed",
)


class InvalidPolicyError(ValueError):
    """The policy string is not one of the recognised values."""


def plan_catch_up(
    policy: str,
    *,
    missed_times: list[datetime],
    now: datetime,
) -> list[datetime]:
    """Decide which missed fires to dispatch under the given policy.

    Args:
        policy: One of ``"skip"`` / ``"fire_one_missed"`` /
            ``"fire_all_missed"``.
        missed_times: Chronologically-ordered list of times the
            schedule should have fired but did not. Empty when the
            schedule is fully caught up; the function still returns
            an empty list in that case.
        now: Current wall-clock time. Reserved for a future
            "do not fire entries older than X" enhancement; not used
            in v1.

    Returns:
        A list of ``scheduled_for`` timestamps the caller should
        dispatch, in chronological order. May be empty.

    Raises:
        InvalidPolicyError: ``policy`` is not one of the valid values.
            Callers should validate at the schema/REST layer; this
            check is defense in depth.
    """
    if policy not in VALID_POLICIES:
        raise InvalidPolicyError(
            f"unknown catch-up policy {policy!r}; "
            f"expected one of {VALID_POLICIES}",
        )

    if not missed_times:
        return []

    if policy == "skip":
        return []

    if policy == "fire_one_missed":
        # APScheduler's coalesce: collapse N missed fires into one
        # fire at the most-recent missed time. The most-recent time
        # is the operational right answer - "the schedule says do X
        # at the latest opportunity, not at every opportunity I
        # missed."
        return [missed_times[-1]]

    # policy == "fire_all_missed"
    return list(missed_times)


__all__ = [
    "VALID_POLICIES",
    "InvalidPolicyError",
    "Policy",
    "plan_catch_up",
]
