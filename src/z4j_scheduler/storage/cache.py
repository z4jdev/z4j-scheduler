"""In-memory authoritative copy of every schedule this scheduler ticks.

Keyed by ``schedule_id``. Updated by:

1. Initial ``list_schedules`` sync on startup
2. ``watch_schedules`` event stream during runtime
3. Periodic full re-sync every ``reconcile_interval_seconds``
   (defensive against missed events)

The cache exposes a :class:`asyncio.Event` named ``changed`` that
the tick engine awaits to wake up immediately when schedules are
added / updated / deleted - no polling required.

Concurrency: a single :class:`asyncio.Lock` serialises mutations.
Reads (``snapshot``, ``get``, ``next_due``) take the lock briefly
to grab a consistent view, then release - they do not hold it
across the read.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime
    from uuid import UUID

    from z4j_scheduler.tick._entry import ScheduleEntry


class _Sentinel:
    """Marker for ``update_fire_state`` "don't touch this field" args.

    A nominal class (not the bare object) so the type hint reads
    cleanly. Callers MUST NOT instantiate; use :data:`_UNSET`.
    """

    __slots__ = ()


_UNSET: _Sentinel = _Sentinel()


class ScheduleCache:
    """Thread-safe (within one asyncio loop) schedule store.

    Designed for a single tick-engine consumer plus N watch-stream
    producers. The producers call :meth:`upsert` and :meth:`remove`;
    the consumer calls :meth:`next_due` to find the earliest fire.

    The cache fires the :attr:`changed` event after every mutation.
    Tick-engine consumers can :meth:`asyncio.Event.wait` on it to
    wake immediately on schedule changes (instead of polling or
    sleeping for the full configured interval).

    Tests construct directly with no arguments and call
    :meth:`upsert` / :meth:`remove` to populate state.
    """

    def __init__(self) -> None:
        self._entries: dict[UUID, ScheduleEntry] = {}
        self._lock = asyncio.Lock()
        # Set whenever the cache mutates. The tick engine clears it
        # before sleeping; if a mutation arrives during sleep, the
        # event is already set when the engine wakes and re-checks.
        self.changed = asyncio.Event()

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    async def upsert(self, entry: ScheduleEntry) -> None:
        """Insert or replace ``entry`` keyed by its id.

        Fires :attr:`changed` on every call (even no-op replaces) so
        the tick engine recomputes its sleep without trying to detect
        whether the change actually moved the next-fire time.
        """
        async with self._lock:
            self._entries[entry.id] = entry
        self.changed.set()

    async def upsert_many(self, entries: Iterable[ScheduleEntry]) -> None:
        """Bulk variant - one lock acquisition + one event fire."""
        items = list(entries)
        if not items:
            return
        async with self._lock:
            for entry in items:
                self._entries[entry.id] = entry
        self.changed.set()

    async def remove(self, schedule_id: UUID) -> bool:
        """Remove ``schedule_id`` if present. Returns True if removed."""
        async with self._lock:
            existed = self._entries.pop(schedule_id, None) is not None
        if existed:
            self.changed.set()
        return existed

    async def clear(self) -> None:
        """Drop every entry. Used for emergency full re-sync."""
        async with self._lock:
            had_entries = bool(self._entries)
            self._entries.clear()
        if had_entries:
            self.changed.set()

    async def update_fire_state(
        self,
        schedule_id: UUID,
        *,
        last_fire_at: datetime | None | _Sentinel = _UNSET,
        next_fire_at: datetime | None | _Sentinel = _UNSET,
        is_enabled: bool | None = None,
    ) -> bool:
        """Mutate the current cache entry's fire-state fields atomically.

        Audit fix (Apr 2026 follow-up) for the lost-update race
        identified in the security audit. Pre-fix, the tick engine
        mutated ``entry.next_fire_at`` / ``entry.last_fire_at``
        directly on the live object - while concurrently the watch
        stream's ``upsert(...)`` replaced the entry in ``_entries``
        with a freshly-constructed ScheduleEntry. The engine then
        stamped state on an orphaned object the cache had already
        evicted; the new entry sat with ``next_fire_at=None`` and
        the schedule briefly stopped firing until the next iteration
        recomputed.

        This method serialises both reads against the cache lock,
        so the field write lands on whatever entry the cache
        currently holds for ``schedule_id`` - even if a watch event
        replaced the entry concurrently. Returns False (and does
        not fire ``changed``) when the entry has been removed; the
        caller can treat that as "the schedule went away" without
        any further bookkeeping.

        We use the :data:`_UNSET` sentinel (not ``None``) for
        "don't touch" because ``None`` is a valid value for both
        ``last_fire_at`` (no fire yet) and ``next_fire_at``
        (uncomputed / one-shot exhausted). Passing ``last_fire_at=
        None`` explicitly clears the field; passing nothing leaves
        the existing value untouched.
        """
        async with self._lock:
            entry = self._entries.get(schedule_id)
            if entry is None:
                return False
            if last_fire_at is not _UNSET:
                entry.last_fire_at = last_fire_at  # type: ignore[assignment]
            if next_fire_at is not _UNSET:
                entry.next_fire_at = next_fire_at  # type: ignore[assignment]
            if is_enabled is not None:
                entry.is_enabled = is_enabled
        self.changed.set()
        return True

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get(self, schedule_id: UUID) -> ScheduleEntry | None:
        """Return the entry for ``schedule_id`` or None.

        Returns the live object from the cache; mutations to its
        ``next_fire_at`` / ``last_fire_at`` by the tick engine are
        reflected back in the cache without a separate ``upsert`` call
        (because the entry is the same identity).
        """
        async with self._lock:
            return self._entries.get(schedule_id)

    async def snapshot(self) -> list[ScheduleEntry]:
        """Return a list copy of every entry. Order is insertion-stable."""
        async with self._lock:
            return list(self._entries.values())

    async def next_due(
        self,
        *,
        before: datetime | None = None,
    ) -> ScheduleEntry | None:
        """Return the entry with the earliest ``next_fire_at``.

        Skips entries that:

        - Have ``is_enabled=False``
        - Have ``next_fire_at`` of None (uncomputed - typically a
          freshly-added one_shot whose target time is in the past)

        If ``before`` is given, only considers entries whose
        ``next_fire_at <= before`` - lets the tick engine ask "what
        should I fire RIGHT NOW" by passing the current wall-clock.
        Returns None if nothing matches.
        """
        async with self._lock:
            candidates = [
                e
                for e in self._entries.values()
                if e.is_enabled and e.next_fire_at is not None
            ]
        if not candidates:
            return None
        if before is not None:
            candidates = [e for e in candidates if e.next_fire_at <= before]
            if not candidates:
                return None
        # Tie-breaker: id sort ensures deterministic ordering when
        # multiple schedules are due at the exact same instant.
        return min(
            candidates,
            key=lambda e: (e.next_fire_at, e.id),  # type: ignore[arg-type, return-value]
        )

    async def all_due(
        self,
        *,
        before: datetime,
    ) -> list[ScheduleEntry]:
        """Return every enabled entry whose next_fire_at <= ``before``.

        Used by the tick engine when multiple schedules became due
        during the same sleep cycle - process them all before
        recomputing the next sleep.
        """
        async with self._lock:
            return sorted(
                (
                    e
                    for e in self._entries.values()
                    if (
                        e.is_enabled
                        and e.next_fire_at is not None
                        and e.next_fire_at <= before
                    )
                ),
                key=lambda e: (e.next_fire_at, e.id),  # type: ignore[arg-type, return-value]
            )

    def __len__(self) -> int:
        """Synchronous len - cheap, no lock. Used by metrics + /info."""
        return len(self._entries)


__all__ = ["ScheduleCache"]
