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
from dataclasses import dataclass
from typing import TYPE_CHECKING

from z4j_scheduler.tick._entry import (
    schedule_brain_payload,
    schedule_control_identity,
    schedule_definition_changed,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime
    from uuid import UUID

    from z4j_scheduler.storage._models import ScheduleSnapshot
    from z4j_scheduler.tick._entry import ScheduleEntry


class _Sentinel:
    """Marker for ``update_fire_state`` "don't touch this field" args.

    A nominal class (not the bare object) so the type hint reads
    cleanly. Callers MUST NOT instantiate; use :data:`_UNSET`.
    """

    __slots__ = ()


_UNSET: _Sentinel = _Sentinel()


@dataclass(frozen=True, slots=True)
class LocalQuarantine:
    """Process-local fail-closed latch for one definition generation."""

    definition_identity: tuple[object, ...]
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class CurrentStop:
    """Process-local stop held until Brain state supersedes what refused a fire.

    Two facts, because the control token alone cannot end the stop. A Brain
    holds a schedule by stamping ``paused_at`` and deliberately keeps the same
    control token, since a hold does not redefine the schedule. A stop keyed on
    the token alone therefore survives the release: the resume carries the same
    token, matches, and re-clamps the row. The schedule stayed dark until the
    process restarted, which is the one remedy an operator has no reason to
    reach for.

    ``refused_at_revision`` is the Brain revision the refusal reported as live,
    and Brain revisions are globally ordered and allocated by every transition
    including a resume. Once this scheduler holds a row at or past it, it holds
    the very state that refused the fire, and the stop has done its work: what
    happens next is decided by that row's own enabled state, which is Brain's
    to set. A hold still reads as not-enabled on the wire, so releasing the stop
    does not release the hold.

    A refusal that carries no live revision (Brain answering that the schedule
    does not exist) leaves this zero, and the stop then ends only when the
    control generation is superseded or the row is removed. There is no state
    to wait for, so waiting for none of it would be the wrong default.
    """

    control_token: UUID
    refused_at_revision: int


class ScheduleProtocolError(RuntimeError):
    """Brain schedule state violated the ordered current protocol."""


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

    def __init__(self, *, max_post_watermark_tombstones: int = 10_000) -> None:
        if max_post_watermark_tombstones <= 0:
            raise ValueError("max_post_watermark_tombstones must be positive")
        self._entries: dict[UUID, ScheduleEntry] = {}
        self._local_quarantines: dict[UUID, LocalQuarantine] = {}
        self._current_stop_latches: dict[UUID, CurrentStop] = {}
        self._brain_payloads: dict[UUID, tuple[object, ...]] = {}
        self._id_revisions: dict[UUID, int] = {}
        self._id_projects: dict[UUID, UUID] = {}
        self._project_watermarks: dict[UUID, int] = {}
        self._global_watermark = 0
        self._max_post_watermark_tombstones = max_post_watermark_tombstones
        self._snapshot_required_projects: set[UUID] = set()
        self._lock = asyncio.Lock()
        # Set whenever the cache mutates. The tick engine clears it
        # before sleeping; if a mutation arrives during sleep, the
        # event is already set when the engine wakes and re-checks.
        self.changed = asyncio.Event()
        # Leader check for the watch echo-merge. When this instance is
        # the LEADER for a project, the tick ENGINE is authoritative for that
        # project's fire-state, so a same-cadence echo preserves the engine's
        # computed fire-state. When it is a FOLLOWER, the LEADER fires and brain
        # advances the row, so the follower must ADOPT the echo's advancement
        # instead of preserving its own stale local state (otherwise it never
        # converges, busy-spins on the due slot, and can replay after failover).
        # None (the default, and every single-instance / test deployment) means
        # "assume leader" -- unchanged behavior. The scheduler wires the real
        # leader gate in after it is built.
        self.is_leader: Callable[[UUID], bool] | None = None

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
            self._validate_protocol_shape(entry)
            if entry.control_token is not None:
                self._brain_payloads[entry.id] = schedule_brain_payload(entry)
                self._id_revisions[entry.id] = entry.schedule_revision
                self._id_projects[entry.id] = entry.project_id
            self._apply_local_quarantine_locked(entry)
            self._apply_current_stop_locked(entry)
            self._entries[entry.id] = entry
        self.changed.set()

    async def upsert_many(self, entries: Iterable[ScheduleEntry]) -> None:
        """Bulk variant - one lock acquisition + one event fire."""
        items = list(entries)
        if not items:
            return
        async with self._lock:
            for entry in items:
                self._validate_protocol_shape(entry)
                if entry.control_token is not None:
                    self._brain_payloads[entry.id] = schedule_brain_payload(entry)
                    self._id_revisions[entry.id] = entry.schedule_revision
                    self._id_projects[entry.id] = entry.project_id
                self._apply_local_quarantine_locked(entry)
                self._apply_current_stop_locked(entry)
                self._entries[entry.id] = entry
        self.changed.set()

    def _apply_watch_update_locked(self, incoming: ScheduleEntry) -> None:
        """Merge one brain watch echo into the cache. Caller holds ``_lock``.

        The engine, NOT brain, is authoritative for fire-state
        (``last_fire_at`` / ``next_fire_at`` / ``anchor_at``). A brain fire-ack
        echo re-sends the SAME cadence with an advanced ``last_run_at``; arriving
        as a wholesale replace it would overwrite the engine's computed
        ``next_fire_at`` with brain's ``next_run_at`` (None for scheduler-managed
        rows) and a fresh wall-clock ``anchor_at`` -- drifting interval cadence by
        the ack latency every fire (H6), and, for a fresh schedule whose first
        fire FAILED (both run timestamps null), re-anchoring to a future slot so
        the failed slot is never retried (H7).

        So: replace WHOLESALE only when the entry is new (a genuine CREATED) or
        its cadence actually changed (a real edit -- the engine recomputes its
        fire-state next tick). On a same-cadence echo of an existing entry, adopt
        brain's display/enabled fields but KEEP the engine's fire-state -- BUT
        ONLY while this instance is the LEADER for the schedule's project.
        A FOLLOWER does not fire; the leader does and brain advances the row, so
        the follower must ADOPT the echo (store it wholesale). The echo's
        next_fire_at is brain's next_run_at (None for scheduler-managed rows), so
        _compute_pending_next_fires recomputes the follower forward from the
        adopted anchor, and it converges instead of busy-spinning / replaying
        after a failover.
        """
        existing = self._entries.get(incoming.id)
        self._validate_protocol_shape(incoming)
        incoming_payload = schedule_brain_payload(incoming)
        current_protocol = (
            existing is not None
            and existing.control_token is not None
            and incoming.control_token is not None
            and existing.schedule_revision > 0
            and incoming.schedule_revision > 0
        )
        if (
            existing is not None
            and existing.control_token is not None
            and incoming.project_id != existing.project_id
        ):
            raise ScheduleProtocolError(
                "schedule id changed project without an ordered tombstone",
            )
        known_revision = self._id_revisions.get(incoming.id, 0)
        watermark = max(
            self._global_watermark,
            self._project_watermarks.get(incoming.project_id, 0),
        )
        if incoming.control_token is not None:
            if incoming.schedule_revision < known_revision:
                return
            if (
                incoming.schedule_revision <= watermark
                and incoming.schedule_revision > known_revision
            ):
                return
        if existing is not None and existing.control_token is not None:
            if not current_protocol:
                raise ScheduleProtocolError(
                    "current schedule received a legacy or malformed update",
                )
            if incoming.schedule_revision < existing.schedule_revision:
                return
            if incoming.schedule_revision == existing.schedule_revision:
                if self._brain_payloads.get(incoming.id) != incoming_payload:
                    raise ScheduleProtocolError(
                        "same schedule revision carried conflicting Brain state",
                    )
                return
        if (
            existing is not None
            and not current_protocol
            and not schedule_definition_changed(existing, incoming)
            and (self.is_leader is None or self.is_leader(existing.project_id))
        ):
            incoming.anchor_at = existing.anchor_at
            incoming.last_fire_at = existing.last_fire_at
            incoming.next_fire_at = existing.next_fire_at
        if incoming.control_token is not None:
            self._brain_payloads[incoming.id] = incoming_payload
            self._id_revisions[incoming.id] = incoming.schedule_revision
            self._id_projects[incoming.id] = incoming.project_id
        self._apply_local_quarantine_locked(incoming)
        self._apply_current_stop_locked(incoming)
        self._entries[incoming.id] = incoming

    @staticmethod
    def _validate_protocol_shape(entry: ScheduleEntry) -> None:
        current = (
            entry.control_token is not None
            and entry.schedule_revision > 0
            and bool(entry.definition_digest)
            and entry.cadence_semantics_version > 0
            and bool(entry.cadence_runtime_fingerprint)
        )
        legacy = (
            entry.control_token is None
            and entry.schedule_revision == 0
            and not entry.definition_digest
            and entry.cadence_semantics_version == 0
            and not entry.cadence_runtime_fingerprint
        )
        if not (current or legacy):
            raise ScheduleProtocolError(
                "current schedule control, revision, definition, and cadence "
                "semantics fields must appear together",
            )

    def _apply_local_quarantine_locked(self, incoming: ScheduleEntry) -> None:
        """Clamp a matching generation, or discard a superseded latch."""

        quarantine = self._local_quarantines.get(incoming.id)
        if quarantine is None:
            return
        if quarantine.definition_identity != schedule_control_identity(incoming):
            self._local_quarantines.pop(incoming.id, None)
            return
        incoming.is_enabled = False

    @staticmethod
    def _stop_is_superseded(*, live_revision: int, refused_at_revision: int) -> bool:
        """Whether Brain state at ``live_revision`` has overtaken a refusal.

        Two moments ask this question from opposite sides: one decides whether
        to install a stop, the other whether to clear one. They have to agree,
        or a stop can be installed on state that would immediately clear it and
        then never see a clearing update, so the predicate is written once.

        A refusal carrying no live revision (see :class:`CurrentStop`) is never
        superseded by a revision, only by a new control generation.
        """

        return refused_at_revision > 0 and live_revision >= refused_at_revision

    def _apply_current_stop_locked(self, incoming: ScheduleEntry) -> None:
        """Clamp a terminal/refresh stop until Brain supersedes what refused."""

        stop = self._current_stop_latches.get(incoming.id)
        if stop is None:
            return
        if incoming.control_token != stop.control_token:
            self._current_stop_latches.pop(incoming.id, None)
            return
        if self._stop_is_superseded(
            live_revision=incoming.schedule_revision,
            refused_at_revision=stop.refused_at_revision,
        ):
            self._current_stop_latches.pop(incoming.id, None)
            return
        incoming.is_enabled = False

    async def apply_watch_update(self, incoming: ScheduleEntry) -> None:
        """Apply a single WatchSchedules event (CREATED / UPDATED) echo-safely."""
        async with self._lock:
            self._apply_watch_update_locked(incoming)
        self.changed.set()

    async def apply_watch_updates(self, entries: Iterable[ScheduleEntry]) -> None:
        """Bulk echo-safe apply (full-sync). One lock acquisition + one event."""
        items = list(entries)
        if not items:
            return
        async with self._lock:
            for entry in items:
                self._apply_watch_update_locked(entry)
        self.changed.set()

    async def remove(self, schedule_id: UUID) -> bool:
        """Remove ``schedule_id`` if present. Returns True if removed."""
        async with self._lock:
            existed = self._entries.pop(schedule_id, None) is not None
            self._local_quarantines.pop(schedule_id, None)
            self._current_stop_latches.pop(schedule_id, None)
            self._brain_payloads.pop(schedule_id, None)
            self._id_revisions.pop(schedule_id, None)
            self._id_projects.pop(schedule_id, None)
        if existed:
            self.changed.set()
        return existed

    async def clear(self) -> None:
        """Drop every entry. Used for emergency full re-sync."""
        async with self._lock:
            had_entries = bool(self._entries)
            self._entries.clear()
            self._local_quarantines.clear()
            self._current_stop_latches.clear()
            self._brain_payloads.clear()
            self._id_revisions.clear()
            self._id_projects.clear()
            self._project_watermarks.clear()
            self._global_watermark = 0
            self._snapshot_required_projects.clear()
        if had_entries:
            self.changed.set()

    async def update_fire_state(
        self,
        schedule_id: UUID,
        *,
        last_fire_at: datetime | _Sentinel | None = _UNSET,
        next_fire_at: datetime | _Sentinel | None = _UNSET,
        is_enabled: bool | None = None,
        expected_definition: ScheduleEntry | None = None,
    ) -> bool:
        """Mutate the current cache entry's fire-state fields atomically.

        Closes a lost-update race: if the tick engine mutated
        ``entry.next_fire_at`` / ``entry.last_fire_at`` directly
        on the live object - while concurrently the watch
        stream's ``upsert(...)`` replaced the entry in
        ``_entries`` with a freshly-constructed ScheduleEntry -
        the engine would stamp state on an orphaned object the
        cache had already evicted; the new entry would sit with
        ``next_fire_at=None`` and the schedule would briefly
        stop firing until the next iteration recomputed.

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

        RM11: when ``expected_definition`` is supplied the write is a
        COMPARE-AND-SET on the schedule DEFINITION (cadence: kind / expression /
        timezone / catch_up -- NOT anchor_at, see schedule_definition_changed).
        The tick engine computes a fire-state from a snapshot; if a real cadence
        EDIT replaced the entry in between, the definition differs and the write
        is skipped (the edited entry recomputes its own next_fire_at).

        NOTE: this CAS guards ONLY the tick engine's OWN in-place
        fire-state writes; it does NOT guard the brain watch echo, which lands via
        ``apply_watch_update`` / ``apply_watch_updates`` (a wholesale entry
        replace), not through this method. Protecting the engine's advance from a
        benign same-cadence echo is therefore done THERE, by preserving the
        engine's fire-state on a same-cadence replace -- not here. Writes with no
        ``expected_definition`` (e.g. an explicit disable) are unconditional.
        """
        async with self._lock:
            entry = self._entries.get(schedule_id)
            if entry is None:
                return False
            if expected_definition is not None and schedule_definition_changed(
                expected_definition, entry
            ):
                return False
            if last_fire_at is not _UNSET:
                entry.last_fire_at = last_fire_at  # type: ignore[assignment]
            if next_fire_at is not _UNSET:
                entry.next_fire_at = next_fire_at  # type: ignore[assignment]
            if is_enabled is not None:
                entry.is_enabled = is_enabled
        self.changed.set()
        return True

    async def apply_cursor_transition(
        self,
        schedule_id: UUID,
        *,
        expected_control_token: UUID,
        expected_revision: int,
        expected_last_run_at: datetime | None,
        expected_next_run_at: datetime | None,
        committed_revision: int,
        committed_last_run_at: datetime | None,
        committed_next_run_at: datetime | None,
    ) -> bool:
        """Install Brain-committed cadence progress by exact current-state CAS.

        This is the only current-protocol local cursor write. The caller first
        persists a prepared transition through ``FireSchedule`` or
        ``AdvanceScheduleCursor``; only the durable response reaches here.
        A concurrent Watch update, operator edit, or another replica's accepted
        fire wins by changing the token/revision/cursor before this lock.
        """

        if expected_revision <= 0 or committed_revision <= expected_revision:
            raise ScheduleProtocolError(
                "cursor transition revisions must be positive and increasing",
            )
        async with self._lock:
            entry = self._entries.get(schedule_id)
            if entry is None or entry.control_token != expected_control_token:
                return False

            committed_cursor = (committed_last_run_at, committed_next_run_at)
            live_cursor = (entry.last_fire_at, entry.next_fire_at)
            if entry.schedule_revision == committed_revision:
                return live_cursor == committed_cursor
            if entry.schedule_revision != expected_revision or live_cursor != (
                expected_last_run_at,
                expected_next_run_at,
            ):
                return False

            entry.last_fire_at = committed_last_run_at
            entry.next_fire_at = committed_next_run_at
            # Current Brain serialization derives the internal cadence anchor
            # from its authoritative cursor. A valid enabled current row always
            # has one member here (including exhausted clocked rows via last).
            entry.anchor_at = committed_last_run_at or committed_next_run_at or entry.anchor_at
            entry.schedule_revision = committed_revision
            self._id_revisions[schedule_id] = committed_revision
            self._brain_payloads[schedule_id] = schedule_brain_payload(entry)
        self.changed.set()
        return True

    async def quarantine_locally(
        self,
        schedule_id: UUID,
        *,
        expected_definition: ScheduleEntry,
        code: str,
        detail: str = "",
    ) -> bool:
        """Latch one observed definition disabled before a Brain round trip.

        The compare uses the control token on current protocol and the
        canonical cadence identity in the explicit legacy mode. A concurrent
        operator edit therefore wins without a stale local disable clobbering
        the repaired definition.
        """

        bounded_code = code.strip()[:64] or "definition_invalid"
        bounded_detail = "".join(
            character
            for character in detail
            if character in {" ", "\t"} or 32 <= ord(character) < 127
        )[:500]
        expected_identity = schedule_control_identity(expected_definition)
        async with self._lock:
            entry = self._entries.get(schedule_id)
            if entry is None or schedule_control_identity(entry) != expected_identity:
                return False
            self._local_quarantines[schedule_id] = LocalQuarantine(
                definition_identity=expected_identity,
                code=bounded_code,
                detail=bounded_detail,
            )
            entry.is_enabled = False
        self.changed.set()
        return True

    async def local_quarantine(
        self,
        schedule_id: UUID,
    ) -> LocalQuarantine | None:
        """Return the local latch for diagnostics and report delivery."""

        async with self._lock:
            return self._local_quarantines.get(schedule_id)

    async def latch_current_stop(
        self,
        schedule_id: UUID,
        *,
        expected_control_token: UUID,
        refused_at_revision: int,
    ) -> bool:
        """Stop one current generation until Brain state supersedes the refusal.

        ``refused_at_revision`` is the live revision the refusing Brain
        reported, and it is required rather than defaulted: a caller with no
        revision to give is asking for a stop nothing can end, and that has to
        be a deliberate zero at the call site rather than an omission.
        See :class:`CurrentStop` for what the stop then waits on.

        Returns False, having stopped nothing, when the Watch stream reached
        the state that refused the fire (or a later one) before this response
        got back. The stop exists to wait for exactly that state, so there is
        nothing left to wait for, and what runs next is the row's own enabled
        value, which is Brain's to set. Installing a stop here instead would
        clamp state this scheduler has already caught up to and then wait for
        an update it has already consumed: a same-revision echo is dropped as
        a duplicate before any stop is examined, and a disabled row never
        fires, so Brain is never asked to move it again. The schedule stays
        dark until the process restarts.
        """

        async with self._lock:
            entry = self._entries.get(schedule_id)
            if entry is None or entry.control_token != expected_control_token:
                return False
            # Deliberately NOT a token comparison. A hold and its release
            # carry the same control token by design, so a token that still
            # matches proves nothing about whether the refusal is stale.
            if self._stop_is_superseded(
                live_revision=entry.schedule_revision,
                refused_at_revision=refused_at_revision,
            ):
                return False
            self._current_stop_latches[schedule_id] = CurrentStop(
                control_token=expected_control_token,
                refused_at_revision=max(0, refused_at_revision),
            )
            entry.is_enabled = False
        self.changed.set()
        return True

    async def apply_tombstone(
        self,
        *,
        schedule_id: UUID,
        project_id: UUID,
        revision: int,
    ) -> bool:
        """Apply one ordered current-protocol delete."""

        if revision <= 0:
            raise ScheduleProtocolError("tombstone revision must be positive")
        async with self._lock:
            known_project = self._id_projects.get(schedule_id)
            if known_project is not None and known_project != project_id:
                raise ScheduleProtocolError(
                    "schedule tombstone changed project identity",
                )
            known_revision = self._id_revisions.get(schedule_id, 0)
            watermark = max(
                self._global_watermark,
                self._project_watermarks.get(project_id, 0),
            )
            if revision < known_revision or (revision <= watermark and revision > known_revision):
                return False
            if revision == known_revision:
                if schedule_id in self._entries:
                    raise ScheduleProtocolError(
                        "same revision conflicts between upsert and tombstone",
                    )
                return False
            self._entries.pop(schedule_id, None)
            self._local_quarantines.pop(schedule_id, None)
            self._current_stop_latches.pop(schedule_id, None)
            self._brain_payloads.pop(schedule_id, None)
            self._id_revisions[schedule_id] = revision
            self._id_projects[schedule_id] = project_id
            if (
                self._post_watermark_tombstone_count_locked(project_id)
                >= self._max_post_watermark_tombstones
            ):
                self._snapshot_required_projects.add(project_id)
        self.changed.set()
        return True

    async def apply_observed_absence(
        self,
        *,
        schedule_id: UUID,
        project_id: UUID,
        observed_revision: int,
    ) -> bool:
        """Apply authenticated per-id absence without widening its scope."""

        if observed_revision <= 0:
            raise ScheduleProtocolError("absence observation must be positive")
        async with self._lock:
            known_project = self._id_projects.get(schedule_id)
            if known_project is not None and known_project != project_id:
                raise ScheduleProtocolError(
                    "schedule absence changed project identity",
                )
            known_revision = self._id_revisions.get(schedule_id, 0)
            if observed_revision < known_revision:
                return False
            if observed_revision == known_revision:
                if schedule_id in self._entries:
                    raise ScheduleProtocolError(
                        "same revision conflicts between row and absence",
                    )
                return False
            self._entries.pop(schedule_id, None)
            self._local_quarantines.pop(schedule_id, None)
            self._current_stop_latches.pop(schedule_id, None)
            self._brain_payloads.pop(schedule_id, None)
            self._id_revisions[schedule_id] = observed_revision
            self._id_projects[schedule_id] = project_id
        self.changed.set()
        return True

    def _post_watermark_tombstone_count_locked(self, project_id: UUID) -> int:
        """Count delete evidence not yet subsumed by a stable snapshot."""

        watermark = max(
            self._global_watermark,
            self._project_watermarks.get(project_id, 0),
        )
        return sum(
            1
            for schedule_id, revision in self._id_revisions.items()
            if (
                self._id_projects.get(schedule_id) == project_id
                and schedule_id not in self._entries
                and revision > watermark
            )
        )

    async def apply_completed_snapshot(  # noqa: PLR0912, PLR0915 - explicit fail-closed frame checks
        self,
        snapshot: ScheduleSnapshot,
    ) -> bool:
        """Validate and atomically reconcile one complete V2 snapshot."""

        from z4j_scheduler.storage._snapshot import snapshot_digest

        if snapshot.watermark < 0:
            raise ScheduleProtocolError("snapshot watermark cannot be negative")
        if snapshot.digest != snapshot_digest(snapshot):
            raise ScheduleProtocolError("snapshot digest does not match its frames")
        seen: set[UUID] = set()
        for row in snapshot.rows:
            self._validate_protocol_shape(row)
            if row.control_token is None:
                raise ScheduleProtocolError("current snapshot carried a legacy row")
            if snapshot.project_id is not None and row.project_id != snapshot.project_id:
                raise ScheduleProtocolError("snapshot row escaped its project scope")
            if row.id in seen:
                raise ScheduleProtocolError("snapshot contains a duplicate schedule id")
            if row.schedule_revision > snapshot.watermark:
                raise ScheduleProtocolError("snapshot row revision exceeds its watermark")
            seen.add(row.id)

        async with self._lock:
            current_watermark = self._global_watermark
            if snapshot.project_id is not None:
                current_watermark = max(
                    current_watermark,
                    self._project_watermarks.get(snapshot.project_id, 0),
                )
            if snapshot.watermark < current_watermark:
                return False

            # Validate all same-revision comparisons before mutating anything.
            for row in snapshot.rows:
                known_revision = self._id_revisions.get(row.id, 0)
                if row.schedule_revision == known_revision and self._brain_payloads.get(
                    row.id
                ) != schedule_brain_payload(row):
                    raise ScheduleProtocolError(
                        "snapshot reused a revision with conflicting state",
                    )

            for row in snapshot.rows:
                self._apply_watch_update_locked(row)

            for schedule_id, entry in list(self._entries.items()):
                if snapshot.project_id is not None and entry.project_id != snapshot.project_id:
                    continue
                if schedule_id in seen:
                    continue
                if self._id_revisions.get(schedule_id, 0) > snapshot.watermark:
                    continue
                self._entries.pop(schedule_id, None)
                self._local_quarantines.pop(schedule_id, None)
                self._current_stop_latches.pop(schedule_id, None)
                self._brain_payloads.pop(schedule_id, None)
                # The project watermark now subsumes this absence; a per-id
                # tombstone at/below it is redundant.
                self._id_revisions.pop(schedule_id, None)
                self._id_projects.pop(schedule_id, None)
            if snapshot.project_id is None:
                self._global_watermark = snapshot.watermark
            else:
                self._project_watermarks[snapshot.project_id] = snapshot.watermark
            # Absence revisions at/below a completed project watermark are now
            # redundant. Compact them only after the atomic snapshot install;
            # an LRU/time-based eviction before this point would permit a
            # delayed upsert to resurrect deleted work.
            for schedule_id, revision in list(self._id_revisions.items()):
                if (
                    (
                        snapshot.project_id is None
                        or self._id_projects.get(schedule_id) == snapshot.project_id
                    )
                    and schedule_id not in self._entries
                    and revision <= snapshot.watermark
                ):
                    self._id_revisions.pop(schedule_id, None)
                    self._id_projects.pop(schedule_id, None)
            if snapshot.project_id is None:
                projects = set(self._id_projects.values())
                self._snapshot_required_projects = {
                    project_id
                    for project_id in projects
                    if self._post_watermark_tombstone_count_locked(project_id)
                    >= self._max_post_watermark_tombstones
                }
            elif (
                self._post_watermark_tombstone_count_locked(
                    snapshot.project_id,
                )
                < self._max_post_watermark_tombstones
            ):
                self._snapshot_required_projects.discard(
                    snapshot.project_id,
                )
            else:
                self._snapshot_required_projects.add(snapshot.project_id)
        self.changed.set()
        return True

    async def project_watermark(self, project_id: UUID) -> int:
        """Return the highest atomically completed snapshot watermark."""

        async with self._lock:
            return max(
                self._global_watermark,
                self._project_watermarks.get(project_id, 0),
            )

    async def requires_stable_snapshot(self, project_id: UUID) -> bool:
        """Whether tombstone pressure has paused cadence for ``project_id``."""

        async with self._lock:
            return project_id in self._snapshot_required_projects

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

    async def count_for_project(self, project_id: UUID) -> int:
        """Number of cached schedules owned by ``project_id``.

        Used by the tick engine to decide whether the fire-variance
        histogram should carry a per-schedule label (bounded cardinality
        on large tenants). O(n) under the lock; called only on a fire,
        which is far rarer than a tick.
        """
        async with self._lock:
            return sum(1 for e in self._entries.values() if e.project_id == project_id)

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
                if (
                    e.project_id not in self._snapshot_required_projects
                    and e.is_enabled
                    and e.next_fire_at is not None
                )
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
                        e.project_id not in self._snapshot_required_projects
                        and e.is_enabled
                        and e.next_fire_at is not None
                        and e.next_fire_at <= before
                    )
                ),
                key=lambda e: (e.next_fire_at, e.id),  # type: ignore[arg-type, return-value]
            )

    def __len__(self) -> int:
        """Synchronous len - cheap, no lock. Used by metrics + /info."""
        return len(self._entries)


__all__ = [
    "CurrentStop",
    "LocalQuarantine",
    "ScheduleCache",
    "ScheduleProtocolError",
]
