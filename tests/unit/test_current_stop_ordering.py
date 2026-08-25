"""What a local stop must do when the fire response comes back late.

The stop the scheduler installs after a refused fire waits for one thing: the
Brain state that refused it. Every existing test hands the response back before
the Watch stream delivers that state, which is the easy ordering and the one
where a stop is obviously right.

The other ordering is the one that happens in production. A refusal travels
back through a gRPC round trip while the Watch stream, a separate stream on its
own connection, is already delivering the transitions that caused it. A hold
placed during an incident is two transitions (the hold, then the release) and
an operator can place and lift one inside a second. So the response can arrive
after this scheduler has already applied the state it names, or a newer one
releasing it.

A stop installed at that point is a stop on state that has already arrived, and
what would clear it has already gone past: a same-revision echo is dropped as a
duplicate before any stop is looked at, and a schedule the stop disabled never
fires, so Brain is never asked to move it again. The schedule is dark until the
process restarts, and nothing in the dashboard says so.

The control token cannot distinguish the two orderings, because a hold and its
release deliberately carry the same one. Only the revision can.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick.engine import TickEngine

pytestmark = pytest.mark.asyncio


_SLOT = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
_PREVIOUS_FIRE = datetime(2026, 4, 26, 14, 55, tzinfo=UTC)


class _AlwaysLeader:
    def is_leader(self, project_id: UUID) -> bool:
        return True


class _ManualClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now


def _current_entry(
    *,
    schedule_id: UUID,
    project_id: UUID,
    control_token: UUID,
    schedule_revision: int,
    is_enabled: bool = True,
) -> ScheduleEntry:
    """One current-protocol row due at :data:`_SLOT`."""

    entry = ScheduleEntry(
        id=schedule_id,
        project_id=project_id,
        kind="interval",
        expression="5m",
        timezone="UTC",
        is_enabled=is_enabled,
        catch_up="skip",
        anchor_at=_PREVIOUS_FIRE,
        last_fire_at=_PREVIOUS_FIRE,
    )
    entry.control_token = control_token
    entry.schedule_revision = schedule_revision
    entry.definition_digest = "d" * 64
    entry.cadence_semantics_version = 1
    entry.cadence_runtime_fingerprint = "f" * 64
    entry.next_fire_at = _SLOT
    return entry


class _RefusesAfterWatchCatchesUp:
    """A refusal whose response loses the race to the Watch stream.

    The watch updates are applied from inside ``dispatch``, which is the shape
    of the production interleaving rather than a stand-in for it: the fire is
    in flight for exactly as long as this call runs, and the Watch stream keeps
    delivering throughout.
    """

    def __init__(
        self,
        *,
        cache: ScheduleCache,
        echoes: list[ScheduleEntry],
        refused_at_revision: int,
        disposition: str = "stale_control_refresh",
    ) -> None:
        self._cache = cache
        self._echoes = echoes
        self._refused_at_revision = refused_at_revision
        self._disposition = disposition
        self.calls = 0

    async def dispatch(self, *, schedule_entry: ScheduleEntry, **_kwargs: object) -> FireResult:
        self.calls += 1
        if self.calls == 1:
            for echo in self._echoes:
                await self._cache.apply_watch_update(echo)
        return FireResult(
            command_id=None,
            error_code="stale_control",
            error_message="schedule control state changed",
            buffered=False,
            disposition=self._disposition,
            live_control_token=schedule_entry.control_token,
            live_revision=self._refused_at_revision,
            live_last_run_at=schedule_entry.last_fire_at,
            live_next_run_at=_SLOT,
        )


def _engine(cache: ScheduleCache, dispatcher: object) -> TickEngine:
    return TickEngine(
        cache=cache,
        leader_gate=_AlwaysLeader(),
        dispatcher=dispatcher,
        clock=_ManualClock(_SLOT),
        max_sleep_seconds=0.01,
    )


async def test_a_release_already_applied_is_not_re_held_by_the_late_refusal() -> None:
    """The operator lifted the hold before the refusal got home.

    Both transitions land while the fire is in flight. By the time the refusal
    arrives, this scheduler holds the release: enabled, and newer than anything
    the refusal can be talking about. Clamping it here re-imposes a hold that
    the person who placed it has already lifted, and no second release is
    coming, because there is nothing left to release.
    """
    cache = ScheduleCache()
    schedule_id, project_id, token = uuid4(), uuid4(), uuid4()
    await cache.upsert(
        _current_entry(
            schedule_id=schedule_id,
            project_id=project_id,
            control_token=token,
            schedule_revision=40,
        ),
    )
    held_revision = 45

    dispatcher = _RefusesAfterWatchCatchesUp(
        cache=cache,
        echoes=[
            # Brain projects a hold as not-enabled; there is no separate field.
            _current_entry(
                schedule_id=schedule_id,
                project_id=project_id,
                control_token=token,
                schedule_revision=held_revision,
                is_enabled=False,
            ),
            _current_entry(
                schedule_id=schedule_id,
                project_id=project_id,
                control_token=token,
                schedule_revision=held_revision + 1,
            ),
        ],
        refused_at_revision=held_revision,
    )
    engine = _engine(cache, dispatcher)

    await engine._iteration()

    live = await cache.get(schedule_id)
    assert live is not None
    assert live.schedule_revision == held_revision + 1
    assert live.is_enabled is True, (
        "a refusal that arrived after the release re-imposed the hold, and the "
        "release that would lift it has already been consumed"
    )

    await engine._iteration()
    assert dispatcher.calls == 2, "the schedule stopped ticking with nothing holding it"


async def test_the_named_state_already_applied_is_not_stopped_by_its_own_refusal() -> None:
    """A refresh refusal only ever said this scheduler's view was behind.

    Nothing is holding this schedule. Brain named the revision it wanted this
    scheduler to be at, the Watch stream delivered exactly that revision while
    the fire was still in flight, and the row is enabled. Stopping it now waits
    for a state that has already been consumed: the same revision arriving
    again is dropped as a duplicate before any stop is examined.
    """
    cache = ScheduleCache()
    schedule_id, project_id, token = uuid4(), uuid4(), uuid4()
    await cache.upsert(
        _current_entry(
            schedule_id=schedule_id,
            project_id=project_id,
            control_token=token,
            schedule_revision=40,
        ),
    )
    live_revision = 43

    caught_up = _current_entry(
        schedule_id=schedule_id,
        project_id=project_id,
        control_token=token,
        schedule_revision=live_revision,
    )
    dispatcher = _RefusesAfterWatchCatchesUp(
        cache=cache,
        echoes=[caught_up],
        refused_at_revision=live_revision,
    )
    engine = _engine(cache, dispatcher)

    await engine._iteration()

    live = await cache.get(schedule_id)
    assert live is not None
    assert live.schedule_revision == live_revision
    assert live.is_enabled is True, (
        "the schedule was stopped waiting for the very revision it is already at"
    )

    # The replay a reconnecting Watch stream sends. It must not be the thing
    # that rescues the schedule, and it cannot be: it is dropped as a duplicate.
    replay = _current_entry(
        schedule_id=schedule_id,
        project_id=project_id,
        control_token=token,
        schedule_revision=live_revision,
    )
    await cache.apply_watch_update(replay)

    await engine._iteration()
    assert dispatcher.calls == 2, "the schedule stopped ticking with nothing holding it"


async def test_a_refusal_that_the_watch_stream_has_not_reached_still_stops_the_schedule() -> None:
    """The positive control: the stop is still a stop.

    The refusal names a revision this scheduler has not seen, so its view really
    is behind and it must not keep firing from it. Nothing here is a reordering,
    and the schedule stays down until the state Brain named arrives.
    """
    cache = ScheduleCache()
    schedule_id, project_id, token = uuid4(), uuid4(), uuid4()
    await cache.upsert(
        _current_entry(
            schedule_id=schedule_id,
            project_id=project_id,
            control_token=token,
            schedule_revision=40,
        ),
    )
    live_revision = 43

    dispatcher = _RefusesAfterWatchCatchesUp(
        cache=cache,
        # An unrelated edit that landed before the refusing transition: newer
        # than what this scheduler had, older than what refused the fire.
        echoes=[
            _current_entry(
                schedule_id=schedule_id,
                project_id=project_id,
                control_token=token,
                schedule_revision=live_revision - 1,
            ),
        ],
        refused_at_revision=live_revision,
    )
    engine = _engine(cache, dispatcher)

    await engine._iteration()

    stopped = await cache.get(schedule_id)
    assert stopped is not None
    assert stopped.is_enabled is False, "a refused fire left the schedule ticking on a stale view"

    await engine._iteration()
    assert dispatcher.calls == 1, "the stop must hold while nothing has changed"

    caught_up = _current_entry(
        schedule_id=schedule_id,
        project_id=project_id,
        control_token=token,
        schedule_revision=live_revision,
    )
    await cache.apply_watch_update(caught_up)
    assert caught_up.is_enabled is True, "the state that refused the fire did not lift the stop"

    await engine._iteration()
    assert dispatcher.calls == 2
