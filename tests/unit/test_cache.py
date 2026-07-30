"""Tests for :class:`z4j_scheduler.storage.cache.ScheduleCache`."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.storage._models import ScheduleSnapshot
from z4j_scheduler.storage._snapshot import snapshot_digest
from z4j_scheduler.storage.cache import ScheduleCache, ScheduleProtocolError
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _entry(
    *,
    next_fire_at: datetime | None = None,
    is_enabled: bool = True,
    schedule_id=None,
    project_id: UUID | None = None,
    control_token=None,
    schedule_revision: int = 0,
) -> ScheduleEntry:
    e = ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=is_enabled,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
        last_fire_at=None,
        control_token=control_token,
        schedule_revision=schedule_revision,
        definition_digest="d" * 64 if control_token is not None else "",
        cadence_semantics_version=1 if control_token is not None else 0,
        cadence_runtime_fingerprint="f" * 64 if control_token is not None else "",
    )
    e.next_fire_at = next_fire_at
    return e


class TestUpsert:
    async def test_add_one(self) -> None:
        cache = ScheduleCache()
        e = _entry()
        await cache.upsert(e)
        assert await cache.get(e.id) is e
        assert len(cache) == 1

    async def test_replace(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        e1 = _entry(schedule_id=sid)
        e2 = _entry(schedule_id=sid)
        await cache.upsert(e1)
        await cache.upsert(e2)
        assert await cache.get(sid) is e2
        assert len(cache) == 1

    async def test_upsert_fires_changed_event(self) -> None:
        cache = ScheduleCache()
        cache.changed.clear()
        await cache.upsert(_entry())
        assert cache.changed.is_set()

    async def test_upsert_many_fires_event_once(self) -> None:
        cache = ScheduleCache()
        cache.changed.clear()
        await cache.upsert_many([_entry(), _entry(), _entry()])
        assert cache.changed.is_set()
        assert len(cache) == 3

    async def test_upsert_many_empty_does_not_fire(self) -> None:
        cache = ScheduleCache()
        cache.changed.clear()
        await cache.upsert_many([])
        assert not cache.changed.is_set()


class TestRemove:
    async def test_remove_existing(self) -> None:
        cache = ScheduleCache()
        e = _entry()
        await cache.upsert(e)
        cache.changed.clear()
        result = await cache.remove(e.id)
        assert result is True
        assert await cache.get(e.id) is None
        assert cache.changed.is_set()

    async def test_remove_missing_returns_false(self) -> None:
        cache = ScheduleCache()
        cache.changed.clear()
        result = await cache.remove(uuid4())
        assert result is False
        assert not cache.changed.is_set()

    async def test_clear_drops_everything(self) -> None:
        cache = ScheduleCache()
        await cache.upsert_many([_entry(), _entry(), _entry()])
        cache.changed.clear()
        await cache.clear()
        assert len(cache) == 0
        assert cache.changed.is_set()


class TestBoundaryDLocalQuarantine:
    async def test_same_token_enabled_echo_remains_clamped(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        project_id = uuid4()
        token = uuid4()
        broken = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=10,
        )
        await cache.upsert(broken)

        assert await cache.quarantine_locally(
            sid,
            expected_definition=broken,
            code="cadence_definition_invalid",
        )
        enabled_echo = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=True,
            control_token=token,
            schedule_revision=11,
        )
        await cache.apply_watch_update(enabled_echo)

        live = await cache.get(sid)
        assert live is not None
        assert live.is_enabled is False
        assert await cache.local_quarantine(sid) is not None

    async def test_different_token_is_explicit_intervention(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        project_id = uuid4()
        broken = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=uuid4(),
            schedule_revision=10,
        )
        await cache.upsert(broken)
        assert await cache.quarantine_locally(
            sid,
            expected_definition=broken,
            code="cadence_definition_invalid",
        )

        repaired = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=True,
            control_token=uuid4(),
            schedule_revision=11,
        )
        await cache.apply_watch_update(repaired)

        live = await cache.get(sid)
        assert live is repaired
        assert live.is_enabled is True
        assert await cache.local_quarantine(sid) is None

    async def test_legacy_same_definition_echo_is_safety_biased(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        broken = _entry(schedule_id=sid)
        await cache.upsert(broken)
        assert await cache.quarantine_locally(
            sid,
            expected_definition=broken,
            code="cadence_definition_invalid",
        )

        same_definition = _entry(schedule_id=sid, is_enabled=True)
        await cache.apply_watch_update(same_definition)
        assert (await cache.get(sid)).is_enabled is False  # type: ignore[union-attr]

        repaired = _entry(schedule_id=sid, is_enabled=True)
        repaired.expression = "30 * * * *"
        await cache.apply_watch_update(repaired)
        assert (await cache.get(sid)).is_enabled is True  # type: ignore[union-attr]
        assert await cache.local_quarantine(sid) is None

    async def test_current_revision_installs_authoritative_cursor(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        project_id = uuid4()
        token = uuid4()
        stale = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=10,
        )
        stale.next_fire_at = datetime(2030, 1, 1, tzinfo=UTC)
        await cache.upsert(stale)

        authoritative = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=11,
        )
        authoritative.next_fire_at = datetime(2031, 1, 1, tzinfo=UTC)
        await cache.apply_watch_update(authoritative)

        assert await cache.get(sid) is authoritative
        assert authoritative.next_fire_at == datetime(2031, 1, 1, tzinfo=UTC)

    async def test_lower_revision_cannot_reactivate_or_replace(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        project_id = uuid4()
        token = uuid4()
        current = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=False,
            control_token=token,
            schedule_revision=12,
        )
        await cache.upsert(current)
        delayed = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=True,
            control_token=token,
            schedule_revision=11,
        )

        await cache.apply_watch_update(delayed)

        assert await cache.get(sid) is current
        assert current.is_enabled is False

    async def test_same_revision_conflicting_payload_fails_closed(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        project_id = uuid4()
        token = uuid4()
        current = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=12,
        )
        await cache.upsert(current)
        conflict = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=False,
            control_token=token,
            schedule_revision=12,
        )

        with pytest.raises(ScheduleProtocolError, match="conflicting"):
            await cache.apply_watch_update(conflict)
        assert await cache.get(sid) is current

    async def test_partial_current_protocol_shape_fails_closed(self) -> None:
        cache = ScheduleCache()
        with pytest.raises(ScheduleProtocolError, match="appear together"):
            await cache.apply_watch_update(
                _entry(control_token=uuid4(), schedule_revision=0),
            )


def _snapshot(
    *,
    project_id: UUID | None,
    watermark: int,
    rows: tuple[ScheduleEntry, ...],
) -> ScheduleSnapshot:
    unfinished = ScheduleSnapshot(
        snapshot_id=uuid4(),
        project_id=project_id,
        watermark=watermark,
        rows=rows,
        digest="",
    )
    return ScheduleSnapshot(
        snapshot_id=unfinished.snapshot_id,
        project_id=project_id,
        watermark=watermark,
        rows=rows,
        digest=snapshot_digest(unfinished),
    )


class TestBoundaryDStableSnapshot:
    async def test_fresh_empty_snapshot_accepts_zero_backfill_boundary(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()

        assert await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=0, rows=()),
        )
        assert await cache.project_watermark(project_id) == 0

    async def test_completed_empty_snapshot_is_authoritative_absence(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()
        stale = _entry(
            project_id=project_id,
            control_token=uuid4(),
            schedule_revision=7,
        )
        await cache.upsert(stale)
        assert await cache.quarantine_locally(
            stale.id,
            expected_definition=stale,
            code="cadence_definition_invalid",
        )

        assert await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=10, rows=()),
        )

        assert await cache.get(stale.id) is None
        assert await cache.local_quarantine(stale.id) is None
        assert await cache.project_watermark(project_id) == 10

    async def test_all_project_snapshot_reconciles_one_global_boundary(
        self,
    ) -> None:
        cache = ScheduleCache()
        first_project = uuid4()
        second_project = uuid4()
        keep = _entry(
            project_id=first_project,
            control_token=uuid4(),
            schedule_revision=10,
        )
        stale = _entry(
            project_id=second_project,
            control_token=uuid4(),
            schedule_revision=9,
        )
        await cache.upsert(stale)

        assert await cache.apply_completed_snapshot(
            _snapshot(
                project_id=None,
                watermark=10,
                rows=(keep,),
            ),
        )

        assert await cache.get(keep.id) is keep
        assert await cache.get(stale.id) is None
        assert await cache.project_watermark(first_project) == 10
        assert await cache.project_watermark(second_project) == 10
        delayed = _entry(
            project_id=second_project,
            control_token=uuid4(),
            schedule_revision=8,
        )
        await cache.apply_watch_update(delayed)
        assert await cache.get(delayed.id) is None

    async def test_bad_digest_changes_nothing(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()
        current = _entry(
            project_id=project_id,
            control_token=uuid4(),
            schedule_revision=7,
        )
        await cache.upsert(current)
        invalid = ScheduleSnapshot(
            snapshot_id=uuid4(),
            project_id=project_id,
            watermark=10,
            rows=(),
            digest="0" * 64,
        )

        with pytest.raises(ScheduleProtocolError, match="digest"):
            await cache.apply_completed_snapshot(invalid)

        assert await cache.get(current.id) is current
        assert await cache.project_watermark(project_id) == 0

    async def test_newer_watch_survives_delayed_snapshot(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()
        sid = uuid4()
        token = uuid4()
        snapshot_row = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=10,
        )
        newer = _entry(
            schedule_id=sid,
            project_id=project_id,
            is_enabled=False,
            control_token=token,
            schedule_revision=11,
        )
        await cache.apply_watch_update(newer)

        await cache.apply_completed_snapshot(
            _snapshot(
                project_id=project_id,
                watermark=10,
                rows=(snapshot_row,),
            ),
        )

        assert await cache.get(sid) is newer
        assert newer.is_enabled is False

    async def test_watermark_rejects_never_seen_delayed_upsert(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()
        await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=20, rows=()),
        )
        delayed = _entry(
            project_id=project_id,
            control_token=uuid4(),
            schedule_revision=19,
        )

        await cache.apply_watch_update(delayed)

        assert await cache.get(delayed.id) is None

    async def test_higher_tombstone_wins_and_blocks_delayed_upsert(self) -> None:
        cache = ScheduleCache()
        project_id = uuid4()
        sid = uuid4()
        token = uuid4()
        current = _entry(
            schedule_id=sid,
            project_id=project_id,
            control_token=token,
            schedule_revision=20,
        )
        await cache.upsert(current)
        assert await cache.apply_tombstone(
            schedule_id=sid,
            project_id=project_id,
            revision=21,
        )

        await cache.apply_watch_update(current)

        assert await cache.get(sid) is None

    async def test_tombstone_cap_pauses_only_until_covering_snapshot(self) -> None:
        cache = ScheduleCache(max_post_watermark_tombstones=2)
        project_id = uuid4()
        other_project_id = uuid4()
        now = datetime(2026, 4, 26, tzinfo=UTC)
        live = _entry(
            project_id=project_id,
            next_fire_at=now,
            control_token=uuid4(),
            schedule_revision=8,
        )
        other = _entry(
            project_id=other_project_id,
            next_fire_at=now,
            control_token=uuid4(),
            schedule_revision=8,
        )
        await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=10, rows=(live,)),
        )
        await cache.apply_completed_snapshot(
            _snapshot(project_id=other_project_id, watermark=10, rows=(other,)),
        )

        await cache.apply_tombstone(
            schedule_id=uuid4(),
            project_id=project_id,
            revision=11,
        )
        assert not await cache.requires_stable_snapshot(project_id)
        await cache.apply_tombstone(
            schedule_id=uuid4(),
            project_id=project_id,
            revision=12,
        )

        assert await cache.requires_stable_snapshot(project_id)
        assert await cache.all_due(before=now) == [other]

        await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=12, rows=(live,)),
        )

        assert not await cache.requires_stable_snapshot(project_id)
        assert {entry.id for entry in await cache.all_due(before=now)} == {
            live.id,
            other.id,
        }

    async def test_noncovering_snapshot_cannot_resume_tombstone_pressure(self) -> None:
        cache = ScheduleCache(max_post_watermark_tombstones=1)
        project_id = uuid4()
        await cache.apply_tombstone(
            schedule_id=uuid4(),
            project_id=project_id,
            revision=11,
        )
        assert await cache.requires_stable_snapshot(project_id)

        await cache.apply_completed_snapshot(
            _snapshot(project_id=project_id, watermark=10, rows=()),
        )

        assert await cache.requires_stable_snapshot(project_id)

    async def test_tombstone_cap_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            ScheduleCache(max_post_watermark_tombstones=0)


class TestBoundaryDCursorTransition:
    async def test_exact_durable_transition_installs_and_is_idempotent(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        token = uuid4()
        prior_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        accepted_slot = prior_next
        prepared_next = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
        current = _entry(
            schedule_id=sid,
            control_token=token,
            schedule_revision=40,
            next_fire_at=prior_next,
        )
        await cache.upsert(current)

        kwargs = {
            "expected_control_token": token,
            "expected_revision": 40,
            "expected_last_run_at": None,
            "expected_next_run_at": prior_next,
            "committed_revision": 41,
            "committed_last_run_at": accepted_slot,
            "committed_next_run_at": prepared_next,
        }
        assert await cache.apply_cursor_transition(sid, **kwargs)
        assert await cache.apply_cursor_transition(sid, **kwargs)

        live = await cache.get(sid)
        assert live is current
        assert live.last_fire_at == accepted_slot
        assert live.next_fire_at == prepared_next
        assert live.anchor_at == accepted_slot
        assert live.schedule_revision == 41

        # The response-installed full payload and the later Watch echo at the
        # same revision must compare as one idempotent Brain state.
        echo = _entry(
            schedule_id=sid,
            project_id=current.project_id,
            control_token=token,
            schedule_revision=41,
            next_fire_at=prepared_next,
        )
        echo.last_fire_at = accepted_slot
        echo.anchor_at = accepted_slot
        await cache.apply_watch_update(echo)
        assert await cache.get(sid) is current

    async def test_concurrent_revision_wins_over_stale_response(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        token = uuid4()
        prior_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        newer = _entry(
            schedule_id=sid,
            control_token=token,
            schedule_revision=42,
            next_fire_at=datetime(2026, 4, 26, 17, 0, tzinfo=UTC),
        )
        await cache.upsert(newer)

        assert not await cache.apply_cursor_transition(
            sid,
            expected_control_token=token,
            expected_revision=40,
            expected_last_run_at=None,
            expected_next_run_at=prior_next,
            committed_revision=41,
            committed_last_run_at=prior_next,
            committed_next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        assert await cache.get(sid) is newer

    async def test_prior_cursor_mismatch_refuses_same_revision(self) -> None:
        cache = ScheduleCache()
        sid = uuid4()
        token = uuid4()
        actual_next = datetime(2026, 4, 26, 15, 0, tzinfo=UTC)
        current = _entry(
            schedule_id=sid,
            control_token=token,
            schedule_revision=40,
            next_fire_at=actual_next,
        )
        await cache.upsert(current)

        assert not await cache.apply_cursor_transition(
            sid,
            expected_control_token=token,
            expected_revision=40,
            expected_last_run_at=None,
            expected_next_run_at=datetime(2026, 4, 26, 14, 0, tzinfo=UTC),
            committed_revision=41,
            committed_last_run_at=actual_next,
            committed_next_run_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        assert current.schedule_revision == 40

    async def test_nonincreasing_committed_revision_is_protocol_fault(self) -> None:
        cache = ScheduleCache()
        with pytest.raises(ScheduleProtocolError, match="increasing"):
            await cache.apply_cursor_transition(
                uuid4(),
                expected_control_token=uuid4(),
                expected_revision=40,
                expected_last_run_at=None,
                expected_next_run_at=None,
                committed_revision=40,
                committed_last_run_at=None,
                committed_next_run_at=None,
            )


class TestNextDue:
    async def test_empty_cache_returns_none(self) -> None:
        cache = ScheduleCache()
        assert await cache.next_due() is None

    async def test_returns_earliest(self) -> None:
        cache = ScheduleCache()
        early = _entry(next_fire_at=datetime(2026, 4, 26, 1, tzinfo=UTC))
        late = _entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC))
        await cache.upsert_many([late, early])
        result = await cache.next_due()
        assert result is early

    async def test_skips_disabled(self) -> None:
        cache = ScheduleCache()
        disabled = _entry(
            next_fire_at=datetime(2026, 4, 26, 1, tzinfo=UTC),
            is_enabled=False,
        )
        enabled = _entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC))
        await cache.upsert_many([disabled, enabled])
        result = await cache.next_due()
        assert result is enabled

    async def test_skips_uncomputed_next_fire(self) -> None:
        cache = ScheduleCache()
        no_next = _entry(next_fire_at=None)
        ready = _entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC))
        await cache.upsert_many([no_next, ready])
        result = await cache.next_due()
        assert result is ready

    async def test_before_filter(self) -> None:
        cache = ScheduleCache()
        early = _entry(next_fire_at=datetime(2026, 4, 26, 1, tzinfo=UTC))
        late = _entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC))
        await cache.upsert_many([early, late])
        result = await cache.next_due(
            before=datetime(2026, 4, 26, 3, tzinfo=UTC),
        )
        # Only `early` is <= 3am; `late` is filtered out.
        assert result is early

    async def test_before_filter_returns_none_when_nothing_due(self) -> None:
        cache = ScheduleCache()
        await cache.upsert(_entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC)))
        result = await cache.next_due(
            before=datetime(2026, 4, 26, 3, tzinfo=UTC),
        )
        assert result is None


class TestAllDue:
    async def test_returns_in_order(self) -> None:
        cache = ScheduleCache()
        a = _entry(next_fire_at=datetime(2026, 4, 26, 3, tzinfo=UTC))
        b = _entry(next_fire_at=datetime(2026, 4, 26, 1, tzinfo=UTC))
        c = _entry(next_fire_at=datetime(2026, 4, 26, 2, tzinfo=UTC))
        await cache.upsert_many([a, b, c])
        result = await cache.all_due(
            before=datetime(2026, 4, 26, 4, tzinfo=UTC),
        )
        assert [e.next_fire_at for e in result] == [
            datetime(2026, 4, 26, 1, tzinfo=UTC),
            datetime(2026, 4, 26, 2, tzinfo=UTC),
            datetime(2026, 4, 26, 3, tzinfo=UTC),
        ]

    async def test_empty_when_nothing_due(self) -> None:
        cache = ScheduleCache()
        await cache.upsert(_entry(next_fire_at=datetime(2026, 4, 26, 5, tzinfo=UTC)))
        result = await cache.all_due(
            before=datetime(2026, 4, 26, 1, tzinfo=UTC),
        )
        assert result == []
