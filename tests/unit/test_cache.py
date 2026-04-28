"""Tests for :class:`z4j_scheduler.storage.cache.ScheduleCache`."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _entry(
    *,
    next_fire_at: datetime | None = None,
    is_enabled: bool = True,
    schedule_id=None,
) -> ScheduleEntry:
    e = ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=is_enabled,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, tzinfo=UTC),
        last_fire_at=None,
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
