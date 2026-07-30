"""Durable quarantine reports retain the local latch through refresh."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from z4j_scheduler.storage._models import (
    QuarantineResult,
    ScheduleStateObservation,
)
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.quarantine import QuarantineReporter
from z4j_scheduler.tick._entry import ScheduleEntry

pytestmark = pytest.mark.asyncio


def _entry(*, schedule_id=None, project_id=None, revision=10) -> ScheduleEntry:
    entry = ScheduleEntry(
        id=schedule_id or uuid4(),
        project_id=project_id or uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        control_token=uuid4(),
        schedule_revision=revision,
        definition_digest="d" * 64,
        cadence_semantics_version=1,
        cadence_runtime_fingerprint="f" * 64,
    )
    entry.next_fire_at = datetime(2026, 4, 26, 16, 0, tzinfo=UTC)
    return entry


async def _latched(cache: ScheduleCache, entry: ScheduleEntry):
    await cache.upsert(entry)
    assert await cache.quarantine_locally(
        entry.id,
        expected_definition=entry,
        code="cadence_definition_invalid",
        detail="bad cron",
    )
    quarantine = await cache.local_quarantine(entry.id)
    assert quarantine is not None
    return quarantine


class FakeClient:
    def __init__(self, result, observation=None) -> None:
        self.result = result
        self.observation = observation
        self.quarantine_calls = 0
        self.state_calls = 0
        self.called = asyncio.Event()
        self.delivered = asyncio.Event()

    async def quarantine_schedule(self, **_kwargs):
        self.quarantine_calls += 1
        self.called.set()
        if isinstance(self.result, Exception):
            raise self.result
        self.delivered.set()
        return self.result

    async def get_schedule_state(self, **_kwargs):
        self.state_calls += 1
        if isinstance(self.observation, Exception):
            raise self.observation
        return self.observation


async def test_applied_report_coalesces_but_latch_waits_for_watch() -> None:
    cache = ScheduleCache()
    entry = _entry()
    quarantine = await _latched(cache, entry)
    client = FakeClient(QuarantineResult("applied", 11))
    reporter = QuarantineReporter(client=client, cache=cache)  # type: ignore[arg-type]
    assert reporter.enqueue(entry=entry, quarantine=quarantine)
    assert reporter.enqueue(entry=entry, quarantine=quarantine)

    await reporter.flush_once()

    assert client.quarantine_calls == 1
    assert len(reporter) == 0
    assert await cache.local_quarantine(entry.id) is quarantine


async def test_stale_control_refreshes_different_token_before_release() -> None:
    cache = ScheduleCache()
    entry = _entry()
    quarantine = await _latched(cache, entry)
    repaired = _entry(
        schedule_id=entry.id,
        project_id=entry.project_id,
        revision=12,
    )
    observation = ScheduleStateObservation(
        project_id=entry.project_id,
        schedule_id=entry.id,
        observed_revision=12,
        schedule=repaired,
    )
    client = FakeClient(
        QuarantineResult("stale_control", 11),
        observation,
    )
    reporter = QuarantineReporter(client=client, cache=cache)  # type: ignore[arg-type]
    reporter.enqueue(entry=entry, quarantine=quarantine)

    await reporter.flush_once()

    assert client.state_calls == 1
    assert len(reporter) == 0
    assert await cache.get(entry.id) is repaired
    assert await cache.local_quarantine(entry.id) is None


async def test_not_found_applies_revision_bounded_absence() -> None:
    cache = ScheduleCache()
    entry = _entry()
    quarantine = await _latched(cache, entry)
    client = FakeClient(QuarantineResult("not_found", 12))
    reporter = QuarantineReporter(client=client, cache=cache)  # type: ignore[arg-type]
    reporter.enqueue(entry=entry, quarantine=quarantine)

    await reporter.flush_once()

    assert await cache.get(entry.id) is None
    assert await cache.local_quarantine(entry.id) is None
    assert len(reporter) == 0


async def test_transport_or_refresh_failure_retains_report_and_latch() -> None:
    cache = ScheduleCache()
    entry = _entry()
    quarantine = await _latched(cache, entry)
    client = FakeClient(RuntimeError("brain unavailable"))
    reporter = QuarantineReporter(client=client, cache=cache)  # type: ignore[arg-type]
    reporter.enqueue(entry=entry, quarantine=quarantine)

    await reporter.flush_once()

    assert len(reporter) == 1
    assert await cache.local_quarantine(entry.id) is quarantine


async def test_run_retries_independently_until_report_is_delivered() -> None:
    cache = ScheduleCache()
    entry = _entry()
    quarantine = await _latched(cache, entry)
    client = FakeClient(RuntimeError("brain unavailable"))
    reporter = QuarantineReporter(
        client=client,  # type: ignore[arg-type]
        cache=cache,
        retry_interval_seconds=0.01,
    )
    reporter.enqueue(entry=entry, quarantine=quarantine)
    task = asyncio.create_task(reporter.run())
    try:
        await asyncio.wait_for(client.called.wait(), timeout=1)
        client.called.clear()
        client.result = QuarantineResult("applied", 11)
        await asyncio.wait_for(client.delivered.wait(), timeout=1)
        await asyncio.sleep(0)
    finally:
        await reporter.stop()
        await task

    assert client.quarantine_calls >= 2
    assert len(reporter) == 0
