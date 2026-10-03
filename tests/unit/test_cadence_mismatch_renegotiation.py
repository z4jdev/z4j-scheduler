"""A cadence mismatch answered mid-flight must not quarantine durably.

The cadence runtime fingerprint is negotiated once at startup and checked by
the brain on every fire. A brain that restarts during a fire's retry window
answers the retry from a runtime this scheduler never negotiated with, and the
refusal it sends, ``cadence_semantics_mismatch``, used to be reported straight
through QuarantineSchedule: a durable disable on the brain for a disagreement
that lasted one answer.

The claims under test: the first mismatch is held locally and the contract is
renegotiated; agreement lifts the hold and the slot is retried; only a mismatch
on that retry, refused under a contract both sides just confirmed, is reported
as the durable quarantine; and when renegotiation does not agree, the hold
stays until the watch stream's own reconnect negotiation reports agreement.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import grpc
import pytest
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage._models import FireResult, QuarantineResult
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.storage.quarantine import QuarantineReporter
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.tick.engine import TickEngine

pytestmark = pytest.mark.asyncio

_SLOT = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
_NEGOTIATED_RUNTIME = "a" * 64
_UPGRADED_RUNTIME = "b" * 64


class _Unavailable(grpc.aio.AioRpcError):
    """A constructible UNAVAILABLE, the transient error the retry loop retries."""

    def __init__(self) -> None:
        pass

    def code(self) -> grpc.StatusCode:  # type: ignore[override]
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:  # type: ignore[override]
        return "fake UNAVAILABLE"

    def initial_metadata(self) -> grpc.aio.Metadata:  # type: ignore[override]
        return grpc.aio.Metadata()

    def trailing_metadata(self) -> grpc.aio.Metadata:  # type: ignore[override]
        return grpc.aio.Metadata()

    def debug_error_string(self) -> str:  # type: ignore[override]
        return ""


class RollingBrain:
    """A brain mid rolling-restart, as one slot's dispatch sees it.

    The first FireSchedule reaches the pod going down (UNAVAILABLE). The retry
    reaches a pod on ``runtime_after_restart``; when that is not the
    negotiated runtime the fire is refused as a cadence mismatch. ``settles``
    says whether the fleet is back on the negotiated runtime after that one
    answer. ``negotiation_agrees`` is what the re-run startup negotiation
    reports, kept separate from the fire path so the "same fingerprint on both
    sides, still refused" case can be stated directly.
    """

    def __init__(
        self,
        *,
        runtime_after_restart: str,
        settles: bool,
        negotiation_agrees: bool,
    ) -> None:
        self.fire_runtime = _NEGOTIATED_RUNTIME
        self._restart_pending = True
        self._runtime_after_restart = runtime_after_restart
        self._settles = settles
        self.negotiation_agrees = negotiation_agrees
        self.fire_calls: list[datetime] = []
        self.acks: list[dict[str, Any]] = []
        self.quarantine_calls: list[dict[str, Any]] = []
        self.negotiations = 0

    async def fire_schedule(
        self,
        *,
        schedule_id: UUID,
        fire_id: UUID,
        scheduled_for: datetime,
        fired_at: datetime,
        triggered_by_user_id: str = "",
        schedule_entry: ScheduleEntry | None = None,
        prepared_fire: Any = None,
        scheduler_protocol_epoch: int = 0,
    ) -> FireResult:
        assert schedule_entry is not None and prepared_fire is not None
        self.fire_calls.append(scheduled_for)
        if self._restart_pending:
            self._restart_pending = False
            self.fire_runtime = self._runtime_after_restart
            raise _Unavailable()
        if self.fire_runtime != _NEGOTIATED_RUNTIME:
            if self._settles:
                self.fire_runtime = _NEGOTIATED_RUNTIME
            return FireResult(
                command_id=None,
                error_code="cadence_semantics_mismatch",
                error_message="brain and scheduler cadence runtimes differ",
                buffered=False,
                disposition="cadence_semantics_mismatch",
                live_control_token=schedule_entry.control_token,
                live_revision=schedule_entry.schedule_revision,
            )
        revision = schedule_entry.schedule_revision + 1
        return FireResult(
            command_id=uuid4(),
            error_code=None,
            error_message=None,
            buffered=False,
            disposition="accepted",
            acceptance_revision=revision,
            accepted_last_run_at=prepared_fire.scheduled_for,
            accepted_next_run_at=prepared_fire.next_run_at,
            live_control_token=schedule_entry.control_token,
            live_revision=revision,
            live_last_run_at=prepared_fire.scheduled_for,
            live_next_run_at=prepared_fire.next_run_at,
        )

    async def acknowledge_result(self, **kwargs: Any) -> None:
        self.acks.append(kwargs)

    async def quarantine_schedule(self, **kwargs: Any) -> QuarantineResult:
        self.quarantine_calls.append(kwargs)
        return QuarantineResult(outcome="applied", observed_revision=41)

    async def negotiate(self) -> bool:
        """What the application wires as the engine's renegotiation."""
        self.negotiations += 1
        return self.negotiation_agrees


class _AlwaysLeader:
    def is_leader(self, project_id: UUID) -> bool:
        return True


def _settings() -> Settings:
    return Settings(  # type: ignore[call-arg]
        brain_grpc_url="brain:7701",
        brain_rest_url="http://brain:7700",
        environment="dev",
        insecure_grpc=True,
        fire_retry_max=2,
        fire_retry_backoff_seconds=0.0,
        _env_file=None,
    )


def _current_entry() -> ScheduleEntry:
    entry = ScheduleEntry(
        id=uuid4(),
        project_id=uuid4(),
        kind="interval",
        expression="5m",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=_SLOT - timedelta(minutes=5),
        last_fire_at=_SLOT - timedelta(minutes=5),
        control_token=uuid4(),
        schedule_revision=40,
        definition_digest="d" * 64,
        cadence_semantics_version=1,
        cadence_runtime_fingerprint=_NEGOTIATED_RUNTIME,
    )
    entry.next_fire_at = _SLOT
    return entry


async def _harness(
    brain: RollingBrain,
) -> tuple[ScheduleCache, ScheduleEntry, TickEngine, QuarantineReporter]:
    """The production dispatcher and reporter around the engine, brain faked."""
    cache = ScheduleCache()
    entry = _current_entry()
    await cache.upsert(entry)
    dispatcher = FireDispatcher(client=brain, settings=_settings())  # type: ignore[arg-type]
    reporter = QuarantineReporter(client=brain, cache=cache)  # type: ignore[arg-type]
    engine = TickEngine(
        cache=cache,
        leader_gate=_AlwaysLeader(),
        dispatcher=dispatcher,
        clock=lambda: _SLOT,
        max_sleep_seconds=0.001,
        quarantine_reporter=reporter,
        renegotiate=brain.negotiate,
    )
    return cache, entry, engine, reporter


async def _enabled(cache: ScheduleCache, schedule_id: UUID) -> bool:
    live = await cache.get(schedule_id)
    assert live is not None
    return live.is_enabled


async def test_mid_flight_mismatch_is_held_and_the_fire_resumes_after_renegotiation() -> None:
    brain = RollingBrain(
        runtime_after_restart=_UPGRADED_RUNTIME,
        settles=True,
        negotiation_agrees=True,
    )
    cache, entry, engine, reporter = await _harness(brain)

    # UNAVAILABLE, retried, refused as a mismatch by the restarted brain.
    await engine._iteration()

    assert brain.fire_calls == [_SLOT, _SLOT]
    assert brain.negotiations == 1, "the mismatch must ask for renegotiation"
    await reporter.flush_once()
    assert brain.quarantine_calls == [], "a mid-flight mismatch reached QuarantineSchedule"
    assert await _enabled(cache, entry.id), "agreement must lift the local hold"
    assert brain.acks == []

    # The retry, under the contract both sides just confirmed.
    await engine._iteration()

    assert brain.fire_calls == [_SLOT, _SLOT, _SLOT]
    assert [ack["status"] for ack in brain.acks] == ["success"]
    live = await cache.get(entry.id)
    assert live is not None
    assert live.last_fire_at == _SLOT
    assert live.next_fire_at == _SLOT + timedelta(minutes=5)
    await reporter.flush_once()
    assert brain.quarantine_calls == []


async def test_mismatch_that_survives_an_agreed_renegotiation_is_the_durable_quarantine() -> None:
    brain = RollingBrain(
        runtime_after_restart=_UPGRADED_RUNTIME,
        settles=False,
        negotiation_agrees=True,
    )
    cache, entry, engine, reporter = await _harness(brain)

    await engine._iteration()
    await reporter.flush_once()
    assert brain.quarantine_calls == [], "the first mismatch is not durable"
    assert await _enabled(cache, entry.id)

    # Same fingerprint on both sides, still refused.
    await engine._iteration()
    await reporter.flush_once()

    assert brain.fire_calls == [_SLOT, _SLOT, _SLOT]
    assert len(brain.quarantine_calls) == 1
    assert brain.quarantine_calls[0]["reason_code"] == "cadence_semantics_mismatch"
    assert brain.quarantine_calls[0]["schedule_id"] == entry.id
    assert not await _enabled(cache, entry.id)

    # And it stays down: nothing further is dispatched or renegotiated.
    await engine._iteration()
    assert len(brain.fire_calls) == 3
    assert brain.negotiations == 1


async def test_disagreeing_renegotiation_holds_the_stop_until_the_watch_renegotiates() -> None:
    brain = RollingBrain(
        runtime_after_restart=_UPGRADED_RUNTIME,
        settles=False,
        negotiation_agrees=False,
    )
    cache, entry, engine, reporter = await _harness(brain)

    await engine._iteration()

    assert brain.negotiations == 1
    assert not await _enabled(cache, entry.id), "no agreement: the hold stays"
    await reporter.flush_once()
    assert brain.quarantine_calls == [], "a genuine mismatch is not reported either"

    # Held: later ticks dispatch nothing and do not spin on renegotiation.
    await engine._iteration()
    assert brain.fire_calls == [_SLOT, _SLOT]
    assert brain.negotiations == 1

    # The brain comes back on the negotiated contract and the watch stream's
    # reconnect negotiation reports it, through the hook the application wires.
    brain.fire_runtime = _NEGOTIATED_RUNTIME
    await engine.on_cadence_contract_renegotiated()

    assert await _enabled(cache, entry.id)
    await engine._iteration()
    assert [ack["status"] for ack in brain.acks] == ["success"]
    await reporter.flush_once()
    assert brain.quarantine_calls == []


async def test_an_accepted_fire_re_arms_the_renegotiation_chance() -> None:
    """A second mismatch long after a clean fire is a new incident, not the
    retry of the old one, and gets its own renegotiation before anything
    durable."""
    brain = RollingBrain(
        runtime_after_restart=_UPGRADED_RUNTIME,
        settles=True,
        negotiation_agrees=True,
    )
    cache, entry, engine, reporter = await _harness(brain)

    await engine._iteration()  # mismatch, held, renegotiated, released
    await engine._iteration()  # accepted
    assert [ack["status"] for ack in brain.acks] == ["success"]

    # The next slot comes due and the brain is mid-restart again.
    live = await cache.get(entry.id)
    assert live is not None
    live.next_fire_at = _SLOT
    brain._restart_pending = True
    brain._settles = True

    await engine._iteration()
    await reporter.flush_once()

    assert brain.negotiations == 2
    assert brain.quarantine_calls == []
    assert await _enabled(cache, entry.id)
