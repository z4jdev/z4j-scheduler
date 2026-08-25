"""What an operator learns when their trigger cannot be carried.

``TriggerSchedule`` puts an attributed, slot-less fire on the FireSchedule
wire. Once a Brain has activated durable schedule control that wire carries
cadence acceptances only, so such a fire cannot be carried at all. The
scheduler settles that from the schedule in front of it, because the schedule
says so: a control token means the wire is the one that refuses. A Brain that
activates control after this scheduler took its snapshot is the remaining
case, and it answers with the legacy-upgrade disposition, whose code is
``scheduler_upgrade_required``. On a cadence fire that code is correct and
actionable. Here it is neither: the scheduler is current, no version of it can
express an operator's extra fire on that wire, and an operator who believes the
code re-deploys the one component that was never at fault.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.settings import Settings
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.tick._entry import ScheduleEntry


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings with synthetic mTLS paths; the dispatcher never reads them."""
    cert = tmp_path / "scheduler.crt"
    key = tmp_path / "scheduler.key"
    ca = tmp_path / "brain-ca.crt"
    for p in (cert, key, ca):
        p.write_text("dummy")

    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_GRPC_URL", "brain:7701")
    monkeypatch.setenv("Z4J_SCHEDULER_BRAIN_REST_URL", "http://brain:7700")
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CERT", str(cert))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_KEY", str(key))
    monkeypatch.setenv("Z4J_SCHEDULER_TLS_CA", str(ca))
    monkeypatch.setenv("Z4J_SCHEDULER_FIRE_RETRY_MAX", "0")
    monkeypatch.setenv("Z4J_SCHEDULER_FIRE_RETRY_BACKOFF_SECONDS", "0.001")
    return Settings(_env_file=None)  # type: ignore[call-arg]


@dataclass
class _FakeBrain:
    """One canned FireSchedule answer, plus the acks it was sent."""

    response: FireResult
    fire_calls: list[dict[str, Any]] = field(default_factory=list)
    ack_calls: list[dict[str, Any]] = field(default_factory=list)

    async def fire_schedule(self, **kwargs: Any) -> FireResult:
        self.fire_calls.append(kwargs)
        return self.response

    async def acknowledge_result(self, **kwargs: Any) -> None:
        self.ack_calls.append(kwargs)


def _refused() -> FireResult:
    """Exactly what an activated Brain answers an attributed manual fire."""
    return FireResult(
        command_id=None,
        error_code="scheduler_upgrade_required",
        error_message=(
            "current schedule control accepts only explicitly granted tokenless cadence fires"
        ),
        buffered=False,
        disposition="legacy_upgrade_required",
    )


def _entry(*, under_control: bool) -> ScheduleEntry:
    """A schedule as the cache holds it, in one of the two protocol shapes."""
    control = uuid4() if under_control else None
    return ScheduleEntry(
        id=uuid4(),
        project_id=uuid4(),
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=True,
        catch_up="skip",
        anchor_at=datetime(2026, 7, 31, tzinfo=UTC),
        control_token=control,
        schedule_revision=7 if under_control else 0,
        definition_digest="d" * 64 if under_control else "",
        cadence_semantics_version=1 if under_control else 0,
        cadence_runtime_fingerprint="f" * 64 if under_control else "",
    )


@pytest.mark.asyncio
async def test_a_schedule_under_control_is_refused_without_asking(
    settings: Settings,
) -> None:
    """The wire is never touched for a fire it structurally cannot carry.

    The absence of the call is the whole point. Sending it spent a round trip
    to be told what the control token already said, and the fire it sent
    carried a ``uuid4`` and no cadence authority, which is not a shape the
    current protocol has any answer for other than refusal.
    """
    brain = _FakeBrain(response=_refused())
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]
    entry = _entry(under_control=True)

    result = await dispatcher.trigger_now(
        schedule_id=entry.id,
        schedule_entry=entry,
        triggered_by_user_id="user-1",
    )

    assert brain.fire_calls == []
    assert brain.ack_calls == []
    assert result.success is False
    assert result.error_code == "manual_trigger_not_accepted"
    assert "upgrade" not in (result.error_message or "").lower()
    assert "trigger from the Brain" in (result.error_message or "")


@pytest.mark.asyncio
async def test_the_local_refusal_claims_no_disposition(
    settings: Settings,
) -> None:
    """A disposition is the Brain's word for what it did, and it did nothing.

    Reporting one here would attribute an answer to a peer that was never
    asked, and ``success`` reads the field, so the honest unset value has to
    still land the operator on a failure.
    """
    brain = _FakeBrain(response=_refused())
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]
    entry = _entry(under_control=True)

    result = await dispatcher.trigger_now(
        schedule_id=entry.id,
        schedule_entry=entry,
    )

    assert result.disposition is None
    assert result.command_id is None
    assert result.buffered is False
    assert result.success is False


@pytest.mark.asyncio
async def test_an_entry_for_a_different_schedule_is_a_programming_error(
    settings: Settings,
) -> None:
    """The refusal is decided from the entry, so it has to be this schedule's.

    A mismatched pair would decide one schedule's protocol from another's, so
    it is refused as loudly as possible rather than resolved in either
    direction.
    """
    brain = _FakeBrain(response=_refused())
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="does not match"):
        await dispatcher.trigger_now(
            schedule_id=uuid4(),
            schedule_entry=_entry(under_control=True),
        )
    assert brain.fire_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "brain_error_code",
    ["scheduler_upgrade_required", "manual_trigger_not_accepted"],
)
async def test_a_brain_that_activates_mid_click_still_reaches_the_operator(
    settings: Settings,
    brain_error_code: str,
) -> None:
    """The race the entry cannot see gets the same answer, minus the ack.

    The entry said legacy, so the fire went out, and the Brain refused it for
    its shape. It recorded nothing, and answers an acknowledgement for an
    unknown fire by failing the call, so sending one only puts a stack trace in
    front of the operator who is reading the log to find out why their click
    did nothing.

    Both spellings of the refusal are exercised, because both are on the wire
    at once: a Brain one minor behind names it as a scheduler upgrade, and a
    Brain that names the refusal itself must not have its own words rewritten
    into something different. The operator's answer has to be the same either
    way, or which Brain they happen to be running decides what they are told.
    """
    brain = _FakeBrain(
        response=replace(_refused(), error_code=brain_error_code),
    )
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]
    entry = _entry(under_control=False)

    result = await dispatcher.trigger_now(
        schedule_id=entry.id,
        schedule_entry=entry,
    )

    assert len(brain.fire_calls) == 1
    assert brain.ack_calls == []
    assert result.success is False
    assert result.disposition == "legacy_upgrade_required"
    assert result.error_code == "manual_trigger_not_accepted"
    assert "upgrade" not in (result.error_message or "").lower()
    assert "trigger from the Brain" in (result.error_message or "")


@pytest.mark.asyncio
async def test_an_accepted_trigger_is_untouched(settings: Settings) -> None:
    """A Brain that has not activated control still fires the click.

    The refusal must not be reachable on the path that works, or a working
    operator trigger would be reported as an error.
    """
    accepted = FireResult(
        command_id=UUID("11111111-2222-3333-4444-555555555555"),
        error_code=None,
        error_message=None,
        buffered=False,
    )
    brain = _FakeBrain(response=accepted)
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]
    entry = _entry(under_control=False)

    result = await dispatcher.trigger_now(
        schedule_id=entry.id,
        schedule_entry=entry,
    )

    assert result.success is True
    assert result.error_code is None
    assert brain.ack_calls[0]["status"] == "success"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "disposition"),
    [
        ("schedule_paused", None),
        ("schedule_disabled", None),
        ("agent_offline", None),
        ("invalid_current_fire", "retryable_or_ambiguous"),
        ("quarantined", "terminal_quarantined"),
    ],
)
async def test_every_other_refusal_is_passed_through_verbatim(
    settings: Settings,
    code: str,
    disposition: str | None,
) -> None:
    """Only the one misleading answer is rewritten, and it still gets its ack.

    A held schedule, a retired one, an offline agent: each already says what it
    means, and an operator acts on the Brain's own words. Rewriting more than
    the one wrong answer would bury the right ones, and withholding the failure
    acknowledgement from more than the one refusal that cannot accept it would
    lose fire history a Brain does keep.
    """
    original = FireResult(
        command_id=None,
        error_code=code,
        error_message=f"{code} as the Brain phrased it",
        buffered=False,
        disposition=disposition,  # type: ignore[arg-type]
    )
    brain = _FakeBrain(response=original)
    dispatcher = FireDispatcher(client=brain, settings=settings)  # type: ignore[arg-type]
    entry = _entry(under_control=False)

    result = await dispatcher.trigger_now(
        schedule_id=entry.id,
        schedule_entry=entry,
    )

    assert result.error_code == code
    assert result.error_message == f"{code} as the Brain phrased it"
    assert brain.ack_calls[0]["status"] == "failed"
    assert brain.ack_calls[0]["error"] == f"{code} as the Brain phrased it"


@pytest.mark.asyncio
async def test_the_operator_gets_the_actionable_answer_through_the_rpc(
    settings: Settings,
) -> None:
    """The whole path an operator's click takes, with only the Brain faked.

    The refusal is only worth anything if it survives to the response the Brain
    turns into a message. Everything between the request and that response is
    the real servicer, the real cache, the real leader gate and the real
    dispatcher, so a refusal that got lost or renamed on the way out would show
    up here rather than in an operator's incident.
    """
    from z4j_scheduler.leader import SingleInstanceLeaderGate
    from z4j_scheduler.proto import scheduler_pb2 as pb
    from z4j_scheduler.storage.cache import ScheduleCache
    from z4j_scheduler.trigger_grpc.handlers import TriggerScheduleServicer

    entry = _entry(under_control=True)
    cache = ScheduleCache()
    await cache.upsert(entry)
    brain = _FakeBrain(response=_refused())
    servicer = TriggerScheduleServicer(
        cache=cache,
        dispatcher=FireDispatcher(client=brain, settings=settings),  # type: ignore[arg-type]
        leader_gate=SingleInstanceLeaderGate(),
    )

    response = await servicer.TriggerSchedule(
        pb.TriggerScheduleRequest(
            schedule_id=str(entry.id),
            user_id="operator-7",
            idempotency_key="click-1",
        ),
        None,
    )

    assert response.command_id == ""
    assert response.error_code == "manual_trigger_not_accepted"
    assert "trigger from the Brain" in response.error_message
    assert brain.fire_calls == []
    assert brain.ack_calls == []
