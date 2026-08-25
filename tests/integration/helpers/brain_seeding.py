"""Seed a migrated Brain the way the Brain seeds itself.

A Boundary-D-activated schema refuses a hand-assembled ``schedules`` row: the
insert trigger demands a control token, a revision, a definition digest and a
matching change-log envelope together, and a bare column write to a cadence
field is refused separately. None of that can be invented by a test, so every
route here goes through the repository the Brain itself writes through. A row
produced by this module is a row an operator's database can actually hold.

The two ownership kinds have two different legitimate entry points, and only
one of them is a single call:

* ``z4j-scheduler`` (reserved owner) is created by
  :meth:`ScheduleControlRepository.create_current`, which is the only writer
  the Brain offers once control is active.
* Any foreign owner (``celery-beat`` and friends) is never written directly at
  all. It arrives as a digest-bound projection frame on an external stream, so
  the ceremony is: open an activation epoch, then apply the snapshot frame.
"""

from __future__ import annotations

import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from z4j_brain.persistence.enums import AgentState
from z4j_brain.persistence.models import Agent, Project
from z4j_brain.persistence.repositories.schedule_control import (
    ScheduleControlRepository,
)
from z4j_brain.persistence.repositories.schedule_external import (
    ScheduleExternalRepository,
)
from z4j_core.schedule_external import (
    external_projection_body,
    external_projection_digest,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from z4j_brain.persistence.database import DatabaseManager

#: The reserved scheduler ownership label. Only rows carrying it are the
#: z4j-scheduler's to tick, and the Brain filters every scheduler-facing RPC on
#: it.
RESERVED_OWNER = "z4j-scheduler"


class UnactivatedSchemaError(RuntimeError):
    """The database under test has no live Boundary-D control state."""


@dataclass(frozen=True, slots=True)
class SeededSchedule:
    """The durable identity of one seeded reserved-owner schedule.

    Read inside the seeding transaction and returned by value: the ORM row is
    expired once its session closes, and a test that touched it afterwards
    would be reading through a lazy load rather than through the wire.
    """

    id: uuid.UUID
    project_id: uuid.UUID
    control_token: uuid.UUID
    schedule_revision: int
    definition_digest: str
    next_run_at: datetime | None


async def assert_boundary_d_active(db: DatabaseManager) -> None:
    """Refuse a database whose Boundary-D control state is not activated.

    The guards this suite exists to exercise are triggers and CHECK
    constraints created by the migration chain, so a schema built any other
    way silently answers every control question with the pre-activation
    branch. That is the exact blind spot that let an end-to-end test pass for
    a protocol it never sent, so it is checked before anything else runs.
    """

    from sqlalchemy import text

    async with db.session() as session:
        try:
            active = await ScheduleControlRepository(session).control_is_active()
        except Exception as exc:
            msg = f"schedule control state is unreadable: {exc}"
            raise UnactivatedSchemaError(msg) from exc
        if not active:
            msg = (
                "schedule control is not activated: this database has no "
                "schedule_revision_state singleton, so every Boundary-D "
                "branch would take its pre-activation path"
            )
            raise UnactivatedSchemaError(msg)
        row = (
            await session.execute(
                text(
                    "SELECT guard_version, activation_id, "
                    "activation_manifest_digest FROM schedule_revision_state",
                ),
            )
        ).first()
    if row is None:
        msg = "schedule_revision_state holds no row"
        raise UnactivatedSchemaError(msg)
    guard_version, activation_id, manifest_digest = row
    if guard_version != 1 or not activation_id or not manifest_digest:
        msg = (
            "schedule control is not fully activated: "
            f"guard_version={guard_version!r} activation_id={activation_id!r} "
            f"activation_manifest_digest={manifest_digest!r}"
        )
        raise UnactivatedSchemaError(msg)


async def seed_project(
    db: DatabaseManager,
    *,
    project_id: uuid.UUID | None = None,
    slug: str | None = None,
) -> uuid.UUID:
    """Create one project and return its id."""

    identifier = project_id or uuid.uuid4()
    async with db.session(write=True) as session:
        session.add(
            Project(
                id=identifier,
                slug=slug or f"p{identifier.hex[:8]}",
                name="Proj",
            ),
        )
        await session.commit()
    return identifier


async def seed_online_agent(
    db: DatabaseManager,
    *,
    project_id: uuid.UUID,
    engine: str = "celery",
) -> uuid.UUID:
    """Register one online agent eligible to execute this engine's fires.

    The Brain picks a target for an accepted fire from the ``agents`` table, so
    the presence or absence of this row is what decides between a Command and a
    buffered pending fire. It is not a stand-in for a connected agent: no
    WebSocket session is registered, and none is needed to observe which of the
    two durable work oracles the Brain writes.
    """

    agent_id = uuid.uuid4()
    async with db.session(write=True) as session:
        session.add(
            Agent(
                id=agent_id,
                project_id=project_id,
                name=f"agent-{agent_id.hex[:6]}",
                token_hash=secrets.token_hex(32),
                protocol_version=1,
                framework_adapter="bare",
                state=AgentState.ONLINE,
                last_seen_at=datetime.now(UTC),
                engine_adapters=[engine],
                scheduler_adapters=[RESERVED_OWNER],
            ),
        )
        await session.commit()
    return agent_id


async def create_reserved_schedule(
    db: DatabaseManager,
    *,
    project_id: uuid.UUID,
    name: str,
    planning_at: datetime,
    task_name: str = "tasks.t",
    kind: str = "cron",
    expression: str = "0 * * * *",
    timezone: str = "UTC",
    engine: str = "celery",
    **extra: Any,
) -> SeededSchedule:
    """Plan one reserved-owner schedule through the Brain's only writer.

    ``planning_at`` is the moment the cadence is anchored from, and the first
    canonical cursor is computed forward from it. A caller that wants a slot
    the Brain will accept as due has to back-date it: an acceptance whose slot
    sits beyond the Brain's clock-skew bound is refused.
    """

    async with db.session(write=True) as session:
        row = await ScheduleControlRepository(session).create_current(
            project_id=project_id,
            data={
                "engine": engine,
                "scheduler": RESERVED_OWNER,
                "name": name,
                "task_name": task_name,
                "kind": kind,
                "expression": expression,
                "timezone": timezone,
                "is_enabled": True,
                **extra,
            },
            planning_at=planning_at,
        )
        seeded = SeededSchedule(
            id=row.id,
            project_id=row.project_id,
            control_token=row.control_token,
            schedule_revision=row.schedule_revision,
            definition_digest=row.definition_digest,
            next_run_at=row.next_run_at,
        )
        await session.commit()
    return seeded


async def grant_legacy_fire(
    db: DatabaseManager,
    *,
    project_id: uuid.UUID,
    schedule_id: uuid.UUID,
    control_token: uuid.UUID,
) -> None:
    """Open the tokenless cadence wire for one locked control generation.

    The operator-facing grant, taken through the Brain's own writer rather than
    by stamping the column: it refuses without the all-replica quiesce
    attestation, and refuses again while unresolved evidence is outstanding, so
    a column write would be a grant no operator could have obtained.

    A test that wants to prove something is refused for its own sake needs this,
    because without a grant every tokenless fire is refused anyway and the
    refusal proves nothing about the shape being sent.
    """

    async with db.session(write=True) as session:
        transition = await ScheduleControlRepository(session).set_legacy_fire_grant(
            project_id=project_id,
            schedule_id=schedule_id,
            observed_control_token=control_token,
            allow=True,
            all_replicas_quiesced_and_resynced=True,
            occurred_at=datetime.now(UTC),
        )
        if transition.disposition != "granted":
            msg = f"legacy fire grant was not applied: {transition.disposition}"
            raise RuntimeError(msg)
        await session.commit()


async def project_external_schedule(
    db: DatabaseManager,
    *,
    project_id: uuid.UUID,
    name: str,
    owner: str = "celery-beat",
    task_name: str = "tasks.foreign",
    kind: str = "cron",
    expression: str = "0 * * * *",
    engine: str = "celery",
    occurred_at: datetime | None = None,
) -> uuid.UUID:
    """Land one foreign-owned schedule and return its external stream id.

    This is the whole reachable route for a non-reserved owner. The REST create
    path refuses foreign ownership outright, ``create_current`` refuses any
    owner but the reserved one, and the legacy per-project writers are disabled
    once control is active, so a foreign row exists only because an adapter
    opened an activation epoch and the Brain applied a digest-bound projection
    frame onto it.
    """

    when = occurred_at or datetime.now(UTC)
    source_scope = f'{{"kind":"scheduler-owner","owner":"{owner}","version":1}}'
    adapter_instance_id = f"adapter-{uuid.uuid4().hex[:8]}"

    async with db.session(write=True) as session:
        stream = await ScheduleExternalRepository(session).ensure_activation_epoch(
            project_id=project_id,
            owner=owner,
            source_scope=source_scope,
            occurred_at=when,
            adapter_instance_id=adapter_instance_id,
            executor_agent_id=uuid.uuid4(),
            executor_registry_owner_id=uuid.uuid4(),
            executor_session_generation=uuid.uuid4().hex,
        )
        stream_id = stream.id
        epoch_uuid = stream.current_epoch_uuid
        epoch_number = stream.current_epoch_number
        await session.commit()

    projected: dict[str, Any] = {
        "source_key": f"external-{name}",
        "engine": engine,
        "scheduler": owner,
        "name": name,
        "task_name": task_name,
        "kind": kind,
        "expression": expression,
        "timezone": "UTC",
        "queue": None,
        "priority": "normal",
        "args": [],
        "kwargs": {},
        "is_enabled": True,
        "last_run_at": None,
        "next_run_at": None,
        "total_runs": 0,
        "external_id": None,
        "catch_up": "skip",
        "source": "agent",
        "source_hash": None,
    }
    frame: dict[str, Any] = {
        "stream_id": str(stream_id),
        "epoch_uuid": str(epoch_uuid),
        "epoch_number": epoch_number,
        "sequence": 1,
        "kind": "snapshot",
        "owner": owner,
        "source_scope": source_scope,
        "adapter_instance_id": adapter_instance_id,
        "schedules": [projected],
        "deleted_source_keys": [],
        "complete": True,
        "stable_source": True,
    }
    digest = external_projection_digest(external_projection_body(**frame))

    async with db.session(write=True) as session:
        applied = await ScheduleExternalRepository(session).apply_projection(
            project_id=project_id,
            stream_id=stream_id,
            epoch_uuid=epoch_uuid,
            epoch_number=epoch_number,
            sequence=1,
            kind="snapshot",
            owner=owner,
            source_scope=source_scope,
            adapter_instance_id=adapter_instance_id,
            schedules=[projected],
            deleted_source_keys=[],
            complete=True,
            stable_source=True,
            payload_digest=digest,
            operation_id=None,
            occurred_at=when,
        )
        if applied.disposition != "applied" or applied.inserted != 1:
            msg = (
                "external projection did not land: "
                f"disposition={applied.disposition!r} inserted={applied.inserted!r}"
            )
            raise RuntimeError(msg)
        await session.commit()
    return stream_id


__all__ = [
    "RESERVED_OWNER",
    "SeededSchedule",
    "UnactivatedSchemaError",
    "assert_boundary_d_active",
    "create_reserved_schedule",
    "grant_legacy_fire",
    "project_external_schedule",
    "seed_online_agent",
    "seed_project",
]
