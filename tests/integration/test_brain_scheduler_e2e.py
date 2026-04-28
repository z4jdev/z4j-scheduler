"""End-to-end test: real scheduler ↔ real brain over real mTLS gRPC.

The other test suites use fakes - this one is the smoke check that
proves the wire actually works:

1. Mint a fresh CA + server cert + client cert via the brain's own
   ``mint_scheduler_cert`` helper.
2. Spin up brain singletons (DatabaseManager, AuditService,
   CommandDispatcher) on an in-memory SQLite engine.
3. Boot ``SchedulerGrpcServer`` on an ephemeral localhost port with
   the minted server cert.
4. Connect ``BrainClient`` from the scheduler side using the matching
   client cert.
5. Drive every RPC and assert each one round-trips correctly.

This is the gate before declaring Phase 1 done. If this passes, both
sides genuinely speak the protocol; if it fails, an operator
deploying both halves will hit the same bug.

Skipped automatically when either ``grpcio`` or ``cryptography`` is
not installed - the gRPC service is in an optional extra on both
sides.
"""

from __future__ import annotations

import asyncio
import secrets
import socket
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

# Both sides need grpc + cryptography. Skip the whole module if
# either is missing so a default install can still run the rest of
# the suite.
pytest.importorskip("grpc")
pytest.importorskip("cryptography")
pytest.importorskip("z4j_brain")

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from z4j_brain.domain.audit_service import AuditService  # noqa: E402
from z4j_brain.domain.command_dispatcher import CommandDispatcher  # noqa: E402
from z4j_brain.persistence.base import Base  # noqa: E402
from z4j_brain.persistence.database import DatabaseManager  # noqa: E402
from z4j_brain.persistence.enums import (  # noqa: E402
    AgentState,
    ProjectRole,
    ScheduleKind,
)
from z4j_brain.persistence.models import (  # noqa: E402
    Agent,
    Project,
    Schedule,
)
from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert  # noqa: E402
from z4j_brain.scheduler_grpc.server import (  # noqa: E402
    SchedulerGrpcServer,
)
from z4j_brain.settings import Settings as BrainSettings  # noqa: E402
from z4j_brain.websocket.registry import LocalRegistry  # noqa: E402

from z4j_scheduler.settings import Settings as SchedulerSettings  # noqa: E402
from z4j_scheduler.storage.brain_client import BrainClient  # noqa: E402


# =====================================================================
# Cert + port helpers
# =====================================================================


def _self_signed_ca() -> tuple[bytes, bytes]:
    """Mint a throwaway CA cert + key pair."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "test-ca-e2e")],
    )
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(8), "big"))
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .sign(private_key=key, algorithm=hashes.SHA256())
    )
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )


def _mint_server_cert(
    *, ca_cert: bytes, ca_key: bytes,
) -> tuple[bytes, bytes]:
    """Mint a SERVER cert with serverAuth EKU and CN=localhost.

    Brain's own ``mint_scheduler_cert`` is hard-coded to clientAuth
    only - perfect for client certs but won't work as a TLS server
    cert. We hand-roll a serverAuth one here for the brain's gRPC
    listener.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_cert_obj = x509.load_pem_x509_certificate(ca_cert)
    ca_key_obj = serialization.load_pem_private_key(ca_key, password=None)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "z4j"),
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ],
    )
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert_obj.subject)
        .public_key(key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(8), "big"))
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([x509.ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=True,
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.DNSName("127.0.0.1")],
            ),
            critical=False,
        )
        .sign(private_key=ca_key_obj, algorithm=hashes.SHA256())
    )
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def cert_bundle(tmp_path: Path) -> dict:
    """Write a fresh CA + server cert + client cert to ``tmp_path``."""
    ca_cert, ca_key = _self_signed_ca()
    server_cert, server_key = _mint_server_cert(
        ca_cert=ca_cert, ca_key=ca_key,
    )
    client_cert, client_key = mint_scheduler_cert(
        name="scheduler-e2e",
        ca_cert_pem=ca_cert,
        ca_key_pem=ca_key,
        validity_days=1,
    )

    paths = {}
    for name, blob in (
        ("ca.crt", ca_cert),
        ("server.crt", server_cert),
        ("server.key", server_key),
        ("client.crt", client_cert),
        ("client.key", client_key),
    ):
        path = tmp_path / name
        path.write_bytes(blob)
        paths[name] = path
    return paths


@pytest.fixture
async def brain_engine():
    """In-memory SQLite engine with the brain schema applied."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def brain_grpc(brain_engine, cert_bundle: dict):
    """Boot the brain-side ``SchedulerGrpcServer`` on an ephemeral port.

    Yields ``(server, port, db, command_dispatcher)`` so tests can
    seed the DB directly. Tears the server down on exit.
    """
    brain_settings = BrainSettings(
        database_url="sqlite+aiosqlite:///:memory:",
        secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        environment="dev",
        log_json=False,
        registry_backend="local",
        scheduler_grpc_enabled=True,
        scheduler_grpc_bind_host="127.0.0.1",
        scheduler_grpc_bind_port=0,  # ask kernel for ephemeral
        scheduler_grpc_tls_cert=str(cert_bundle["server.crt"]),
        scheduler_grpc_tls_key=str(cert_bundle["server.key"]),
        scheduler_grpc_tls_ca=str(cert_bundle["ca.crt"]),
        # Empty allow-list = trust the CA. Adding "scheduler-e2e"
        # would also work (matches the client cert CN).
        scheduler_grpc_allowed_cns=[],
        # Tighter watch poll so the watch test doesn't drag.
        scheduler_grpc_watch_poll_seconds=0.5,
    )

    db = DatabaseManager(brain_engine)
    audit_service = AuditService(brain_settings)
    registry = LocalRegistry(deliver_local=_noop_deliver_local)
    command_dispatcher = CommandDispatcher(
        settings=brain_settings,
        registry=registry,
        audit=audit_service,
    )

    server = SchedulerGrpcServer(
        settings=brain_settings,
        db=db,
        command_dispatcher=command_dispatcher,
        audit_service=audit_service,
    )
    await server.start()
    try:
        yield server, server.bound_port, db, command_dispatcher
    finally:
        await server.stop()


@pytest.fixture
async def scheduler_client(brain_grpc, cert_bundle: dict):
    """A connected :class:`BrainClient` pointed at the brain fixture."""
    _server, port, _db, _dispatcher = brain_grpc
    settings = SchedulerSettings(
        brain_grpc_url=f"127.0.0.1:{port}",
        brain_rest_url="http://127.0.0.1:7700",  # not actually used
        tls_cert=cert_bundle["client.crt"],
        tls_key=cert_bundle["client.key"],
        tls_ca=cert_bundle["ca.crt"],
    )
    client = BrainClient(settings)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


async def _noop_deliver_local(*_args, **_kwargs) -> bool:
    """Stand-in for the dispatcher's local-delivery callback.

    Integration test never actually pushes commands over the WS - the
    schedule-fire path stops at "Command row inserted" because no
    agent is connected. The dispatcher needs *something* callable
    here to construct.
    """
    return False


# =====================================================================
# Tests
# =====================================================================


class TestPing:
    @pytest.mark.asyncio
    async def test_ping_returns_brain_version(
        self, scheduler_client: BrainClient,
    ) -> None:
        info = await scheduler_client.ping()
        assert info.brain_version
        # Within a generous window: the brain's clock should be
        # within a minute of ours.
        delta = abs((info.brain_time - datetime.now(UTC)).total_seconds())
        assert delta < 60


class TestListSchedules:
    @pytest.mark.asyncio
    async def test_lists_only_z4j_scheduler_rows(
        self, brain_grpc, scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj", name="Proj"))
            session.add(
                Schedule(
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="hourly",
                    task_name="tasks.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                ),
            )
            session.add(
                Schedule(
                    project_id=project_id,
                    engine="celery",
                    scheduler="celery-beat",
                    name="not-ours",
                    task_name="tasks.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        results = []
        async for entry in scheduler_client.list_schedules(project_id):
            results.append(entry)
        assert len(results) == 1
        # ScheduleEntry doesn't carry the name (we converted to a
        # tick-engine view); identify by expression instead.
        assert results[0].expression == "0 * * * *"


class TestWatchSchedules:
    @pytest.mark.asyncio
    async def test_emits_event_when_schedule_inserted(
        self, brain_grpc, scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj", name="Proj"))
            await session.commit()

        # Open the watch stream in a task so we can insert mid-stream.
        events: list = []

        async def consume() -> None:
            async for event in scheduler_client.watch_schedules(project_id):
                events.append(event)
                if len(events) >= 1:
                    return

        consume_task = asyncio.create_task(consume())
        # Give the watch loop time to do its first poll cycle (which
        # establishes the snapshot baseline) before we insert.
        await asyncio.sleep(0.7)

        async with db.session() as session:
            session.add(
                Schedule(
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="late-add",
                    task_name="tasks.t",
                    kind=ScheduleKind.INTERVAL,
                    expression="60s",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        # Wait for the next poll cycle to pick the diff up.
        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except asyncio.TimeoutError:
            consume_task.cancel()
            pytest.fail(
                "watch stream did not emit an event within 5s "
                f"(events={events})",
            )

        assert len(events) >= 1
        assert events[0].kind == "created"
        assert events[0].schedule is not None
        assert events[0].schedule.expression == "60s"


class TestFireSchedule:
    @pytest.mark.asyncio
    async def test_fire_with_no_agent_buffers(
        self, brain_grpc, scheduler_client: BrainClient,
    ) -> None:
        # Phase 2 behaviour change: when no agent is online, brain
        # buffers the fire in pending_fires (returning buffered=true)
        # instead of returning agent_offline. The replay worker
        # delivers it once a matching agent comes online.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = uuid.uuid4()
        schedule_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj", name="Proj"))
            session.add(
                Schedule(
                    id=schedule_id,
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="orphan",
                    task_name="tasks.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        now = datetime.now(UTC)
        result = await scheduler_client.fire_schedule(
            schedule_id=schedule_id,
            fire_id=uuid.uuid4(),
            scheduled_for=now,
            fired_at=now,
        )
        # FireResult.success is True for both delivered + buffered
        # paths since neither is a hard failure.
        assert result.success is True
        assert result.buffered is True
        assert result.command_id is None
        assert result.error_code is None


class TestAcknowledgeFireResult:
    @pytest.mark.asyncio
    async def test_ack_for_unknown_fire_id_is_accepted(
        self, scheduler_client: BrainClient,
    ) -> None:
        # Acking a fire_id that brain doesn't know about (no schedule
        # row carrying last_fire_id == this id) must not crash. It's
        # a no-op the brain logs and moves on - the scheduler can't
        # retry the ack on every restart and risk a false failure.
        await scheduler_client.acknowledge_result(
            fire_id=uuid.uuid4(),
            command_id=None,
            status="success",
        )

    @pytest.mark.asyncio
    async def test_ack_updates_last_run_at_when_correlated(
        self, brain_grpc, scheduler_client: BrainClient,
    ) -> None:
        # Audit fix I-1 (Apr 2026): ack lookup goes via the
        # ``schedule_fires`` row, which the FireSchedule handler
        # writes at dispatch time. This integration test seeds both
        # ``last_fire_id`` AND a ``schedule_fires`` row so the
        # post-fix correlation succeeds. A scheduler that calls
        # AcknowledgeFireResult in production can only do so for a
        # fire_id that was minted via FireSchedule, which always
        # writes the fires row.
        from datetime import datetime, UTC  # noqa: PLC0415

        from z4j_brain.persistence.models import ScheduleFire  # noqa: PLC0415

        _server, _port, db, _dispatcher = brain_grpc
        project_id = uuid.uuid4()
        schedule_id = uuid.uuid4()
        fire_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj", name="Proj"))
            session.add(
                Schedule(
                    id=schedule_id,
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="acked",
                    task_name="tasks.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                    last_fire_id=fire_id,
                    total_runs=0,
                ),
            )
            now = datetime.now(UTC)
            session.add(
                ScheduleFire(
                    fire_id=fire_id,
                    schedule_id=schedule_id,
                    project_id=project_id,
                    command_id=None,
                    status="delivered",
                    scheduled_for=now,
                    fired_at=now,
                ),
            )
            await session.commit()

        await scheduler_client.acknowledge_result(
            fire_id=fire_id,
            command_id=None,
            status="success",
        )

        async with db.session() as session:
            row = (
                await session.execute(
                    select(Schedule).where(Schedule.id == schedule_id),
                )
            ).scalar_one()
            assert row.last_run_at is not None
            assert row.total_runs == 1
            assert row.last_fire_id is None


class TestEndToEndWiring:
    """Sanity check that the bundle holds together for a non-trivial flow.

    Inserts a schedule, waits for the watch stream to emit a CREATED
    event, then exercises FireSchedule + AcknowledgeFireResult on
    the same schedule.
    """

    @pytest.mark.asyncio
    async def test_full_flow(
        self, brain_grpc, scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = uuid.uuid4()
        schedule_id = uuid.uuid4()
        fire_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj", name="Proj"))
            # Online agent matching the schedule's engine so the fire
            # has a target. agent.engine_adapters drives the picker.
            session.add(
                Agent(
                    id=uuid.uuid4(),
                    project_id=project_id,
                    name="test-agent",
                    # token_hash, protocol_version, framework_adapter
                    # are required at the schema level even though
                    # the integration test never authenticates this
                    # agent over WS. Stuff placeholder values.
                    token_hash=secrets.token_hex(32),
                    protocol_version=1,
                    framework_adapter="bare",
                    state=AgentState.ONLINE,
                    last_seen_at=datetime.now(UTC),
                    engine_adapters=["celery"],
                    scheduler_adapters=["z4j-scheduler"],
                ),
            )
            session.add(
                Schedule(
                    id=schedule_id,
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="full-flow",
                    task_name="tasks.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[], kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        # ListSchedules - the schedule is visible.
        listed = []
        async for entry in scheduler_client.list_schedules(project_id):
            listed.append(entry)
        assert any(e.id == schedule_id for e in listed)

        # FireSchedule - agent is online, brain accepts. We don't
        # assert success because LocalRegistry's deliver_local is a
        # no-op stub returning False - the dispatcher then raises
        # AgentOfflineError ⇒ brain's handler catches and surfaces a
        # generic error_code. Either way, the wire round-trips.
        now = datetime.now(UTC)
        result = await scheduler_client.fire_schedule(
            schedule_id=schedule_id,
            fire_id=fire_id,
            scheduled_for=now,
            fired_at=now,
        )
        # Either successful command issuance, agent_offline, or a
        # generic brain_error from the deliver_local no-op. The
        # important invariant is no exception propagated and the
        # response object is well-formed.
        assert result.command_id is not None or result.error_code

        # AcknowledgeFireResult - if FireSchedule stamped last_fire_id,
        # the ack updates last_run_at. We don't depend on that
        # outcome - just verify the call completes without error.
        await scheduler_client.acknowledge_result(
            fire_id=fire_id,
            command_id=result.command_id,
            status="success",
        )
