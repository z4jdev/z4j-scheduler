"""End-to-end test for the LISTEN/NOTIFY WatchSchedules path.

Postgres-only. Spins up a brain bound to a shared or disposable real
PostgreSQL, runs the alembic migrations (so the trigger function lands), then:

1. Create a schedule through the brain's guarded repository.
2. Verify the LISTEN-driven WatchSchedules stream emits a CREATED
   event without relying on a polling refresh.
3. UPDATE the same schedule -> UPDATED event arrives.
4. DELETE -> DELETED event arrives.

This proves the trigger fires AND that the asyncpg add_listener
path round-trips through the dedicated connection inside the
handler.

Uses ``Z4J_TEST_POSTGRES_URL`` when supplied and otherwise starts a disposable
PostgreSQL through the shared integration fixture.
"""

from __future__ import annotations

import asyncio
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("asyncpg")
pytest.importorskip("grpc")
pytest.importorskip("z4j_brain")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy.ext.asyncio import create_async_engine
from z4j_brain.domain.audit_service import AuditService
from z4j_brain.domain.command_dispatcher import CommandDispatcher
from z4j_brain.persistence.database import DatabaseManager
from z4j_brain.persistence.repositories.schedule_control import (
    ScheduleControlRepository,
)
from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert
from z4j_brain.scheduler_grpc.server import (
    SchedulerGrpcServer,
)
from z4j_brain.settings import Settings as BrainSettings
from z4j_brain.websocket.registry import LocalRegistry
from z4j_scheduler.settings import Settings as SchedulerSettings
from z4j_scheduler.storage.brain_client import BrainClient

from .helpers.brain_seeding import create_reserved_schedule, seed_project

# A deadlock guard for the test process, not a delivery-latency SLA.  The
# production contract promises push delivery, not a fixed wall-clock bound.
_HARNESS_DEADLOCK_TIMEOUT_SECONDS = 5.0
_BRAIN_SECRETS = {
    "secret": secrets.token_urlsafe(48),
    "session_secret": secrets.token_urlsafe(48),
    "audit_chain_secret": secrets.token_urlsafe(48),
}

# =====================================================================
# Cert + CA helpers (mirror test_brain_scheduler_e2e.py)
# =====================================================================


def _self_signed_ca() -> tuple[bytes, bytes]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "test-ca-listen")],
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


def _server_cert(ca_cert: bytes, ca_key: bytes) -> tuple[bytes, bytes]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_cert_obj = x509.load_pem_x509_certificate(ca_cert)
    ca_key_obj = serialization.load_pem_private_key(ca_key, password=None)
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "localhost")],
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


@pytest.fixture(scope="module")
def asyncpg_dsn(postgres_url: str) -> str:
    return postgres_url.replace(
        "postgresql://",
        "postgresql+asyncpg://",
        1,
    )


@pytest.fixture
async def brain_engine(
    asyncpg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Engine + schema + trigger applied via alembic migrations."""

    from alembic import command
    from alembic.config import Config
    from z4j_brain.migrations import __file__ as migrations_init

    migration_dir = Path(migrations_init).resolve().parent  # noqa: ASYNC240 - fixture setup
    alembic_ini = migration_dir.parent / "alembic.ini"
    secrets_for_brain = _BRAIN_SECRETS
    monkeypatch.setenv("Z4J_DATABASE_URL", asyncpg_dsn)
    monkeypatch.setenv("Z4J_SECRET", secrets_for_brain["secret"])
    monkeypatch.setenv("Z4J_SESSION_SECRET", secrets_for_brain["session_secret"])
    monkeypatch.setenv(
        "Z4J_AUDIT_CHAIN_SECRET",
        secrets_for_brain["audit_chain_secret"],
    )
    monkeypatch.setenv("Z4J_ENVIRONMENT", "dev")
    monkeypatch.setenv("Z4J_REQUIRE_DB_SSL", "false")
    monkeypatch.setenv("Z4J_HOME", str(tmp_path / "brain-home"))

    config = Config(str(alembic_ini))
    config.set_main_option("script_location", str(migration_dir))
    # Avoid Alembic's fileConfig disabling loggers that the rest of the test
    # process already owns.  All options needed above have been read already.
    config.config_file_name = None
    await asyncio.to_thread(command.upgrade, config, "head")

    engine = create_async_engine(asyncpg_dsn)
    try:
        # Guard the guard: prove the migration, rather than this fixture, owns
        # both the function and trigger before exercising LISTEN.
        async with engine.connect() as conn:
            installed = (
                await conn.exec_driver_sql(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_trigger "
                    "WHERE tgname = 'z4j_schedules_notify_trigger' "
                    "AND NOT tgisinternal"
                    ")",
                )
            ).scalar_one()
            function_installed = (
                await conn.exec_driver_sql(
                    "SELECT to_regprocedure('z4j_schedules_notify()') IS NOT NULL",
                )
            ).scalar_one()
        assert installed is True
        assert function_installed is True
        yield engine, secrets_for_brain
    finally:
        await engine.dispose()


@pytest.fixture
def cert_bundle(tmp_path: Path) -> dict:
    ca_cert, ca_key = _self_signed_ca()
    server_cert, server_key = _server_cert(ca_cert, ca_key)
    client_cert, client_key = mint_scheduler_cert(
        name="scheduler-listen-test",
        ca_cert_pem=ca_cert,
        ca_key_pem=ca_key,
        validity_days=1,
    )
    paths: dict[str, Path] = {}
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
async def brain_grpc(brain_engine, cert_bundle: dict, asyncpg_dsn: str):
    engine, migrated_secrets = brain_engine
    brain_settings = BrainSettings(
        database_url=asyncpg_dsn,
        secret=migrated_secrets["secret"],  # type: ignore[arg-type]
        session_secret=migrated_secrets["session_secret"],  # type: ignore[arg-type]
        audit_chain_secret=migrated_secrets["audit_chain_secret"],  # type: ignore[arg-type]
        environment="dev",
        log_json=False,
        registry_backend="local",
        scheduler_grpc_enabled=True,
        scheduler_grpc_bind_host="127.0.0.1",
        scheduler_grpc_bind_port=0,
        scheduler_grpc_tls_cert=str(cert_bundle["server.crt"]),
        scheduler_grpc_tls_key=str(cert_bundle["server.key"]),
        scheduler_grpc_tls_ca=str(cert_bundle["ca.crt"]),
        scheduler_grpc_allowed_cns=[],
    )

    db = DatabaseManager(engine)
    audit_service = AuditService(brain_settings)
    registry = LocalRegistry(deliver_local=lambda *a, **kw: False)
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
        yield server, server.bound_port, db
    finally:
        await server.stop()


@pytest.fixture
async def scheduler_client(brain_grpc, cert_bundle: dict):
    _server, port, _db = brain_grpc
    settings = SchedulerSettings(
        brain_grpc_url=f"127.0.0.1:{port}",
        brain_rest_url="http://127.0.0.1:7700",
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


# =====================================================================
# Tests
# =====================================================================


class TestListenPush:
    @pytest.mark.asyncio
    async def test_create_event_is_pushed_over_listen(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        """A guarded create is pushed without waiting for a polling refresh."""
        _server, _port, db = brain_grpc
        project_id = await seed_project(db, slug=f"proj-listen-{uuid.uuid4().hex[:8]}")

        events: list = []

        async def consume() -> None:
            async for event in scheduler_client.watch_schedules(project_id):
                events.append(event)
                if len(events) >= 1:
                    return

        consume_task = asyncio.create_task(consume())
        # Give the LISTEN connection time to subscribe.
        await asyncio.sleep(0.5)

        await create_reserved_schedule(
            db,
            project_id=project_id,
            name="listen-add",
            task_name="t.t",
            expression="0 * * * *",
            planning_at=datetime.now(UTC),
        )

        try:
            await asyncio.wait_for(
                consume_task,
                timeout=_HARNESS_DEADLOCK_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            consume_task.cancel()
            pytest.fail(
                "LISTEN-driven WatchSchedules did not emit CREATED before the "
                f"test deadlock guard fired (events={events})",
            )

        assert len(events) >= 1
        assert events[0].kind == "created"
        assert events[0].schedule is not None
        assert events[0].schedule.expression == "0 * * * *"

    @pytest.mark.asyncio
    async def test_update_then_delete_event_sequence(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        """Insert + UPDATE + DELETE all flow through LISTEN."""
        _server, _port, db = brain_grpc
        project_id = await seed_project(db, slug=f"proj-listen-{uuid.uuid4().hex[:8]}")
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="listen-evolve",
            task_name="t.t",
            expression="0 * * * *",
            planning_at=datetime.now(UTC),
        )
        schedule_id = seeded.id

        events: list = []
        updated_seen = asyncio.Event()

        async def consume() -> None:
            async for event in scheduler_client.watch_schedules(project_id):
                events.append(event)
                if event.kind == "updated":
                    updated_seen.set()
                if len(events) >= 2:  # UPDATED + DELETED
                    return

        consume_task = asyncio.create_task(consume())
        await asyncio.sleep(0.5)

        # UPDATE.
        async with db.session(write=True) as session:
            updated = await ScheduleControlRepository(session).update_current(
                project_id=project_id,
                schedule_id=schedule_id,
                data={"expression": "*/5 * * * *"},
                planning_at=datetime.now(UTC),
            )
            assert updated is not None
            await session.commit()

        # Wait for the pushed update before deleting so the assertion observes
        # stream order, rather than depending on a sleep duration.
        await asyncio.wait_for(
            updated_seen.wait(),
            timeout=_HARNESS_DEADLOCK_TIMEOUT_SECONDS,
        )

        # DELETE.
        async with db.session(write=True) as session:
            deleted = await ScheduleControlRepository(session).delete_current(
                project_id=project_id,
                schedule_id=schedule_id,
                occurred_at=datetime.now(UTC),
            )
            assert deleted.disposition == "deleted"
            await session.commit()

        try:
            await asyncio.wait_for(
                consume_task,
                timeout=_HARNESS_DEADLOCK_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            consume_task.cancel()
            pytest.fail(
                "LISTEN path did not deliver both UPDATED + DELETED before the "
                f"test deadlock guard fired (events={events})",
            )

        kinds = [e.kind for e in events]
        assert "updated" in kinds
        assert "deleted" in kinds
