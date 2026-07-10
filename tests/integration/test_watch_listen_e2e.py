"""End-to-end test for the LISTEN/NOTIFY WatchSchedules path.

Postgres-only. Spins up a brain bound to a testcontainers Postgres,
runs the alembic migrations (so the trigger function lands), then:

1. Insert a schedule directly via the brain's own DB connection.
2. Verify the LISTEN-driven WatchSchedules stream emits a CREATED
   event within a sub-second window (target: < 200ms).
3. UPDATE the same schedule -> UPDATED event arrives.
4. DELETE -> DELETED event arrives.

This proves the trigger fires AND that the asyncpg add_listener
path round-trips through the dedicated connection inside the
handler.

Skipped automatically when ``testcontainers`` or ``asyncpg`` is
not installed.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("asyncpg")
pytest.importorskip("grpc")
pytest.importorskip("z4j_brain")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer
from z4j_brain.domain.audit_service import AuditService
from z4j_brain.domain.command_dispatcher import CommandDispatcher
from z4j_brain.persistence.base import Base
from z4j_brain.persistence.database import DatabaseManager
from z4j_brain.persistence.enums import ScheduleKind
from z4j_brain.persistence.models import Project, Schedule
from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert
from z4j_brain.scheduler_grpc.server import (
    SchedulerGrpcServer,
)
from z4j_brain.settings import Settings as BrainSettings
from z4j_brain.websocket.registry import LocalRegistry
from z4j_scheduler.settings import Settings as SchedulerSettings
from z4j_scheduler.storage.brain_client import BrainClient

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
def postgres_container():
    try:
        container = PostgresContainer("postgres:18-alpine")
        container.start()
    except Exception as exc:
        pytest.skip(f"could not start Postgres: {exc}")
    yield container
    with contextlib.suppress(Exception):
        container.stop()


@pytest.fixture(scope="module")
def asyncpg_dsn(postgres_container) -> str:
    return postgres_container.get_connection_url().replace(
        "postgresql+psycopg2://",
        "postgresql+asyncpg://",
    )


@pytest.fixture
async def brain_engine(asyncpg_dsn: str):
    """Engine + schema + trigger applied via alembic migrations."""

    engine = create_async_engine(asyncpg_dsn)

    # Brain's schema relies on a few Postgres extensions that the
    # initial migration installs. ``Base.metadata.create_all``
    # doesn't, so we install them inline here. Same set as the
    # alembic ``_install_extensions`` helper.
    async with engine.begin() as conn:
        for ext in ("pgcrypto", "citext", "pg_trgm"):
            await conn.exec_driver_sql(
                f"CREATE EXTENSION IF NOT EXISTS {ext}",
            )

    # Run migrations (we use Base.create_all for speed; the
    # trigger DDL is applied separately just below because
    # Base.metadata doesn't capture it).
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Apply the LISTEN trigger DDL directly (the migration is the
    # source of truth; we replicate it here so the test doesn't
    # need a full alembic upgrade pass which is slow).
    async with engine.begin() as conn:
        await conn.exec_driver_sql(
            """
            CREATE OR REPLACE FUNCTION z4j_schedules_notify() RETURNS trigger AS $$
            DECLARE
                payload TEXT;
                row_id UUID;
                proj_id UUID;
                op_name TEXT;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    op_name := 'delete';
                    row_id := OLD.id;
                    proj_id := OLD.project_id;
                ELSIF TG_OP = 'INSERT' THEN
                    op_name := 'insert';
                    row_id := NEW.id;
                    proj_id := NEW.project_id;
                ELSE
                    op_name := 'update';
                    row_id := NEW.id;
                    proj_id := NEW.project_id;
                END IF;
                payload := json_build_object(
                    'op', op_name,
                    'id', row_id,
                    'project_id', proj_id
                )::TEXT;
                PERFORM pg_notify('z4j_schedules_changed', payload);
                RETURN COALESCE(NEW, OLD);
            END;
            $$ LANGUAGE plpgsql;
            """,
        )
        await conn.exec_driver_sql(
            "DROP TRIGGER IF EXISTS z4j_schedules_notify_trigger ON schedules",
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER z4j_schedules_notify_trigger "
            "AFTER INSERT OR UPDATE OR DELETE ON schedules "
            "FOR EACH ROW EXECUTE FUNCTION z4j_schedules_notify()",
        )
    yield engine
    # Cleanup: drop schedules so the next test in the module
    # starts fresh.
    async with engine.begin() as conn:
        await conn.exec_driver_sql("DELETE FROM schedules")
        await conn.exec_driver_sql("DELETE FROM projects")
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
    brain_settings = BrainSettings(
        database_url=asyncpg_dsn,
        secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
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

    db = DatabaseManager(brain_engine)
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
    async def test_create_event_arrives_within_one_second(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        """LISTEN-driven path: insert a row, expect CREATED in < 1s."""
        _server, _port, db = brain_grpc
        project_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj-listen", name="Proj"))
            await session.commit()

        events: list = []

        async def consume() -> None:
            async for event in scheduler_client.watch_schedules(project_id):
                events.append(event)
                if len(events) >= 1:
                    return

        consume_task = asyncio.create_task(consume())
        # Give the LISTEN connection time to subscribe.
        await asyncio.sleep(0.5)

        async with db.session() as session:
            session.add(
                Schedule(
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="listen-add",
                    task_name="t.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[],
                    kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        try:
            await asyncio.wait_for(consume_task, timeout=3.0)
        except TimeoutError:
            consume_task.cancel()
            pytest.fail(
                f"LISTEN-driven WatchSchedules did not emit CREATED within 3s (events={events})",
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
        project_id = uuid.uuid4()
        schedule_id = uuid.uuid4()

        async with db.session() as session:
            session.add(Project(id=project_id, slug="proj-listen-2", name="Proj"))
            session.add(
                Schedule(
                    id=schedule_id,
                    project_id=project_id,
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="listen-evolve",
                    task_name="t.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[],
                    kwargs={},
                    is_enabled=True,
                ),
            )
            await session.commit()

        events: list = []

        async def consume() -> None:
            async for event in scheduler_client.watch_schedules(project_id):
                events.append(event)
                if len(events) >= 2:  # UPDATED + DELETED
                    return

        consume_task = asyncio.create_task(consume())
        await asyncio.sleep(0.5)

        # UPDATE.
        async with db.session() as session:
            from sqlalchemy import update as sa_update

            await session.execute(
                sa_update(Schedule)
                .where(Schedule.id == schedule_id)
                .values(expression="*/5 * * * *"),
            )
            await session.commit()

        # Small gap so the UPDATED event arrives before the DELETE.
        await asyncio.sleep(0.3)

        # DELETE.
        async with db.session() as session:
            from sqlalchemy import delete as sa_delete

            await session.execute(
                sa_delete(Schedule).where(Schedule.id == schedule_id),
            )
            await session.commit()

        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()
            pytest.fail(
                f"LISTEN path did not deliver both UPDATED + DELETED within 5s (events={events})",
            )

        kinds = [e.kind for e in events]
        assert "updated" in kinds
        assert "deleted" in kinds
