"""End-to-end test: real scheduler, real brain, real mTLS gRPC, real schema.

The other test suites use fakes. This one is the gate: if it passes, both sides
genuinely speak the protocol that ships; if it fails, an operator deploying
both halves hits the same bug.

1. Mint a fresh CA + server cert + client cert via the brain's own
   ``mint_scheduler_cert`` helper.
2. Spin up brain singletons (DatabaseManager, AuditService, CommandDispatcher)
   on a MIGRATED SQLite engine.
3. Boot ``SchedulerGrpcServer`` on an ephemeral localhost port.
4. Connect ``BrainClient`` from the scheduler side.
5. Drive the current protocol and assert each answer.

Two things this file is strict about, because the absence of either is what
made an earlier version of it pass for years without ever sending the protocol
it claims to prove.

**The schema is migrated, and activation is enforced.** Every Boundary-D guard
lives in a migration as a trigger or a CHECK constraint, so a
``Base.metadata.create_all`` schema refuses nothing: it has an empty
``schedule_revision_state``, no control triggers, and it accepts a schedule row
assembled by hand that no writer in the brain can produce. On such a schema the
brain answers every control question with its pre-activation branch, so an
end-to-end test seeded that way proves the protocol works against a database no
operator has. ``_require_activated_schema`` is autouse and fails every test in
this module the moment that is true again.

**The current protocol is what gets driven.** The shipped scheduler negotiates
``scheduler_protocol_epoch=1`` at startup and then talks
``ListScheduleSnapshot`` / ``WatchSchedulesV2`` / token-bearing
``FireSchedule``. A ``FireSchedule`` that omits the control token, the prepared
transition, or the epoch is the tokenless 1.7 wire, and an activated brain
answers it with ``scheduler_upgrade_required``. Fires here are built by
``prepare_current_fire`` from what the wire returned, and
``test_tokenless_fire_is_refused_without_a_grant`` is the negative control that
keeps that honest: if the helper ever stops carrying authority, that test stops
failing to fire and this suite stops meaning anything.

Skipped automatically when either ``grpcio`` or ``cryptography`` is not
installed - the gRPC service is in an optional extra on both sides.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import secrets
import shutil
import sqlite3
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

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from z4j_brain.domain.audit_service import AuditService
from z4j_brain.domain.command_dispatcher import CommandDispatcher
from z4j_brain.persistence.database import DatabaseManager
from z4j_brain.persistence.enums import ScheduleKind
from z4j_brain.persistence.models import (
    Command,
    PendingFire,
    Schedule,
    ScheduleFire,
)
from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert
from z4j_brain.scheduler_grpc.server import SchedulerGrpcServer
from z4j_brain.settings import Settings as BrainSettings
from z4j_brain.websocket.registry import LocalRegistry
from z4j_scheduler.dispatch.fire import FireDispatcher, derive_fire_id
from z4j_scheduler.settings import Settings as SchedulerSettings
from z4j_scheduler.storage._models import ScannedThrough, ScheduleChange
from z4j_scheduler.storage._protocol import (
    CURRENT_PROTOCOL_EPOCH,
    current_capabilities,
    require_exact_current,
)
from z4j_scheduler.storage.brain_client import BrainClient
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick.cadence import cadence_runtime_fingerprint

from .helpers.brain_seeding import (
    RESERVED_OWNER,
    assert_boundary_d_active,
    create_reserved_schedule,
    grant_legacy_fire,
    project_external_schedule,
    seed_online_agent,
    seed_project,
)
from .helpers.current_protocol import prepare_current_fire, send_current_fire

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
    *,
    ca_cert: bytes,
    ca_key: bytes,
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
        ca_cert=ca_cert,
        ca_key=ca_key,
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


#: The audit-chain key the migrated template is activated with. Boundary F
#: binds the activated state to the key that signed it, so a brain opened
#: against a copy of the template has to present this same key.
MIGRATED_AUDIT_CHAIN_SECRET = "z4j-test-audit-chain-key-do-not-use-in-production"

#: How far back a seeded schedule is planned from. The first canonical cursor
#: is computed forward from this moment, so a back-dated plan leaves a slot
#: that is already due and a successor that is still in the past, which is what
#: lets one schedule be fired twice inside a test without waiting.
_PLANNING_BACKDATE = timedelta(hours=3)


def _utc(value: datetime | None) -> datetime | None:
    """Read a database timestamp as UTC.

    SQLite hands back naive datetimes; the wire is always aware. Comparing the
    two directly raises rather than failing an assertion, which reads as an
    error in the test instead of a difference in the values.
    """
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _live_loggers() -> list[logging.Logger]:
    return [
        logger
        for logger in logging.Logger.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    ]


def _upgrade_to_head(root: Path, database: Path) -> None:
    """Run the brain's migration chain into ``database``.

    From an isolated working directory: alembic's env hook captures
    configuration from the current directory, and a repository ``.env`` whose
    permissions z4j refuses would otherwise fail the build on a developer
    machine while passing in CI. ``script_location`` is pinned absolutely
    because one of the two alembic.ini files declares it relatively.

    ``config_file_name`` is cleared before the upgrade, and only after the
    options have been read out of the file. The brain's alembic ``env`` hands
    that path to ``logging.config.fileConfig``, which disables every logger
    that already exists; running a migration inside a test session would
    otherwise silence the rest of the suite's loggers for the remainder of the
    process, and any later test that asserts on log output would fail with no
    visible connection to the migration that broke it.
    """
    from alembic import command
    from alembic.config import Config
    from z4j_brain.cli import _find_alembic_config_path
    from z4j_brain.migrations import __file__ as migrations_init

    home = root / "home"
    home.mkdir(mode=0o700, exist_ok=True)
    config_path = str(_find_alembic_config_path())
    saved_environment = {k: v for k, v in os.environ.items() if k.startswith("Z4J_")}
    saved_cwd = Path.cwd()
    observed = _live_loggers()
    disabled_before = {logger.name for logger in observed if logger.disabled}
    try:
        for key in tuple(os.environ):
            if key.startswith("Z4J_"):
                del os.environ[key]
        os.environ.update(
            {
                "Z4J_DATABASE_URL": f"sqlite+aiosqlite:///{database}",
                "Z4J_HOME": str(home),
                "Z4J_ENVIRONMENT": "dev",
                "Z4J_SECRET": secrets.token_hex(32),
                "Z4J_SESSION_SECRET": secrets.token_hex(32),
                "Z4J_AUDIT_CHAIN_SECRET": MIGRATED_AUDIT_CHAIN_SECRET,
            },
        )
        os.chdir(root)
        config = Config(config_path)
        config.set_main_option(
            "script_location",
            str(Path(migrations_init).resolve().parent),
        )
        config.config_file_name = None
        command.upgrade(config, "head")
    finally:
        os.chdir(saved_cwd)
        for key in tuple(os.environ):
            if key.startswith("Z4J_"):
                del os.environ[key]
        os.environ.update(saved_environment)

    silenced = sorted(
        logger.name for logger in observed if logger.disabled and logger.name not in disabled_before
    )
    if silenced:
        msg = (
            "the migration chain silenced loggers that already existed, so "
            f"later tests can no longer observe them: {silenced}"
        )
        raise RuntimeError(msg)


@pytest.fixture(scope="session")
def migrated_brain_template(tmp_path_factory) -> Path:
    """One brain database at the release head, built once per session."""
    root = tmp_path_factory.mktemp("migrated-brain")
    template = root / "template.db"
    _upgrade_to_head(root, template)

    # A template without live guards would silently restore the blind spot
    # this fixture exists to close, so refuse to hand one out.
    #
    # ``closing`` rather than the connection's own context manager: that one
    # only ends the transaction, so the handle survives to be collected at some
    # arbitrary later point and reported as an unraisable ResourceWarning
    # against whichever unrelated test happened to be running.
    with contextlib.closing(sqlite3.connect(template)) as probe:
        try:
            activation = probe.execute(
                "SELECT guard_version, activation_id, "
                "activation_manifest_digest FROM schedule_revision_state",
            ).fetchone()
        except sqlite3.OperationalError:
            activation = None
    if not activation or activation[0] != 1 or not activation[1] or not activation[2]:
        msg = f"migrated template has no live Boundary-D activation: {activation!r}"
        raise RuntimeError(msg)
    return template


@pytest.fixture
def migrated_brain_url(migrated_brain_template: Path, tmp_path: Path) -> str:
    """A private copy of the migrated template, as a database URL."""
    database = tmp_path / "brain.db"
    shutil.copyfile(migrated_brain_template, database)
    return f"sqlite+aiosqlite:///{database}"


@pytest.fixture
async def brain_engine(migrated_brain_url: str):
    """File-backed SQLite engine at the brain's migrated schema.

    File-backed rather than in-memory because the migration chain has to run
    against it in a separate connection, and because a StaticPool hides every
    pool-shaped behaviour the gRPC server has.
    """
    engine = create_async_engine(migrated_brain_url)
    yield engine
    await engine.dispose()


async def _noop_deliver_local(*_args, **_kwargs) -> bool:
    """Stand-in for the registry's local-delivery callback.

    Agent delivery is the WebSocket layer, on the far side of the boundary
    under test: no agent session is registered here, so the registry never
    calls this and the return value is never consulted. It exists because
    ``LocalRegistry`` requires something callable to construct. Which durable
    work oracle the brain writes (a Command or a buffered pending fire) is
    decided from the ``agents`` table before any delivery is attempted, and
    that is what the fire tests assert.
    """
    return False


@pytest.fixture
async def brain_grpc(brain_engine, migrated_brain_url: str, cert_bundle: dict):
    """Boot the brain-side ``SchedulerGrpcServer`` on an ephemeral port.

    Yields ``(server, port, db, command_dispatcher)`` so tests can
    seed the DB directly. Tears the server down on exit.
    """
    brain_settings = BrainSettings(
        database_url=migrated_brain_url,
        secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        # A migrated database has Boundary F activated, and activated state is
        # bound to the key that signed it, so an audit write with any other key
        # is refused.
        audit_chain_secret=MIGRATED_AUDIT_CHAIN_SECRET,  # type: ignore[arg-type]
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
        # Tighter watch poll so the watch tests don't drag.
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


@pytest.fixture(autouse=True)
async def _require_activated_schema(brain_grpc) -> None:
    """Fail every test in this module against an unactivated schema.

    This is the load-bearing guard. Boundary D changes what the brain does, not
    only what it stores: on an unactivated database ``FireSchedule`` takes a
    branch that asks for no control token, ``ListSchedules`` filters differently,
    and the acknowledge path owns the cadence advance instead of recording a
    receipt. A suite that ran there would assert a coherent set of answers that
    no shipped deployment ever produces, and it would go on passing while the
    protocol rotted. Refuse to start instead.
    """
    _server, _port, db, _dispatcher = brain_grpc
    await assert_boundary_d_active(db)


@pytest.fixture
def scheduler_settings(brain_grpc, cert_bundle: dict) -> SchedulerSettings:
    """Scheduler-side settings pointed at the brain fixture."""
    _server, port, _db, _dispatcher = brain_grpc
    return SchedulerSettings(
        brain_grpc_url=f"127.0.0.1:{port}",
        brain_rest_url="http://127.0.0.1:7700",  # not actually used
        tls_cert=cert_bundle["client.crt"],
        tls_key=cert_bundle["client.key"],
        tls_ca=cert_bundle["ca.crt"],
    )


@pytest.fixture
async def scheduler_client(scheduler_settings: SchedulerSettings):
    """A connected :class:`BrainClient` pointed at the brain fixture."""
    client = BrainClient(scheduler_settings)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
def fire_dispatcher(
    scheduler_client: BrainClient,
    scheduler_settings: SchedulerSettings,
) -> FireDispatcher:
    """The production dispatcher, wired to the real client.

    Fires go through this rather than through ``BrainClient.fire_schedule``
    wherever the test is about the protocol as a whole: the dispatcher is what
    decides to stamp ``scheduler_protocol_epoch``, and skipping it would leave
    that decision untested.
    """
    return FireDispatcher(client=scheduler_client, settings=scheduler_settings)


# =====================================================================
# Small readers
# =====================================================================


async def _schedule_row(db: DatabaseManager, schedule_id: uuid.UUID) -> Schedule:
    async with db.session() as session:
        return (
            await session.execute(
                select(Schedule).where(Schedule.id == schedule_id),
            )
        ).scalar_one()


async def _cadence_state(db: DatabaseManager, schedule_id: uuid.UUID) -> tuple:
    """The durable cadence fence, as one comparable tuple."""
    row = await _schedule_row(db, schedule_id)
    return (
        _utc(row.last_run_at),
        _utc(row.next_run_at),
        row.total_runs,
        row.last_fire_id,
        row.schedule_revision,
        row.control_token,
    )


async def _fire_rows(db: DatabaseManager) -> list[ScheduleFire]:
    async with db.session() as session:
        return list((await session.execute(select(ScheduleFire))).scalars())


async def _command_rows(db: DatabaseManager) -> list[Command]:
    async with db.session() as session:
        return list((await session.execute(select(Command))).scalars())


async def _pending_rows(db: DatabaseManager) -> list[PendingFire]:
    async with db.session() as session:
        return list((await session.execute(select(PendingFire))).scalars())


async def _foreign_revisions(db: DatabaseManager) -> list[int]:
    """Transport revisions allocated to rows the scheduler must never see."""
    async with db.session() as session:
        return list(
            (
                await session.execute(
                    select(Schedule.schedule_revision).where(
                        Schedule.scheduler != RESERVED_OWNER,
                    ),
                )
            ).scalars(),
        )


# =====================================================================
# The guard
# =====================================================================


class TestBoundaryDActivation:
    """Prove the schema under test is the one operators run.

    Each of these is a fact about the database or the wire that is true only
    after the migration chain activated schedule control. If any of them stops
    holding, every other test in this file is measuring a different product.
    """

    @pytest.mark.asyncio
    async def test_brain_advertises_the_current_protocol_epoch(
        self,
        scheduler_client: BrainClient,
    ) -> None:
        # Ping's epoch is the fact the shipped scheduler keys its whole mode
        # selection on. An unactivated brain answers zero here, and
        # ``_select_protocol_mode`` then requires a matching UNIMPLEMENTED from
        # negotiation before it will speak the 1.7 wire.
        info = await scheduler_client.ping()
        assert info.brain_version
        assert info.scheduler_protocol_epoch == CURRENT_PROTOCOL_EPOCH
        delta = abs((info.brain_time - datetime.now(UTC)).total_seconds())
        assert delta < 60

    @pytest.mark.asyncio
    async def test_negotiation_selects_the_exact_offered_tuple(
        self,
        scheduler_client: BrainClient,
    ) -> None:
        # What ``SchedulerApp.start`` does before it ticks anything. An exact
        # match is the only accepted outcome: a partial tuple, a zero, or a
        # cadence fingerprint the brain computes differently all raise.
        offered = current_capabilities(
            cadence_runtime_fingerprint=cadence_runtime_fingerprint(),
        )
        info = await scheduler_client.ping()
        selected = await scheduler_client.negotiate_protocol(offered)
        require_exact_current(
            selected=selected,
            expected=offered,
            ping_protocol_epoch=info.scheduler_protocol_epoch,
        )
        assert selected == offered

    @pytest.mark.asyncio
    async def test_hand_built_schedule_row_is_refused(self, brain_grpc) -> None:
        # The insert trigger, observed refusing rather than assumed present.
        # This exact row is what a create_all schema accepts silently, and
        # accepting it is what let a foreign-owner negative control look real
        # while proving nothing.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)

        with pytest.raises(IntegrityError) as excinfo:
            async with db.session(write=True) as session:
                session.add(
                    Schedule(
                        project_id=project_id,
                        engine="celery",
                        scheduler="celery-beat",
                        name="hand-built",
                        task_name="tasks.t",
                        kind=ScheduleKind.CRON,
                        expression="0 * * * *",
                        timezone="UTC",
                        args=[],
                        kwargs={},
                        is_enabled=True,
                    ),
                )
                await session.commit()
        assert "schedule D identity is incomplete" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_bare_cadence_column_write_is_refused(self, brain_grpc) -> None:
        # The transition trigger. ``last_fire_id`` is the cadence fence, and
        # nothing outside an accepted fire may stamp it: a test that could
        # would be able to fabricate the precondition for an acknowledge and
        # then assert against a state the brain never produces.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="fenced",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )

        with pytest.raises(IntegrityError) as excinfo:
            async with db.session(write=True) as session:
                row = (
                    await session.execute(
                        select(Schedule).where(Schedule.id == seeded.id),
                    )
                ).scalar_one()
                row.last_fire_id = uuid.uuid4()
                await session.commit()
        assert "invalid schedule transition identity" in str(excinfo.value)


# =====================================================================
# Read surfaces
# =====================================================================


class TestScheduleSnapshot:
    """``ListScheduleSnapshot`` - the read the shipped scheduler boots from."""

    @pytest.mark.asyncio
    async def test_snapshot_carries_complete_current_authority(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="hourly",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )

        snapshot = await scheduler_client.list_schedule_snapshot(project_id)

        assert len(snapshot.rows) == 1
        entry = snapshot.rows[0]
        # Every field a current FireSchedule needs has to arrive here, because
        # the scheduler has no other source for any of them.
        assert entry.id == seeded.id
        assert entry.control_token == seeded.control_token
        assert entry.schedule_revision == seeded.schedule_revision
        assert entry.definition_digest == seeded.definition_digest
        assert entry.cadence_semantics_version > 0
        assert entry.cadence_runtime_fingerprint == cadence_runtime_fingerprint()
        assert entry.next_fire_at == _utc(seeded.next_run_at)
        assert snapshot.watermark >= seeded.schedule_revision

    @pytest.mark.asyncio
    async def test_snapshot_excludes_foreign_owned_rows(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="ours",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        # The negative half, landed the only way a foreign owner can be landed.
        await project_external_schedule(
            db,
            project_id=project_id,
            name="not-ours",
            owner="celery-beat",
        )

        # Both rows really are in the table, so the filter has something to do.
        async with db.session() as session:
            owners = sorted(
                (await session.execute(select(Schedule.scheduler))).scalars(),
            )
        assert owners == ["celery-beat", RESERVED_OWNER]

        snapshot = await scheduler_client.list_schedule_snapshot(project_id)
        assert [row.id for row in snapshot.rows] == [seeded.id]


class TestListSchedules:
    """``ListSchedules`` - the N-1 read a 1.7 scheduler still issues."""

    @pytest.mark.asyncio
    async def test_lists_only_reserved_owner_rows(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="ours",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        await project_external_schedule(
            db,
            project_id=project_id,
            name="not-ours",
            owner="celery-beat",
        )

        results = [entry async for entry in scheduler_client.list_schedules(project_id)]

        assert [entry.id for entry in results] == [seeded.id]
        assert results[0].name == "ours"


class TestWatchSchedulesV2:
    """``WatchSchedulesV2`` - the change stream the shipped scheduler runs."""

    @pytest.mark.asyncio
    async def test_emits_reserved_changes_and_filters_foreign_ones(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        baseline = await scheduler_client.list_schedule_snapshot(project_id)

        frames: list = []
        reserved_seen = asyncio.Event()

        async def consume() -> None:
            async for frame in scheduler_client.watch_schedules_v2(
                project_id,
                after_revision=baseline.watermark,
            ):
                frames.append(frame)
                if isinstance(frame, ScheduleChange):
                    reserved_seen.set()

        consume_task = asyncio.create_task(consume())
        # Let the stream establish its cursor before anything lands.
        await asyncio.sleep(0.7)
        try:
            # Foreign first: its revision is allocated before the reserved
            # row's, so a stream that leaked it would emit it first.
            await project_external_schedule(
                db,
                project_id=project_id,
                name="not-ours",
                owner="celery-beat",
            )
            seeded = await create_reserved_schedule(
                db,
                project_id=project_id,
                name="late-add",
                planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
                kind="interval",
                expression="60s",
            )
            try:
                await asyncio.wait_for(reserved_seen.wait(), timeout=10.0)
            except TimeoutError:
                pytest.fail(
                    f"watch stream emitted no schedule change within 10s (frames={frames})",
                )
        finally:
            consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, grpc.aio.AioRpcError):
                await consume_task

        changes = [f for f in frames if isinstance(f, ScheduleChange)]
        # Emitting the foreign row as a schedule would put a celery-beat row
        # into the tick cache.
        assert [c.schedule.id for c in changes if c.schedule is not None] == [seeded.id]

        # And it is filtered by being conveyed as progress, not by being
        # dropped. The foreign row's own revision has to come back as a
        # checkpoint: the scheduler advances its cursor from what it is told,
        # so a silently skipped revision can only be passed by some later
        # frame, and a stream that ends before one arrives resumes from behind
        # it again on every reconnect.
        foreign_revision = min(
            revision for revision in await _foreign_revisions(db) if revision > baseline.watermark
        )
        checkpoints = {f.revision for f in frames if isinstance(f, ScannedThrough)}
        assert foreign_revision in checkpoints


# =====================================================================
# FireSchedule
# =====================================================================


class TestFireSchedule:
    @pytest.mark.asyncio
    async def test_tokenless_fire_is_refused_without_a_grant(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # The negative control for every current fire below. This is the same
        # call the others make, minus the control token, the prepared
        # transition and the epoch, and an activated brain must refuse it. If
        # this ever returns an acceptance, the helper that builds current fires
        # has stopped carrying authority and the rest of this class is testing
        # the 1.7 wire again.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="tokenless",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        slot = _utc(seeded.next_run_at)
        assert slot is not None
        before = await _cadence_state(db, seeded.id)

        result = await scheduler_client.fire_schedule(
            schedule_id=seeded.id,
            fire_id=derive_fire_id(seeded.id, slot),
            scheduled_for=slot,
            fired_at=datetime.now(UTC),
        )

        assert result.success is False
        assert result.disposition == "legacy_upgrade_required"
        assert result.error_code == "scheduler_upgrade_required"
        assert result.command_id is None
        assert await _cadence_state(db, seeded.id) == before
        assert await _command_rows(db) == []
        assert await _fire_rows(db) == []

    @pytest.mark.asyncio
    async def test_fire_with_no_agent_buffers(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # No agent online: the brain still commits the cadence transition (the
        # fire is what advances it) and parks the work in ``pending_fires`` for
        # the replay worker, rather than refusing the slot and leaving the
        # scheduler to re-offer it forever.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)  # deliberately no agent
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="orphan",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            schedule_name="orphan",
            engine="celery",
            project_id=project_id,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )

        assert result is not None
        assert result.success is True
        assert result.disposition == "accepted"
        assert result.buffered is True
        assert result.command_id is None
        assert result.error_code is None
        assert result.acceptance_revision > seeded.schedule_revision
        assert result.accepted_last_run_at == fire.slot
        assert result.accepted_next_run_at == fire.prepared.next_run_at

        # The cadence fence moved, committed by the fire itself.
        assert await _cadence_state(db, seeded.id) == (
            fire.slot,
            fire.prepared.next_run_at,
            1,
            fire.fire_id,
            result.acceptance_revision,
            seeded.control_token,
        )

        pending = await _pending_rows(db)
        assert len(pending) == 1
        assert pending[0].fire_id == fire.fire_id
        assert pending[0].receipt_control_token == seeded.control_token
        assert await _command_rows(db) == []

        fires = await _fire_rows(db)
        assert len(fires) == 1
        assert fires[0].fire_id == fire.fire_id
        assert fires[0].status == "buffered"
        assert fires[0].command_id is None
        assert fires[0].protocol_marker == 1
        assert fires[0].receipt_control_token == seeded.control_token

    @pytest.mark.asyncio
    async def test_fire_with_online_agent_creates_a_receipt_bound_command(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="targeted",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            schedule_name="targeted",
            engine="celery",
            project_id=project_id,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )

        assert result is not None
        assert result.success is True
        assert result.disposition == "accepted"
        assert result.buffered is False
        assert result.command_id is not None
        assert result.error_code is None
        assert result.live_control_token == seeded.control_token
        assert result.live_revision == result.acceptance_revision
        assert result.live_last_run_at == fire.slot
        assert result.live_next_run_at == fire.prepared.next_run_at

        assert await _pending_rows(db) == []
        commands = await _command_rows(db)
        assert len(commands) == 1
        command = commands[0]
        assert command.id == result.command_id
        # The receipt binding is the whole point of the current protocol: the
        # unit of work is keyed to the control generation that accepted it, so
        # a command minted under a superseded generation cannot be replayed
        # into a live one.
        assert command.schedule_receipt_control_token == seeded.control_token
        assert command.schedule_protocol_marker == 1
        assert command.idempotency_key == (
            f"schedule:{seeded.id}:fire:{fire.fire_id}:receipt:{seeded.control_token}"
        )
        assert command.payload["schedule_fire_id"] == str(fire.fire_id)
        assert command.payload["task_name"] == "tasks.t"

        fires = await _fire_rows(db)
        assert len(fires) == 1
        assert fires[0].fire_id == fire.fire_id
        assert fires[0].status == "accepted"
        assert fires[0].command_id == result.command_id
        assert fires[0].protocol_marker == 1

    @pytest.mark.asyncio
    async def test_replay_of_an_accepted_slot_returns_the_same_command(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # A response-loss retry: the scheduler re-sends the identical request
        # because it never saw the answer. The brain has to recognise its own
        # acceptance and return the same work, not accept the slot twice.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="replayed",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        first = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )
        after_first = await _cadence_state(db, seeded.id)

        # Same authority, same slot: byte-identical to the request above.
        second = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )

        assert first is not None
        assert second is not None
        assert second.disposition == "accepted"
        assert second.command_id == first.command_id
        assert len(await _command_rows(db)) == 1
        assert len(await _fire_rows(db)) == 1
        assert await _cadence_state(db, seeded.id) == after_first

    @pytest.mark.asyncio
    async def test_stale_authority_is_answered_with_a_refresh(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # Authority from before an accepted fire is stale. Re-offering the NEXT
        # slot under it must not be accepted: the brain answers with a refresh
        # so the scheduler re-reads rather than firing off an old cursor.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="stale",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        first = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        accepted = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=first.slot,
            prepared_fire=first.prepared,
            schedule_entry=first.entry,
        )
        assert accepted is not None
        assert accepted.disposition == "accepted"
        after_first = await _cadence_state(db, seeded.id)

        # The successor slot, offered under the pre-fire entry.
        successor = first.prepared.next_run_at
        assert successor is not None
        from z4j_scheduler.tick._prepared import PreparedFire

        stale_prepared = PreparedFire(
            scheduled_for=successor,
            next_run_at=successor + timedelta(hours=1),
        )
        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=successor,
            prepared_fire=stale_prepared,
            schedule_entry=first.entry,
        )

        assert result is not None
        assert result.success is False
        # Specifically stale control, not slot-resolved: the offered slot is
        # still ahead of the committed cursor, so what is wrong is the
        # authority the scheduler is holding, and the live values in the answer
        # are what it re-plans from.
        assert result.disposition == "stale_control_refresh"
        assert result.live_revision == accepted.acceptance_revision
        assert result.live_next_run_at == first.prepared.next_run_at
        assert await _cadence_state(db, seeded.id) == after_first
        assert len(await _command_rows(db)) == 1

    @pytest.mark.asyncio
    async def test_fire_rejects_unsupported_uuid_version(
        self,
        scheduler_client: BrainClient,
    ) -> None:
        # A fire_id whose UUID version is neither v4 (manual trigger) nor v5
        # (cadence) is rejected up front, so an out-of-protocol version cannot
        # be silently classified as manual by the ack path, which reads
        # version != 5 as manual.
        now = datetime.now(UTC)
        result = await scheduler_client.fire_schedule(
            schedule_id=uuid.uuid4(),
            fire_id=uuid.uuid1(),  # version 1 -- unsupported
            scheduled_for=now,
            fired_at=now,
        )
        assert result.error_code == "invalid_request"


# =====================================================================
# AdvanceScheduleCursor
# =====================================================================


class TestAdvanceScheduleCursor:
    @pytest.mark.asyncio
    async def test_zero_work_advance_commits_the_prepared_cursor(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # The skip path: a catch-up policy chose to fire nothing, but the
        # cursor still has to move durably or the same slots stay due forever.
        # It is a current-protocol RPC in its own right and carries the same
        # authority a fire does.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="skipper",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        transition = await fire_dispatcher.advance_cursor(
            entry=fire.entry,
            prepared=fire.prepared,
        )

        assert transition.disposition == "applied"
        assert transition.committed_next_run_at == fire.prepared.next_run_at
        assert transition.live_control_token == seeded.control_token
        assert transition.live_revision > seeded.schedule_revision

        row = await _schedule_row(db, seeded.id)
        assert _utc(row.next_run_at) == fire.prepared.next_run_at
        # A skip is not a run: no work was created and nothing was counted.
        assert row.total_runs == 0
        assert await _command_rows(db) == []
        assert await _pending_rows(db) == []
        assert await _fire_rows(db) == []


# =====================================================================
# AcknowledgeFireResult
# =====================================================================


class TestAcknowledgeFireResult:
    """The receipt reports the FireSchedule round trip, not task execution.

    Worth being precise about, because the name invites the other reading. The
    scheduler never learns whether the task ran: the agent reports that to the
    brain over its own connection. What the scheduler can report is that the
    fire it offered came back accepted, and on an activated schema that is all
    this RPC writes. Cadence progress was already committed by the fire itself.

    Fires here go through the raw client rather than the dispatcher, because
    the dispatcher sends this receipt for the caller (see
    ``test_dispatch_acknowledges_the_delivered_round_trip``) and a test about
    the RPC's own contract needs to be the one sending it.
    """

    @pytest.mark.asyncio
    async def test_dispatch_acknowledges_the_delivered_round_trip(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # Nothing else in the scheduler acknowledges a cadence fire, so this is
        # the only receipt a delivered current fire will ever get. It is sent
        # the moment the brain answers, which is exactly what it reports.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="auto-acked",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )

        assert result is not None
        assert result.command_id is not None
        acked = (await _fire_rows(db))[0]
        assert acked.scheduler_ack_status == "success"
        assert acked.scheduler_acknowledged_at is not None

    @pytest.mark.asyncio
    async def test_dispatch_leaves_a_buffered_fire_unacknowledged(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        # A buffered fire has no command to name, so the dispatcher sends no
        # receipt for it. The pairing matters: the auto-ack above is keyed on
        # having a command, not on the fire having succeeded.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)  # deliberately no agent
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="buffered-unacked",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )

        assert result is not None
        assert result.buffered is True
        assert result.command_id is None
        assert (await _fire_rows(db))[0].scheduler_ack_status is None

    @pytest.mark.asyncio
    async def test_ack_records_the_receipt_and_leaves_cadence_alone(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # An ack that also advanced ``last_run_at`` would double-count the slot
        # it is reporting on, because the fire already committed it.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="acked",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        result = await send_current_fire(scheduler_client, fire)
        assert result.disposition == "accepted"
        assert result.command_id is not None

        before_cadence = await _cadence_state(db, seeded.id)
        before_fire = (await _fire_rows(db))[0]
        assert before_fire.scheduler_ack_status is None
        assert before_fire.scheduler_acknowledged_at is None

        await scheduler_client.acknowledge_result(
            fire_id=fire.fire_id,
            command_id=result.command_id,
            status="success",
            new_task_id="celery-task-1",
        )

        after_fire = (await _fire_rows(db))[0]
        assert after_fire.scheduler_ack_status == "success"
        assert after_fire.scheduler_acknowledged_at is not None
        assert after_fire.scheduler_ack_task_id == "celery-task-1"
        assert after_fire.latency_ms is not None
        assert await _cadence_state(db, seeded.id) == before_cadence

    @pytest.mark.asyncio
    async def test_ack_of_a_buffered_fire_needs_no_command(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # A buffered fire correlates on the retained fire evidence instead of a
        # command. The scheduler cannot know which of the two the brain chose,
        # so one call shape has to reach both.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="buffered-ack",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        result = await send_current_fire(scheduler_client, fire)
        assert result.buffered is True
        assert result.command_id is None
        before_cadence = await _cadence_state(db, seeded.id)

        await scheduler_client.acknowledge_result(
            fire_id=fire.fire_id,
            command_id=None,
            status="success",
        )

        after_fire = (await _fire_rows(db))[0]
        assert after_fire.scheduler_ack_status == "success"
        assert after_fire.scheduler_acknowledged_at is not None
        assert await _cadence_state(db, seeded.id) == before_cadence

    @pytest.mark.asyncio
    async def test_failed_ack_is_recorded_without_touching_cadence(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # A round trip the scheduler could not complete is history too, and the
        # slot stays consumed: the brain committed it at acceptance, and
        # rolling it back on a failure report would re-offer a slot whose task
        # may already be running.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="failed-ack",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        result = await send_current_fire(scheduler_client, fire)
        before_cadence = await _cadence_state(db, seeded.id)

        await scheduler_client.acknowledge_result(
            fire_id=fire.fire_id,
            command_id=result.command_id,
            status="failed",
            error="worker refused the task",
        )

        after_fire = (await _fire_rows(db))[0]
        assert after_fire.scheduler_ack_status == "failed"
        assert after_fire.scheduler_ack_error_message == "worker refused the task"
        assert await _cadence_state(db, seeded.id) == before_cadence

    @pytest.mark.asyncio
    async def test_a_recorded_success_is_not_downgraded_by_a_later_failure(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # Receipts are retried blindly across scheduler restarts, so they
        # arrive out of order and more than once. A completed round trip stays
        # completed: a stale failure report cannot rewrite history into
        # something that never happened.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="latched",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        result = await send_current_fire(scheduler_client, fire)

        await scheduler_client.acknowledge_result(
            fire_id=fire.fire_id,
            command_id=result.command_id,
            status="success",
        )
        await scheduler_client.acknowledge_result(
            fire_id=fire.fire_id,
            command_id=result.command_id,
            status="failed",
            error="stale retry",
        )

        after_fire = (await _fire_rows(db))[0]
        assert after_fire.scheduler_ack_status == "success"
        assert after_fire.scheduler_ack_error_message is None

    @pytest.mark.asyncio
    async def test_ack_for_an_unknown_fire_is_a_no_op(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # The scheduler re-drives unacknowledged fires after a restart and
        # cannot always tell which ones the brain retained. An ack it has no
        # record of has to be absorbed, not turned into a hard error the
        # scheduler would retry forever, and it must not write anything.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="untouched",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        before = await _cadence_state(db, seeded.id)

        await scheduler_client.acknowledge_result(
            fire_id=uuid.uuid5(uuid.NAMESPACE_OID, str(uuid.uuid4())),
            command_id=None,
            status="success",
        )

        assert await _cadence_state(db, seeded.id) == before
        assert await _fire_rows(db) == []

    @pytest.mark.asyncio
    async def test_ack_naming_a_foreign_command_is_refused(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        # Correlation is checked, not trusted. An ack that names a command
        # which does not identify the fire is refused at the RPC rather than
        # silently applied to whichever row the fire_id happened to match.
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="mismatched",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        await send_current_fire(scheduler_client, fire)

        with pytest.raises(grpc.aio.AioRpcError) as excinfo:
            await scheduler_client.acknowledge_result(
                fire_id=fire.fire_id,
                command_id=uuid.uuid4(),
                status="success",
            )
        assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION

        after_fire = (await _fire_rows(db))[0]
        assert after_fire.scheduler_ack_status is None


# =====================================================================
# Manual trigger
# =====================================================================


class TestManualTrigger:
    """The scheduler-side operator trigger, against a current brain.

    ``FireDispatcher.trigger_now`` is reached from the scheduler's own
    TriggerSchedule service, which a brain calls only when an operator wired
    ``scheduler_trigger_url``. A current brain does not: it fires operator
    triggers itself, because it is the only side that can see a hold. The
    current FireSchedule wire therefore carries cadence acceptances only, and
    an attributed extra fire is not one.

    Both halves of that are asserted here, and they have to be separate tests.
    The scheduler settles the refusal from the entry the snapshot gave it and
    never calls, so a test that only drove the dispatcher would stop proving
    anything about the brain, and the guard it is trusting could be deleted
    without a single test going red.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attributed", [True, False])
    async def test_a_trigger_never_reaches_the_brain(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
        attributed: bool,
    ) -> None:
        """The production path: the entry comes off the wire, as it does live.

        The unattributed case is here because dropping the attribution must not
        open a side door. A manual trigger mints a ``uuid4`` whichever way, and
        it is the absence of a cadence slot, not the attribution alone, that
        makes it uncarriable.
        """
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="triggered" if attributed else "anon-triggered",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        before = await _cadence_state(db, seeded.id)
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )

        result = await fire_dispatcher.trigger_now(
            schedule_id=seeded.id,
            schedule_entry=fire.entry,
            triggered_by_user_id=str(uuid.uuid4()) if attributed else "",
        )

        assert result.success is False
        # The brain was never asked, so there is no answer of its to report.
        assert result.disposition is None
        # "upgrade the scheduler" is the opposite of the truth here: no version
        # of the scheduler can put an operator's extra fire on this wire. The
        # operator is told the one action that resolves it instead.
        assert result.error_code == "manual_trigger_not_accepted"
        assert "scheduler_trigger_url" in (result.error_message or "")
        assert await _cadence_state(db, seeded.id) == before
        assert await _command_rows(db) == []
        assert await _fire_rows(db) == []
        assert await _pending_rows(db) == []

    @pytest.mark.asyncio
    async def test_a_granted_tokenless_wire_still_refuses_the_trigger_shape(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
    ) -> None:
        """The guard the local refusal is an optimisation of, driven directly.

        A peer that has not learned to refuse locally (an older scheduler, or a
        caller that is not this scheduler at all) must still be turned away. On
        an ungranted schedule that proves nothing, because every tokenless fire
        is refused regardless of its shape, so the grant is taken first and the
        two calls differ only in shape:

        a cadence fire, slot-derived and unattributed, is ACCEPTED, which is
        what says the wire is genuinely open;
        an operator trigger, ``uuid4`` and attributed, is REFUSED on the same
        open wire, leaving the cadence and every durable row exactly as the
        accepted fire left them.

        Neither call can be dropped. Without the acceptance the refusal is
        indistinguishable from a closed wire; without the refusal the grant
        would be free to carry an extra fire it was never asked to authorise.
        """
        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="granted",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        await grant_legacy_fire(
            db,
            project_id=project_id,
            schedule_id=seeded.id,
            control_token=seeded.control_token,
        )
        slot = _utc(seeded.next_run_at)
        assert slot is not None

        cadence = await scheduler_client.fire_schedule(
            schedule_id=seeded.id,
            fire_id=derive_fire_id(seeded.id, slot),
            scheduled_for=slot,
            fired_at=datetime.now(UTC),
        )
        assert cadence.success is True, (
            "the grant did not open the tokenless wire, so the refusal below "
            "would prove nothing about the shape being sent"
        )
        after_cadence = await _cadence_state(db, seeded.id)
        commands_after_cadence = [row.id for row in await _command_rows(db)]
        fires_after_cadence = [row.fire_id for row in await _fire_rows(db)]

        trigger = await scheduler_client.fire_schedule(
            schedule_id=seeded.id,
            fire_id=uuid.uuid4(),
            scheduled_for=datetime.now(UTC),
            fired_at=datetime.now(UTC),
            triggered_by_user_id=str(uuid.uuid4()),
        )

        assert trigger.success is False
        assert trigger.disposition == "legacy_upgrade_required"
        assert trigger.command_id is None
        assert await _cadence_state(db, seeded.id) == after_cadence
        assert [row.id for row in await _command_rows(db)] == commands_after_cadence
        assert [row.fire_id for row in await _fire_rows(db)] == fires_after_cadence
        assert await _pending_rows(db) == []


# =====================================================================
# Hold and release
# =====================================================================


class _AlwaysLeader:
    """This process leads every project, as a single-instance deployment does."""

    def is_leader(self, _project_id: uuid.UUID) -> bool:
        return True


async def _resync_cache(
    cache: ScheduleCache,
    client: BrainClient,
    project_id: uuid.UUID,
) -> None:
    """Install the brain's live state the way a full re-sync does."""

    await cache.apply_completed_snapshot(
        await client.list_schedule_snapshot(project_id),
    )


class TestHoldAndRelease:
    """A hold that lands while a fire is in flight, and the release after it.

    Every other test in this file drives one RPC. This one drives the tick
    engine, the schedule cache, the brain's own hold writer and the wire
    projection together, because the failure it exists to catch is not visible
    in any one of them: each part behaves correctly on its own, and the
    schedule still never ticks again.
    """

    @pytest.mark.asyncio
    async def test_a_released_hold_lets_the_schedule_tick_again(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        """The whole round trip: due, held mid-flight, released, fires.

        A hold keeps the same control token on purpose, because holding a
        schedule does not redefine it. So a scheduler that stops itself on the
        refusal and waits for a new token waits forever: the release carries
        the token it already has. What moves is the revision, and the only
        remedy for the token-keyed version of this was restarting the process,
        which is not something an operator would ever think to try against a
        schedule that looks enabled in the dashboard.
        """
        from z4j_brain.persistence.repositories.schedule_control import (
            ScheduleControlRepository,
        )
        from z4j_scheduler.tick.engine import TickEngine

        _server, _port, db, _dispatcher = brain_grpc
        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="held",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
            catch_up="fire_one_missed",
        )

        cache = ScheduleCache()
        await _resync_cache(cache, scheduler_client, project_id)
        engine = TickEngine(
            cache=cache,
            leader_gate=_AlwaysLeader(),
            dispatcher=fire_dispatcher,
            max_sleep_seconds=0.01,
        )

        async def hold(*, paused: bool) -> None:
            async with db.session(write=True) as session:
                transition = await ScheduleControlRepository(session).set_paused(
                    project_id=project_id,
                    schedule_id=seeded.id,
                    paused=paused,
                    occurred_at=datetime.now(UTC),
                )
                assert transition.outcome == "applied"
                await session.commit()

        # The race: the hold commits after this scheduler read its snapshot and
        # before its fire lands, which is the whole window an operator holding
        # a schedule during an incident is aiming at.
        await hold(paused=True)
        await engine._iteration()

        assert await _command_rows(db) == [], "a held schedule must not fire"
        assert await _fire_rows(db) == []
        entry = await cache.get(seeded.id)
        assert entry is not None
        assert entry.is_enabled is False

        # The hold itself reaches the cache. Still no fire, and now for the
        # brain's own reason rather than this scheduler's.
        await _resync_cache(cache, scheduler_client, project_id)
        await engine._iteration()
        assert await _command_rows(db) == []

        await hold(paused=False)
        await _resync_cache(cache, scheduler_client, project_id)
        released = await cache.get(seeded.id)
        assert released is not None
        assert released.is_enabled is True, (
            "the release carries the same control token as the hold, so a stop "
            "keyed on the token clamps it here and only a restart clears it"
        )

        await engine._iteration()

        commands = await _command_rows(db)
        assert len(commands) == 1
        fires = await _fire_rows(db)
        assert len(fires) == 1
        assert fires[0].status == "accepted"
        row = await _schedule_row(db, seeded.id)
        assert row.last_run_at is not None
        assert row.total_runs == 1


# =====================================================================
# Whole flow
# =====================================================================


class TestEndToEndWiring:
    """Boot, observe, fire, acknowledge - in the order a scheduler does it."""

    @pytest.mark.asyncio
    async def test_full_current_protocol_flow(
        self,
        brain_grpc,
        scheduler_client: BrainClient,
        fire_dispatcher: FireDispatcher,
    ) -> None:
        _server, _port, db, _dispatcher = brain_grpc

        # 1. Negotiate, exactly as SchedulerApp.start does.
        offered = current_capabilities(
            cadence_runtime_fingerprint=cadence_runtime_fingerprint(),
        )
        info = await scheduler_client.ping()
        selected = await scheduler_client.negotiate_protocol(offered)
        require_exact_current(
            selected=selected,
            expected=offered,
            ping_protocol_epoch=info.scheduler_protocol_epoch,
        )

        project_id = await seed_project(db)
        await seed_online_agent(db, project_id=project_id)
        seeded = await create_reserved_schedule(
            db,
            project_id=project_id,
            name="full-flow",
            planning_at=datetime.now(UTC) - _PLANNING_BACKDATE,
        )
        await project_external_schedule(
            db,
            project_id=project_id,
            name="foreign",
            owner="celery-beat",
        )

        # 2. Boot snapshot: our row, and only our row.
        snapshot = await scheduler_client.list_schedule_snapshot(project_id)
        assert [row.id for row in snapshot.rows] == [seeded.id]

        # 3. Fire the due slot on the authority the snapshot carried.
        fire = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=fire.slot,
            schedule_name="full-flow",
            engine="celery",
            project_id=project_id,
            prepared_fire=fire.prepared,
            schedule_entry=fire.entry,
        )
        assert result is not None
        assert result.disposition == "accepted"
        assert result.command_id is not None
        receipt = (await _fire_rows(db))[0]
        assert receipt.scheduler_ack_status == "success"

        # 4. The next snapshot shows the advance, so the next tick plans from
        # the brain's cursor rather than from anything held locally.
        advanced = await scheduler_client.list_schedule_snapshot(project_id)
        assert advanced.rows[0].last_fire_at == fire.slot
        assert advanced.rows[0].next_fire_at == fire.prepared.next_run_at
        assert advanced.rows[0].schedule_revision == result.acceptance_revision

        # 5. And the second slot is fireable on refreshed authority, which is
        # what proves the loop closes rather than firing once.
        second = await prepare_current_fire(
            scheduler_client,
            project_id=project_id,
            schedule_id=seeded.id,
        )
        assert second.slot == fire.prepared.next_run_at
        second_result = await fire_dispatcher.dispatch(
            schedule_id=seeded.id,
            scheduled_for=second.slot,
            prepared_fire=second.prepared,
            schedule_entry=second.entry,
        )
        assert second_result is not None
        assert second_result.disposition == "accepted"
        assert second_result.command_id != result.command_id
        row = await _schedule_row(db, seeded.id)
        assert row.total_runs == 2
