"""End-to-end test for TriggerSchedule (brain → scheduler).

Spins up the scheduler-side TriggerGrpcServer with real mTLS,
connects with the brain-side TriggerScheduleClient, and verifies:

1. A valid TriggerSchedule call resolves to the cached schedule
   and returns a command_id.
2. An unknown schedule_id returns ``not_in_cache`` cleanly (not a
   gRPC exception).
3. A disabled schedule still accepts a manual trigger.

The scheduler's FireDispatcher is wired to a fake BrainClient so
we don't need to bring brain-side gRPC up for this test - that's
covered by the existing ``test_brain_scheduler_e2e.py``.
"""

from __future__ import annotations

import asyncio
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("grpc")
pytest.importorskip("cryptography")
pytest.importorskip("z4j_brain")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert
from z4j_brain.scheduler_grpc.trigger_client import (
    TriggerScheduleClient,
)
from z4j_brain.settings import Settings as BrainSettings
from z4j_scheduler.dispatch.fire import FireDispatcher
from z4j_scheduler.settings import Settings as SchedulerSettings
from z4j_scheduler.storage._models import FireResult
from z4j_scheduler.storage.cache import ScheduleCache
from z4j_scheduler.tick._entry import ScheduleEntry
from z4j_scheduler.trigger_grpc.server import TriggerGrpcServer

# =====================================================================
# Cert helpers
# =====================================================================


def _self_signed_ca() -> tuple[bytes, bytes]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "test-ca-trigger")],
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
    """Mint a serverAuth cert for localhost (for the scheduler side)."""
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
# Fakes
# =====================================================================


class _FakeBrainClient:
    """Stand-in for BrainClient that records fire_schedule calls.

    Returns a successful FireResult so the dispatcher's success path
    runs end to end. Tests can inspect ``calls`` to verify the
    trigger handler called through.
    """

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.block_fire = False
        self.fire_started = asyncio.Event()
        self.release_fire = asyncio.Event()

    async def fire_schedule(self, **kwargs):
        self.calls.append(kwargs)
        self.fire_started.set()
        if self.block_fire:
            await self.release_fire.wait()
        return FireResult(
            command_id=uuid.uuid4(),
            error_code=None,
            error_message=None,
            buffered=False,
        )

    async def acknowledge_result(self, **kwargs):
        # Best-effort ack the dispatcher fires after success.
        return None


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def cert_bundle(tmp_path: Path) -> dict:
    ca_cert, ca_key = _self_signed_ca()
    server_cert, server_key = _server_cert(ca_cert, ca_key)
    client_cert, client_key = mint_scheduler_cert(
        name="brain-trigger",
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
async def trigger_server(cert_bundle: dict):
    """Boot the scheduler-side TriggerGrpcServer on an ephemeral port."""
    cache = ScheduleCache()
    fake_client = _FakeBrainClient()

    sched_settings = SchedulerSettings(
        brain_grpc_url="127.0.0.1:1",
        brain_rest_url="http://127.0.0.1:7700",
        tls_cert=cert_bundle["client.crt"],  # not actually used here
        tls_key=cert_bundle["client.key"],
        tls_ca=cert_bundle["ca.crt"],
        trigger_grpc_enabled=True,
        trigger_grpc_bind_host="127.0.0.1",
        trigger_grpc_bind_port=0,
        trigger_grpc_tls_cert=cert_bundle["server.crt"],
        trigger_grpc_tls_key=cert_bundle["server.key"],
        trigger_grpc_tls_ca=cert_bundle["ca.crt"],
        trigger_grpc_allowed_cns=[],
    )
    dispatcher = FireDispatcher(client=fake_client, settings=sched_settings)
    from z4j_scheduler.leader import SingleInstanceLeaderGate

    server = TriggerGrpcServer(
        settings=sched_settings,
        cache=cache,
        dispatcher=dispatcher,
        leader_gate=SingleInstanceLeaderGate(),
    )
    await server.start()
    try:
        yield server, server.bound_port, cache, fake_client
    finally:
        await server.stop()


@pytest.fixture
async def trigger_client(trigger_server, cert_bundle: dict):
    """Brain-side client pointed at the scheduler fixture."""
    _server, port, _cache, _fake = trigger_server
    brain_settings = BrainSettings(
        database_url="sqlite+aiosqlite:///:memory:",
        secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        environment="dev",
        log_json=False,
        scheduler_trigger_url=f"127.0.0.1:{port}",
        scheduler_trigger_tls_cert=str(cert_bundle["client.crt"]),
        scheduler_trigger_tls_key=str(cert_bundle["client.key"]),
        scheduler_trigger_tls_ca=str(cert_bundle["ca.crt"]),
    )
    client = TriggerScheduleClient(settings=brain_settings)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


# =====================================================================
# Tests
# =====================================================================


def _seed_entry(cache: ScheduleCache, *, enabled: bool = True) -> uuid.UUID:
    schedule_id = uuid.uuid4()
    project_id = uuid.uuid4()
    entry = ScheduleEntry(
        id=schedule_id,
        project_id=project_id,
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        is_enabled=enabled,
        catch_up="skip",
        anchor_at=datetime.now(UTC),
        last_fire_at=None,
    )
    return schedule_id, entry


class TestTriggerHappyPath:
    @pytest.mark.asyncio
    async def test_returns_command_id(
        self,
        trigger_server,
        trigger_client,
    ) -> None:
        _server, _port, cache, fake = trigger_server
        schedule_id, entry = _seed_entry(cache)
        await cache.upsert(entry)

        response = await trigger_client.trigger(
            schedule_id=schedule_id,
            user_id=uuid.uuid4(),
            idempotency_key="test-1",
        )
        assert response.command_id
        assert not response.error_code
        # And the dispatcher actually called brain.fire_schedule.
        assert len(fake.calls) == 1
        assert fake.calls[0]["schedule_id"] == schedule_id


class TestTriggerNotInCache:
    @pytest.mark.asyncio
    async def test_unknown_schedule_returns_clean_error(
        self,
        trigger_client,
    ) -> None:
        response = await trigger_client.trigger(
            schedule_id=uuid.uuid4(),
            user_id=None,
        )
        assert response.command_id == ""
        assert response.error_code == "not_in_cache"


class TestTriggerDisabled:
    @pytest.mark.asyncio
    async def test_a_disabled_schedule_still_triggers(
        self,
        trigger_server,
        trigger_client,
    ) -> None:
        """Disabling retires the cadence, it does not withdraw the button.

        "Off the timer, I will run it by hand when I need it" is a workflow, and
        this path is how an operator acts on it. The holds that genuinely stop a
        fire, a pause and a quarantine, are refused by the brain before the
        request reaches here, so a refusal at this layer would only ever remove
        a capability rather than protect anything.
        """
        _server, _port, cache, fake = trigger_server
        schedule_id, entry = _seed_entry(cache, enabled=False)
        await cache.upsert(entry)

        response = await trigger_client.trigger(
            schedule_id=schedule_id,
            user_id=None,
        )
        assert response.command_id
        assert not response.error_code
        assert len(fake.calls) == 1
        assert fake.calls[0]["schedule_id"] == schedule_id


class TestServerStop:
    @pytest.mark.asyncio
    async def test_stop_drains_inflight_calls(
        self,
        trigger_server,
        trigger_client,
    ) -> None:
        server, _port, cache, fake = trigger_server
        schedule_id, entry = _seed_entry(cache)
        await cache.upsert(entry)
        fake.block_fire = True

        request_task = asyncio.create_task(
            trigger_client.trigger(
                schedule_id=schedule_id,
                user_id=None,
            ),
        )
        stop_task: asyncio.Task | None = None
        try:
            await asyncio.wait_for(fake.fire_started.wait(), timeout=5.0)
            stop_task = asyncio.create_task(server.stop())

            # Give stop() a scheduling turn. It must wait for the live handler,
            # rather than cancelling the RPC or returning before it drains.
            await asyncio.sleep(0)
            assert not stop_task.done()

            fake.release_fire.set()
            response = await asyncio.wait_for(request_task, timeout=5.0)
            await asyncio.wait_for(stop_task, timeout=5.0)
            assert response.command_id
            assert len(fake.calls) == 1
        finally:
            fake.release_fire.set()
            if not request_task.done():
                request_task.cancel()
            await asyncio.gather(request_task, return_exceptions=True)
            if stop_task is not None and not stop_task.done():
                await asyncio.wait_for(stop_task, timeout=5.0)
