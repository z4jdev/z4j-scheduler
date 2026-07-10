"""Fire-to-dispatch latency benchmark, closes the §23 unmeasured target.

§23 lists "Fire-to-broker latency p50/p99 < 100 / 250 ms." We
broke that target into two halves:

- **scheduler-controlled half** (this bench): time from
  ``FireDispatcher.fire(...)`` returning to brain's
  ``CommandDispatcher`` having inserted the Command row + acked.
  This is the gRPC + brain-side handler cost, which z4j-scheduler
  ENTIRELY controls.
- **agent-controlled half**: time from brain dispatching to the
  agent's ``engine.submit_task()`` returning. This depends on
  WebSocket latency, agent-process load, broker response time. We
  can't bench it without a real broker + worker fixture; operators
  measure it via the existing ``z4j_scheduler_fire_latency_seconds``
  Prometheus histogram in production.

The full §23 budget covers BOTH halves. This bench reports the
scheduler-controlled half in isolation so operators can subtract
from their production observation to estimate the agent-side cost.

Running the bench:

    pip install z4j-brain[scheduler-grpc] cryptography
    cd packages/z4j-scheduler
    python -m tests.benchmarks.bench_fire_to_dispatch

Output: a JSON report + a printable summary. JSON path is
``tests/benchmarks/results/fire_to_dispatch.json`` by default.

Skips gracefully when ``z4j-brain`` or ``cryptography`` is not
importable (the same gate as the integration tests).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import statistics
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# Brain-side bench harness; skip if the optional deps are missing.
try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import StaticPool
    from z4j_brain.domain.audit_service import AuditService
    from z4j_brain.domain.command_dispatcher import CommandDispatcher
    from z4j_brain.persistence import models  # noqa: F401
    from z4j_brain.persistence.base import Base
    from z4j_brain.persistence.database import DatabaseManager
    from z4j_brain.persistence.enums import ScheduleKind
    from z4j_brain.persistence.models import Project, Schedule
    from z4j_brain.scheduler_grpc.auth import mint_scheduler_cert
    from z4j_brain.scheduler_grpc.server import SchedulerGrpcServer
    from z4j_brain.settings import Settings as BrainSettings
    from z4j_brain.websocket.registry import LocalRegistry
    from z4j_scheduler.dispatch.fire import FireDispatcher
    from z4j_scheduler.settings import Settings as SchedulerSettings
    from z4j_scheduler.storage.brain_client import BrainClient

    _DEPS_AVAILABLE = True
    _IMPORT_ERROR: str | None = None
except Exception as exc:
    _DEPS_AVAILABLE = False
    _IMPORT_ERROR = str(exc)


# =====================================================================
# Cert minting helpers (mirrors test_brain_scheduler_e2e.py)
# =====================================================================


def _mint_pki(out_dir: Path) -> dict[str, Path]:
    """Self-signed CA + server cert (CN=localhost) + scheduler client cert."""
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "bench-ca")],
    )
    now = datetime.now(UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(8), "big"))
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    ca_key_pem = ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(8), "big"))
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=2))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
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
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )
    server_pem = server_cert.public_bytes(serialization.Encoding.PEM)
    server_key_pem = server_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    client_pem, client_key_pem = mint_scheduler_cert(
        name="bench-scheduler",
        ca_cert_pem=ca_pem,
        ca_key_pem=ca_key_pem,
        validity_days=1,
    )

    paths = {}
    for name, blob in (
        ("ca.crt", ca_pem),
        ("server.crt", server_pem),
        ("server.key", server_key_pem),
        ("client.crt", client_pem),
        ("client.key", client_key_pem),
    ):
        path = out_dir / name
        path.write_bytes(blob)
        paths[name] = path
    return paths


# =====================================================================
# Bench
# =====================================================================


async def _noop_deliver(*_args, **_kwargs) -> bool:
    """Dispatcher's local-delivery callback. The bench doesn't care
    whether the command ever lands on a real agent; it measures up
    to the point where brain has committed the Command row."""
    return False


async def _bench(iterations: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="z4j-fire-bench-") as tmp:
        certs = _mint_pki(Path(tmp))

        # Brain-side bootstrap: in-memory SQLite + gRPC server on
        # ephemeral port.
        engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        brain_settings = BrainSettings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
            session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
            environment="dev",
            log_json=False,
            registry_backend="local",
            scheduler_grpc_enabled=True,
            scheduler_grpc_bind_host="127.0.0.1",
            scheduler_grpc_bind_port=0,
            scheduler_grpc_tls_cert=str(certs["server.crt"]),
            scheduler_grpc_tls_key=str(certs["server.key"]),
            scheduler_grpc_tls_ca=str(certs["ca.crt"]),
            scheduler_grpc_allowed_cns=[],
        )

        db = DatabaseManager(engine)
        audit = AuditService(brain_settings)
        registry = LocalRegistry(deliver_local=_noop_deliver)
        cmd_dispatcher = CommandDispatcher(
            settings=brain_settings,
            registry=registry,
            audit=audit,
        )

        server = SchedulerGrpcServer(
            settings=brain_settings,
            db=db,
            command_dispatcher=cmd_dispatcher,
            audit_service=audit,
        )
        await server.start()
        try:
            # Seed a project + schedule the scheduler can fire.
            project_id = uuid.uuid4()
            schedule_id = uuid.uuid4()
            async with db.session() as s:
                s.add(Project(id=project_id, slug="bench", name="Bench"))
                s.add(
                    Schedule(
                        id=schedule_id,
                        project_id=project_id,
                        engine="celery",
                        scheduler="z4j-scheduler",
                        name="bench-sched",
                        task_name="app.bench",
                        kind=ScheduleKind.CRON,
                        expression="* * * * *",
                        timezone="UTC",
                        args=[],
                        kwargs={},
                        is_enabled=True,
                    )
                )
                await s.commit()

            # Scheduler-side bootstrap.
            sched_settings = SchedulerSettings(
                brain_grpc_url=f"127.0.0.1:{server.bound_port}",
                brain_rest_url="http://127.0.0.1:7700",
                tls_cert=certs["client.crt"],
                tls_key=certs["client.key"],
                tls_ca=certs["ca.crt"],
            )
            client = BrainClient(sched_settings)
            await client.connect()
            try:
                dispatcher = FireDispatcher(
                    client=client,
                    settings=sched_settings,
                )

                # Warm up - first 5 calls amortize gRPC channel + DB
                # connection setup; we don't want them in the percentile.
                for _ in range(5):
                    await dispatcher.dispatch(
                        schedule_id=schedule_id,
                        scheduled_for=datetime.now(UTC),
                        schedule_name="bench-sched",
                    )

                # Measured pass.
                latencies_ms: list[float] = []
                for _ in range(iterations):
                    t0 = time.perf_counter()
                    await dispatcher.dispatch(
                        schedule_id=schedule_id,
                        scheduled_for=datetime.now(UTC),
                        schedule_name="bench-sched",
                    )
                    latencies_ms.append((time.perf_counter() - t0) * 1000)
            finally:
                await client.close()
        finally:
            await server.stop()
            await engine.dispose()

    latencies_ms.sort()
    return {
        "iterations": iterations,
        "p50_ms": round(statistics.median(latencies_ms), 2),
        "p90_ms": round(_quantile(latencies_ms, 0.90), 2),
        "p99_ms": round(_quantile(latencies_ms, 0.99), 2),
        "max_ms": round(max(latencies_ms), 2),
        "mean_ms": round(statistics.mean(latencies_ms), 2),
    }


def _quantile(samples: list[float], q: float) -> float:
    if not samples:
        return 0.0
    idx = max(0, min(len(samples) - 1, round(q * (len(samples) - 1))))
    return samples[idx]


def render_summary(report: dict[str, Any]) -> str:
    lines = [
        "=" * 70,
        "Fire-to-dispatch latency (scheduler-controlled half of §23)",
        "=" * 70,
        f"Generated:  {report['generated']}",
        f"Iterations: {report['result']['iterations']}",
        "",
        "Measures: scheduler.FireDispatcher.fire() round-trip via",
        "          gRPC -> brain -> Command row inserted -> ack",
        "          (does NOT include agent + broker time)",
        "",
        f"  p50:  {report['result']['p50_ms']:.2f} ms  (target: <100 ms incl. agent)",
        f"  p90:  {report['result']['p90_ms']:.2f} ms",
        f"  p99:  {report['result']['p99_ms']:.2f} ms  (target: <250 ms incl. agent)",
        f"  max:  {report['result']['max_ms']:.2f} ms",
        f"  mean: {report['result']['mean_ms']:.2f} ms",
        "",
        "Operators: subtract these numbers from your production",
        "z4j_scheduler_fire_latency_seconds histogram p50/p99 to",
        "estimate the agent-side cost of your specific deployment.",
        "=" * 70,
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--iterations",
        type=int,
        default=200,
        help="Number of FireSchedule calls to time (default 200).",
    )
    parser.add_argument(
        "--out",
        default="tests/benchmarks/results/fire_to_dispatch.json",
        help="Path to write the JSON report.",
    )
    args = parser.parse_args()

    if not _DEPS_AVAILABLE:
        print(
            f"[skip] required deps not importable: {_IMPORT_ERROR}\n"
            f"  Install: pip install z4j-brain[scheduler-grpc] cryptography",
            file=sys.stderr,
        )
        return 1

    print("Running fire-to-dispatch bench...", file=sys.stderr)
    result = asyncio.run(_bench(args.iterations))

    report = {
        "generated": datetime.now(UTC).isoformat(),
        "result": result,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\nJSON report: {out_path}", file=sys.stderr)
    print()
    print(render_summary(report))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["main", "render_summary"]
