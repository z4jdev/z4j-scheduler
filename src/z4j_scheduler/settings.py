"""z4j-scheduler runtime configuration via :mod:`pydantic_settings`.

Twelve-factor: every value is sourced from an environment variable
prefixed ``Z4J_SCHEDULER_`` or, in development, from a ``.env`` file
at the process working directory. Missing required values cause
startup to fail fast with a Pydantic ``ValidationError``.

The full env-var reference is in
``docs/SCHEDULER.md §20``. This module is the canonical
implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_instance_id() -> str:
    """Hostname-based default instance id.

    Lazy ``socket`` import keeps the module import cheap and lets
    tests stub this function without depending on socket internals.
    """
    import socket  # noqa: PLC0415  - lazy on purpose, see docstring

    return socket.gethostname()


class Settings(BaseSettings):
    """Resolved scheduler configuration.

    See ``docs/SCHEDULER.md §20`` for the canonical operator-facing
    documentation of every field.
    """

    model_config = SettingsConfigDict(
        env_prefix="Z4J_SCHEDULER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    # ------------------------------------------------------------------
    # Required - brain endpoints
    # ------------------------------------------------------------------
    brain_grpc_url: str = Field(
        ...,
        description="brain gRPC endpoint, e.g. 'brain:7701'",
    )
    brain_rest_url: str = Field(
        ...,
        description="brain REST endpoint, e.g. 'http://brain:7700'",
    )

    # ------------------------------------------------------------------
    # Required - mTLS
    # ------------------------------------------------------------------
    tls_cert: Path = Field(..., description="mTLS client cert for gRPC")
    tls_key: Path = Field(..., description="mTLS client key for gRPC")
    tls_ca: Path = Field(..., description="mTLS CA for verifying brain server cert")

    # ------------------------------------------------------------------
    # Required for HA - Postgres for advisory lock
    # ------------------------------------------------------------------
    database_url: str | None = Field(
        default=None,
        description=(
            "Postgres URL for advisory lock; same DB as brain. "
            "Required when running multiple instances for HA; "
            "single-instance deployments may omit (no leader gate)."
        ),
    )

    # ------------------------------------------------------------------
    # Identity + scope
    # ------------------------------------------------------------------
    instance_id: str = Field(
        default_factory=_default_instance_id,
        description="Unique identifier for this instance (audit log)",
    )
    projects: str = Field(
        default="*",
        description=(
            "Comma-separated list of project slugs to serve, or '*' "
            "for all projects this scheduler is enrolled with."
        ),
    )

    # ------------------------------------------------------------------
    # API server (operational endpoints)
    # ------------------------------------------------------------------
    # Binding 0.0.0.0 is intentional for an infrastructure service
    # that operators expose via reverse proxy / service mesh / k8s
    # Service. Operators who want to bind loopback override via the
    # Z4J_SCHEDULER_BIND_HOST env var.
    bind_host: str = "0.0.0.0"  # noqa: S104  - intentional, see comment
    bind_port: int = Field(default=7800, ge=1, le=65535)

    # ------------------------------------------------------------------
    # Tick + leader behavior
    # ------------------------------------------------------------------
    leader_poll_interval_seconds: int = Field(default=2, ge=1, le=60)
    reconcile_interval_seconds: int = Field(default=900, ge=60, le=86_400)

    # ------------------------------------------------------------------
    # Leader election (Phase 2)
    # ------------------------------------------------------------------
    #: Backend to use for the LeaderGate.
    #:
    #: - ``single`` (default): always-true gate; right for solo
    #:   deployments where the operator runs exactly one scheduler
    #:   process.
    #: - ``postgres``: HA via Postgres advisory lock. Multiple
    #:   instances race; the lock holder is leader. Requires
    #:   ``leader_pg_dsn`` to be set.
    leader_backend: Literal["single", "postgres", "postgres_per_project"] = (
        "single"
    )
    #: Postgres DSN used by the ``postgres`` leader backend. Format:
    #: ``postgres://user:pass@host:port/db``. Typically points at the
    #: brain's own database (the scheduler doesn't need its own DB).
    leader_pg_dsn: SecretStr | None = None
    #: Free-form namespace identifying the scheduler cluster. Hashed
    #: to a 63-bit advisory-lock key so two clusters running against
    #: the same Postgres can pick distinct namespaces and not
    #: collide. The default is fine when running a single cluster.
    leader_namespace: str = Field(
        default="z4j-scheduler-global",
        max_length=200,
    )
    #: Heartbeat cadence for the leader's connection-liveness probe
    #: AND the standby's acquire retry interval. Lower values mean
    #: faster failover at the cost of more DB round-trips. The
    #: default 2s gives 1-3s failover under clean leader death.
    leader_heartbeat_seconds: float = Field(
        default=2.0, ge=0.5, le=60.0,
    )

    # ------------------------------------------------------------------
    # TriggerSchedule gRPC server (Phase 2, reverse direction)
    # ------------------------------------------------------------------
    #: Off by default. Operators opt in once they want the
    #: dashboard's "fire now" button to flow through the scheduler
    #: rather than have brain dispatch directly. When disabled the
    #: scheduler does not bind a server port and brain falls back
    #: to its own direct-dispatch path on trigger-now.
    trigger_grpc_enabled: bool = False
    trigger_grpc_bind_host: str = "0.0.0.0"  # noqa: S104  - opt-in service
    #: Distinct from the FastAPI port (7800) and from the brain's
    #: scheduler-grpc port (7701) so all three coexist on one host
    #: without env-var dance. Port 0 is allowed for tests.
    trigger_grpc_bind_port: int = Field(default=7802, ge=0, le=65535)
    trigger_grpc_tls_cert: Path | None = Field(
        default=None,
        description="Server cert presented to brain (PEM)",
    )
    trigger_grpc_tls_key: Path | None = Field(
        default=None,
        description="Server private key (PEM)",
    )
    trigger_grpc_tls_ca: Path | None = Field(
        default=None,
        description="CA bundle used to validate brain's client cert",
    )
    trigger_grpc_allowed_cns: list[str] = Field(default_factory=list)
    trigger_grpc_grace_seconds: float = Field(
        default=5.0, ge=0.1, le=60.0,
    )

    # ------------------------------------------------------------------
    # Fire dispatch
    # ------------------------------------------------------------------
    fire_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    fire_retry_max: int = Field(default=3, ge=0, le=10)
    fire_retry_backoff_seconds: float = Field(default=1.0, ge=0.0, le=60.0)

    # ------------------------------------------------------------------
    # gRPC tuning
    # ------------------------------------------------------------------
    grpc_keepalive_seconds: int = Field(default=30, ge=5, le=300)
    grpc_reconnect_backoff_max_seconds: int = Field(default=30, ge=1, le=300)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_json: bool = True
    metrics_enabled: bool = True
    metrics_auth_token: SecretStr | None = None

    # ------------------------------------------------------------------
    # Pro features
    # ------------------------------------------------------------------
    pro_license_key: SecretStr | None = Field(
        default=None,
        description=(
            "If set, enables z4j-scheduler-pro features (HA via raft, "
            "SLA monitoring, schedule dependency graphs, multi-tenant "
            "isolation). Requires the z4j-scheduler-pro package "
            "installed alongside."
        ),
    )


__all__ = ["Settings"]
