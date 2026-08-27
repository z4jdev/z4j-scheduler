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

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_instance_id() -> str:
    """Hostname-based default instance id.

    Lazy ``socket`` import keeps the module import cheap and lets
    tests stub this function without depending on socket internals.
    """
    import socket

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
    # Required - mTLS (unless ``insecure_grpc`` is True)
    # ------------------------------------------------------------------
    tls_cert: Path | None = Field(
        default=None,
        description="mTLS client cert for gRPC",
    )
    tls_key: Path | None = Field(
        default=None,
        description="mTLS client key for gRPC",
    )
    tls_ca: Path | None = Field(
        default=None,
        description="mTLS CA for verifying brain server cert",
    )

    # ------------------------------------------------------------------
    # Insecure-gRPC opt-in for local dev / CI / fixture-test scenarios.
    # When True the scheduler's gRPC client uses an insecure channel
    # (no TLS, no client cert). Refused unless ``environment`` is exactly
    # ``"dev"`` to make the security posture explicit. The
    # brain side must also be configured for insecure listening
    # via ``Z4J_SCHEDULER_GRPC_INSECURE=true`` for the connection to
    # succeed.
    # ------------------------------------------------------------------
    insecure_grpc: bool = Field(
        default=False,
        description=(
            "DEV/TEST ONLY: skip mTLS on the gRPC channel to brain. "
            "Allowed only when environment is exactly 'dev'. Use only on trusted "
            "loopback or container networks."
        ),
    )

    # ------------------------------------------------------------------
    # Deprecated compatibility field. Leader election uses leader_pg_dsn.
    # ------------------------------------------------------------------
    database_url: str | None = Field(
        default=None,
        description=(
            "Deprecated compatibility field; not read by the leader gate. "
            "Set leader_pg_dsn (Z4J_SCHEDULER_LEADER_PG_DSN) for Postgres "
            "leader election."
        ),
    )

    # ------------------------------------------------------------------
    # Identity + scope
    # ------------------------------------------------------------------
    instance_id: str = Field(
        default_factory=_default_instance_id,
        description="Unique identifier shown in scheduler logs and /info",
    )
    projects: str = Field(
        default="*",
        description=(
            "Deprecated compatibility field; the current watch stream does "
            "not read it and watches every project authorized by the scheduler "
            "credential."
        ),
    )

    # ------------------------------------------------------------------
    # API server (operational endpoints)
    # ------------------------------------------------------------------
    # Binding 0.0.0.0 is intentional for an infrastructure service
    # that operators expose via reverse proxy / service mesh / k8s
    # Service. Operators who want loopback-only override via the
    # Z4J_SCHEDULER_BIND_HOST env var.
    #
    # Audit note (S003, 1.4.0): an earlier draft of this fix
    # flipped the default to ``127.0.0.1`` for defense in depth.
    # That broke the standard container / k8s / same-host-but-
    # different-laptop operator-polls-dashboard topology. The
    # control that closes S003 is the ``/info`` payload redaction
    # at ``api/info.py`` (no longer leaks ``brain_grpc_url`` or
    # the projects list). Keeping 0.0.0.0 here preserves
    # deployment ergonomics; the ``/info`` redaction means the
    # network exposure carries no useful intelligence to an
    # unauthenticated caller.
    bind_host: str = "0.0.0.0"  # noqa: S104  - intentional, see comment
    bind_port: int = Field(default=7800, ge=1, le=65535)

    # ------------------------------------------------------------------
    # Tick + leader behavior
    # ------------------------------------------------------------------
    leader_poll_interval_seconds: int = Field(
        default=2,
        ge=1,
        le=60,
        description=(
            "Deprecated compatibility field; use leader_heartbeat_seconds "
            "for Postgres acquire/liveness cadence."
        ),
    )
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
    leader_backend: Literal["single", "postgres", "postgres_per_project"] = "single"
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
        default=2.0,
        ge=0.5,
        le=60.0,
    )

    #: How late a slot may be and still count as an on-time fire rather than a
    #: missed one. It bounds ordinary scheduling jitter: the tick loop's own
    #: wake granularity, dispatch latency, and clock skew between the scheduler
    #: and the brain. A slot later than this is classified missed and the
    #: schedule's catch_up policy decides its fate, so raising it makes a busy
    #: or slow deployment less likely to treat its own latency as an outage.
    #: The promotion-scoped grace applied to a slot inherited at failover is
    #: derived from this value plus the failover timings, so it moves with it.
    on_time_grace_seconds: float = Field(
        default=5.0,
        ge=0.0,
        le=300.0,
    )

    # ------------------------------------------------------------------
    # TriggerSchedule gRPC server (Phase 2, reverse direction)
    # ------------------------------------------------------------------
    #: Off by default, and it should stay off against a Brain that has
    #: activated durable schedule control. That Brain fires an operator
    #: trigger itself, because it is the authority on whether the schedule may
    #: run and the only side that can see a hold, and the wire this server
    #: would answer on carries cadence acceptances only. A trigger routed here
    #: is refused with an error saying so, so enabling it against such a Brain
    #: converts a working button into a broken one. It remains for a Brain that
    #: predates that activation and still calls out to the scheduler. When
    #: disabled the scheduler does not bind a server port.
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
    #: When True, the trigger gRPC server refuses to start unless
    #: ``trigger_grpc_allowed_cns`` is non-empty. Audit fix S004
    #: (1.4.0): defaults False to preserve "trust the CA"
    #: deployments; operators wanting fail-closed defense-in-depth
    #: set ``Z4J_SCHEDULER_TRIGGER_GRPC_REQUIRE_ALLOWLIST=true``
    #: and the scheduler raises on startup if the allow-list was
    #: forgotten.
    trigger_grpc_require_allowlist: bool = False
    trigger_grpc_grace_seconds: float = Field(
        default=5.0,
        ge=0.1,
        le=60.0,
    )

    # ------------------------------------------------------------------
    # Fire dispatch
    # ------------------------------------------------------------------
    fire_timeout_seconds: int = Field(
        default=10,
        ge=1,
        le=3600,
        description="Deadline for each FireSchedule gRPC attempt",
    )
    fire_retry_max: int = Field(default=3, ge=0, le=10)
    fire_retry_backoff_seconds: float = Field(default=1.0, ge=0.0, le=60.0)

    # ------------------------------------------------------------------
    # gRPC tuning
    # ------------------------------------------------------------------
    grpc_keepalive_seconds: int = Field(default=30, ge=5, le=300)
    grpc_reconnect_backoff_max_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
        description="Maximum watch-stream reconnect backoff",
    )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_json: bool = True
    metrics_enabled: bool = True
    metrics_auth_token: SecretStr | None = None

    # ------------------------------------------------------------------
    # Environment - controls security gating
    # ------------------------------------------------------------------
    environment: str = Field(
        default="production",
        description=(
            "Deployment environment. Exactly 'dev' relaxes the security "
            "gating: it is the only value that permits insecure_grpc or "
            "skips the metrics-auth fail-fast. Every other value, including "
            "'test' and 'staging', is held to the production posture. "
            "Mirrors the brain's Z4J_ENVIRONMENT semantics, which compare "
            "against the same exact string."
        ),
    )

    # ------------------------------------------------------------------
    # Pro features
    # ------------------------------------------------------------------
    pro_license_key: SecretStr | None = Field(
        default=None,
        description=(
            "Reserved compatibility field for a possible separate commercial "
            "package. The open-source scheduler does not read this value and "
            "setting it enables no feature."
        ),
    )

    @property
    def is_dev(self) -> bool:
        """Whether this process runs with development relaxations. Exactly ``dev``.

        The brain settles the same question the same way, and the two have to
        agree: an operator sets one environment name for a deployment and
        expects both halves of it to behave alike.

        Both gates below used to compare against the literal ``production``,
        which inverts the posture for every other label. A scheduler tagged
        ``staging`` skipped the metrics-auth fail-fast, so a bind on
        ``0.0.0.0`` with metrics enabled and no token served an unauthenticated
        ``/metrics`` carrying project labels, schedule names and leadership
        state. One tagged ``prod`` was allowed to talk plaintext gRPC to the
        brain. Both refusal messages already told the reader to set ``dev``,
        so the code and its own error text disagreed.
        """
        # Exact, byte for byte, like the brain. Keeping ``.strip().lower()``
        # here while the field description and the release notes both promised
        # "the exact string dev" and "mirrors the brain" published a guarantee
        # the code did not honour: DEV and Dev relaxed the scheduler's metrics
        # authentication and gRPC transport while the brain refused them. The
        # tolerance was inherited from the comparisons this replaced, and
        # carrying it forward silently was the mistake.
        return self.environment == "dev"

    @model_validator(mode="after")
    def _enforce_metrics_auth_in_production(self) -> Settings:
        """Refuse to start a production scheduler that exposes
        ``/metrics`` publicly.

        z4j-scheduler 1.6.5 (security advisory): the
        scheduler's ``/metrics`` endpoint publishes operational
        metadata (project labels, schedule names, leadership state,
        fire status, latency). Pre-1.6.5 it was unauthenticated by
        default; an operator who set
        ``Z4J_SCHEDULER_BIND_HOST=0.0.0.0`` in production without
        also setting ``Z4J_SCHEDULER_METRICS_AUTH_TOKEN`` exposed
        every schedule label to anyone who could reach port 7800.

        The scheduler fails fast at startup. If:
        - the environment is anything but ``dev`` AND
        - ``bind_host`` is not a loopback address AND
        - ``metrics_enabled`` is true AND
        - ``metrics_auth_token`` is unset
        then the scheduler refuses to start with a clear error
        naming the four-way condition and the fix options.

        The first condition used to compare the environment against the
        literal production label, which meant a scheduler tagged ``staging``
        skipped the check entirely and served that metadata to anyone who
        could reach the port. It asks :attr:`is_dev` now, so every label
        except ``dev`` is held to the same standard.

        Written without quoting the old comparison, because the guard in
        ``test_environment_predicate_is_single`` reads source text and a
        docstring spelling it out is exactly how someone copies it back.

        Operators have three valid configurations to resolve:

        1. Set ``Z4J_SCHEDULER_METRICS_AUTH_TOKEN=<32+ random
           bytes>`` so the metrics endpoint requires bearer auth.
        2. Bind to loopback (``Z4J_SCHEDULER_BIND_HOST=127.0.0.1``)
           and front the scheduler behind a reverse proxy that
           handles its own auth + selective scrape exposure.
        3. Disable metrics entirely
           (``Z4J_SCHEDULER_METRICS_ENABLED=false``) -- the
           ``/metrics`` route is then not mounted at all.

        Only the exact environment value ``dev`` skips the check. Labels such
        as ``test``, ``staging``, or ``DEV`` retain the production posture.
        """
        if self.is_dev:
            return self
        if not self.metrics_enabled:
            # Metrics endpoint won't be mounted (see ``api/app.py``);
            # auth token is irrelevant.
            return self
        loopback_hosts = {"127.0.0.1", "localhost", "::1", "[::1]"}
        if self.bind_host.strip().lower() in loopback_hosts:
            # Loopback exposure: operator's choice; reverse-proxy
            # in front owns the auth layer.
            return self
        if self.metrics_auth_token is None:
            raise ValueError(
                "z4j-scheduler: refusing to start with "
                f"bind_host='{self.bind_host}' (non-loopback) AND "
                "metrics_enabled=true AND no metrics_auth_token set. "
                "The /metrics endpoint publishes project labels, "
                "schedule names, leadership state, and fire status to "
                "anyone who can reach port 7800. Pick one fix:\n"
                "  (1) Set Z4J_SCHEDULER_METRICS_AUTH_TOKEN to a "
                "    32-byte random string (recommended; matches the "
                "    brain's /metrics gating).\n"
                "  (2) Bind to loopback: "
                "    Z4J_SCHEDULER_BIND_HOST=127.0.0.1 and front "
                "    the scheduler behind a reverse proxy.\n"
                "  (3) Disable metrics entirely: "
                "    Z4J_SCHEDULER_METRICS_ENABLED=false.\n"
                "Only Z4J_SCHEDULER_ENVIRONMENT=dev skips this check. "
                "Every other value, staging and test included, is held to "
                "it: this check used to compare against the production "
                "label alone, which is how a scheduler tagged staging came "
                "to serve this endpoint unauthenticated.",
            )
        return self

    @model_validator(mode="after")
    def _enforce_grpc_tls_or_insecure(self) -> Settings:
        """Require either complete TLS bundle OR explicit insecure opt-in.

        The combinations:

        - TLS bundle complete (cert + key + ca): production-shaped,
          allowed in any environment.
        - ``insecure_grpc=True`` + environment exactly ``dev``: allowed,
          for a laptop or a fixture-cert-less CI job.
        - ``insecure_grpc=True`` + any other environment: REFUSED. That
          includes ``test`` and ``staging``, which this docstring used to say
          were allowed while the validator refused them.
        - All TLS fields missing AND ``insecure_grpc=False``:
          REFUSED. Pre-1.5 the TLS fields were required-by-Pydantic;
          we now allow None to support the insecure path, but the
          model_validator catches the misconfiguration where neither
          path was selected.
        """
        tls_complete = (
            self.tls_cert is not None and self.tls_key is not None and self.tls_ca is not None
        )
        if self.insecure_grpc:
            if not self.is_dev:
                raise ValueError(
                    "z4j-scheduler: insecure_grpc=True is refused in "
                    "outside environment='dev'. Either set Z4J_SCHEDULER_INSECURE_GRPC="
                    "false and provide a valid TLS bundle "
                    "(Z4J_SCHEDULER_TLS_CERT/KEY/CA), or set "
                    "Z4J_SCHEDULER_ENVIRONMENT=dev, which is the only value "
                    "that acknowledges the trade-off.",
                )
            return self
        if not tls_complete:
            raise ValueError(
                "z4j-scheduler: gRPC channel requires either a complete "
                "mTLS bundle (Z4J_SCHEDULER_TLS_CERT, "
                "Z4J_SCHEDULER_TLS_KEY, Z4J_SCHEDULER_TLS_CA all set) "
                "or insecure_grpc=true (Z4J_SCHEDULER_INSECURE_GRPC=true), "
                "which requires Z4J_SCHEDULER_ENVIRONMENT=dev.",
            )
        return self


__all__ = ["Settings"]
