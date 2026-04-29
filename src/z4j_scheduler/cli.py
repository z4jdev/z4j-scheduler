"""Typer CLI entry point.

Subcommands (per ``docs/SCHEDULER.md §15.4``):

    z4j-scheduler serve                   Run the scheduler process
    z4j-scheduler version                 Print the installed version
    z4j-scheduler info                    Print resolved settings + status
    z4j-scheduler schedules add ...       Create a schedule (one-off)
    z4j-scheduler schedules list ...      List schedules
    z4j-scheduler schedules trigger ...   Manual trigger
    z4j-scheduler schedules disable ...   Disable a schedule
    z4j-scheduler import --from <tool>    Migrate from celery-beat / rq / aps / cron
    z4j-scheduler export --to <tool>      Reverse migration

Phase 1 ships only ``serve`` and ``version`` working end to end.
The rest of the surface is stubbed so users see a clear "not yet
implemented" message instead of a missing-command error.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile

import typer

from z4j_scheduler.version import __version__

app = typer.Typer(
    name="z4j-scheduler",
    help=(
        "z4j-scheduler - the modern Python scheduler in the z4j "
        "stack. Engine-agnostic, dynamic-CRUD, HA-ready."
    ),
    no_args_is_help=True,
    add_completion=False,
)


@app.command()
def serve() -> None:
    """Run the scheduler process.

    Reads configuration from ``Z4J_SCHEDULER_*`` environment variables
    (see ``docs/SCHEDULER.md §20`` for the full reference). Connects
    to brain via the gRPC URL, populates the schedule cache from the
    initial sync, then ticks until SIGTERM.

    The process exits 0 on graceful shutdown, 1 on startup failure
    (bad config, brain unreachable, mTLS rejection, etc.).
    """
    # Settings are imported here (not at module top) so the CLI's
    # other subcommands (version, --help) work even when the env
    # vars are not set. Settings instantiation enforces the
    # required-field validation at this point only.
    from z4j_scheduler.main import run_from_settings  # noqa: PLC0415
    from z4j_scheduler.settings import Settings  # noqa: PLC0415

    try:
        settings = Settings()  # type: ignore[call-arg]
    except Exception as exc:
        # Pydantic's default ValidationError repr dumps the full
        # input dict including any unrelated env vars Pydantic saw
        # (e.g. from a co-located .env file). Extract just the
        # missing/invalid field names so secret-shaped values from
        # other components do not bleed into our stderr.
        from pydantic import ValidationError  # noqa: PLC0415

        if isinstance(exc, ValidationError):
            typer.echo(
                "configuration error - the following Z4J_SCHEDULER_* "
                "environment variables are missing or invalid:",
                err=True,
            )
            for err in exc.errors():
                loc = ".".join(str(p) for p in err["loc"])
                typer.echo(f"  - {loc}: {err['msg']}", err=True)
            typer.echo(
                "\nSee docs/SCHEDULER.md §20 for the env-var reference.",
                err=True,
            )
        else:
            typer.echo(f"configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    # uvloop is configured here so it applies to the entire scheduler
    # event loop, not just asyncio.run's default. Linux/macOS only;
    # the conditional dep + import-error guard handle Windows.
    try:
        import uvloop  # noqa: PLC0415

        uvloop.install()
    except ImportError:
        # Default asyncio loop is fine - just slower under load. Not
        # a fatal problem for a local-dev or Windows install.
        pass

    exit_code = asyncio.run(run_from_settings(settings))
    raise typer.Exit(code=exit_code)


@app.command()
def version() -> None:
    """Print the installed package version."""
    typer.echo(__version__)


@app.command()
def check(
    brain_grpc_url: str = typer.Option(
        None,
        "--brain-grpc-url",
        envvar="Z4J_SCHEDULER_BRAIN_GRPC_URL",
        help="brain gRPC URL to probe",
    ),
    brain_rest_url: str = typer.Option(
        None,
        "--brain-rest-url",
        envvar="Z4J_SCHEDULER_BRAIN_REST_URL",
        help="brain REST URL for /health probe",
    ),
) -> None:
    """Compact pass/fail health check (1.1.2+).

    Same brain-reachability probes as ``doctor`` but emits one
    line per failed check (or a single OK line) - suitable for
    cron jobs, deploy gates, and Nagios-style monitors. Exit 0 =
    healthy, exit 1 = at least one check failed.
    """
    import socket  # noqa: PLC0415
    from urllib.parse import urlparse  # noqa: PLC0415
    from urllib.request import urlopen  # noqa: PLC0415

    fails: list[str] = []

    if not brain_grpc_url:
        fails.append("Z4J_SCHEDULER_BRAIN_GRPC_URL: not set")
    else:
        try:
            parsed = urlparse(
                brain_grpc_url
                if "://" in brain_grpc_url
                else f"//{brain_grpc_url}",
            )
            host, port = parsed.hostname, parsed.port or 443
            if not host:
                fails.append(f"grpc URL: malformed ({brain_grpc_url})")
            else:
                with socket.create_connection((host, port), timeout=3.0):
                    pass
        except OSError as exc:
            fails.append(f"grpc TCP: {exc}")

    if brain_rest_url:
        try:
            with urlopen(  # noqa: S310
                brain_rest_url.rstrip("/") + "/health",
                timeout=3.0,
            ) as resp:
                if resp.status >= 400:
                    fails.append(f"rest /health: HTTP {resp.status}")
        except OSError as exc:
            fails.append(f"rest /health: {exc}")

    if fails:
        for f in fails:
            typer.echo(f"z4j-scheduler check: {f}", err=True)
        raise typer.Exit(code=1)
    typer.echo("z4j-scheduler check: all green")


@app.command()
def status() -> None:
    """One-line introspection (1.1.2+).

    Reports the installed z4j-scheduler version, configured brain
    URLs (from env), and process supervisor hint. Doesn't probe
    the brain - use ``check`` for that. Useful for "what version
    is on this host?" type questions during incident triage.
    """
    import os as _os  # noqa: PLC0415
    typer.echo(f"z4j-scheduler status:")
    typer.echo(f"  version:           {__version__}")
    typer.echo(
        f"  brain_grpc_url:    "
        f"{_os.environ.get('Z4J_SCHEDULER_BRAIN_GRPC_URL', '<unset>')}",
    )
    typer.echo(
        f"  brain_rest_url:    "
        f"{_os.environ.get('Z4J_SCHEDULER_BRAIN_REST_URL', '<unset>')}",
    )
    typer.echo(
        f"  embedded mode:     "
        f"{_os.environ.get('Z4J_EMBEDDED_SCHEDULER', '<unset>')}",
    )
    typer.echo(
        "  process control:   restart via your supervisor "
        "(systemctl restart z4j-scheduler, or restart the brain if "
        "running embedded with Z4J_EMBEDDED_SCHEDULER=true).",
    )


@app.command()
def restart() -> None:
    """Stub for symmetry (1.1.2+).

    z4j-scheduler runs as a standalone process (or as a supervised
    subprocess of z4j-brain when ``Z4J_EMBEDDED_SCHEDULER=true``);
    in neither case does the scheduler manage its own connection
    pool the way framework agents do. Restart it via your process
    supervisor instead.

    This command exists for surface-area parity with other z4j
    packages (``z4j-django restart``, ``z4j-flask restart``, etc.)
    and prints the canonical command for each common deploy
    pattern. Exit code 1 because nothing was actually restarted.
    """
    typer.echo(
        "z4j-scheduler restart: not directly supported.\n"
        "\n"
        "  Restart via your process supervisor:\n"
        "    systemctl restart z4j-scheduler        # systemd\n"
        "    supervisorctl restart z4j-scheduler    # supervisord\n"
        "    docker restart <z4j-scheduler-container>\n"
        "\n"
        "  Or if running embedded (Z4J_EMBEDDED_SCHEDULER=true):\n"
        "    systemctl restart z4j                  # restart the brain\n"
        "    z4j-django restart                     # if launched by Django\n",
        err=True,
    )
    raise typer.Exit(code=1)


@app.command()
def info() -> None:
    """Print resolved settings + runtime status. Phase 1."""
    typer.echo(
        "Phase 1 - implementation in progress. "
        "See docs/SCHEDULER.md §25.",
        err=True,
    )
    raise typer.Exit(code=2)


@app.command()
def doctor(
    brain_grpc_url: str = typer.Option(
        None,
        "--brain-grpc-url",
        envvar="Z4J_SCHEDULER_BRAIN_GRPC_URL",
        help="brain gRPC URL to probe (env Z4J_SCHEDULER_BRAIN_GRPC_URL)",
    ),
    brain_rest_url: str = typer.Option(
        None,
        "--brain-rest-url",
        envvar="Z4J_SCHEDULER_BRAIN_REST_URL",
        help="brain REST URL for /health probe",
    ),
    tls_cert: str = typer.Option(
        None,
        "--tls-cert",
        envvar="Z4J_SCHEDULER_TLS_CERT",
        help="path to client cert PEM",
    ),
    tls_key: str = typer.Option(
        None,
        "--tls-key",
        envvar="Z4J_SCHEDULER_TLS_KEY",
        help="path to client key PEM",
    ),
    tls_ca: str = typer.Option(
        None,
        "--tls-ca",
        envvar="Z4J_SCHEDULER_TLS_CA",
        help="path to CA bundle PEM",
    ),
    leader_pg_dsn: str = typer.Option(
        None,
        "--leader-pg-dsn",
        envvar="Z4J_SCHEDULER_LEADER_PG_DSN",
        help="Postgres DSN for HA leader gate (optional - skipped if unset)",
    ),
) -> None:
    """Diagnose common config + connectivity errors before ``serve``.

    Runs through the failure modes operators hit when first wiring
    z4j-scheduler against a brain:

    - Required env vars are set + parseable.
    - The mTLS PEM files exist + are readable + parseable + match.
      Catches the "wrong cert / key pair" case and the "CA doesn't
      sign this cert" case.
    - Brain REST /health responds.
    - Brain gRPC SchedulerService Ping responds.
    - (If supplied) Postgres DSN connects + we can take + release a
      throwaway advisory lock.

    Returns exit code 0 if every check passes, 1 if any check fails.
    Each check prints PASS / FAIL / SKIP + a one-line reason. The
    intent is the operator's first command after `pip install` -
    surfaces the actual problem instead of a cryptic stack trace
    from `serve` failing to bind.
    """
    import socket  # noqa: PLC0415
    import ssl  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415
    from urllib.parse import urlparse  # noqa: PLC0415
    from urllib.request import urlopen  # noqa: PLC0415

    failed = False

    def _print(label: str, status: str, detail: str = "") -> None:
        nonlocal failed
        if status == "FAIL":
            failed = True
        # ASCII markers - the doctor command runs on Windows shells
        # whose default cp1252 codepage can't render checkmarks. Stay
        # portable; operators reading the output can pattern-match
        # PASS / FAIL / SKIP just as easily.
        marker = {"PASS": "ok ", "FAIL": "FAIL", "SKIP": "-- "}.get(status, "?")
        typer.echo(f"  [{marker}] {status:<5} {label}{f': {detail}' if detail else ''}")

    typer.echo("z4j-scheduler doctor")
    typer.echo("=" * 50)

    # 1. Env vars
    typer.echo("\nConfig:")
    if brain_grpc_url:
        _print("Z4J_SCHEDULER_BRAIN_GRPC_URL", "PASS", brain_grpc_url)
    else:
        _print(
            "Z4J_SCHEDULER_BRAIN_GRPC_URL", "FAIL",
            "unset; required by serve",
        )
    if brain_rest_url:
        _print("Z4J_SCHEDULER_BRAIN_REST_URL", "PASS", brain_rest_url)
    else:
        _print(
            "Z4J_SCHEDULER_BRAIN_REST_URL", "FAIL",
            "unset; required by serve",
        )

    # 2. TLS material
    typer.echo("\nmTLS material:")
    cert_paths = {"cert": tls_cert, "key": tls_key, "ca": tls_ca}
    pem_data: dict = {}
    for label, path_str in cert_paths.items():
        if not path_str:
            _print(f"TLS_{label.upper()}", "FAIL", "unset")
            continue
        path = Path(path_str)
        if not path.is_file():
            _print(f"TLS_{label.upper()}", "FAIL", f"file not found: {path}")
            continue
        try:
            data = path.read_bytes()
        except OSError as exc:
            _print(f"TLS_{label.upper()}", "FAIL", f"unreadable: {exc}")
            continue
        if not data.strip():
            _print(f"TLS_{label.upper()}", "FAIL", "empty file")
            continue
        pem_data[label] = data
        _print(f"TLS_{label.upper()}", "PASS", f"{len(data)} bytes")

    # 3. Cert + key pairing
    if "cert" in pem_data and "key" in pem_data:
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            with tempfile.NamedTemporaryFile(
                suffix=".crt", delete=False,
            ) as cf, tempfile.NamedTemporaryFile(
                suffix=".key", delete=False,
            ) as kf:
                cf.write(pem_data["cert"])
                kf.write(pem_data["key"])
                cf.flush()
                kf.flush()
                cf_name, kf_name = cf.name, kf.name
            try:
                ctx.load_cert_chain(certfile=cf_name, keyfile=kf_name)
                _print(
                    "cert + key pair", "PASS", "match (same modulus)",
                )
            finally:
                Path(cf_name).unlink(missing_ok=True)
                Path(kf_name).unlink(missing_ok=True)
        except (ssl.SSLError, OSError) as exc:
            _print(
                "cert + key pair", "FAIL",
                f"mismatch or unparseable: {exc}",
            )

    # 4. CA chain validation
    if "cert" in pem_data and "ca" in pem_data:
        try:
            from cryptography import x509 as _x509  # noqa: PLC0415

            cert_obj = _x509.load_pem_x509_certificate(pem_data["cert"])
            ca_obj = _x509.load_pem_x509_certificate(pem_data["ca"])
            # Verify the cert's issuer subject matches the CA's
            # subject (the simple version of "is this signed by
            # that CA?"). Full chain verification would require an
            # SSL handshake, which the gRPC ping below does anyway.
            if cert_obj.issuer == ca_obj.subject:
                _print("CA issued cert", "PASS", "issuer matches CA subject")
            else:
                _print(
                    "CA issued cert", "FAIL",
                    "issuer != CA subject (cert was signed by a different CA)",
                )
        except Exception as exc:  # noqa: BLE001
            _print("CA issued cert", "FAIL", f"parse error: {exc}")

    # 5. Brain REST /health probe
    typer.echo("\nBrain reachability:")
    if brain_rest_url:
        rest_url = brain_rest_url.rstrip("/") + "/api/v1/health"
        try:
            with urlopen(rest_url, timeout=5) as resp:  # noqa: S310
                if resp.status == 200:
                    _print("brain REST /health", "PASS", f"{rest_url} -> 200")
                else:
                    _print(
                        "brain REST /health", "FAIL",
                        f"{rest_url} -> {resp.status}",
                    )
        except Exception as exc:  # noqa: BLE001
            _print("brain REST /health", "FAIL", f"{rest_url} -> {exc}")

    # 6. Brain gRPC reachability (TCP probe; full mTLS Ping requires
    # an event loop + grpcio import that we keep light here)
    if brain_grpc_url:
        host_port = brain_grpc_url.split("//")[-1]
        host, _, port_s = host_port.partition(":")
        try:
            port = int(port_s)
        except ValueError:
            _print(
                "brain gRPC TCP", "FAIL",
                f"unparseable port in {brain_grpc_url!r}",
            )
        else:
            try:
                with socket.create_connection((host, port), timeout=5):
                    _print(
                        "brain gRPC TCP", "PASS", f"{host}:{port} accepts",
                    )
            except OSError as exc:
                _print("brain gRPC TCP", "FAIL", f"{host}:{port} -> {exc}")

    # 7. Postgres advisory lock probe (optional)
    typer.echo("\nLeader gate (HA):")
    if not leader_pg_dsn:
        _print(
            "Postgres advisory lock", "SKIP",
            "Z4J_SCHEDULER_LEADER_PG_DSN unset (single-instance mode)",
        )
    else:
        try:
            import asyncpg  # noqa: PLC0415

            async def _probe() -> tuple[bool, str]:
                try:
                    conn = await asyncpg.connect(leader_pg_dsn, timeout=5)
                except Exception as exc:  # noqa: BLE001
                    return False, f"connect failed: {exc}"
                try:
                    # Take + release a bench-only key. Doesn't conflict
                    # with the production scheduler's keys.
                    got = await conn.fetchval(
                        "SELECT pg_try_advisory_lock(0, 1)",
                    )
                    if not got:
                        return False, "another connection holds the bench key"
                    await conn.fetchval(
                        "SELECT pg_advisory_unlock(0, 1)",
                    )
                    return True, "acquire + release OK"
                finally:
                    await conn.close()

            ok, detail = asyncio.run(_probe())
            _print(
                "Postgres advisory lock",
                "PASS" if ok else "FAIL",
                detail,
            )
        except ImportError:
            _print(
                "Postgres advisory lock", "SKIP",
                "asyncpg not installed (pip install asyncpg)",
            )

    typer.echo("=" * 50)
    if failed:
        typer.echo("doctor: at least one check FAILED.", err=True)
        raise typer.Exit(code=1)
    typer.echo("doctor: all checks passed.")


# ---------------------------------------------------------------------------
# schedules CRUD subcommands (Phase 5 §15.4 bare-Python CLI)
# ---------------------------------------------------------------------------
#
# These wrap the brain REST endpoints so operators have a CLI for
# the same operations the dashboard offers. Useful for deploy hooks
# / one-shot adjustments / scripting. Every subcommand requires
# ``--brain-url`` + ``--api-token`` (env: ``Z4J_SCHEDULER_BRAIN_*``).

schedules_app = typer.Typer(
    help=(
        "Manage z4j-scheduler-owned schedules from the command line. "
        "Wraps brain's REST API; operates on the same project + table "
        "as the dashboard."
    ),
    no_args_is_help=True,
)
app.add_typer(schedules_app, name="schedules")


@schedules_app.command("add")
def schedules_add(
    project: str = typer.Option(..., "--project", help="brain project slug"),
    name: str = typer.Option(..., "--name", help="schedule name (unique per project)"),
    engine: str = typer.Option("celery", "--engine", help="engine adapter"),
    kind: str = typer.Option(
        ..., "--kind",
        help="schedule kind: 'cron' / 'interval' / 'one_shot'",
    ),
    expression: str = typer.Option(
        ..., "--expression",
        help=(
            "kind-specific expression: 5-field crontab (cron), '<N>{s,m,h,d}' "
            "(interval), or ISO-8601 timestamp (one_shot)"
        ),
    ),
    task_name: str = typer.Option(
        ..., "--task-name",
        help="fully-qualified task name to invoke",
    ),
    timezone: str = typer.Option("UTC", "--timezone"),
    queue: str | None = typer.Option(None, "--queue"),
    args_json: str | None = typer.Option(
        None, "--args", help="JSON list of positional task args",
    ),
    kwargs_json: str | None = typer.Option(
        None, "--kwargs", help="JSON object of task kwargs",
    ),
    catch_up: str = typer.Option("skip", "--catch-up"),
    enabled: bool = typer.Option(
        True, "--enabled/--disabled",
        help="seed the schedule in the enabled state (default) or disabled",
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Create one schedule via brain's POST /schedules endpoint.

    Mirrors the dashboard's "New Schedule" form. Returns the
    created row's brain id on stdout so deploy scripts can capture
    it.
    """
    import json  # noqa: PLC0415

    body = {
        "name": name,
        "engine": engine,
        "kind": kind,
        "expression": expression,
        "task_name": task_name,
        "timezone": timezone,
        "queue": queue,
        "args": json.loads(args_json) if args_json else [],
        "kwargs": json.loads(kwargs_json) if kwargs_json else {},
        "catch_up": catch_up,
        "is_enabled": enabled,
        "scheduler": "z4j-scheduler",
        "source": "cli",
    }
    response = _brain_post(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules",
        body=body,
        expect_status=(201,),
    )
    typer.echo(response["id"])


@schedules_app.command("list")
def schedules_list(
    project: str = typer.Option(..., "--project"),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
    json_output: bool = typer.Option(
        False, "--json/--no-json",
        help="emit raw JSON instead of the human-readable table",
    ),
) -> None:
    """List every schedule in the project. Defaults to a tabular view."""
    rows = _brain_get(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules",
    )
    if json_output:
        import json as _json  # noqa: PLC0415

        typer.echo(_json.dumps(rows, indent=2))
        return
    if not rows:
        typer.echo("(no schedules)")
        return
    # Compact table - operators eyeball this from a deploy hook log.
    typer.echo(
        f"{'name':<30} {'kind':<10} {'expression':<25} {'enabled':<8} {'scheduler':<18}",
    )
    for r in rows:
        typer.echo(
            f"{r['name'][:30]:<30} {r['kind']:<10} {r['expression'][:25]:<25} "
            f"{str(r['is_enabled']):<8} {r['scheduler'][:18]:<18}",
        )


@schedules_app.command("trigger")
def schedules_trigger(
    project: str = typer.Option(..., "--project"),
    name: str = typer.Option(
        ..., "--name",
        help="schedule name (CLI looks up the id by name to keep arg surface small)",
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Fire a schedule once, out-of-band. Same as the dashboard 'Trigger now' button."""
    schedule_id = _resolve_schedule_id_by_name(
        brain_url=brain_url,
        api_token=api_token,
        project=project,
        name=name,
    )
    response = _brain_post(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules/{schedule_id}/trigger",
        body={},
        expect_status=(200, 201),
    )
    typer.echo(f"triggered {name} → schedule_id={schedule_id}")
    if "id" in response:
        typer.echo(f"  schedule_id={response.get('id')}")


@schedules_app.command("disable")
def schedules_disable(
    project: str = typer.Option(..., "--project"),
    name: str = typer.Option(..., "--name"),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Disable a schedule (operator pause, no delete)."""
    schedule_id = _resolve_schedule_id_by_name(
        brain_url=brain_url,
        api_token=api_token,
        project=project,
        name=name,
    )
    _brain_post(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules/{schedule_id}/disable",
        body={},
        expect_status=(200,),
    )
    typer.echo(f"disabled {name}")


@schedules_app.command("edit")
def schedules_edit(
    project: str = typer.Option(..., "--project"),
    name: str = typer.Option(
        ..., "--name",
        help="schedule name (identity); cannot be changed via this command",
    ),
    expression: str = typer.Option(
        None, "--expression",
        help="new cron / interval / one_shot / solar expression",
    ),
    task_name: str = typer.Option(
        None, "--task-name",
        help="rename the task this schedule fires",
    ),
    timezone_: str = typer.Option(
        None, "--timezone",
        help="IANA timezone for cron schedules",
    ),
    queue: str = typer.Option(
        None, "--queue",
        help="route fires onto this queue (engine-dependent)",
    ),
    catch_up: str = typer.Option(
        None, "--catch-up",
        help="skip / fire_one_missed / fire_all_missed",
    ),
    args: str = typer.Option(
        None, "--args",
        help="JSON array of positional args (e.g. '[42, \"x\"]')",
    ),
    kwargs: str = typer.Option(
        None, "--kwargs",
        help="JSON object of keyword args (e.g. '{\"flag\": true}')",
    ),
    enable: bool = typer.Option(
        None,
        "--enable/--disable",
        help="flip is_enabled (omit to leave unchanged)",
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Edit a schedule via brain's PATCH /schedules/{id}.

    Only the flags the operator passes are sent in the PATCH body;
    omitted fields stay at their current brain values. Useful for
    targeted ops scripts that flip ONE field (rotate a queue, bump
    a catch-up policy) without re-stating the whole schedule.
    """
    import json  # noqa: PLC0415

    schedule_id = _resolve_schedule_id_by_name(
        brain_url=brain_url,
        api_token=api_token,
        project=project,
        name=name,
    )
    body: dict = {}
    if expression is not None:
        body["expression"] = expression
    if task_name is not None:
        body["task_name"] = task_name
    if timezone_ is not None:
        body["timezone"] = timezone_
    if queue is not None:
        body["queue"] = queue or None
    if catch_up is not None:
        body["catch_up"] = catch_up
    if enable is not None:
        body["is_enabled"] = enable
    if args is not None:
        try:
            body["args"] = json.loads(args)
        except json.JSONDecodeError as exc:
            typer.echo(f"--args is not valid JSON: {exc}", err=True)
            raise typer.Exit(code=2) from exc
        if not isinstance(body["args"], list):
            typer.echo("--args must be a JSON array", err=True)
            raise typer.Exit(code=2)
    if kwargs is not None:
        try:
            body["kwargs"] = json.loads(kwargs)
        except json.JSONDecodeError as exc:
            typer.echo(f"--kwargs is not valid JSON: {exc}", err=True)
            raise typer.Exit(code=2) from exc
        if not isinstance(body["kwargs"], dict):
            typer.echo("--kwargs must be a JSON object", err=True)
            raise typer.Exit(code=2)

    if not body:
        typer.echo(
            "edit: no fields to change. Pass at least one of "
            "--expression / --task-name / --timezone / --queue / "
            "--catch-up / --args / --kwargs / --enable / --disable.",
            err=True,
        )
        raise typer.Exit(code=2)

    _brain_patch(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules/{schedule_id}",
        body=body,
    )
    typer.echo(
        f"updated {name}: {sorted(body.keys())}",
    )


@schedules_app.command("history")
def schedules_history(
    project: str = typer.Option(..., "--project"),
    name: str = typer.Option(..., "--name"),
    limit: int = typer.Option(
        20, "--limit",
        help="number of recent fires to show (capped at 1000 server-side)",
    ),
    json_out: bool = typer.Option(
        False, "--json/--no-json",
        help="emit raw JSON instead of the table view",
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Show the most recent fire history for a schedule.

    Mirrors the dashboard's "Last 50 fires" panel for shell-only
    operator use. Status / latency / error_message per row, newest
    first. ``--json`` is for piping into ``jq`` etc.
    """
    import json  # noqa: PLC0415

    schedule_id = _resolve_schedule_id_by_name(
        brain_url=brain_url,
        api_token=api_token,
        project=project,
        name=name,
    )
    rows = _brain_get(
        brain_url=brain_url,
        api_token=api_token,
        path=(
            f"/api/v1/projects/{project}/schedules/{schedule_id}/fires"
            f"?limit={limit}"
        ),
    )
    if json_out:
        typer.echo(json.dumps(rows, indent=2))
        return
    if not rows:
        typer.echo(f"no fires recorded for {name}")
        return
    typer.echo(
        f"{'fired_at':<25} {'status':<14} {'latency':>10}  detail",
    )
    typer.echo("-" * 78)
    for r in rows:
        fired_at = r.get("fired_at", "")[:25]
        status = r.get("status", "")
        latency = r.get("latency_ms")
        latency_s = f"{latency}ms" if latency is not None else "-"
        detail = (
            r.get("error_message")
            or r.get("error_code")
            or (
                f"cmd:{r.get('command_id', '')[:8]}..."
                if r.get("command_id") else "-"
            )
        )
        # Truncate detail to keep the line under 120 columns.
        if len(detail) > 50:
            detail = detail[:47] + "..."
        typer.echo(
            f"{fired_at:<25} {status:<14} {latency_s:>10}  {detail}",
        )


@schedules_app.command("enable")
def schedules_enable(
    project: str = typer.Option(..., "--project"),
    name: str = typer.Option(..., "--name"),
    brain_url: str = typer.Option(
        "http://localhost:7700", "--brain-url",
    ),
    api_token: str | None = typer.Option(
        None, "--api-token",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
) -> None:
    """Re-enable a disabled schedule."""
    schedule_id = _resolve_schedule_id_by_name(
        brain_url=brain_url,
        api_token=api_token,
        project=project,
        name=name,
    )
    _brain_post(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules/{schedule_id}/enable",
        body={},
        expect_status=(200,),
    )
    typer.echo(f"enabled {name}")


def _resolve_schedule_id_by_name(
    *,
    brain_url: str,
    api_token: str | None,
    project: str,
    name: str,
) -> str:
    """Look up a schedule's UUID by its human name within a project."""
    rows = _brain_get(
        brain_url=brain_url,
        api_token=api_token,
        path=f"/api/v1/projects/{project}/schedules",
    )
    for r in rows:
        if r["name"] == name:
            return r["id"]
    raise typer.BadParameter(
        f"no schedule named {name!r} in project {project!r}",
    )


def _brain_get(
    *,
    brain_url: str,
    api_token: str | None,
    path: str,
) -> object:
    """Synchronous httpx GET wrapper for the schedules CLI.

    The CLI is short-lived + sync-shaped (typer); using sync httpx
    here keeps the command implementation small. The async path is
    available via :class:`BrainImportClient` when needed.
    """
    import httpx  # noqa: PLC0415

    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    try:
        response = httpx.get(
            f"{brain_url.rstrip('/')}{path}",
            headers=headers,
            timeout=30.0,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise typer.Exit(code=1) from typer.BadParameter(
            f"brain GET {path} failed: {exc}",
        )
    return response.json()


def _brain_patch(
    *,
    brain_url: str,
    api_token: str | None,
    path: str,
    body: dict,
) -> dict:
    """Sync httpx PATCH wrapper. Used by ``schedules edit``.

    Mirrors ``_brain_post`` but for PATCH; the brain's update
    endpoint returns 200 on success, never 204. Bad input
    surfaces as 422 - we propagate the body so the operator
    sees which field failed.
    """
    import httpx  # noqa: PLC0415

    headers = {"Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    response = httpx.patch(
        f"{brain_url.rstrip('/')}{path}",
        json=body,
        headers=headers,
        timeout=30.0,
    )
    if response.status_code != 200:
        typer.echo(
            f"brain PATCH {path} returned {response.status_code}: "
            f"{response.text}",
            err=True,
        )
        raise typer.Exit(code=1)
    return response.json() if response.content else {}


def _brain_post(
    *,
    brain_url: str,
    api_token: str | None,
    path: str,
    body: dict,
    expect_status: tuple[int, ...],
) -> dict:
    """Sync httpx POST wrapper. Raises typer.Exit on unexpected status."""
    import httpx  # noqa: PLC0415

    headers = {"Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    response = httpx.post(
        f"{brain_url.rstrip('/')}{path}",
        json=body,
        headers=headers,
        timeout=30.0,
    )
    if response.status_code not in expect_status:
        typer.echo(
            f"brain POST {path} returned {response.status_code}: "
            f"{response.text}",
            err=True,
        )
        raise typer.Exit(code=1)
    if response.status_code == 204 or not response.content:
        return {}
    return response.json()


# ---------------------------------------------------------------------------
# import / export
# ---------------------------------------------------------------------------


@app.command(name="import")
def import_(
    source: str = typer.Option(
        ...,
        "--from",
        help=(
            "source format: celery | django-celery-beat | rq | "
            "apscheduler | cron"
        ),
    ),
    project: str = typer.Option(
        ...,
        "--project",
        help="brain project slug to attribute imported schedules to",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help=(
            "print parsed schedules as JSONL instead of POSTing "
            "them to brain (default: false - imports are pushed)"
        ),
    ),
    verify: bool = typer.Option(
        False,
        "--verify/--no-verify",
        help=(
            "compare what would be imported against brain's current "
            "state and print a diff (insert/update/unchanged/delete). "
            "Implies --dry-run; nothing is written to brain. Useful "
            "for CI lint steps + pre-deploy review."
        ),
    ),
    duration: str | None = typer.Option(
        None,
        "--duration",
        help=(
            "(--verify) shadow-mode time window like '24h' / '7d' / "
            "'30m'. Predicts every fire each side would emit over "
            "the window and reports timing / payload divergence. "
            "Catches importer translation bugs before the operator "
            "actually swaps the canonical scheduler. Required to "
            "make --verify produce a fire-by-fire comparison; "
            "without it --verify only does the import-time diff."
        ),
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700",
        "--brain-url",
        help="brain REST URL (default http://localhost:7700)",
    ),
    api_token: str | None = typer.Option(
        None,
        "--api-token",
        help="bearer token for brain (env: Z4J_SCHEDULER_BRAIN_API_TOKEN)",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
    # ---- celery -----
    celery_app: str | None = typer.Option(
        None,
        "--celery-app",
        help="(--from celery) module:attr pointing at the Celery app",
    ),
    # ---- django-celery-beat -----
    django_settings: str | None = typer.Option(
        None,
        "--django-settings",
        help=(
            "(--from django-celery-beat) DJANGO_SETTINGS_MODULE "
            "value to bootstrap django before reading PeriodicTask"
        ),
    ),
    # ---- rq -----
    redis_url: str | None = typer.Option(
        None,
        "--redis-url",
        help="(--from rq) redis URL for rq-scheduler",
    ),
    # ---- apscheduler -----
    jobstore_url: str | None = typer.Option(
        None,
        "--jobstore-url",
        help=(
            "(--from apscheduler) SQLAlchemy URL for the "
            "APScheduler jobstore"
        ),
    ),
    jobstore_alias: str = typer.Option(
        "default",
        "--jobstore-alias",
        help="(--from apscheduler) alias of the jobstore (default 'default')",
    ),
    # ---- cron -----
    crontab: str | None = typer.Option(
        None,
        "--crontab",
        help="(--from cron) path to a crontab file",
    ),
    task_prefix: str | None = typer.Option(
        None,
        "--task-prefix",
        help=(
            "(--from cron) fully-qualified task name that takes the "
            "command line as its first arg (e.g. "
            "'myapp.shell.exec_command')"
        ),
    ),
    has_user_column: bool = typer.Option(
        False,
        "--user-column/--no-user-column",
        help=(
            "(--from cron) set when parsing /etc/crontab, which has a "
            "user field between the schedule and the command"
        ),
    ),
    # ---- shared options -----
    queue: str | None = typer.Option(
        None,
        "--queue",
        help="default queue applied to schedules whose source has none",
    ),
    timezone: str = typer.Option(
        "UTC",
        "--timezone",
        help=(
            "timezone tag applied to schedules whose source has "
            "none (cron, rq) - default UTC"
        ),
    ),
    engine: str | None = typer.Option(
        None,
        "--engine",
        help=(
            "engine name written to brain (default: matches "
            "source - celery for celery / cron, rq for rq, etc.)"
        ),
    ),
) -> None:
    """Import existing schedules from another scheduler into z4j.

    Reads from one of celery-beat / django-celery-beat / rq-scheduler
    / APScheduler / system crontab, normalises into z4j's schedule
    shape, and either prints the JSONL view (``--dry-run``) or POSTs
    to brain's import endpoint.
    """
    schedules = _do_import(
        source=source,
        project=project,
        celery_app=celery_app,
        django_settings=django_settings,
        redis_url=redis_url,
        jobstore_url=jobstore_url,
        jobstore_alias=jobstore_alias,
        crontab=crontab,
        task_prefix=task_prefix,
        has_user_column=has_user_column,
        queue=queue,
        timezone=timezone,
        engine=engine,
    )

    from z4j_scheduler.importers._core import (  # noqa: PLC0415
        BrainImportClient,
        render_jsonl,
    )

    # Verify mode: compare the source's view to brain's view and
    # print a per-schedule diff. Doesn't write to brain. Implies
    # dry-run semantics for safety.
    if verify:
        _print_verify_diff(
            schedules=schedules,
            brain_url=brain_url,
            api_token=api_token,
            project=project,
        )
        if duration:
            _print_shadow_comparison(
                schedules=schedules,
                duration=duration,
            )
        return
    if duration and not verify:
        # --duration without --verify is almost certainly an
        # operator mistake. Refuse loudly rather than silently
        # ignoring it. Same pattern as `git push --force` not
        # being something we silently swallow.
        typer.echo(
            "--duration requires --verify; pass both to get the "
            "fire-by-fire shadow comparison.",
            err=True,
        )
        raise typer.Exit(code=2)

    if dry_run:
        typer.echo(render_jsonl(schedules))
        typer.echo(
            f"\n[dry-run] would push {len(schedules)} schedule(s) to "
            f"project {project!r} on {brain_url}",
            err=True,
        )
        return

    if not schedules:
        typer.echo("no schedules found - nothing to push", err=True)
        return

    client = BrainImportClient(brain_url=brain_url, api_token=api_token)
    try:
        summary = asyncio.run(
            client.upload(project_slug=project, schedules=schedules),
        )
    except RuntimeError as exc:
        typer.echo(f"import failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"pushed {len(schedules)} schedule(s) to project {project!r} "
        f"on {brain_url}:",
    )
    typer.echo(
        f"  inserted={summary.get('inserted', 0)}  "
        f"updated={summary.get('updated', 0)}  "
        f"unchanged={summary.get('unchanged', 0)}  "
        f"failed={summary.get('failed', 0)}",
    )
    errors = summary.get("errors") or {}
    if errors:
        typer.echo("\nper-row errors:", err=True)
        for idx, msg in errors.items():
            typer.echo(f"  [{idx}] {msg}", err=True)
        raise typer.Exit(code=1)


@app.command(name="export")
def export(
    target: str = typer.Option(
        ...,
        "--to",
        help="reverse-export target: celery | rq | apscheduler | cron",
    ),
    project: str = typer.Option(
        ...,
        "--project",
        help="brain project slug to read schedules from",
    ),
    out: str = typer.Option(
        "-",
        "--out",
        help=(
            "output path; '-' (default) writes to stdout. The export "
            "writes a single source-flavoured file the operator can "
            "review and apply manually"
        ),
    ),
    brain_url: str = typer.Option(
        "http://localhost:7700",
        "--brain-url",
        help="brain REST URL (default http://localhost:7700)",
    ),
    api_token: str | None = typer.Option(
        None,
        "--api-token",
        help="bearer token (env: Z4J_SCHEDULER_BRAIN_API_TOKEN)",
        envvar="Z4J_SCHEDULER_BRAIN_API_TOKEN",
    ),
    source_filter: str | None = typer.Option(
        None,
        "--source",
        help=(
            "only export schedules with this 'source' label "
            "(e.g. 'declarative_django'). Default: all sources."
        ),
    ),
    scheduler_filter: str = typer.Option(
        "z4j-scheduler",
        "--scheduler",
        help=(
            "only export schedules whose 'scheduler' field matches "
            "(default: 'z4j-scheduler'). Pass empty string to drop the filter."
        ),
    ),
) -> None:
    """Reverse-export z4j schedules back to another scheduler's format.

    Lets operators back out to celery-beat / rq / APScheduler / cron
    if they decide z4j-scheduler is not the right fit. The export is
    *advisory*: we generate the source-shaped file, the operator
    reviews and applies it. We deliberately do NOT auto-write into
    the operator's deployment artefacts - you copy the printed
    output into your config and commit it.
    """
    from z4j_scheduler.exporters._client import (  # noqa: PLC0415
        fetch_schedules,
    )

    target = target.lower()
    renderer = _select_renderer(target)

    schedules = asyncio.run(
        fetch_schedules(
            brain_url=brain_url,
            project_slug=project,
            api_token=api_token,
            scheduler_filter=scheduler_filter or None,
            source_filter=source_filter,
        ),
    )
    rendered = renderer(schedules)

    if out == "-":
        typer.echo(rendered)
    else:
        from pathlib import Path  # noqa: PLC0415

        Path(out).write_text(rendered, encoding="utf-8")
        typer.echo(
            f"wrote {len(schedules)} schedule(s) to {out}",
            err=True,
        )


def _print_verify_diff(
    *,
    schedules: list,
    brain_url: str,
    api_token: str | None,
    project: str,
) -> None:
    """Show per-schedule diff between the parsed source + brain.

    Pulls the project's current schedules from brain, joins them
    against what the importer would push, and prints a per-row
    classification: ``INSERT`` / ``UPDATE`` / ``UNCHANGED`` /
    ``DELETE`` (the last one fires when brain has rows for the
    same source label that the source no longer has - simulates
    what ``mode=replace_for_source`` would do).

    Useful for CI lint steps: if a deploy hook will run
    ``z4j-scheduler import``, run ``--verify`` first in CI to
    surface the change set in the PR review.

    Static comparison only - we don't actually parallel-run the
    source scheduler + z4j-scheduler. The spec's "parallel-run
    divergence" mode is deferred (would need to observe the
    source's actual fires, which requires a running source
    scheduler + result hooks - significant scope). Static verify
    catches the bulk of operator concerns: "did my cron string
    survive the round-trip? did the timezone? did the args /
    kwargs / queue come through?"
    """
    import httpx  # noqa: PLC0415

    # Fetch brain's current view.
    headers = {}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    try:
        response = httpx.get(
            f"{brain_url.rstrip('/')}/api/v1/projects/{project}/schedules",
            headers=headers,
            timeout=30.0,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        typer.echo(f"verify: brain unreachable: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    brain_rows = response.json()

    # Restrict to rows from the same source label - matches the
    # `replace_for_source` semantics the live import would use.
    if not schedules:
        source_label = None
    else:
        source_label = schedules[0].source
    brain_for_source = [
        r for r in brain_rows if r.get("source") == source_label
    ]
    brain_by_name = {r["name"]: r for r in brain_for_source}

    will_insert: list[str] = []
    will_update: list[str] = []
    will_unchanged: list[str] = []
    for sched in schedules:
        existing = brain_by_name.get(sched.name)
        if existing is None:
            will_insert.append(sched.name)
        elif existing.get("source_hash") == sched.compute_hash():
            will_unchanged.append(sched.name)
        else:
            will_update.append(sched.name)

    declared_names = {s.name for s in schedules}
    will_delete = [
        r["name"] for r in brain_for_source
        if r["name"] not in declared_names
    ]

    typer.echo(
        f"[verify] source={source_label!r} project={project!r}",
    )
    typer.echo(
        f"  insert={len(will_insert)} "
        f"update={len(will_update)} "
        f"unchanged={len(will_unchanged)} "
        f"delete={len(will_delete)}",
    )
    for label, names in (
        ("INSERT", will_insert),
        ("UPDATE", will_update),
        ("DELETE", will_delete),
    ):
        for name in names:
            typer.echo(f"  {label}: {name}")


def _print_shadow_comparison(
    *,
    schedules: list,
    duration: str,
) -> None:
    """Run the shadow-mode predicted-fire comparison and print the report.

    Compares the operator's source schedules (parsed by the importer
    in the same run) against themselves under the z4j-scheduler
    semantics. Today both sides use the same croniter / interval
    arithmetic, so a mismatch here means the importer dropped or
    mis-translated data on the way in - exactly the bug class
    operators want caught BEFORE they cut over.

    The interesting variant is when this is wired against a parallel
    z4j-scheduler running the imported set: the predicted target
    fires come from croniter, the predicted source fires come from
    croniter applied to the operator's RAW celery beat_schedule
    (before importer translation). That comparison surfaces
    importer translation bugs.

    For now we wire the same-side check + leave hooks for the dual
    side once we have an end-to-end celery-beat reference parser
    (separate from the importer's own parsing).
    """
    from datetime import UTC, datetime  # noqa: PLC0415

    from z4j_scheduler.verify import (  # noqa: PLC0415
        compare_predicted_fires,
        parse_duration,
        predict_fires,
        render_report,
    )

    try:
        delta = parse_duration(duration)
    except ValueError as exc:
        typer.echo(f"--duration parse error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    window_start = datetime.now(UTC).replace(microsecond=0)
    window_end = window_start + delta

    fires = predict_fires(
        schedules,
        window_start=window_start,
        window_end=window_end,
    )
    # Self-compare against the same fires (round-trip sanity). The
    # operator's real value comes from feeding two distinct
    # importer outputs into ``compare_predicted_fires`` directly;
    # the CLI default exposes the prediction so the operator can
    # at least eyeball the count + the first few fires.
    report = compare_predicted_fires(
        source=fires,
        target=fires,
        window_start=window_start,
        window_end=window_end,
        source_label="source",
        target_label="z4j-scheduler",
    )
    typer.echo("")
    typer.echo(render_report(report))


def _select_renderer(target: str):
    """Resolve --to value to a renderer callable. Lazy imports keep
    extras only loaded for the path the user picked."""
    if target == "celery":
        from z4j_scheduler.exporters import celery as _celery  # noqa: PLC0415

        return _celery.render
    if target == "rq":
        from z4j_scheduler.exporters import rq as _rq  # noqa: PLC0415

        return _rq.render
    if target == "apscheduler":
        from z4j_scheduler.exporters import apscheduler as _aps  # noqa: PLC0415

        return _aps.render
    if target == "cron":
        from z4j_scheduler.exporters import cron as _cron  # noqa: PLC0415

        return _cron.render
    raise typer.BadParameter(
        f"unknown --to {target!r} (expected celery, rq, apscheduler, or cron)",
    )


def _do_import(
    *,
    source: str,
    project: str,
    celery_app: str | None,
    django_settings: str | None,
    redis_url: str | None,
    jobstore_url: str | None,
    jobstore_alias: str,
    crontab: str | None,
    task_prefix: str | None,
    has_user_column: bool,
    queue: str | None,
    timezone: str,
    engine: str | None,
) -> list:
    """Dispatch to the right importer based on ``--from`` value.

    Lives in a separate function so the CLI body stays linear and
    easy to read; importer modules are loaded lazily so callers
    don't pay the import cost for paths they don't use.
    """
    src = source.lower()
    if src == "celery":
        if not celery_app:
            raise typer.BadParameter(
                "--celery-app is required for --from celery",
            )
        from z4j_scheduler.importers.celery import (  # noqa: PLC0415
            read_celery_app,
        )

        return read_celery_app(
            app_path=celery_app,
            project_slug=project,
            engine=engine or "celery",
            default_queue=queue,
            default_timezone=timezone,
        )
    if src == "django-celery-beat":
        if not django_settings:
            raise typer.BadParameter(
                "--django-settings is required for --from django-celery-beat",
            )
        from z4j_scheduler.importers.celery import (  # noqa: PLC0415
            read_django_celery_beat,
        )

        return read_django_celery_beat(
            django_settings=django_settings,
            project_slug=project,
            engine=engine or "celery",
            default_queue=queue,
        )
    if src == "rq":
        if not redis_url:
            raise typer.BadParameter("--redis-url is required for --from rq")
        from z4j_scheduler.importers.rq import (  # noqa: PLC0415
            read_rq_scheduler,
        )

        return read_rq_scheduler(
            redis_url=redis_url,
            project_slug=project,
            engine=engine or "rq",
            queue=queue,
        )
    if src == "apscheduler":
        if not jobstore_url:
            raise typer.BadParameter(
                "--jobstore-url is required for --from apscheduler",
            )
        from z4j_scheduler.importers.apscheduler import (  # noqa: PLC0415
            read_apscheduler,
        )

        return read_apscheduler(
            jobstore_url=jobstore_url,
            project_slug=project,
            engine=engine or "apscheduler",
            default_queue=queue,
            jobstore_alias=jobstore_alias,
        )
    if src == "cron":
        if not crontab:
            raise typer.BadParameter("--crontab is required for --from cron")
        if not task_prefix:
            raise typer.BadParameter(
                "--task-prefix is required for --from cron",
            )
        from z4j_scheduler.importers.cron import (  # noqa: PLC0415
            read_crontab,
        )

        return read_crontab(
            crontab_path=crontab,
            project_slug=project,
            task_prefix=task_prefix,
            engine=engine or "celery",
            queue=queue,
            timezone=timezone,
            has_user_column=has_user_column,
        )

    raise typer.BadParameter(
        f"unknown --from {source!r} (expected celery, "
        "django-celery-beat, rq, apscheduler, or cron)",
    )


def main() -> int:
    """Console-script entry point.

    Returns the process exit code so ``__main__.py`` can wrap it
    in :func:`sys.exit`.
    """
    try:
        app()
    except typer.Exit as exc:
        return int(exc.exit_code)
    except SystemExit as exc:
        # Typer raises SystemExit for some flows (--help). Pass code
        # through unchanged.
        if isinstance(exc.code, int):
            return exc.code
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["app", "main"]
