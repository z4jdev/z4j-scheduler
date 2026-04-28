"""Flask integration for declarative schedule reconciliation.

Flask's startup hooks differ enough from Django/FastAPI that the
helper takes a different shape: a function the operator calls to
register a CLI command + an explicit one-shot reconciler the
operator invokes from their app factory.

There is no ``before_first_request`` hook in modern Flask (it
was deprecated and removed in 2.3+), so we recommend running
reconcile from the app factory at boot time::

    from flask import Flask
    from z4j_scheduler.declarative import ScheduleSpec
    from z4j_scheduler.declarative.frameworks.flask import (
        register_z4j_schedules,
    )

    SCHEDULES = [
        ScheduleSpec(
            name="hourly-cleanup",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.cleanup",
        ),
    ]

    def create_app() -> Flask:
        app = Flask(__name__)
        register_z4j_schedules(
            app,
            schedules=SCHEDULES,
            project="acme-prod",
            brain_url="https://brain.example.com",
            api_token=os.environ["Z4J_API_TOKEN"],
        )
        return app

The function reconciles immediately on call (so the schedules are
in brain by the time the app starts serving) and registers a
``flask z4j-schedules-sync`` CLI for ad-hoc re-runs.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.declarative._reconciler import (
    ScheduleSpec,
    reconcile_sync,
)

logger = logging.getLogger("z4j.scheduler.declarative.flask")


def register_z4j_schedules(
    app: Any,  # flask.Flask
    *,
    schedules: list[ScheduleSpec] | dict[str, ScheduleSpec],
    project: str,
    source: str = "declarative_flask",
    brain_url: str = "http://brain:7700",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    sync_now: bool = True,
) -> None:
    """Register schedules with brain + add a Flask CLI for re-runs.

    Args:
        app: The Flask application instance.
        schedules / project / source / brain_url / api_token /
            timeout_seconds: Forwarded to :func:`reconcile_sync`.
        sync_now: When True (default), call reconcile immediately so
            schedules are in brain by the time the first request
            arrives. Set False to register the CLI command only -
            useful when the operator wants explicit control over
            timing (e.g. only sync from a deploy-hook script).

    The CLI command lands as ``flask z4j-schedules-sync`` and
    re-uses the same parameters captured here.
    """
    if sync_now:
        try:
            summary = reconcile_sync(
                schedules=schedules,
                project=project,
                source=source,
                brain_url=brain_url,
                api_token=api_token,
                timeout_seconds=timeout_seconds,
            )
            logger.info(
                "z4j.scheduler.declarative.flask: reconcile complete %r",
                summary,
            )
        except Exception:  # noqa: BLE001
            # Never crash Flask startup over a brain outage.
            logger.exception(
                "z4j.scheduler.declarative.flask: reconcile failed; "
                "app will continue",
            )

    # Register the CLI command. ``app.cli.command`` requires Flask >=
    # 1.0. Failing import (very old Flask) is silent - the operator
    # gets the in-app reconcile but no CLI.
    try:
        @app.cli.command("z4j-schedules-sync")
        def _z4j_sync_cmd() -> None:
            """Re-run the z4j-scheduler reconcile from the CLI."""
            summary = reconcile_sync(
                schedules=schedules,
                project=project,
                source=source,
                brain_url=brain_url,
                api_token=api_token,
                timeout_seconds=timeout_seconds,
            )
            print(f"z4j reconcile: {summary}")
    except Exception:  # noqa: BLE001
        logger.debug(
            "z4j.scheduler.declarative.flask: could not register CLI",
            exc_info=True,
        )


__all__ = ["register_z4j_schedules"]
