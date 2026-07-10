"""Django integration for declarative schedule reconciliation.

Two entry points the operator wires into their Django project:

1. :func:`reconcile_from_settings` - call from
   ``AppConfig.ready()`` to sync on app startup. Idempotent;
   re-running is a no-op when nothing changed.

2. ``manage.py z4j_schedules sync|list|diff`` (Django command) -
   the operator runs this from CI / a deploy hook. Same code
   path as :func:`reconcile_from_settings` but with a CLI for
   ad-hoc invocation.

Settings read
-------------

Both entry points read these from ``django.conf.settings``:

- ``Z4J_SCHEDULES`` - list of :class:`ScheduleSpec`. Required.
- ``Z4J_SCHEDULES_PROJECT`` - brain project slug. Required.
- ``Z4J_SCHEDULES_BRAIN_URL`` - brain REST URL (defaults to
  ``http://brain:7700``).
- ``Z4J_SCHEDULES_API_TOKEN`` - bearer token (or set via
  ``Z4J_SCHEDULES_API_TOKEN`` env var). The Django settings path
  is preferred so the token lives next to other secrets the
  project already manages.
- ``Z4J_SCHEDULES_SOURCE`` - source label (defaults to
  ``"declarative_django"``). Override when the same Django app
  manages multiple schedule sets.

Example
-------

In ``settings.py``::

    from z4j_scheduler.declarative import ScheduleSpec

    Z4J_SCHEDULES_PROJECT = "acme-prod"
    Z4J_SCHEDULES_BRAIN_URL = "https://brain.example.com"
    Z4J_SCHEDULES_API_TOKEN = os.environ["Z4J_API_TOKEN"]

    Z4J_SCHEDULES = [
        ScheduleSpec(
            name="hourly-cleanup",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.cleanup",
        ),
    ]

In ``apps.py``::

    from django.apps import AppConfig


    class MyAppConfig(AppConfig):
        name = "myapp"

        def ready(self):
            from z4j_scheduler.declarative.frameworks.django import (
                reconcile_from_settings,
            )

            reconcile_from_settings()
"""

from __future__ import annotations

import logging

logger = logging.getLogger("z4j.scheduler.declarative.django")


def reconcile_from_settings() -> dict[str, int] | None:
    """Read ``settings.Z4J_SCHEDULES`` + reconcile against brain.

    Returns the brain summary dict (``inserted`` / ``updated`` /
    ``unchanged`` / ``deleted`` / ``failed``), or ``None`` when
    Django is not importable (the helper is a no-op outside Django
    so the call site can stay simple).

    Safe to call from ``AppConfig.ready()``: failures are logged
    but never raised, so a brain outage during deploy doesn't
    crash the Django app's startup.
    """
    try:
        from django.conf import settings as django_settings
    except ImportError:
        logger.debug(
            "z4j.scheduler.declarative.django: django not installed; "
            "skipping reconcile_from_settings",
        )
        return None

    schedules = getattr(django_settings, "Z4J_SCHEDULES", None)
    if not schedules:
        # Empty list IS a meaningful state (= delete all
        # declarative_django schedules in this project) but the
        # ``None`` / unset case is "operator hasn't wired anything
        # yet" so we skip.
        logger.info(
            "z4j.scheduler.declarative.django: Z4J_SCHEDULES unset; no reconcile performed",
        )
        return None

    project = getattr(django_settings, "Z4J_SCHEDULES_PROJECT", None)
    if not project:
        logger.error(
            "z4j.scheduler.declarative.django: Z4J_SCHEDULES_PROJECT not set; cannot reconcile",
        )
        return None

    brain_url = getattr(
        django_settings,
        "Z4J_SCHEDULES_BRAIN_URL",
        "http://brain:7700",
    )
    api_token = getattr(
        django_settings,
        "Z4J_SCHEDULES_API_TOKEN",
        None,
    )
    source = getattr(
        django_settings,
        "Z4J_SCHEDULES_SOURCE",
        "declarative_django",
    )

    try:
        from z4j_scheduler.declarative import reconcile_sync

        return reconcile_sync(
            schedules=schedules,
            project=project,
            source=source,
            brain_url=brain_url,
            api_token=api_token,
        )
    except Exception:
        # Never crash Django startup over a brain outage. The
        # operator sees the error in the Django logs; the next
        # deploy / restart retries.
        logger.exception(
            "z4j.scheduler.declarative.django: reconcile failed",
        )
        return None


__all__ = ["reconcile_from_settings"]
