"""z4j-scheduler - the modern Python scheduler in the z4j stack.

Engine-agnostic, dynamic-CRUD, HA-ready scheduler. Replaces
celery-beat / rq-scheduler / APScheduler with a single tool that
fires scheduled tasks at any Python task queue (Celery, RQ,
Dramatiq, arq, taskiq, huey) via the existing z4j agent network.

z4j-scheduler does not execute tasks. It does not import a queue
engine. It does not run inside the user's application process. It
is a standalone async-Python process that maintains a set of
schedules, ticks them at the right time, and tells z4j-brain "fire
schedule X now." The brain dispatches that fire as a normal command
to the appropriate agent, which calls the engine adapter's
``submit_task`` primitive.

Architecturally this package is a member of the z4j stack, not a
free-standing alternative to celery-beat for non-z4j projects. It
requires z4j-brain to function.

Public API surface:

- :class:`~z4j_scheduler.settings.Settings` - 12-factor config
- :class:`~z4j_scheduler.main.SchedulerApp` - the application factory
- :func:`~z4j_scheduler.cli.main` - typer CLI entry point

See ``docs/SCHEDULER.md`` in the z4j monorepo for the full
specification.

Licensed under Apache License 2.0.
"""

from __future__ import annotations

from z4j_scheduler.version import __version__

__all__ = ["__version__"]
