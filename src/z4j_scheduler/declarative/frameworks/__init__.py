"""Framework-specific helpers for declarative reconciliation.

The core :func:`z4j_scheduler.declarative.reconcile` is framework-
agnostic - it accepts a Python list/dict of :class:`ScheduleSpec`
and POSTs to brain. These submodules wrap that core for the three
frameworks z4j supports out of the box, so app code reads the
schedule definitions from the framework's natural location and
calls one helper at the right startup hook:

- :mod:`~z4j_scheduler.declarative.frameworks.django` reads
  ``settings.Z4J_SCHEDULES`` (a list of ScheduleSpec) and is
  called from ``AppConfig.ready()`` or via
  ``manage.py z4j_schedules sync``.
- :mod:`~z4j_scheduler.declarative.frameworks.fastapi` returns a
  lifespan context manager that runs reconcile once on startup.
- :mod:`~z4j_scheduler.declarative.frameworks.flask` registers a
  ``before_first_request`` handler.

All three are thin shims over :func:`reconcile_sync` /
:func:`reconcile`. The framework helpers exist so users don't
have to remember "where do I put the asyncio.run() call in a
Django app?" - the answer is right here.
"""

from __future__ import annotations
