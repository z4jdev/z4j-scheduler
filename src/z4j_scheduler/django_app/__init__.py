"""z4j-scheduler Django app shim.

Add ``"z4j_scheduler.django_app"`` to ``INSTALLED_APPS`` to:

1. Pick up the ``manage.py z4j_schedules sync/list/diff/trigger``
   commands.
2. Optionally trigger declarative reconciliation on Django startup
   via the existing ``AppConfig.ready`` hook.

Settings the app reads (none are auto-imported by Django; see the
docstrings in ``apps.py`` + the management command):

- ``Z4J_SCHEDULES`` - list of :class:`ScheduleSpec` (required)
- ``Z4J_SCHEDULES_PROJECT`` - brain project slug (required)
- ``Z4J_SCHEDULES_BRAIN_URL`` (default ``http://brain:7700``)
- ``Z4J_SCHEDULES_API_TOKEN`` - bearer token
- ``Z4J_SCHEDULES_SOURCE`` - source label (default ``"declarative_django"``)
- ``Z4J_SCHEDULES_AUTO_RECONCILE`` - bool; when True the AppConfig's
  ``ready`` hook fires reconcile_from_settings on startup. Default
  False - operators usually prefer to run sync from a deploy hook
  rather than on every web-worker boot.
"""

from __future__ import annotations

default_app_config = "z4j_scheduler.django_app.apps.Z4JSchedulerConfig"
