"""Reverse-export tools.

Read the schedule set from brain and emit a source-shaped file the
operator can review and apply manually. The exit ramp from
z4j-scheduler back to celery-beat / rq-scheduler / APScheduler /
system crontab / Huey / arq / taskiq.

We deliberately do NOT auto-write into the operator's deployment
artifacts. The export is advisory: print a Python module / crontab
file to stdout (or a chosen path), the operator reviews + commits.
That keeps z4j honest about ownership boundaries and gives the
operator a clean revert path.

Submodules:

- :mod:`~z4j_scheduler.exporters._client` - REST client that
  fetches the schedule list from brain
- :mod:`~z4j_scheduler.exporters.celery` - render as
  ``celery.schedules.crontab`` / ``timedelta`` beat config
- :mod:`~z4j_scheduler.exporters.rq` - render as ``rq_scheduler``
  Python script
- :mod:`~z4j_scheduler.exporters.apscheduler` - render as
  APScheduler ``add_job`` calls
- :mod:`~z4j_scheduler.exporters.cron` - render as a system
  crontab file (with operator wrapper-script note)
- :mod:`~z4j_scheduler.exporters.huey` - render as
  ``@huey.periodic_task(crontab(...))`` registrations
- :mod:`~z4j_scheduler.exporters.arq` - render as
  ``WorkerSettings.cron_jobs`` entries
- :mod:`~z4j_scheduler.exporters.taskiq` - render as ``schedule``
  label attachments
- :mod:`~z4j_scheduler.exporters.dramatiq` - guidance only (Dramatiq
  has no native scheduler config)

CLI entry point: ``z4j-scheduler export --to <target>`` (see
:func:`z4j_scheduler.cli.export`).
"""

from __future__ import annotations
