"""Migration importers - one-shot CLI subcommands that read existing
scheduler definitions and push them into brain's schedules table.

Each importer:

1. Reads source format (celery-beat config, rq-scheduler Redis
   sorted set, APScheduler jobstore, system crontab)
2. Maps to z4j Schedule shape (cron / interval / one_shot)
3. Writes to brain via the existing schedule REST API
4. Tags rows with ``source=imported_<tool>`` for audit visibility

Both ``--dry-run`` and ``--verify`` modes are supported on every
importer. ``--verify`` watches both the original and the new
scheduler in parallel for a configurable duration and reports any
divergence in fire timing, args, queues, or results.

A reverse migration tool (``z4j-scheduler export --to <tool>``)
lives here too - lets operators back out to celery-beat / rq /
apscheduler if they decide z4j-scheduler is not the right fit.

Submodules:

- :mod:`~z4j_scheduler.importers.celery` - celery-beat + django-celery-beat
- :mod:`~z4j_scheduler.importers.rq` - rq-scheduler
- :mod:`~z4j_scheduler.importers.apscheduler` - APScheduler jobstores
- :mod:`~z4j_scheduler.importers.cron` - system crontab
"""
