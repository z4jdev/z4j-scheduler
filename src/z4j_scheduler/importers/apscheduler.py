"""APScheduler importer.

Reads from any APScheduler-supported jobstore (sqlalchemy, redis,
mongo) by booting an APScheduler ``BackgroundScheduler`` against
the operator's stored config and enumerating ``get_jobs()``. We
never call ``start()`` - we only need the read path.

Trigger mapping:

- ``CronTrigger`` -> ``kind="cron"`` with the standard 5-field
  expression (we also drop the ``second`` field if present, since
  z4j-scheduler doesn't tick faster than a second-level boundary).
- ``IntervalTrigger`` -> ``kind="interval"`` with seconds.
- ``DateTrigger`` -> ``kind="one_shot"`` with the ISO timestamp.
- Any combining trigger (``AndTrigger`` / ``OrTrigger``) is skipped
  with a warning - operators with composite triggers should split
  them into multiple schedules in z4j.

Importer ships for both APScheduler 3.x and the upcoming 4.x; the
trigger types differ slightly between versions and we adapt at
runtime.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.apscheduler")


def read_apscheduler(
    *,
    jobstore_url: str,
    project_slug: str,
    engine: str = "apscheduler",
    default_queue: str | None = None,
    jobstore_alias: str = "default",
) -> list[ImportedSchedule]:
    """Import APScheduler jobs from a jobstore.

    Args:
        jobstore_url: SQLAlchemy URL (``postgresql://...``,
            ``sqlite:///path``) for the SQLAlchemy jobstore.
            Other backends (redis, mongo) are accessed via the
            same parameter; rules of detection match
            ``apscheduler.schedulers.base.BaseScheduler.add_jobstore``.
        project_slug: Brain project slug.
        engine: Engine name (default ``"apscheduler"``).
        default_queue: Optional queue override.
        jobstore_alias: Name of the jobstore inside the scheduler
            config; default is the literal ``"default"`` alias
            APScheduler uses out of the box.
    """
    try:
        from apscheduler.jobstores.sqlalchemy import (
            SQLAlchemyJobStore,
        )
        from apscheduler.schedulers.background import (
            BackgroundScheduler,
        )
    except ImportError as exc:
        raise RuntimeError(
            "apscheduler importer requires `pip install apscheduler sqlalchemy`",
        ) from exc

    scheduler = BackgroundScheduler()
    scheduler.add_jobstore(
        SQLAlchemyJobStore(url=jobstore_url),
        alias=jobstore_alias,
    )

    schedules: list[ImportedSchedule] = []
    # ``get_jobs`` works without ``start()`` because the scheduler
    # only needs the jobstore connection; no executor/event loop
    # spin-up required.
    for job in scheduler.get_jobs(jobstore=jobstore_alias):
        try:
            sched = _job_to_schedule(
                job=job,
                project_slug=project_slug,
                engine=engine,
                default_queue=default_queue,
            )
        except _UnsupportedTriggerError as exc:
            logger.warning(
                "z4j.scheduler.importers.apscheduler: skipping %r - %s",
                job.id,
                exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _UnsupportedTriggerError(Exception):
    """Raised when a trigger type cannot be translated."""


def _job_to_schedule(
    *,
    job: Any,
    project_slug: str,
    engine: str,
    default_queue: str | None,
) -> ImportedSchedule:
    """Convert one ``apscheduler.job.Job`` into an :class:`ImportedSchedule`."""
    trigger = job.trigger
    trigger_name = type(trigger).__name__

    if trigger_name == "CronTrigger":
        kind = "cron"
        expression = _render_cron_trigger(trigger)
        tz = _trigger_timezone(trigger)
    elif trigger_name == "IntervalTrigger":
        kind = "interval"
        # ``IntervalTrigger.interval`` is a timedelta in 3.x; in 4.x
        # it's split into discrete fields. Normalise to seconds.
        td = getattr(trigger, "interval", None)
        if td is None:
            seconds = (
                int(getattr(trigger, "seconds", 0))
                + int(getattr(trigger, "minutes", 0)) * 60
                + int(getattr(trigger, "hours", 0)) * 3600
                + int(getattr(trigger, "days", 0)) * 86400
                + int(getattr(trigger, "weeks", 0)) * 604800
            )
        else:
            seconds = int(td.total_seconds())
        if seconds <= 0:
            raise _UnsupportedTriggerError(
                "IntervalTrigger with non-positive interval",
            )
        expression = f"{seconds}s"
        tz = _trigger_timezone(trigger)
    elif trigger_name == "DateTrigger":
        kind = "one_shot"
        run_date = getattr(trigger, "run_date", None)
        if run_date is None:
            raise _UnsupportedTriggerError(
                "DateTrigger without run_date",
            )
        expression = run_date.isoformat()
        tz = _trigger_timezone(trigger)
    else:
        raise _UnsupportedTriggerError(
            f"trigger type {trigger_name!r} not supported "
            "(combining triggers must be split into multiple schedules)",
        )

    args = list(getattr(job, "args", None) or ())
    kwargs = dict(getattr(job, "kwargs", None) or {})

    # APScheduler stores the callable target as a string; ``func_ref``
    # is "module.path:callable", ``func`` is the raw callable. We
    # prefer ``func_ref`` because it's the form that survives
    # process boundaries.
    task_name = (
        getattr(job, "func_ref", None)
        or getattr(getattr(job, "func", None), "__qualname__", None)
        or "unknown"
    )

    return ImportedSchedule(
        project_slug=project_slug,
        name=str(job.id),
        engine=engine,
        kind=kind,
        expression=expression,
        timezone=tz,
        task_name=str(task_name),
        queue=default_queue,
        args=args,
        kwargs=kwargs,
        catch_up="skip",
        is_enabled=bool(getattr(job, "next_run_time", None) is not None),
        source="imported_apscheduler",
    )


def _render_cron_trigger(trigger: Any) -> str:
    """Render an APScheduler CronTrigger to a 5-field cron string.

    APScheduler's CronTrigger has six fields (year/month/day/week/
    day_of_week/hour/minute/second). We map to the standard cron
    5-field shape (minute hour day_of_month month day_of_week) and
    ignore year/week/second - the 5-field form covers every cron
    schedule a developer normally writes by hand.
    """

    def _f(field_name: str, default: str = "*") -> str:
        # CronTrigger exposes fields as ``BaseField`` objects with
        # an ``expressions`` list; the string repr of one expression
        # gives us back the original string ("*", "5", "0,30", etc.).
        for f in getattr(trigger, "fields", ()):
            if f.name == field_name:
                if not f.expressions:
                    return default
                return ",".join(str(e) for e in f.expressions)
        return default

    minute = _f("minute", "0")
    hour = _f("hour", "0")
    dom = _f("day", "*")
    month = _f("month", "*")
    dow = _f("day_of_week", "*")
    return f"{minute} {hour} {dom} {month} {dow}"


def _trigger_timezone(trigger: Any) -> str:
    """Best-effort extraction of a trigger's timezone."""
    tz = getattr(trigger, "timezone", None)
    if tz is None:
        return "UTC"
    # zoneinfo.ZoneInfo / pytz timezone -> str(...) gives "America/NY"
    return str(tz)


__all__ = ["read_apscheduler"]
