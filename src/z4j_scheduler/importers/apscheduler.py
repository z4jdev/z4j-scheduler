"""APScheduler importer.

Reads APScheduler 3.x ``SQLAlchemyJobStore`` data by booting a
``BackgroundScheduler`` against the operator's SQLAlchemy URL. The
scheduler is started in its paused state so the persistent job store is
loaded without executing jobs, then shut down after enumeration.

Trigger mapping:

- ``CronTrigger`` -> ``kind="cron"`` with the standard 5-field
  expression, but only when APScheduler and z4j/croniter give that
  expression the same meaning. Unsupported dimensions and incompatible
  day constraints are refused instead of silently changing cadence.
- ``IntervalTrigger`` -> ``kind="interval"`` with seconds.
- ``DateTrigger`` -> ``kind="clocked"`` with the ISO timestamp.
- Any combining trigger (``AndTrigger`` / ``OrTrigger``) is skipped
  with a warning - operators with composite triggers should split
  them into multiple schedules in z4j.

The package pins APScheduler to ``>=3.10,<4``. APScheduler 4 uses a
different scheduler and datastore API and is not supported by this
importer.
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
            ``sqlite:///path``) for the SQLAlchemy jobstore. Redis and
            MongoDB jobstores are not accepted by this importer.
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
        from sqlalchemy.pool import NullPool
    except ImportError as exc:
        raise RuntimeError(
            "apscheduler importer requires `pip install apscheduler sqlalchemy`",
        ) from exc

    scheduler = BackgroundScheduler()
    scheduler.add_jobstore(
        # This importer is a one-shot reader. Avoid retaining a connection
        # pool after enumeration; APScheduler shuts the jobstore down below.
        SQLAlchemyJobStore(
            url=jobstore_url,
            engine_options={"poolclass": NullPool},
        ),
        alias=jobstore_alias,
    )

    schedules: list[ImportedSchedule] = []
    started = False
    try:
        # A stopped APScheduler exposes only jobs queued locally before start;
        # it has not started the persistent job store. ``paused=True`` loads
        # stored jobs while keeping the processing loop from executing them.
        scheduler.start(paused=True)
        started = True
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
    finally:
        if started:
            scheduler.shutdown()


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
        # ``IntervalTrigger.interval`` is a timedelta in APScheduler 3.x.
        # Retain a discrete-field fallback for compatible wrappers and
        # test doubles, then normalise either shape to seconds.
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
        kind = "clocked"
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

    This is deliberately conservative. APScheduler has dimensions that
    five-field cron cannot carry, combines day-of-month and day-of-week with
    AND while croniter defaults to OR, and numbers Monday as day zero while
    croniter numbers Sunday as day zero. Only forms with equivalent semantics
    are rendered; the rest are rejected with an actionable reason.
    """

    modifiers = [
        name
        for name in ("start_date", "end_date", "jitter")
        if getattr(trigger, name, None) is not None
    ]
    if modifiers:
        raise _UnsupportedTriggerError(
            "CronTrigger cannot preserve these APScheduler-only modifiers: " + ", ".join(modifiers)
        )

    fields = {field.name: field for field in getattr(trigger, "fields", ())}

    def _f(field_name: str, default: str = "*") -> str:
        # CronTrigger exposes fields as ``BaseField`` objects with
        # an ``expressions`` list; the string repr of one expression
        # gives us back the original string ("*", "5", "0,30", etc.).
        field = fields.get(field_name)
        if field is not None:
            if not field.expressions:
                return default
            return ",".join(str(e) for e in field.expressions)
        return default

    year = _f("year", "*")
    week = _f("week", "*")
    second = _f("second", "0")
    unsupported: list[str] = []
    if year != "*":
        unsupported.append(f"year={year!r} (must be '*')")
    if week != "*":
        unsupported.append(f"week={week!r} (must be '*')")
    if second != "0":
        unsupported.append(f"second={second!r} (must be '0')")
    if unsupported:
        detail = ", ".join(unsupported)
        raise _UnsupportedTriggerError(
            f"CronTrigger cannot be represented by five-field cron without broadening it: {detail}"
        )

    minute = _f("minute", "0")
    hour = _f("hour", "0")
    dom = _f("day", "*")
    month = _f("month", "*")
    dow = _f("day_of_week", "*")

    day_field = fields.get("day")
    if day_field is not None:
        special_day_expressions = [
            str(expression)
            for expression in day_field.expressions
            if type(expression).__name__
            in {"LastDayOfMonthExpression", "WeekdayPositionExpression"}
        ]
        if special_day_expressions:
            raise _UnsupportedTriggerError(
                "CronTrigger day expression cannot be represented by z4j's "
                "five-field cron: " + ", ".join(special_day_expressions)
            )

    # APScheduler: Monday=0. croniter: Sunday=0. Weekday names have the same
    # meaning in both parsers, so accept only '*' or name-based expressions.
    weekday_names = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
    if dow != "*":
        weekday_parts = dow.lower().split(",")
        names_only = all(
            all(name in weekday_names for name in part.split("-"))
            and "/" not in part
            and 1 <= len(part.split("-")) <= 2
            for part in weekday_parts
        )
        if not names_only:
            raise _UnsupportedTriggerError(
                "CronTrigger day_of_week must use weekday names "
                "(for example 'mon-fri'); APScheduler numbers Monday as 0 "
                "but z4j's cron parser numbers Sunday as 0"
            )

    if dom != "*" and dow != "*":
        raise _UnsupportedTriggerError(
            "CronTrigger constrains both day and day_of_week; APScheduler "
            "requires both to match but z4j's cron parser matches either"
        )

    return f"{minute} {hour} {dom} {month} {dow}"


def _trigger_timezone(trigger: Any) -> str:
    """Best-effort extraction of a trigger's timezone."""
    tz = getattr(trigger, "timezone", None)
    if tz is None:
        return "UTC"
    # zoneinfo.ZoneInfo / pytz timezone -> str(...) gives "America/NY"
    return str(tz)


__all__ = ["read_apscheduler"]
