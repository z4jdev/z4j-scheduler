"""arq cron-jobs importer.

arq registers cron jobs on the WorkerSettings class:

    from arq import cron

    class WorkerSettings:
        functions = [send_email, generate_report]
        cron_jobs = [
            cron(send_email, name="daily-greeting", hour=8, minute=0),
            cron(generate_report, hour={9, 17}, minute=0),
        ]

This importer reads the WorkerSettings, walks ``cron_jobs``, and
converts each :class:`arq.cron.CronJob` into an
:class:`ImportedSchedule` record.

Operators run::

    z4j-scheduler import --from arq \\
        --arq-settings myapp.worker:WorkerSettings ...

Limitations:

- arq's cron uses sets for matching multiple values
  (``hour={9, 17}``); we render them as a comma-separated cron
  field (``"9,17"``).
- arq has a ``microsecond`` argument; z4j-scheduler's resolution
  is second / minute. The microsecond is dropped on import.

Optional dep: requires ``arq``. Delayed import.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.arq")


def read_arq_settings(
    *,
    settings_path: str,
    project_slug: str,
    engine: str = "arq",
    default_queue: str | None = None,
    default_timezone: str = "UTC",
) -> list[ImportedSchedule]:
    """Import cron jobs from an arq WorkerSettings class.

    Args:
        settings_path: ``module.path:ClassName`` pointing at the
            WorkerSettings class. Mirrors arq's own
            ``arq myapp.worker.WorkerSettings`` CLI shape.
        project_slug: Brain project slug.
        engine: Engine name written to brain. Default ``"arq"``.
        default_queue: Queue label applied to imported rows. arq
            uses ``queue_name`` per WorkerSettings, not per cron
            job - rendered as a label only.
        default_timezone: Timezone applied to imported schedules.

    Returns:
        A list of :class:`ImportedSchedule` records.

    Raises:
        RuntimeError: settings cannot be resolved or arq missing.
    """
    try:
        import arq.cron  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "arq importer requires `pip install arq`",
        ) from exc

    settings_cls = _resolve_arq_settings(settings_path)
    cron_jobs = list(getattr(settings_cls, "cron_jobs", None) or [])
    if not cron_jobs:
        logger.info(
            "z4j.scheduler.importers.arq: %r has no cron_jobs",
            settings_path,
        )
        return []

    schedules: list[ImportedSchedule] = []
    for job in cron_jobs:
        try:
            sched = _cron_job_to_schedule(
                job=job,
                project_slug=project_slug,
                engine=engine,
                default_queue=default_queue,
                default_timezone=default_timezone,
            )
        except _UnsupportedCronJobError as exc:
            logger.warning(
                "z4j.scheduler.importers.arq: skipping %r - %s",
                _job_label(job),
                exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)

    logger.info(
        "z4j.scheduler.importers.arq: parsed %d cron job(s) from %r",
        len(schedules),
        settings_path,
    )
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _UnsupportedCronJobError(Exception):
    """Raised when a CronJob can't be converted (custom callable, etc.)."""


def _resolve_arq_settings(settings_path: str) -> Any:
    if ":" not in settings_path:
        raise RuntimeError(
            f"arq importer: --arq-settings must be 'module:Class', got {settings_path!r}",
        )
    module_path, _, attr = settings_path.partition(":")
    import importlib

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"arq importer: could not import {module_path!r}: {exc}",
        ) from exc
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise RuntimeError(
            f"arq importer: {module_path!r} has no attribute {attr!r}",
        ) from exc


def _cron_job_to_schedule(
    *,
    job: Any,
    project_slug: str,
    engine: str,
    default_queue: str | None,
    default_timezone: str,
) -> ImportedSchedule | None:
    """Convert one arq ``CronJob`` to an :class:`ImportedSchedule`."""
    coroutine = getattr(job, "coroutine", None)
    name = getattr(job, "name", None) or _coroutine_name(coroutine)
    if not name:
        raise _UnsupportedCronJobError(
            "cron job has no name + no coroutine",
        )

    fn_name = _coroutine_name(coroutine) or name
    cron_expr = _cron_job_to_cron_string(job)

    return ImportedSchedule(
        project_slug=project_slug,
        name=name,
        engine=engine,
        kind="cron",
        expression=cron_expr,
        task_name=fn_name,
        timezone=default_timezone,
        queue=default_queue,
        args=[],
        kwargs={},
        catch_up="skip",
        is_enabled=True,
        source="imported_arq",
    )


def _cron_job_to_cron_string(job: Any) -> str:
    """Turn a ``CronJob``'s month/day/weekday/hour/minute fields into
    a 5-field cron expression.

    arq's cron fields are ``int | set[int] | None``. ``None`` → ``*``.
    A set is rendered comma-joined (``{9, 17}`` → ``"9,17"``).
    """
    minute = _arq_field_to_cron(getattr(job, "minute", None))
    hour = _arq_field_to_cron(getattr(job, "hour", None))
    day = _arq_field_to_cron(getattr(job, "day", None))
    month = _arq_field_to_cron(getattr(job, "month", None))
    weekday = _arq_field_to_cron(getattr(job, "weekday", None))
    # arq's weekday accepts string aliases (``"mon"``); croniter
    # accepts them too but we rendered the safe-mode parser to
    # reject letters. Strip aliases - if the user had ``"mon"`` they
    # probably want ``1`` in cron (Mon = 1 in cron's Sun-0..Sat-6
    # ordering).
    weekday = _arq_weekday_alias(weekday)
    return f"{minute} {hour} {day} {month} {weekday}"


_WEEKDAY_ALIASES: dict[str, str] = {
    "mon": "1",
    "tues": "2",
    "wed": "3",
    "thurs": "4",
    "fri": "5",
    "sat": "6",
    "sun": "0",
}


def _arq_weekday_alias(value: str) -> str:
    if value in _WEEKDAY_ALIASES:
        return _WEEKDAY_ALIASES[value]
    return value


def _arq_field_to_cron(value: Any) -> str:
    if value is None:
        return "*"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (set, list, tuple, frozenset)):
        return ",".join(str(v) for v in sorted(value))
    return str(value)


def _coroutine_name(coroutine: Any) -> str:
    if coroutine is None:
        return ""
    name = getattr(coroutine, "__name__", None)
    module = getattr(coroutine, "__module__", "")
    if name and module:
        return f"{module}.{name}"
    return name or ""


def _job_label(job: Any) -> str:
    return (
        getattr(job, "name", None)
        or _coroutine_name(
            getattr(job, "coroutine", None),
        )
        or "<unnamed>"
    )


__all__ = ["read_arq_settings"]
