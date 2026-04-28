"""celery-beat + django-celery-beat importer.

Reads schedule definitions from one of two sources:

1. **Static beat config** - the dict on
   ``app.conf.beat_schedule``. Loaded by importing the user's Celery
   app via ``--celery-app pkg.module:app``. This is the configuration
   path most projects use - the dict is hand-edited or built up at
   startup from a settings module.

2. **django-celery-beat DB rows** - the ``PeriodicTask`` table that
   ships with django-celery-beat. Loaded via
   ``--django-settings DJANGO_SETTINGS_MODULE``. Used by Django
   projects that wanted a dashboard-style admin and didn't want to
   redeploy to add a new schedule.

Both paths produce :class:`ImportedSchedule` records the operator
can :func:`render_jsonl` for review or push to brain via the
import endpoint.

Per-source notes:

- celery's ``crontab(...)`` -> ``kind="cron"`` with the standard
  five-field crontab string. We re-build the string from the
  crontab object's fields rather than try to round-trip the original
  source - the crontab object is the canonical truth.
- celery's ``timedelta(seconds=N)`` -> ``kind="interval"`` with the
  expression ``"<N>s"``.
- django-celery-beat's ``ClockedSchedule`` -> ``kind="one_shot"`` with
  the ISO-8601 timestamp.
- ``SolarSchedule`` is mapped to ``kind="solar"`` with the
  expression ``"<event>:<lat>:<lon>"``. Pre-1.1 we emitted a
  warning and skipped these; v1.1+ supports them via the
  ``astral`` library. Operators who would rather skip should
  celery-beat for that schedule and let z4j-scheduler manage the
  rest. The CLI surfaces the skip count.

Optional dependency: importing the user's celery app requires
``celery`` itself; we delay the import to ``read_*`` so a
``--from cron`` or ``--from rq`` invocation doesn't drag celery in.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from z4j_scheduler.importers._core import ImportedSchedule

if TYPE_CHECKING:  # pragma: no cover
    pass

logger = logging.getLogger("z4j.scheduler.importers.celery")


def read_celery_app(
    *,
    app_path: str,
    project_slug: str,
    engine: str = "celery",
    default_queue: str | None = None,
    default_timezone: str = "UTC",
) -> list[ImportedSchedule]:
    """Import schedules from a Celery ``app.conf.beat_schedule`` dict.

    Args:
        app_path: ``module.path:attr`` pointing at the celery app.
            Mirrors the convention used by ``celery -A app.path``.
        project_slug: Brain project to attribute imported rows to.
        engine: Engine name written to brain. Almost always
            ``"celery"`` - it's a parameter so the same code path
            can serve celery-compatible workalikes.
        default_queue: Queue applied to schedules whose entry has no
            explicit ``queue`` option. ``None`` lets brain fall back
            to the project's default queue.
        default_timezone: Timezone applied to interval/one_shot
            schedules. crontab schedules carry their own ``tz``
            field and override this default.

    Returns the parsed schedules. Empty list is a successful "no
    beat_schedule was defined" result; the CLI prints a hint when
    that happens.
    """
    app = _load_celery_app(app_path)
    beat = getattr(app.conf, "beat_schedule", None) or {}
    timezone = (
        getattr(app.conf, "timezone", None) or default_timezone or "UTC"
    )

    schedules: list[ImportedSchedule] = []
    for name, entry in beat.items():
        try:
            sched = _entry_to_schedule(
                name=str(name),
                entry=entry,
                project_slug=project_slug,
                engine=engine,
                default_queue=default_queue,
                default_timezone=timezone,
            )
        except _UnsupportedScheduleError as exc:
            logger.warning(
                "z4j.scheduler.importers.celery: skipping %r - %s",
                name, exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)
    return schedules


def read_django_celery_beat(
    *,
    django_settings: str,
    project_slug: str,
    engine: str = "celery",
    default_queue: str | None = None,
) -> list[ImportedSchedule]:
    """Import schedules from the django-celery-beat ``PeriodicTask`` table.

    Args:
        django_settings: ``DJANGO_SETTINGS_MODULE`` value to apply
            before importing django models.
        project_slug: Brain project slug.
        engine: Engine name (default ``"celery"``).
        default_queue: Fallback queue.
    """
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", django_settings)

    try:
        import django  # noqa: PLC0415

        django.setup()
    except ImportError as exc:
        raise RuntimeError(
            "django-celery-beat importer requires `django` "
            "(`pip install django django-celery-beat`)",
        ) from exc

    try:
        from django_celery_beat.models import PeriodicTask  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError(
            "django-celery-beat importer requires "
            "`pip install django-celery-beat`",
        ) from exc

    schedules: list[ImportedSchedule] = []
    for task in PeriodicTask.objects.all():
        try:
            sched = _periodic_task_to_schedule(
                task=task,
                project_slug=project_slug,
                engine=engine,
                default_queue=default_queue,
            )
        except _UnsupportedScheduleError as exc:
            logger.warning(
                "z4j.scheduler.importers.celery: skipping django-celery-beat "
                "%r - %s",
                task.name, exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _UnsupportedScheduleError(Exception):
    """Raised when an entry uses a schedule type we cannot translate."""


def _load_celery_app(app_path: str) -> Any:
    """Resolve ``module.path:attr`` to a celery app object."""
    if ":" not in app_path:
        raise ValueError(
            f"--celery-app must be 'module.path:attr', got {app_path!r}",
        )
    module_path, attr = app_path.rsplit(":", 1)
    try:
        import importlib  # noqa: PLC0415

        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"failed to import {module_path!r}: {exc}",
        ) from exc
    if not hasattr(module, attr):
        raise RuntimeError(
            f"module {module_path!r} has no attribute {attr!r}",
        )
    return getattr(module, attr)


def _entry_to_schedule(
    *,
    name: str,
    entry: dict[str, Any],
    project_slug: str,
    engine: str,
    default_queue: str | None,
    default_timezone: str,
) -> ImportedSchedule | None:
    """Convert one ``beat_schedule[name] = {...}`` entry."""
    task = entry.get("task")
    if not task:
        raise _UnsupportedScheduleError("entry has no 'task' key")

    raw_schedule = entry.get("schedule")
    if raw_schedule is None:
        raise _UnsupportedScheduleError("entry has no 'schedule' key")

    kind, expression, tz = _classify_schedule(raw_schedule, default_timezone)

    options = entry.get("options") or {}
    queue = options.get("queue") or default_queue

    args = list(entry.get("args") or ())
    kwargs = dict(entry.get("kwargs") or {})

    return ImportedSchedule(
        project_slug=project_slug,
        name=name,
        engine=engine,
        kind=kind,
        expression=expression,
        timezone=tz,
        task_name=str(task),
        queue=queue,
        args=args,
        kwargs=kwargs,
        catch_up="skip",  # celery-beat default; operator can re-enable
        is_enabled=bool(entry.get("enabled", True)),
        source="imported_celerybeat",
    )


def _periodic_task_to_schedule(
    *,
    task: Any,
    project_slug: str,
    engine: str,
    default_queue: str | None,
) -> ImportedSchedule | None:
    """Convert one django-celery-beat ``PeriodicTask`` row."""
    import json as _json

    if task.crontab is not None:
        c = task.crontab
        expression = " ".join(
            [
                c.minute, c.hour, c.day_of_month,
                c.month_of_year, c.day_of_week,
            ],
        )
        kind = "cron"
        tz = str(c.timezone) if getattr(c, "timezone", None) else "UTC"
    elif task.interval is not None:
        i = task.interval
        # django-celery-beat encodes period as "seconds"/"minutes"/etc.
        period = (i.period or "seconds").lower()
        suffix_map = {
            "seconds": "s", "minutes": "m", "hours": "h",
            "days": "d",
        }
        suffix = suffix_map.get(period, "s")
        expression = f"{i.every}{suffix}"
        kind = "interval"
        tz = "UTC"
    elif task.clocked is not None:
        kind = "one_shot"
        expression = task.clocked.clocked_time.isoformat()
        tz = "UTC"
    elif task.solar is not None:
        # SolarSchedule -> z4j solar kind. Encoded as
        # ``"<event>:<lat>:<lon>"``. The event vocabulary is shared
        # with celery (sunrise / sunset / dawn / dusk / noon /
        # solar_noon / midnight / solar_midnight). docs/SCHEDULER.md
        # §5.1 lists solar in the v1 surface.
        sol = task.solar
        kind = "solar"
        expression = f"{sol.event}:{sol.latitude}:{sol.longitude}"
        tz = "UTC"
    else:
        raise _UnsupportedScheduleError(
            "PeriodicTask has no schedule attached",
        )

    args = _json.loads(task.args) if task.args else []
    kwargs = _json.loads(task.kwargs) if task.kwargs else {}

    return ImportedSchedule(
        project_slug=project_slug,
        name=task.name,
        engine=engine,
        kind=kind,
        expression=expression,
        timezone=tz,
        task_name=task.task,
        queue=task.queue or default_queue,
        args=args,
        kwargs=kwargs,
        catch_up="skip",
        is_enabled=bool(task.enabled),
        source="imported_celerybeat",
    )


def _classify_schedule(
    raw: Any, default_timezone: str,
) -> tuple[str, str, str]:
    """Identify a celery schedule object and return (kind, expr, tz).

    Imports ``celery.schedules`` lazily so this module remains
    importable for unit tests without celery installed.
    """
    if isinstance(raw, timedelta):
        # Plain timedelta -> interval. Convert to seconds; keep the
        # smallest unit that's clean.
        seconds = int(raw.total_seconds())
        if seconds <= 0:
            raise _UnsupportedScheduleError(
                f"timedelta must be positive; got {raw!r}",
            )
        return "interval", f"{seconds}s", default_timezone

    try:
        from celery.schedules import (  # noqa: PLC0415
            crontab,
            schedule as celery_schedule,
            solar as celery_solar,
        )
    except ImportError:
        crontab = None  # type: ignore[assignment]
        celery_schedule = None  # type: ignore[assignment]
        celery_solar = None  # type: ignore[assignment]

    if celery_solar is not None and isinstance(raw, celery_solar):
        # SolarSchedule - encode as ``"event:lat:lon"``. UTC because
        # solar events are absolute astronomical instants; the
        # latitude / longitude already pin the location.
        return (
            "solar",
            f"{raw.event}:{raw.lat}:{raw.lon}",
            "UTC",
        )

    if crontab is not None and isinstance(raw, crontab):
        # crontab fields are sets in modern celery; render back to
        # crontab-string form.
        expr = " ".join(
            [
                _render_cron_field(raw._orig_minute),
                _render_cron_field(raw._orig_hour),
                _render_cron_field(raw._orig_day_of_month),
                _render_cron_field(raw._orig_month_of_year),
                _render_cron_field(raw._orig_day_of_week),
            ],
        )
        tz = str(raw.tz) if getattr(raw, "tz", None) else default_timezone
        return "cron", expr, tz

    if celery_schedule is not None and isinstance(raw, celery_schedule):
        # ``schedule(timedelta(...))`` - same as plain timedelta.
        run_every = getattr(raw, "run_every", None)
        if isinstance(run_every, timedelta):
            return _classify_schedule(run_every, default_timezone)

    raise _UnsupportedScheduleError(
        f"unrecognised schedule type {type(raw).__name__}",
    )


def _render_cron_field(value: Any) -> str:
    """Render one crontab field back to its string form.

    ``crontab._orig_*`` is usually the original string but can be a
    Python int for the ``crontab(minute=5)`` shorthand. Normalise.
    """
    if value is None:
        return "*"
    return str(value)


__all__ = [
    "read_celery_app",
    "read_django_celery_beat",
]
