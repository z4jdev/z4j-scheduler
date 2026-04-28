"""huey periodic-task importer.

Huey ships its own periodic-task primitive: tasks decorated with
``@huey.periodic_task(crontab(...))`` are registered on the Huey
instance and the consumer's scheduler thread fires them on cadence.

This importer scans a Huey instance's task registry for periodic
tasks and converts each one into an :class:`ImportedSchedule`
record. The operator runs:

    z4j-scheduler import --from huey --huey-app pkg.module:huey ...

and the periodic_task entries land in brain as ``engine="huey"``
schedules. From there z4j-scheduler manages firing instead of the
huey consumer's scheduler thread.

Supported Huey periodic forms:

- ``crontab(minute=..., hour=..., ...)`` → ``kind="cron"`` with
  the standard 5-field cron expression rebuilt from the crontab
  fields.
- Custom callable validators (``periodic_task(lambda dt: ...)``)
  → unsupported (the cron string can't be derived); the task is
  skipped with a warning. Operators with these can hand-create
  the schedule in the dashboard.

Optional dep: importing requires ``huey`` itself. Delayed import
so ``--from celery`` doesn't drag huey in.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.huey")


def read_huey_app(
    *,
    app_path: str,
    project_slug: str,
    engine: str = "huey",
    default_queue: str | None = None,
    default_timezone: str = "UTC",
) -> list[ImportedSchedule]:
    """Import schedules from a Huey instance's periodic-task registry.

    Args:
        app_path: ``module.path:attr`` pointing at the Huey instance
            (typically the same import path the operator's huey
            consumer is configured with).
        project_slug: Brain project slug to attribute the imports to.
        engine: Engine name written to brain. Default ``"huey"``.
        default_queue: Queue label applied to imported rows. None
            lets brain fall back to the project default. Huey itself
            doesn't have queue-routing the way celery does (a Huey
            instance IS the queue), so this is a labelling field for
            the dashboard.
        default_timezone: Timezone applied to imported schedules.
            Huey's crontab is always evaluated in the consumer's
            local time; operators should set this to the consumer's
            tz for accurate display.

    Returns:
        A list of :class:`ImportedSchedule` records.

    Raises:
        RuntimeError: ``app_path`` cannot be resolved or ``huey``
            is not installed.
    """
    try:
        from huey import Huey  # noqa: F401, PLC0415
    except ImportError as exc:
        raise RuntimeError(
            "huey importer requires `pip install huey`",
        ) from exc

    huey_inst = _resolve_huey_app(app_path)

    schedules: list[ImportedSchedule] = []
    registry = getattr(huey_inst, "_registry", None)
    if registry is None:
        logger.warning(
            "z4j.scheduler.importers.huey: huey instance %r has no "
            "_registry; nothing to import", app_path,
        )
        return schedules

    seen_periodic = 0
    for task_name, task_cls in dict(registry._registry).items():
        validator = getattr(task_cls, "validate_datetime", None)
        if validator is None:
            # Not a periodic task; skip silently.
            continue
        seen_periodic += 1

        cron_expr = _crontab_to_expression(validator)
        if cron_expr is None:
            logger.warning(
                "z4j.scheduler.importers.huey: skipping %r - "
                "validator is not a recognised crontab() form "
                "(custom lambda validators have no derivable "
                "cron string)", task_name,
            )
            continue

        schedules.append(
            ImportedSchedule(
                project_slug=project_slug,
                name=task_name,
                engine=engine,
                kind="cron",
                expression=cron_expr,
                task_name=task_name,
                timezone=default_timezone,
                queue=default_queue,
                args=[],
                kwargs={},
                catch_up="skip",
                is_enabled=True,
                source="imported_huey",
            ),
        )

    logger.info(
        "z4j.scheduler.importers.huey: parsed %d periodic task(s) "
        "from huey instance %r (%d total)",
        len(schedules), app_path, seen_periodic,
    )
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _resolve_huey_app(app_path: str) -> Any:
    """Resolve ``"pkg.module:attr"`` to the Huey instance.

    Same convention as celery's ``-A`` / huey's ``-A`` flag:
    ``module:attr_name``.
    """
    if ":" not in app_path:
        raise RuntimeError(
            f"huey importer: --huey-app must be 'module:attr', "
            f"got {app_path!r}",
        )
    module_path, _, attr = app_path.partition(":")
    import importlib  # noqa: PLC0415

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"huey importer: could not import {module_path!r}: {exc}",
        ) from exc
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise RuntimeError(
            f"huey importer: {module_path!r} has no attribute "
            f"{attr!r}",
        ) from exc


def _crontab_to_expression(validator: Any) -> str | None:
    """Convert a Huey ``crontab(...)``-produced validator to a cron string.

    Huey's ``crontab()`` returns a closure that pre-expands each
    field to the full set of valid integer values. The closure
    layout differs across Huey versions:

    - Huey 2.x: separate freevars ``minute, hour, day, month,
      day_of_week`` (each holding the original string spec).
    - Huey 3.x: single freevar ``cron_settings`` holding a list
      of 5 pre-expanded integer lists in
      ``[months, days, day_of_week, hours, minutes]`` order.

    We try both shapes and emit a 5-field cron expression. Each
    field collapses to ``*`` when its set covers the full
    cron-spec range; otherwise it's rendered as a comma-list of
    the individual values.

    Returns None if the validator wasn't produced by
    ``huey.crontab(...)`` (e.g. operator passed a custom lambda).
    """
    code = getattr(validator, "__code__", None)
    closure = getattr(validator, "__closure__", None)
    if code is None or closure is None:
        return None

    freevars = list(code.co_freevars)
    closure_map: dict[str, Any] = {}
    for name, cell in zip(freevars, closure, strict=False):
        try:
            closure_map[name] = cell.cell_contents
        except ValueError:
            return None

    # Huey 3.x: ``periodic_task`` wraps the original ``crontab(...)``
    # validator inside a ``method_validate`` closure - the wrapper's
    # only freevar is ``validate_datetime`` pointing at the inner
    # closure. Unwrap one level so we see the cron_settings.
    if list(closure_map.keys()) == ["validate_datetime"] and callable(
        closure_map["validate_datetime"],
    ):
        return _crontab_to_expression(closure_map["validate_datetime"])

    # Huey 3.x: single ``cron_settings`` list of 5 pre-expanded sets.
    if "cron_settings" in closure_map:
        settings = closure_map["cron_settings"]
        if not isinstance(settings, (list, tuple)) or len(settings) != 5:
            return None
        # Order per huey 3.x source:
        # [months, days, day_of_week, hours, minutes]
        months, days, dow, hours, minutes = settings
        return (
            f"{_collapse(minutes, _MINUTE_FULL)} "
            f"{_collapse(hours, _HOUR_FULL)} "
            f"{_collapse(days, _DOM_FULL)} "
            f"{_collapse(months, _MONTH_FULL)} "
            f"{_collapse(dow, _DOW_FULL)}"
        )

    # Huey 2.x: separate string freevars.
    expected = {"minute", "hour", "day", "month", "day_of_week"}
    if expected.issubset(closure_map.keys()):
        return (
            f"{_coerce_cron_field(closure_map['minute'])} "
            f"{_coerce_cron_field(closure_map['hour'])} "
            f"{_coerce_cron_field(closure_map['day'])} "
            f"{_coerce_cron_field(closure_map['month'])} "
            f"{_coerce_cron_field(closure_map['day_of_week'])}"
        )

    return None


# Full-range sets for collapsing pre-expanded cron field values
# back to ``*``. Huey accepts ``day_of_week=0..7`` (both 0 and 7
# meaning Sunday); cron uses 0..6 conventionally so we treat
# either {0..6} or {0..7} as "every day."
_MINUTE_FULL = frozenset(range(60))
_HOUR_FULL = frozenset(range(24))
_DOM_FULL = frozenset(range(1, 32))
_MONTH_FULL = frozenset(range(1, 13))
_DOW_FULL = (frozenset(range(7)), frozenset(range(8)))


def _collapse(values: Any, full: Any) -> str:
    """Render an iterable of ints as a cron field.

    Returns ``"*"`` when ``values`` matches the full range
    (collapse), or a comma-list otherwise.
    """
    try:
        as_set = frozenset(int(v) for v in values)
    except (TypeError, ValueError):
        return "*"
    if isinstance(full, tuple):
        if any(as_set == f for f in full):
            return "*"
    elif as_set == full:
        return "*"
    return ",".join(str(v) for v in sorted(as_set))


def _coerce_cron_field(value: Any) -> str:
    """Coerce a huey 2.x crontab field value to a cron field string."""
    if value == "*":
        return "*"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (set, list, tuple)):
        return ",".join(str(v) for v in sorted(value))
    return str(value)


__all__ = ["read_huey_app"]
