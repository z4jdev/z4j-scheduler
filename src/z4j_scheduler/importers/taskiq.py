"""taskiq schedule-source importer.

taskiq's scheduling lives in ``TaskiqScheduler`` instances driven
by sources. The most common source is ``LabelScheduleSource``,
which scans the broker's registered tasks for a ``schedule`` label::

    @broker.task(schedule=[{"cron": "0 3 * * *"}])
    async def nightly_cleanup() -> None:
        ...

This importer reads a broker's task registry, extracts the
``schedule`` label entries, and converts each into an
:class:`ImportedSchedule` record.

Operators run::

    z4j-scheduler import --from taskiq \\
        --taskiq-broker myapp.tkq:broker ...

Supported schedule label shapes (matching
``taskiq.scheduler.scheduled_task.ScheduledTask``):

- ``{"cron": "0 3 * * *"}`` → ``kind="cron"``
- ``{"time": "2026-12-31T23:59:59Z"}`` → ``kind="one_shot"``
- Anything else is logged + skipped (operator can hand-create in
  the dashboard).

Optional dep: requires ``taskiq``. Delayed import.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.taskiq")


def read_taskiq_broker(
    *,
    broker_path: str,
    project_slug: str,
    engine: str = "taskiq",
    default_queue: str | None = None,
    default_timezone: str = "UTC",
) -> list[ImportedSchedule]:
    """Import schedule-labelled tasks from a taskiq broker.

    Args:
        broker_path: ``module.path:attr`` pointing at the
            ``AsyncBroker`` instance.
        project_slug: Brain project slug.
        engine: Engine name written to brain. Default ``"taskiq"``.
        default_queue: Queue label applied to imported rows.
        default_timezone: Timezone applied to imported schedules.

    Returns:
        A list of :class:`ImportedSchedule` records.

    Raises:
        RuntimeError: broker cannot be resolved or taskiq missing.
    """
    try:
        import taskiq  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "taskiq importer requires `pip install taskiq`",
        ) from exc

    broker = _resolve_taskiq_broker(broker_path)

    schedules: list[ImportedSchedule] = []
    tasks = broker.get_all_tasks() or {}
    for task_name, task_obj in tasks.items():
        labels = getattr(task_obj, "labels", None) or {}
        schedule_label = labels.get("schedule")
        if not schedule_label:
            continue
        # ``schedule`` label is a list of {cron|time, ...} dicts.
        if not isinstance(schedule_label, (list, tuple)):
            logger.warning(
                "z4j.scheduler.importers.taskiq: skipping %r - schedule label is not a list",
                task_name,
            )
            continue
        for idx, entry in enumerate(schedule_label):
            sched = _label_entry_to_schedule(
                task_name=task_name,
                entry=entry,
                idx=idx,
                project_slug=project_slug,
                engine=engine,
                default_queue=default_queue,
                default_timezone=default_timezone,
            )
            if sched is not None:
                schedules.append(sched)

    logger.info(
        "z4j.scheduler.importers.taskiq: parsed %d schedule(s) from %r",
        len(schedules),
        broker_path,
    )
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _resolve_taskiq_broker(broker_path: str) -> Any:
    if ":" not in broker_path:
        raise RuntimeError(
            f"taskiq importer: --taskiq-broker must be 'module:attr', got {broker_path!r}",
        )
    module_path, _, attr = broker_path.partition(":")
    import importlib

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"taskiq importer: could not import {module_path!r}: {exc}",
        ) from exc
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise RuntimeError(
            f"taskiq importer: {module_path!r} has no attribute {attr!r}",
        ) from exc


def _label_entry_to_schedule(
    *,
    task_name: str,
    entry: dict[str, Any],
    idx: int,
    project_slug: str,
    engine: str,
    default_queue: str | None,
    default_timezone: str,
) -> ImportedSchedule | None:
    if not isinstance(entry, dict):
        logger.warning(
            "z4j.scheduler.importers.taskiq: skipping non-dict schedule entry on %r",
            task_name,
        )
        return None

    label_args = entry.get("args", []) or []
    label_kwargs = entry.get("kwargs", {}) or {}
    name_suffix = f":{idx}" if idx > 0 else ""
    schedule_name = f"{task_name}{name_suffix}"

    if "cron" in entry:
        cron_str = str(entry["cron"]).strip()
        return ImportedSchedule(
            project_slug=project_slug,
            name=schedule_name,
            engine=engine,
            kind="cron",
            expression=cron_str,
            task_name=task_name,
            timezone=default_timezone,
            queue=default_queue,
            args=list(label_args),
            kwargs=dict(label_kwargs),
            catch_up="skip",
            is_enabled=True,
            source="imported_taskiq",
        )

    if "time" in entry:
        # taskiq uses ISO-8601 timestamps for one-shot.
        when = str(entry["time"]).strip()
        return ImportedSchedule(
            project_slug=project_slug,
            name=schedule_name,
            engine=engine,
            kind="one_shot",
            expression=when,
            task_name=task_name,
            timezone=default_timezone,
            queue=default_queue,
            args=list(label_args),
            kwargs=dict(label_kwargs),
            catch_up="skip",
            is_enabled=True,
            source="imported_taskiq",
        )

    logger.warning(
        "z4j.scheduler.importers.taskiq: skipping %r entry %d - "
        "no 'cron' or 'time' field; got keys=%r",
        task_name,
        idx,
        sorted(entry.keys()),
    )
    return None


__all__ = ["read_taskiq_broker"]
