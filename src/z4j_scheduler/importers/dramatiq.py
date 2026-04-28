"""dramatiq importer - documentation stub.

Dramatiq itself does NOT ship a built-in scheduler primitive
(unlike celery-beat, huey.periodic_task, arq cron, taskiq labels).
The two common third-party options are:

- ``apscheduler-dramatiq`` (or plain APScheduler used to enqueue
  Dramatiq tasks)
- ``dramatiq-cron``

Both are out-of-tree and have no shared schedule data structure
to import from. Operators in either camp should:

1. **APScheduler-based:** use ``z4j-scheduler import --from
   apscheduler --apscheduler-app ... --engine dramatiq``. The
   apscheduler importer reads from a ``BackgroundScheduler`` /
   ``BlockingScheduler`` instance and emits ImportedSchedule rows.
   Pass ``--engine dramatiq`` to mark them as dramatiq-fired.

2. **dramatiq-cron-based:** the cron strings live in your code
   alongside the actor decorators. Either:

   - Re-create the schedules in the z4j dashboard (one-time
     migration), or
   - Pre-build a JSONL file matching the ImportedSchedule shape
     and POST it to ``/api/v1/projects/{slug}/schedules:import``
     directly.

3. **Pure dashboard creation:** dramatiq workers don't need a
   pre-existing scheduler at all - z4j-scheduler IS the
   scheduler. Just create the schedule rows in the dashboard
   with ``engine="dramatiq"``.

This module exists so the CLI's ``--from dramatiq`` flag doesn't
raise an ImportError; instead it raises a clear RuntimeError
pointing at the three options above.
"""

from __future__ import annotations

from z4j_scheduler.importers._core import ImportedSchedule


_GUIDANCE = """\
Dramatiq has no built-in scheduler to import from. Three options:

  1. If your scheduling lives in APScheduler:
       z4j-scheduler import --from apscheduler \\
         --apscheduler-app YOUR_APP --engine dramatiq ...

  2. If your scheduling lives in dramatiq-cron or another ad-hoc
     library: re-create the schedules in the z4j dashboard, OR
     write a JSONL file matching ImportedSchedule and POST to
     /api/v1/projects/{slug}/schedules:import directly.

  3. If you have no existing scheduler: create the schedule rows
     in the z4j dashboard with engine="dramatiq". z4j-scheduler
     fires them; your dramatiq worker enqueues + runs the task.
"""


def read_dramatiq() -> list[ImportedSchedule]:
    """Always raises with operator guidance.

    Dramatiq has no native scheduler primitive. See module
    docstring for migration paths.
    """
    raise RuntimeError(_GUIDANCE)


__all__ = ["read_dramatiq"]
