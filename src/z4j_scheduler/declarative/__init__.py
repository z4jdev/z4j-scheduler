"""Declarative schedule reconciliation.

Lets framework code (Django settings, FastAPI module, Flask module)
own the schedule definitions in source control. The reconciler reads
that dict on app startup, computes a content hash per schedule,
and posts a single batch to brain's
``POST /api/v1/projects/{slug}/schedules:import?mode=replace_for_source``
endpoint. Brain idempotently inserts/updates rows whose hash
changed and DELETES rows with the same source label that are no
longer in the dict.

Why declarative over CLI imports
--------------------------------

Declarative wins when:

- Schedules belong in the application's repo (review, history,
  rollback via git revert).
- Multiple environments need different schedules (staging vs prod
  read different values from the same module via env-driven
  toggles).
- Schedules change with deploys (adding a feature → adding a
  schedule should land atomically with the code that handles it).

CLI imports win when:

- Migrating from celery-beat / rq-scheduler / cron (one-shot job).
- Schedules are defined dashboard-side and live there.

Both surfaces talk to the same brain endpoint - they're
interchangeable, so a project can mix them.

Usage
-----

The framework adapter calls :func:`reconcile` once on app startup,
passing the dict and the brain coordinates::

    from z4j_scheduler.declarative import reconcile, ScheduleSpec

    schedules = [
        ScheduleSpec(
            name="hourly-cleanup",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.cleanup",
        ),
        ScheduleSpec(
            name="poll-orders",
            engine="celery",
            kind="interval",
            expression="60s",
            task_name="myapp.tasks.poll_orders",
        ),
    ]

    summary = await reconcile(
        schedules=schedules,
        project="my-app",
        source="declarative_django",
        brain_url="http://brain:7700",
        api_token=settings.Z4J_BRAIN_API_TOKEN,
    )
    # summary == {"inserted": 2, "updated": 0, "unchanged": 0,
    #             "failed": 0, "deleted": 0}

Idempotency
-----------

Re-running ``reconcile`` with the same dict is a no-op (every
schedule's source_hash matches the brain's stored hash). Only edits
land in the audit log. Adding a schedule = one insert; deleting a
schedule = one delete; changing a field = one update. The dict is
the canonical source.

Source-label scoping
--------------------

The ``source`` argument MUST be unique per framework adapter
(``declarative_django``, ``declarative_fastapi_orders_app``, ...).
Brain scopes the replace-for-source delete to rows with this exact
label, so two adapters with different labels don't step on each
other and dashboard-managed schedules (``source="dashboard"``) are
never touched.
"""

from __future__ import annotations

from z4j_scheduler.declarative._reconciler import (
    ScheduleSpec,
    reconcile,
    reconcile_sync,
)

__all__ = ["ScheduleSpec", "reconcile", "reconcile_sync"]
