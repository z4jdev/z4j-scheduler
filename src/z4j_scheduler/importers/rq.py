"""rq-scheduler importer.

rq-scheduler stores scheduled jobs in a Redis sorted set named
``rq:scheduler:scheduled_jobs``. Each member is a job-id string
whose score is the unix timestamp of the next scheduled run.
The job's full payload (callable, args, kwargs, interval, repeat,
cron string) lives at ``rq:job:<id>`` as a Redis hash.

We support the cron jobs (``cron('* * * * *', ...)``), the
recurring interval jobs (``schedule(interval=N, ...)``), and the
one-shot ``enqueue_at(...)`` jobs.

Reads happen via ``redis-py`` + ``rq-scheduler.Scheduler``. Both
are optional dependencies pulled in via the ``rq-import`` extras
group; the importer raises a clear error if either is missing.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.rq")


def read_rq_scheduler(
    *,
    redis_url: str,
    project_slug: str,
    engine: str = "rq",
    queue: str | None = None,
) -> list[ImportedSchedule]:
    """Import scheduled jobs from rq-scheduler's Redis sorted set.

    Args:
        redis_url: ``redis://host:port/db`` URL.
        project_slug: Brain project slug.
        engine: Engine name (default ``"rq"``).
        queue: Default queue name applied to schedules whose
            original job didn't pin a queue. ``None`` lets brain
            pick.

    Returns the parsed schedules. rq-scheduler doesn't carry per-job
    timezones - we assume UTC and let the operator override later.
    """
    try:
        from redis import Redis  # noqa: PLC0415
        from rq_scheduler import Scheduler  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError(
            "rq importer requires `pip install redis rq-scheduler`",
        ) from exc

    # Wrap the Redis connection in a try/except so any error
    # (auth failure, DNS failure, refused connection) doesn't
    # bubble up with the URL - which contains the password if the
    # operator passed ``redis://:password@host`` - in the
    # traceback. The raw exception text would otherwise include
    # the URL as part of the ConnectionError repr, so a CI log
    # pasted to a public issue tracker would leak the password.
    try:
        redis_conn = Redis.from_url(redis_url)
        scheduler = Scheduler(connection=redis_conn)
    except Exception as exc:
        raise RuntimeError(
            f"could not connect to Redis ({_redact_redis_url(redis_url)}): "
            f"{type(exc).__name__}",
        ) from None  # ``from None`` chops the original exception
        # so its message (which may also embed the URL) doesn't
        # leak into the chained traceback.

    schedules: list[ImportedSchedule] = []
    for job in scheduler.get_jobs():
        try:
            sched = _job_to_schedule(
                job=job,
                project_slug=project_slug,
                engine=engine,
                default_queue=queue,
            )
        except _UnsupportedJobError as exc:
            logger.warning(
                "z4j.scheduler.importers.rq: skipping %r - %s",
                job.id, exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _UnsupportedJobError(Exception):
    """Raised when a job uses a schedule shape we cannot translate."""


def _redact_redis_url(url: str) -> str:
    """Replace the password in a Redis URL with ``***``.

    Audit fix 6.1 (Apr 2026): operators paste the import command
    output (including error tracebacks) into CI logs / issue
    trackers / Slack. The Redis URL is a connection-string
    primary; if it carries credentials they MUST not appear in
    error text we surface up the stack.

    Returns ``"redis://:***@host:port/db"`` when a password is
    present; passes the URL through unchanged when it isn't (no
    secrets, no work).
    """
    from urllib.parse import urlparse, urlunparse  # noqa: PLC0415

    try:
        parsed = urlparse(url)
    except Exception:  # noqa: BLE001
        return "<unparseable redis url>"
    if not parsed.password:
        return url
    netloc = parsed.hostname or ""
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    user = parsed.username or ""
    netloc_with_creds = f"{user}:***@{netloc}"
    return urlunparse(parsed._replace(netloc=netloc_with_creds))


def _job_to_schedule(
    *,
    job: Any,
    project_slug: str,
    engine: str,
    default_queue: str | None,
) -> ImportedSchedule:
    """Convert one ``rq.job.Job`` into an :class:`ImportedSchedule`.

    rq-scheduler stuffs schedule metadata into the job's ``meta``
    dict at enqueue time:

    - ``cron_string`` - cron expression for cron jobs
    - ``interval`` - seconds between runs for interval jobs

    A job with neither ``cron_string`` nor ``interval`` is a
    one-shot ``enqueue_at`` job; we use the original schedule time
    as the one_shot expression.
    """
    meta = getattr(job, "meta", None) or {}
    func_name = job.func_name  # "module.func" form used by rq

    cron_string = meta.get("cron_string")
    interval = meta.get("interval")

    args = list(job.args or ())
    kwargs = dict(job.kwargs or {})

    queue = (
        getattr(job, "origin", None)
        or default_queue
    )

    if cron_string:
        return ImportedSchedule(
            project_slug=project_slug,
            name=str(job.id),
            engine=engine,
            kind="cron",
            expression=str(cron_string),
            timezone="UTC",
            task_name=str(func_name),
            queue=queue,
            args=args,
            kwargs=kwargs,
            catch_up="skip",
            is_enabled=True,
            source="imported_rq",
        )

    if interval:
        return ImportedSchedule(
            project_slug=project_slug,
            name=str(job.id),
            engine=engine,
            kind="interval",
            expression=f"{int(interval)}s",
            timezone="UTC",
            task_name=str(func_name),
            queue=queue,
            args=args,
            kwargs=kwargs,
            catch_up="skip",
            is_enabled=True,
            source="imported_rq",
        )

    # One-shot enqueue_at: the scheduled_at attribute is a datetime.
    schedule_at = getattr(job, "scheduled_at", None) or meta.get("scheduled_at")
    if schedule_at is None:
        raise _UnsupportedJobError(
            "no cron_string, interval, or scheduled_at - job is not a "
            "recognised rq-scheduler shape",
        )
    expression = (
        schedule_at.isoformat()
        if hasattr(schedule_at, "isoformat")
        else str(schedule_at)
    )
    return ImportedSchedule(
        project_slug=project_slug,
        name=str(job.id),
        engine=engine,
        kind="one_shot",
        expression=expression,
        timezone="UTC",
        task_name=str(func_name),
        queue=queue,
        args=args,
        kwargs=kwargs,
        catch_up="skip",
        is_enabled=True,
        source="imported_rq",
    )


__all__ = ["read_rq_scheduler"]
