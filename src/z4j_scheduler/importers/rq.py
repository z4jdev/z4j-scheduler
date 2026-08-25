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
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from z4j_core.redaction import redact_url_password

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.rq")


def read_rq_scheduler(
    *,
    redis_url: str,
    project_slug: str,
    engine: str = "rq",
    queue: str | None = None,
    default_timezone: str = "UTC",
) -> list[ImportedSchedule]:
    """Import scheduled jobs from rq-scheduler's Redis sorted set.

    Args:
        redis_url: ``redis://host:port/db`` URL.
        project_slug: Brain project slug.
        engine: Engine name (default ``"rq"``).
        queue: Default queue name applied to schedules whose
            original job didn't pin a queue. ``None`` lets brain
            pick.

    rq-scheduler records whether cron evaluation used the scheduler host's
    local timezone, but not the local zone's IANA name. ``default_timezone``
    supplies that name for local-time cron rows. Other cron rows are UTC.
    """
    try:
        from redis import Redis
        from redis.exceptions import RedisError
        from rq_scheduler import Scheduler
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
        # Materialize here, inside the guard. ``Redis.from_url`` and the
        # Scheduler constructor perform NO I/O -- redis-py connects lazily -- so
        # this block used to be unreachable by any connection failure. The first
        # real I/O was the ``get_jobs()`` iteration below, outside it.
        #
        # That is not only why the RESP3 hint never fired. The redaction this
        # guard exists for did not fire either: a refused connection, a DNS
        # failure or an auth failure raised straight out of the loop with the
        # driver's own message, which embeds the URL, password and all. The
        # exact leak into a pasted traceback that the comment above describes.
        # One-shot execution times live only in the sorted-set score. Asking
        # for jobs alone discards that score, and Job has no scheduled_at
        # attribute in rq-scheduler 0.14.
        jobs_with_times = list(scheduler.get_jobs(with_times=True))
    except Exception as exc:
        # Only the exception TYPE is surfaced below, so a RESP3 negotiation
        # failure against a pre-6.0 server arrives as a bare "ResponseError"
        # with nothing to act on. Detect it and append our own fixed text: the
        # hint is a literal, so it cannot leak the URL the redaction above is
        # protecting. Not worked around with protocol=2, because rq fails on
        # the same pairing and a silent fallback would hide that.
        hint = (
            ""
            if "HELLO" not in str(exc)
            else (
                "; Redis server predates 6.0 but redis-py is 6.0+, which "
                'negotiates RESP3 - pin the client: pip install "redis<6"'
            )
        )
        # Pulling the iteration inside this block made the redaction reachable,
        # which was the point, but it also put every job-decoding failure under
        # a message that blames the connection. rq raises ValueError from its
        # own ``restore`` on an unknown job status, and an operator was then
        # told "could not connect to Redis" about a broker they were plainly
        # connected to, with the real cause suppressed by ``from None``.
        #
        # Classify instead. Anything the driver raises is a connection-class
        # failure and keeps the redacted, cause-suppressed message, because
        # those are the ones whose text embeds the URL. Anything else came from
        # reading the jobs, so it says so and keeps its cause.
        driver_failure = isinstance(exc, RedisError | OSError)
        if not driver_failure:
            # The branch is about which LAYER failed, not about an argument's
            # type, so RuntimeError is right here despite the isinstance above.
            raise RuntimeError(
                f"connected to Redis ({_redact_redis_url(redis_url)}) but could "
                f"not read the rq-scheduler jobs: {type(exc).__name__}",
            ) from exc
        raise RuntimeError(
            f"could not connect to Redis ({_redact_redis_url(redis_url)}): {type(exc).__name__}{hint}",
        ) from None  # ``from None`` chops the original exception
        # so its message (which may also embed the URL) doesn't
        # leak into the chained traceback.

    schedules: list[ImportedSchedule] = []
    _validate_timezone(default_timezone)
    for job, scheduled_time in jobs_with_times:
        try:
            sched = _job_to_schedule(
                job=job,
                project_slug=project_slug,
                engine=engine,
                default_queue=queue,
                default_timezone=default_timezone,
                scheduled_time=scheduled_time,
                scheduled_time_is_utc=True,
            )
        except _UnsupportedJobError as exc:
            logger.warning(
                "z4j.scheduler.importers.rq: skipping %r - %s",
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


class _UnsupportedJobError(Exception):
    """Raised when a job uses a schedule shape we cannot translate."""


def _redact_redis_url(url: str) -> str:
    """Replace the password in a Redis URL with ``***``.

    Operators paste import output, tracebacks included, into CI logs, issue
    trackers and chat. A Redis URL is a connection-string primary and often
    carries credentials, which must not appear in anything we surface.

    Delegates to ``z4j_core``. This was a private copy, and while it was the
    only one the same URL was being logged verbatim from the rq worker
    bootstrap in another package, which could not import it. One redactor,
    one place.
    """
    return redact_url_password(url)


def _job_to_schedule(
    *,
    job: Any,
    project_slug: str,
    engine: str,
    default_queue: str | None,
    default_timezone: str = "UTC",
    scheduled_time: datetime | None = None,
    scheduled_time_is_utc: bool = False,
) -> ImportedSchedule:
    """Convert one ``rq.job.Job`` into an :class:`ImportedSchedule`.

    rq-scheduler stuffs schedule metadata into the job's ``meta``
    dict at enqueue time:

    - ``cron_string`` - cron expression for cron jobs
    - ``interval`` - seconds between runs for interval jobs

    A job with neither ``cron_string`` nor ``interval`` is a
    one-shot ``enqueue_at`` job; we use the original schedule time
    as the clocked expression.
    """
    meta = getattr(job, "meta", None) or {}
    func_name = job.func_name  # "module.func" form used by rq

    cron_string = meta.get("cron_string")
    interval = meta.get("interval")

    args = list(job.args or ())
    kwargs = dict(job.kwargs or {})

    queue = getattr(job, "origin", None) or default_queue

    if cron_string:
        timezone = default_timezone if meta.get("use_local_timezone") else "UTC"
        return ImportedSchedule(
            project_slug=project_slug,
            name=str(job.id),
            engine=engine,
            kind="cron",
            expression=str(cron_string),
            timezone=timezone,
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
            timezone=default_timezone,
            task_name=str(func_name),
            queue=queue,
            args=args,
            kwargs=kwargs,
            catch_up="skip",
            is_enabled=True,
            source="imported_rq",
        )

    # One-shot enqueue_at: the scheduled_at attribute is a datetime.
    schedule_at = scheduled_time or getattr(job, "scheduled_at", None) or meta.get("scheduled_at")
    if schedule_at is None:
        raise _UnsupportedJobError(
            "no cron_string, interval, or scheduled_at - job is not a "
            "recognised rq-scheduler shape",
        )
    if isinstance(schedule_at, datetime):
        if schedule_at.tzinfo is None:
            if not scheduled_time_is_utc:
                raise _UnsupportedJobError(
                    "one-shot time is timezone-naive and its source timezone "
                    "is unknown; refusing to shift the execution instant"
                )
            # rq-scheduler's get_jobs(with_times=True) converts the Redis epoch
            # score with datetime.utcfromtimestamp, which is naive but UTC.
            schedule_at = schedule_at.replace(tzinfo=UTC)
        expression = schedule_at.isoformat()
    else:
        expression = str(schedule_at)
    return ImportedSchedule(
        project_slug=project_slug,
        name=str(job.id),
        engine=engine,
        kind="clocked",
        expression=expression,
        timezone=default_timezone,
        task_name=str(func_name),
        queue=queue,
        args=args,
        kwargs=kwargs,
        catch_up="skip",
        is_enabled=True,
        source="imported_rq",
    )


def _validate_timezone(value: str) -> None:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown timezone {value!r}") from exc


__all__ = ["read_rq_scheduler"]
