"""Core reconcile() - turns a Python schedule list into a brain import.

Public surface:

- :class:`ScheduleSpec` - dataclass for one declarative schedule
- :func:`reconcile` - async; preferred for FastAPI / aiohttp /
  starlette / any asyncio-aware framework
- :func:`reconcile_sync` - blocking wrapper for Django/Flask/WSGI
  startup hooks that don't expose an event loop

Implementation note: the heavy lifting (HTTP, hash, batching) is
already in :class:`BrainImportClient` and :class:`ImportedSchedule`
from the importers module. This module is just the type-safe
declarative facade so app code stays clean.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from z4j_scheduler.importers._core import BrainImportClient, ImportedSchedule

logger = logging.getLogger("z4j.scheduler.declarative")

ScheduleKind = Literal["cron", "interval", "one_shot"]
CatchUpPolicy = Literal["skip", "fire_one_missed", "fire_all_missed"]


@dataclass(slots=True)
class ScheduleSpec:
    """One schedule in a declarative dict.

    Mirrors :class:`ImportedSchedule` minus the project_slug
    (which the caller passes once to :func:`reconcile`) and the
    source_hash (which the reconciler computes per-row).

    Required:
        name: Unique within ``(project, scheduler)``. Acts as the
            stable identifier - renaming is delete + create.
        engine: Engine adapter that runs the task (``celery``, ``rq``,
            ``dramatiq``, ``huey``, ``arq``, ``taskiq``).
        kind: ``"cron"`` / ``"interval"`` / ``"one_shot"``.
        expression: Schedule-kind-specific expression.
            - cron: standard 5-field crontab (e.g. ``"0 * * * *"``)
            - interval: ``<N><unit>`` (``"30s"``, ``"5m"``, ``"2h"``, ``"1d"``)
            - one_shot: ISO-8601 timestamp
        task_name: Fully-qualified task name to invoke.

    Optional:
        timezone: Timezone the cron expression is evaluated in
            (defaults to UTC).
        queue: Queue override (None = engine's default).
        args / kwargs: Task arguments at fire time.
        catch_up: ``skip`` / ``fire_one_missed`` / ``fire_all_missed``.
            Drives behavior when fires were missed during an outage.
        is_enabled: Soft-disable. Reconciler still keeps the row
            but the scheduler won't tick it.
    """

    name: str
    engine: str
    kind: ScheduleKind
    expression: str
    task_name: str
    timezone: str = "UTC"
    queue: str | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    catch_up: CatchUpPolicy = "skip"
    is_enabled: bool = True


async def reconcile(
    *,
    schedules: list[ScheduleSpec] | dict[str, ScheduleSpec],
    project: str,
    source: str,
    brain_url: str,
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, int]:
    """Push the declarative schedule set to brain. Returns the summary.

    Args:
        schedules: Either a list of :class:`ScheduleSpec` (each
            carrying its own ``name``) or a dict mapping name -> spec.
            The dict form is convenient when the framework already
            uses a name-keyed dict (matches celery-beat's
            ``beat_schedule`` shape).
        project: Brain project slug to attribute these schedules to.
        source: Source label used by brain's replace-for-source
            delete scoping. MUST be unique per framework adapter
            (e.g. ``"declarative_django"``, ``"declarative_fastapi"``).
            See module docstring for why.
        brain_url: Brain REST URL, e.g. ``http://brain:7700``.
        api_token: Bearer token with ADMIN role on the project.
            Required because the underlying ``:import`` endpoint
            requires ADMIN.
        timeout_seconds: HTTP timeout for the single batch POST.

    Returns:
        Counter dict with keys ``inserted``, ``updated``,
        ``unchanged``, ``failed``, ``deleted`` and an optional
        ``errors`` map of per-row messages.

    Raises:
        :class:`RuntimeError` if brain returns 404 (the import
        endpoint isn't on this brain release - upgrade) or any
        other HTTP error.

    Idempotent: re-running with the same dict produces a summary
    with ``unchanged`` equal to the schedule count and zeroes
    everywhere else. Audit log only records the batch row, not
    per-schedule noise.
    """
    if isinstance(schedules, dict):
        # Name-keyed dict form: enforce the dict key matches the
        # spec's name. Mismatches are usually a developer typo.
        materialised: list[ScheduleSpec] = []
        for key, spec in schedules.items():
            if spec.name != key:
                raise ValueError(
                    f"declarative dict key {key!r} does not match "
                    f"ScheduleSpec.name {spec.name!r}",
                )
            materialised.append(spec)
        schedules = materialised

    imported = [_to_imported(s, project=project, source=source) for s in schedules]

    client = BrainImportClient(
        brain_url=brain_url,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
    )
    summary = await _upload_with_mode(
        client=client,
        project_slug=project,
        schedules=imported,
        mode="replace_for_source",
        source_filter=source,
    )
    logger.info(
        "z4j.scheduler.declarative: reconciled project=%s source=%s "
        "inserted=%d updated=%d unchanged=%d deleted=%d failed=%d",
        project, source,
        summary.get("inserted", 0),
        summary.get("updated", 0),
        summary.get("unchanged", 0),
        summary.get("deleted", 0),
        summary.get("failed", 0),
    )
    return summary


def reconcile_sync(
    *,
    schedules: list[ScheduleSpec] | dict[str, ScheduleSpec],
    project: str,
    source: str,
    brain_url: str,
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, int]:
    """Blocking wrapper around :func:`reconcile` for sync hooks.

    Use from Django ``AppConfig.ready()`` or Flask
    ``before_first_request`` where there is no event loop. Async
    code should call :func:`reconcile` directly.

    Spawns a fresh event loop if none is running. If called from
    inside an event loop (e.g. async Django via ASGI), prefer
    :func:`reconcile` directly.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        raise RuntimeError(
            "reconcile_sync called inside a running event loop. "
            "Use the async reconcile() instead.",
        )
    return asyncio.run(
        reconcile(
            schedules=schedules,
            project=project,
            source=source,
            brain_url=brain_url,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        ),
    )


# =====================================================================
# Internals
# =====================================================================


def _to_imported(
    spec: ScheduleSpec, *, project: str, source: str,
) -> ImportedSchedule:
    """Convert a ScheduleSpec into the ImportedSchedule shape brain expects.

    The hash is computed by ImportedSchedule.compute_hash() at
    serialization time so callers don't need to think about it.
    """
    return ImportedSchedule(
        project_slug=project,
        name=spec.name,
        engine=spec.engine,
        kind=spec.kind,
        expression=spec.expression,
        task_name=spec.task_name,
        timezone=spec.timezone,
        queue=spec.queue,
        args=list(spec.args),
        kwargs=dict(spec.kwargs),
        catch_up=spec.catch_up,
        is_enabled=spec.is_enabled,
        source=source,
    )


async def _upload_with_mode(
    *,
    client: BrainImportClient,
    project_slug: str,
    schedules: list[ImportedSchedule],
    mode: str,
    source_filter: str,
) -> dict[str, int]:
    """Same as ``BrainImportClient.upload`` but with the mode + filter.

    The base ``upload`` method always uses ``mode="upsert"``.
    Declarative reconciliation needs ``replace_for_source`` so we
    re-implement the HTTP call here. Same wire format, same
    response shape.
    """
    import httpx  # noqa: PLC0415

    payload = {
        "schedules": [s.to_dict() for s in schedules],
        "mode": mode,
        "source_filter": source_filter,
    }
    headers = {"Content-Type": "application/json"}
    if client._api_token:  # noqa: SLF001 - private but stable within package
        headers["Authorization"] = f"Bearer {client._api_token}"  # noqa: SLF001

    url = (
        f"{client._brain_url}/api/v1/projects/{project_slug}"  # noqa: SLF001
        f"/schedules:import"
    )

    async with httpx.AsyncClient(timeout=client._timeout) as http:  # noqa: SLF001
        response = await http.post(url, json=payload, headers=headers)
        if response.status_code == 404:
            raise RuntimeError(
                "brain has no /schedules:import endpoint or no replace-"
                "for-source mode - upgrade brain to >= v1.2",
            )
        response.raise_for_status()
        data = response.json()
    return {
        "inserted": int(data.get("inserted", 0)),
        "updated": int(data.get("updated", 0)),
        "unchanged": int(data.get("unchanged", 0)),
        "failed": int(data.get("failed", 0)),
        "deleted": int(data.get("deleted", 0)),
        "errors": data.get("errors", {}),  # type: ignore[dict-item]
    }


__all__ = ["ScheduleSpec", "reconcile", "reconcile_sync"]
