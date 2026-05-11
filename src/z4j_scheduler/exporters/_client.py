"""REST client + shared dataclass for the exporters.

Fetches the schedule set from brain via
``GET /api/v1/projects/{slug}/schedules`` and converts each row to
the :class:`ExportedSchedule` shape the per-target renderers
consume.

Symmetric with :class:`z4j_scheduler.importers._core.BrainImportClient`
but read-side. We deliberately don't share a base class - the read
flow is half the lines of the write flow and a Protocol is enough
abstraction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("z4j.scheduler.exporters")


@dataclass(slots=True)
class ExportedSchedule:
    """One brain schedule row, normalised for export rendering.

    Mirrors :class:`z4j_scheduler.importers._core.ImportedSchedule`
    plus a few extra fields the renderers use:

    - ``id`` - brain UUID, included so the rendered output can
      reference it as a stable id (APScheduler uses it as the
      ``job_id`` argument).
    - ``last_run_at`` / ``next_run_at`` - rendered as comments
      next to each schedule for operator context, never as
      machine-readable fields (export targets compute their own).
    """

    id: str
    name: str
    engine: str
    kind: str
    expression: str
    task_name: str
    timezone: str = "UTC"
    queue: str | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    catch_up: str = "skip"
    is_enabled: bool = True
    scheduler: str = "z4j-scheduler"
    source: str = ""
    last_run_at: str | None = None
    next_run_at: str | None = None


async def fetch_schedules(
    *,
    brain_url: str,
    project_slug: str,
    api_token: str | None,
    timeout_seconds: float = 30.0,
    scheduler_filter: str | None = "z4j-scheduler",
    source_filter: str | None = None,
) -> list[ExportedSchedule]:
    """Fetch + filter schedules from brain.

    Args:
        brain_url: Brain REST URL (e.g. ``http://brain:7700``).
        project_slug: Project slug to fetch from.
        api_token: Bearer token with VIEWER+ role.
        scheduler_filter: Only return schedules where
            ``scheduler == this``. Defaults to ``"z4j-scheduler"``
            (the export targets are migrating off z4j; we don't
            want to surface schedules brain holds for celery-beat
            etc.). Pass ``None`` to skip the filter.
        source_filter: Only return schedules where ``source == this``.
            Useful for exporting only declarative-managed rows
            while leaving dashboard-managed rows in place.

    Raises :class:`RuntimeError` on HTTP errors so the CLI can
    surface them.
    """
    import httpx  # noqa: PLC0415

    headers = {"Accept": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    url = f"{brain_url.rstrip('/')}/api/v1/projects/{project_slug}/schedules"

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.get(url, headers=headers)
        if response.status_code == 404:
            raise RuntimeError(
                f"brain returned 404 for project {project_slug!r} - "
                "wrong slug or token lacks access",
            )
        response.raise_for_status()
        body = response.json()

    # Brain v1.1.0 paginated the
    # ``GET /schedules`` endpoint - the response is now a dict
    # ``{"items": [...], "next_cursor": "..."}`` instead of a
    # flat list. Tolerate both shapes so an old brain (flat list)
    # and the new brain (envelope) work with the same exporter.
    if isinstance(body, dict):
        rows = body.get("items", [])
    else:
        rows = body

    out: list[ExportedSchedule] = []
    for row in rows:
        if scheduler_filter and row.get("scheduler") != scheduler_filter:
            continue
        if source_filter and row.get("source") != source_filter:
            continue
        out.append(
            ExportedSchedule(
                id=str(row["id"]),
                name=str(row["name"]),
                engine=str(row.get("engine", "celery")),
                kind=str(row.get("kind", "cron")),
                expression=str(row.get("expression", "")),
                task_name=str(row.get("task_name", "")),
                timezone=str(row.get("timezone", "UTC")) or "UTC",
                queue=row.get("queue"),
                args=list(row.get("args") or []),
                kwargs=dict(row.get("kwargs") or {}),
                catch_up=str(row.get("catch_up", "skip")),
                is_enabled=bool(row.get("is_enabled", True)),
                scheduler=str(row.get("scheduler", "z4j-scheduler")),
                source=str(row.get("source", "")),
                last_run_at=row.get("last_run_at"),
                next_run_at=row.get("next_run_at"),
            ),
        )
    return out


def py_repr(value: Any) -> str:
    """Render value as a Python literal (not JSON).

    Shared by every exporter so a schedule's args/kwargs round-trip
    cleanly when an operator pastes the rendered output into their
    own settings.py / startup script. The naive ``json.dumps`` path
    emits ``true`` / ``false`` / ``null`` (JSON literals) which
    Python rejects as ``NameError``. We do the conversion explicitly
    instead. Recursive so nested dicts/lists with bool leaves work
    correctly.

    For anything exotic (datetime, UUID, ...) we fall back to
    ``json.dumps(str(value))`` which mirrors the previous
    ``default=str`` behaviour.
    """
    import json  # noqa: PLC0415

    if isinstance(value, bool):
        # bool MUST come before int (bool is a subclass of int).
        return "True" if value else "False"
    if value is None:
        return "None"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)  # safe quoting + escaping
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(py_repr(v) for v in value) + "]"
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(
                f"{py_repr(str(k))}: {py_repr(v)}" for k, v in value.items()
            )
            + "}"
        )
    return json.dumps(str(value))


__all__ = ["ExportedSchedule", "fetch_schedules", "py_repr"]
