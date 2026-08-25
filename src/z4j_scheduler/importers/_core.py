"""Shared types + writer plumbing for the migration importers.

Each importer module (celery / rq / apscheduler / cron) reads its
source format and produces a list of :class:`ImportedSchedule`
records. This module owns the pieces that don't vary per source:

- :class:`ImportedSchedule` - normalised schedule shape that maps
  cleanly onto brain's ``Schedule`` row.
- :func:`render_jsonl` - emit dry-run output as JSON Lines so the
  operator can pipe into ``jq`` or commit alongside the change.
- :class:`BrainImportClient` - thin HTTP wrapper that POSTs to
  brain's import endpoint and reports an actionable error when the
  connected brain predates that endpoint.

Importer authors only touch source-specific parsing; everything
post-parse goes through this module to keep the surface narrow.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

logger = logging.getLogger("z4j.scheduler.importers")

ImportedKind = Literal["cron", "interval", "clocked", "solar"]
ImportedCatchUp = Literal["skip", "fire_one_missed", "fire_all_missed"]


@dataclass(slots=True)
class ImportedSchedule:
    """One parsed schedule, ready to push to brain.

    Field shape mirrors brain's ``Schedule`` row plus the fields
    z4j-scheduler cares about (``catch_up``, ``source``,
    ``source_hash``).

    ``source_hash`` covers the execution-affecting imported fields, including
    engine, cadence, routing, arguments, catch-up policy, and enabled state, so
    a re-import of the same source state is a no-op. Importers that re-read
    after the operator edits that state produce a different hash.

    ``project_slug`` is required - we deliberately don't infer one
    from the source format because mapping a celery app to a brain
    project is the operator's call. The CLI requires
    ``--project SLUG``.
    """

    project_slug: str
    name: str
    engine: str  # "celery" | "rq" | "huey" | ...
    kind: ImportedKind
    expression: str
    task_name: str
    timezone: str = "UTC"
    queue: str | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    catch_up: ImportedCatchUp = "skip"
    is_enabled: bool = True
    source: str = "imported"
    source_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialise for JSONL output / HTTP upload.

        Computes ``source_hash`` lazily on demand so importer
        authors can mutate fields right up to writing without
        worrying about stale hashes.
        """
        out = asdict(self)
        if not out["source_hash"]:
            out["source_hash"] = self.compute_hash()
        return out

    def compute_hash(self) -> str:
        """Stable content hash for re-import idempotency.

        The hash covers every field that affects fire behavior.
        If it covered only the user-facing fields (name, kind,
        expression, timezone, task_name, args, kwargs), an
        importer with write access could:

        - flip ``engine: celery -> rq`` without changing the hash
          (brain treats it as ``unchanged``, no audit row, but the
          schedule now fires on a completely different engine)
        - flip ``is_enabled: true -> false`` covertly (silently
          disable a monitoring schedule with no event trail)
        - flip ``catch_up`` (silently change failure-mode behavior)
        - flip ``queue`` (silently route fires to a different queue)

        ``source`` is intentionally excluded - a rebrand of the
        importer source label (e.g. ``imported_celerybeat`` ->
        ``imported_celery_beat``) should not trigger spurious
        update events for every schedule.
        """
        import hashlib

        body = json.dumps(
            {
                "name": self.name,
                "engine": self.engine,
                "kind": self.kind,
                "expression": self.expression,
                "timezone": self.timezone,
                "task_name": self.task_name,
                "queue": self.queue,
                "args": self.args,
                "kwargs": self.kwargs,
                "catch_up": self.catch_up,
                "is_enabled": self.is_enabled,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(body.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


def render_jsonl(schedules: Iterable[ImportedSchedule]) -> str:
    """Render a list of schedules as newline-delimited JSON.

    Operators run ``z4j-scheduler import --from <tool> --dry-run``
    to see this output before committing - the JSONL form is friendly
    to ``jq``, ``diff``, and check-in to the operator's IaC repo.
    """
    return "\n".join(json.dumps(s.to_dict()) for s in schedules)


# ---------------------------------------------------------------------------
# Brain upload (best-effort fallback to JSONL on disk)
# ---------------------------------------------------------------------------


class BrainImportClient:
    """POST a batch of imported schedules to brain.

    Brain's REST API ships an ``/api/v1/projects/{slug}/schedules:import``
    endpoint that accepts a list of schedule dicts and upserts them. A 404 from
    an older brain raises ``RuntimeError``; operators can rerun with
    ``--dry-run`` to capture JSONL after upgrading or for manual inspection.

    Construction is cheap; :meth:`upload` opens an HTTPX client per
    call to avoid leaking sockets when the importer is invoked as a
    one-shot CLI.
    """

    def __init__(
        self,
        *,
        brain_url: str,
        api_token: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._brain_url = brain_url.rstrip("/")
        self._api_token = api_token
        self._timeout = timeout_seconds

    async def upload(
        self,
        *,
        project_slug: str,
        schedules: list[ImportedSchedule],
    ) -> dict[str, int]:
        """Upload schedules to brain. Returns the per-batch summary.

        The summary keys (``inserted`` / ``updated`` / ``unchanged``
        / ``failed``) come straight from brain's
        :class:`ImportSchedulesResponse`. The CLI prints these so
        the operator can see whether a re-import was a no-op or
        landed real changes.

        Raises :class:`RuntimeError` on HTTP errors so the CLI can
        surface them to the operator. We deliberately don't swallow
        exceptions here - a partial-import scenario is exactly the
        situation where the operator wants to see the error.
        """
        import httpx

        if not schedules:
            return {"inserted": 0, "updated": 0, "unchanged": 0, "failed": 0}

        payload = {
            "schedules": [s.to_dict() for s in schedules],
        }
        headers = {"Content-Type": "application/json"}
        if self._api_token:
            headers["Authorization"] = f"Bearer {self._api_token}"

        url = f"{self._brain_url}/api/v1/projects/{project_slug}/schedules:import"

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, json=payload, headers=headers)
            if response.status_code == 404:
                # Brain does not yet implement the import endpoint
                # (older release). Tell the caller; they can fall
                # back to dry-run + manual upload.
                raise RuntimeError(
                    "brain has no /schedules:import endpoint - upgrade "
                    "brain to >= v1.1 or use --dry-run + manual upload",
                )
            response.raise_for_status()
            data = response.json()
        # Surface per-row errors verbatim so the CLI can echo them.
        return {
            "inserted": int(data.get("inserted", 0)),
            "updated": int(data.get("updated", 0)),
            "unchanged": int(data.get("unchanged", 0)),
            "failed": int(data.get("failed", 0)),
            "errors": data.get("errors", {}),  # type: ignore[dict-item]
        }


__all__ = [
    "BrainImportClient",
    "ImportedCatchUp",
    "ImportedKind",
    "ImportedSchedule",
    "render_jsonl",
]
