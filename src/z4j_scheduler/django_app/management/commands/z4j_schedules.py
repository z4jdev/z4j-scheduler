"""``manage.py z4j_schedules sync|list|diff|trigger`` Django command.

Per ``docs/SCHEDULER.md §15.1``, this is the operator-facing CLI
for declarative reconciliation against brain. Wraps the reconcile
helpers + brain REST endpoints so deploy hooks, cron jobs, and
manual operator use all hit the same code path.

Subcommands
-----------

- ``sync`` - read ``settings.Z4J_SCHEDULES`` + push to brain. Deletes
  brain rows with the same source label that aren't in the dict
  (``replace_for_source`` semantics). Idempotent.
- ``list`` - show what brain currently has for the project.
- ``diff`` - dry-run: show what ``sync`` WOULD do.
- ``trigger <name>`` - one-off out-of-band fire.

Settings (read from ``django.conf.settings``)
---------------------------------------------

Same shape as :mod:`z4j_scheduler.declarative.frameworks.django`:
``Z4J_SCHEDULES``, ``Z4J_SCHEDULES_PROJECT``,
``Z4J_SCHEDULES_BRAIN_URL``, ``Z4J_SCHEDULES_API_TOKEN``,
``Z4J_SCHEDULES_SOURCE``.
"""

from __future__ import annotations

import json
from typing import Any

try:
    from django.core.management.base import BaseCommand, CommandError
except ImportError:  # pragma: no cover - module skipped outside Django
    BaseCommand = object  # type: ignore[misc,assignment]
    CommandError = RuntimeError  # type: ignore[misc,assignment]


class Command(BaseCommand):  # type: ignore[misc]
    help = (
        "Manage z4j schedules from Django: sync (push declarative dict), "
        "list (show brain state), diff (dry-run sync), or trigger one schedule."
    )

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "subcommand",
            choices=("sync", "list", "diff", "trigger"),
            help="which action to perform",
        )
        parser.add_argument(
            "--name",
            default=None,
            help="schedule name (required for `trigger`)",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            dest="json_output",
            help="emit raw JSON instead of human-readable output",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        subcommand = options["subcommand"]
        name = options.get("name")
        json_output = options.get("json_output", False)

        if subcommand == "sync":
            self._cmd_sync(json_output=json_output)
        elif subcommand == "list":
            self._cmd_list(json_output=json_output)
        elif subcommand == "diff":
            self._cmd_diff(json_output=json_output)
        elif subcommand == "trigger":
            if not name:
                raise CommandError("`trigger` requires --name")
            self._cmd_trigger(name=name)

    # ------------------------------------------------------------------
    # Subcommand bodies
    # ------------------------------------------------------------------

    def _cmd_sync(self, *, json_output: bool) -> None:
        from z4j_scheduler.declarative.frameworks.django import (
            reconcile_from_settings,
        )

        summary = reconcile_from_settings()
        if summary is None:
            raise CommandError(
                "reconcile failed - check Z4J_SCHEDULES + Z4J_SCHEDULES_PROJECT settings",
            )
        if json_output:
            self.stdout.write(json.dumps(summary, indent=2))
        else:
            self.stdout.write(
                f"sync complete: inserted={summary.get('inserted', 0)}, "
                f"updated={summary.get('updated', 0)}, "
                f"unchanged={summary.get('unchanged', 0)}, "
                f"deleted={summary.get('deleted', 0)}, "
                f"failed={summary.get('failed', 0)}",
            )

    def _cmd_list(self, *, json_output: bool) -> None:
        cfg = self._read_brain_config()
        rows = self._brain_get(cfg, f"/api/v1/projects/{cfg['project']}/schedules")
        if json_output:
            self.stdout.write(json.dumps(rows, indent=2))
            return
        if not rows:
            self.stdout.write("(no schedules)")
            return
        self.stdout.write(
            f"{'name':<30} {'kind':<10} {'expression':<25} {'enabled':<8} {'source':<22}",
        )
        for r in rows:
            self.stdout.write(
                f"{r['name'][:30]:<30} {r['kind']:<10} "
                f"{r['expression'][:25]:<25} "
                f"{r['is_enabled']!s:<8} {r.get('source', '')[:22]:<22}",
            )

    def _cmd_diff(self, *, json_output: bool) -> None:
        """Dry-run sync. Compares declarative dict to brain state.

        Reports per-schedule diff in the operator's terminal so they
        know what an actual sync would do. Doesn't write anything to
        brain - safe to run from a CI lint step.
        """
        from django.conf import settings

        from z4j_scheduler.declarative._reconciler import _to_imported

        declared = getattr(settings, "Z4J_SCHEDULES", None)
        if not declared:
            self.stdout.write("(Z4J_SCHEDULES is empty)")
            return
        cfg = self._read_brain_config()

        brain_rows = self._brain_get(
            cfg,
            f"/api/v1/projects/{cfg['project']}/schedules",
        )
        # Only the rows from our source matter for diff scoping.
        brain_for_source = [r for r in brain_rows if r.get("source") == cfg["source"]]
        brain_by_name = {r["name"]: r for r in brain_for_source}

        # Convert each declared spec to its hash to compare with
        # brain's stored source_hash.
        declared_specs = list(declared.values()) if isinstance(declared, dict) else list(declared)

        will_insert: list[str] = []
        will_update: list[str] = []
        will_unchanged: list[str] = []
        for spec in declared_specs:
            imported = _to_imported(
                spec,
                project=cfg["project"],
                source=cfg["source"],
            )
            current = brain_by_name.get(imported.name)
            if current is None:
                will_insert.append(imported.name)
            elif current.get("source_hash") == imported.compute_hash():
                will_unchanged.append(imported.name)
            else:
                will_update.append(imported.name)
        # Anything in brain (for our source) that we're not declaring
        # would be deleted by replace_for_source.
        declared_names = {s.name for s in declared_specs}
        will_delete = [r["name"] for r in brain_for_source if r["name"] not in declared_names]

        if json_output:
            self.stdout.write(
                json.dumps(
                    {
                        "insert": will_insert,
                        "update": will_update,
                        "unchanged": will_unchanged,
                        "delete": will_delete,
                    },
                    indent=2,
                ),
            )
            return
        self.stdout.write(
            f"sync would: insert={len(will_insert)} "
            f"update={len(will_update)} "
            f"unchanged={len(will_unchanged)} "
            f"delete={len(will_delete)}",
        )
        for label, names in (
            ("INSERT", will_insert),
            ("UPDATE", will_update),
            ("DELETE", will_delete),
        ):
            for name in names:
                self.stdout.write(f"  {label}: {name}")

    def _cmd_trigger(self, *, name: str) -> None:
        cfg = self._read_brain_config()
        rows = self._brain_get(cfg, f"/api/v1/projects/{cfg['project']}/schedules")
        target = next((r for r in rows if r["name"] == name), None)
        if target is None:
            raise CommandError(
                f"no schedule named {name!r} in project {cfg['project']!r}",
            )
        self._brain_post(
            cfg,
            f"/api/v1/projects/{cfg['project']}/schedules/{target['id']}/trigger",
            body={},
        )
        self.stdout.write(f"triggered {name}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_brain_config(self) -> dict[str, Any]:
        from django.conf import settings

        project = getattr(settings, "Z4J_SCHEDULES_PROJECT", None)
        if not project:
            raise CommandError(
                "settings.Z4J_SCHEDULES_PROJECT is required",
            )
        return {
            "project": project,
            "brain_url": getattr(
                settings,
                "Z4J_SCHEDULES_BRAIN_URL",
                "http://brain:7700",
            ),
            "api_token": getattr(
                settings,
                "Z4J_SCHEDULES_API_TOKEN",
                None,
            ),
            "source": getattr(
                settings,
                "Z4J_SCHEDULES_SOURCE",
                "declarative_django",
            ),
        }

    def _brain_get(self, cfg: dict[str, Any], path: str) -> Any:
        import httpx

        headers = {}
        if cfg.get("api_token"):
            headers["Authorization"] = f"Bearer {cfg['api_token']}"
        try:
            response = httpx.get(
                f"{cfg['brain_url'].rstrip('/')}{path}",
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise CommandError(f"brain GET {path} failed: {exc}") from exc
        return response.json()

    def _brain_post(
        self,
        cfg: dict[str, Any],
        path: str,
        body: dict,
    ) -> Any:
        import httpx

        headers = {"Content-Type": "application/json"}
        if cfg.get("api_token"):
            headers["Authorization"] = f"Bearer {cfg['api_token']}"
        response = httpx.post(
            f"{cfg['brain_url'].rstrip('/')}{path}",
            json=body,
            headers=headers,
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise CommandError(
                f"brain POST {path} returned {response.status_code}: {response.text}",
            )
        if response.status_code == 204 or not response.content:
            return None
        return response.json()
