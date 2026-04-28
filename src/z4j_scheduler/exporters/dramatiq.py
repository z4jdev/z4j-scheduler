"""dramatiq exporter - documentation stub.

See ``importers/dramatiq.py`` for the rationale: dramatiq has no
built-in scheduler, so there's no "native dramatiq scheduler
config" to export to. Operators leaving z4j-scheduler for a
dramatiq-native scheduler should:

1. **APScheduler:** use ``z4j-scheduler export --to apscheduler``
   - the rendered APScheduler config can fire dramatiq actors.
2. **dramatiq-cron / hand-roll:** the rendered output for these
   has no standard shape; write a small adapter using the JSONL
   from ``z4j-scheduler export --to jsonl`` (the operator-
   readable export).

This module exists so the CLI's ``--to dramatiq`` flag returns a
clear guidance message instead of an ImportError.
"""

from __future__ import annotations

from collections.abc import Iterable

from z4j_scheduler.exporters._client import ExportedSchedule


_GUIDANCE = """\
Dramatiq has no native scheduler config to export to. Two options:

  1. Use --to apscheduler: APScheduler can fire dramatiq actors.
     The rendered output is a Python module the operator wires
     into their startup.

  2. Use --to jsonl (the operator-readable export) and write a
     small adapter that consumes the JSONL into your custom
     dramatiq-cron / hand-rolled scheduler.
"""


def render(schedules: Iterable[ExportedSchedule]) -> str:  # noqa: ARG001
    """Always returns the guidance message as a comment-prefixed string.

    Dramatiq has no native scheduler config to render to.
    """
    body = "\n".join(f"# {line}" for line in _GUIDANCE.splitlines())
    return body + "\n"


__all__ = ["render"]
