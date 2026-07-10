"""System crontab importer.

Reads a Unix crontab file (``/etc/crontab``, a per-user crontab, or
any file shaped like one) and converts each schedule line into an
:class:`ImportedSchedule`. Fires call a configured shell-exec task
on the user's worker:

    z4j-scheduler import --from cron \\
        --crontab /etc/crontab \\
        --task-prefix myapp.shell.exec_command \\
        --project acme-prod ...

The shell command becomes the first positional arg to the task; the
operator's worker is expected to implement ``exec_command(cmd)`` as
a normal celery/rq/whatever task. We deliberately do NOT shell out
from the scheduler - the scheduler sends a fire to brain, brain
dispatches the command to the agent, and the agent's existing
engine workers run the task. This is the same execution path as
every other z4j fire.

Format support:

- Standard 5-field cron lines (``min hour dom mon dow command``)
- The 6-field form with a leading ``user`` column used by
  ``/etc/crontab`` (we drop the user column - the engine worker's
  identity is already what runs the task)
- ``@reboot`` / ``@yearly`` / etc. shortcuts -> expanded to
  equivalent 5-field expressions (``@reboot`` is skipped since
  z4j-scheduler doesn't model reboot triggers)
- ``MAILTO=`` and ``PATH=`` env-line directives -> ignored

No optional deps - the parser is stdlib + croniter (which is
already a hard dep of z4j-scheduler).
"""

from __future__ import annotations

import logging
from pathlib import Path

from z4j_scheduler.importers._core import ImportedSchedule

logger = logging.getLogger("z4j.scheduler.importers.cron")


# Standard cron @-shortcut expansions. ``@reboot`` is intentionally
# omitted - z4j-scheduler doesn't model "fire on every brain start".
_SHORTCUTS: dict[str, str] = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def read_crontab(
    *,
    crontab_path: str | Path,
    project_slug: str,
    task_prefix: str,
    engine: str = "celery",
    queue: str | None = None,
    timezone: str = "UTC",
    has_user_column: bool = False,
) -> list[ImportedSchedule]:
    """Parse a crontab file and return :class:`ImportedSchedule` records.

    Args:
        crontab_path: Path to the crontab file. ``/etc/crontab``,
            a per-user file from ``crontab -l > out.cron``, or any
            other crontab-shaped text file.
        project_slug: Brain project slug to attribute imports to.
        task_prefix: Fully-qualified task name (e.g.
            ``"myapp.shell.exec_command"``) that takes the cron
            command as a string arg.
        engine: Engine name written to the imported row (default
            ``"celery"``). The chosen engine must have a registered
            implementation of ``task_prefix`` on the worker.
        queue: Default queue. ``None`` lets brain pick.
        timezone: Timezone tag applied to every imported schedule.
            Crontabs themselves don't carry timezones; the operator
            tells us what their host clock is.
        has_user_column: Set True when parsing ``/etc/crontab`` (or
            other system crontabs that include a username field
            between the schedule and the command).
    """
    # Defend against symlink redirection and unbounded file size.
    #
    # Without O_NOFOLLOW: an attacker who controls the path
    # (e.g., a shared upload directory) can swap the file for a
    # symlink to ``/etc/shadow`` or ``/proc/self/environ``. The
    # parser only extracts cron-shaped lines, but the
    # malformed-line warnings log fragments via
    # ``logger.warning(... line, ...)`` - info disclosure.
    #
    # Without a size cap: a 10GB file path crashes brain on OOM
    # because ``read_text`` materialises the whole file before
    # split. We cap at 1 MB which is ~3 orders of magnitude above
    # any legitimate crontab.
    import os

    max_crontab_bytes = 1 * 1024 * 1024
    path = Path(crontab_path)
    if not path.is_file():
        raise FileNotFoundError(
            f"crontab file not found: {crontab_path}",
        )
    if path.stat().st_size > max_crontab_bytes:
        raise ValueError(
            f"crontab file {crontab_path} is "
            f"{path.stat().st_size} bytes; refusing to import "
            f"(cap is {max_crontab_bytes} bytes). Trim the file "
            f"or remove non-z4j entries before re-running.",
        )
    # Open with O_NOFOLLOW (POSIX) so a pre-planted symlink can't
    # redirect the read. On Windows O_NOFOLLOW is 0 (no-op) -
    # acceptable since Windows symlinks require specific privilege
    # to create.
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags)
    except OSError as exc:
        # On POSIX, opening a symlink with O_NOFOLLOW raises
        # ELOOP. Surface a clear message.
        raise ValueError(
            f"crontab file {crontab_path} could not be opened safely (symlink? permission?): {exc}",
        ) from exc
    try:
        with os.fdopen(fd, "rb") as fh:
            raw_bytes = fh.read(max_crontab_bytes + 1)
    except OSError:
        os.close(fd)
        raise
    if len(raw_bytes) > max_crontab_bytes:
        raise ValueError(
            f"crontab file {crontab_path} grew past "
            f"{max_crontab_bytes} bytes during read; refusing to "
            f"import",
        )
    contents = raw_bytes.decode("utf-8", errors="replace")

    schedules: list[ImportedSchedule] = []
    counter = 1
    for raw_line in contents.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line.split(None, 1)[0]:
            # ``MAILTO=foo`` / ``PATH=...`` style env directive.
            continue
        try:
            sched = _parse_line(
                line=line,
                project_slug=project_slug,
                engine=engine,
                queue=queue,
                timezone=timezone,
                task_prefix=task_prefix,
                index=counter,
                has_user_column=has_user_column,
            )
        except _UnsupportedLineError as exc:
            logger.warning(
                "z4j.scheduler.importers.cron: skipping line %r - %s",
                line,
                exc,
            )
            continue
        if sched is not None:
            schedules.append(sched)
            counter += 1
    return schedules


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _UnsupportedLineError(Exception):
    """Raised when a crontab line cannot be translated."""


def _parse_line(
    *,
    line: str,
    project_slug: str,
    engine: str,
    queue: str | None,
    timezone: str,
    task_prefix: str,
    index: int,
    has_user_column: bool,
) -> ImportedSchedule:
    """Parse one schedule line into an :class:`ImportedSchedule`."""
    if line.startswith("@"):
        # Shortcut form: ``@hourly some command --here``
        head, _, command = line.partition(" ")
        head = head.lower()
        if head == "@reboot":
            raise _UnsupportedLineError(
                "@reboot is not modelled by z4j-scheduler",
            )
        expression = _SHORTCUTS.get(head)
        if expression is None:
            raise _UnsupportedLineError(
                f"unknown cron shortcut {head!r}",
            )
        command = command.strip()
        if has_user_column:
            # ``@reboot user command`` form - drop the user column.
            _user, _, command = command.partition(" ")
            command = command.strip()
        if not command:
            raise _UnsupportedLineError(
                "shortcut form has no command",
            )
        return _build_schedule(
            project_slug=project_slug,
            engine=engine,
            queue=queue,
            timezone=timezone,
            task_prefix=task_prefix,
            index=index,
            expression=expression,
            command=command,
        )

    fields = line.split(None, 6 if has_user_column else 5)
    if (has_user_column and len(fields) < 7) or (not has_user_column and len(fields) < 6):
        raise _UnsupportedLineError(
            "line does not have enough fields for a cron entry",
        )

    expression = " ".join(fields[:5])
    # fields[5] is user, fields[6] is the command (split-7 caps it).
    command = fields[6] if has_user_column else fields[5]

    _validate_cron_expression(expression)
    return _build_schedule(
        project_slug=project_slug,
        engine=engine,
        queue=queue,
        timezone=timezone,
        task_prefix=task_prefix,
        index=index,
        expression=expression,
        command=command,
    )


def _build_schedule(
    *,
    project_slug: str,
    engine: str,
    queue: str | None,
    timezone: str,
    task_prefix: str,
    index: int,
    expression: str,
    command: str,
) -> ImportedSchedule:
    return ImportedSchedule(
        project_slug=project_slug,
        name=f"cron-import-{index:04d}",
        engine=engine,
        kind="cron",
        expression=expression,
        timezone=timezone,
        task_name=task_prefix,
        queue=queue,
        args=[command],
        kwargs={},
        catch_up="skip",
        is_enabled=True,
        source="imported_cron",
    )


def _validate_cron_expression(expression: str) -> None:
    """Round-trip the expression through croniter to catch malformed lines.

    Raises :class:`_UnsupportedLineError` so the caller can skip the
    line without aborting the whole import.
    """
    try:
        from croniter import croniter
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "z4j-scheduler missing croniter dep (should be a hard dep)",
        ) from exc

    if not croniter.is_valid(expression):
        raise _UnsupportedLineError(
            f"croniter rejected expression {expression!r}",
        )


__all__ = ["read_crontab"]
