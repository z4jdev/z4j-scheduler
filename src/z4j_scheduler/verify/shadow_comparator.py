"""Compare independently constructed schedule-fire predictions.

This module supplies the prediction model and comparison primitive; it does
not obtain the two independent inputs itself. A caller must derive the source
and target lists from distinct representations. In particular, predicting
twice from one importer's normalized output cannot detect importer
translation drift and must not be presented as cutover proof.

When a caller has independent inputs, the interesting divergence cases are:

- **Importer translation bugs.** Operator's celery
  ``crontab(minute='*/15', hour='9-17')`` should translate to the
  5-field cron ``*/15 9-17 * * *``. If the importer mis-translates
  one of the cron fields, the predicted fire times diverge and we
  flag it.
- **Timezone misconfiguration.** Operator runs celery-beat in
  ``Europe/Berlin`` but the importer captured ``timezone="UTC"``.
  Predicted fires shift by N hours; loud divergence.
- **One-side-only fires.** A schedule the operator forgot to
  include in the migration set - or one z4j-scheduler would tick
  but celery-beat wouldn't (rare; would indicate a bad import).
- **Args / kwargs / queue drift.** The same fire on both sides
  should carry identical task args. If not, the importer dropped
  data.

Because the comparison is deterministic, a caller can run it locally in
seconds even for a 7-day window with hundreds of schedules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from croniter import croniter

if TYPE_CHECKING:
    from z4j_scheduler.importers._core import ImportedSchedule


# =====================================================================
# Duration parsing
# =====================================================================

_DURATION_RE = re.compile(r"^\s*(?P<n>\d+(?:\.\d+)?)\s*(?P<unit>[smhd])?\s*$")


def parse_duration(value: str) -> timedelta:
    """Parse human duration strings to a :class:`timedelta`.

    Accepts ``"30s"`` / ``"5m"`` / ``"24h"`` / ``"7d"`` and bare
    numbers (treated as seconds). Raises :class:`ValueError` on
    anything else - the operator should see a clear error message
    rather than a silent wrong value.
    """
    if not value:
        raise ValueError("duration must be non-empty")
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(
            f"unparseable duration {value!r}; expected ``Ns`` / ``Nm`` / "
            f"``Nh`` / ``Nd`` (e.g. ``24h``)",
        )
    n = float(match.group("n"))
    unit = match.group("unit") or "s"
    return {
        "s": timedelta(seconds=n),
        "m": timedelta(minutes=n),
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
    }[unit]


# =====================================================================
# Predicted fire model + prediction helpers
# =====================================================================


@dataclass(frozen=True, slots=True)
class PredictedFire:
    """One predicted fire event for a single schedule.

    The comparison is keyed on (schedule_name, fire_time) so the
    other fields are payload that the report renders side-by-side
    when divergence is found.
    """

    schedule_name: str
    fire_time: datetime
    task_name: str
    args: tuple = ()
    kwargs: tuple = ()  # tuple of (key, value) pairs - hashable
    queue: str | None = None

    @property
    def key(self) -> tuple[str, datetime]:
        return (self.schedule_name, self.fire_time)


def _kwargs_tuple(kwargs: dict) -> tuple:
    """Hashable, sort-stable representation of a kwargs dict."""
    return tuple(sorted(kwargs.items()))


def predict_fires(
    schedules: list[ImportedSchedule],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[PredictedFire]:
    """Compute every fire each schedule would emit in the window.

    Cron fires use croniter against the schedule's declared
    timezone (defaults UTC). Interval fires are evenly spaced from
    ``window_start``. One-shot fires are included only if their
    target instant lies inside the window.

    Returns an unordered list - the comparator sorts by ``key`` for
    stable output.
    """
    out: list[PredictedFire] = []
    for sched in schedules:
        if not sched.is_enabled:
            # Disabled schedules emit nothing on either side.
            continue
        if sched.kind == "cron":
            out.extend(_predict_cron(sched, window_start, window_end))
        elif sched.kind == "interval":
            out.extend(_predict_interval(sched, window_start, window_end))
        elif sched.kind in ("clocked", "one_shot"):
            out.extend(_predict_one_shot(sched, window_start, window_end))
        elif sched.kind == "solar":
            out.extend(_predict_solar(sched, window_start, window_end))
        # Unknown kinds: silently skip. The comparator's job is to
        # surface DIVERGENCE in known kinds; a kind we don't support
        # gets caught by the importer earlier in the pipeline.
    return out


def _predict_cron(
    sched: ImportedSchedule,
    window_start: datetime,
    window_end: datetime,
) -> list[PredictedFire]:
    try:
        tz = _resolve_tz(sched.timezone)
    except Exception:
        # Unparseable timezone falls back to UTC. Operator sees this
        # in the importer's earlier pass; we don't double-warn here.
        tz = UTC
    # Anchor at window_start in the schedule's tz so the first
    # ``get_next`` returns the first fire INSIDE the window, not
    # whatever was last triggered.
    anchor = window_start.astimezone(tz)
    try:
        cron = croniter(sched.expression, anchor)
    except Exception:
        # Unparseable cron - return zero fires. The importer
        # rejected the row earlier; we just don't double-fault.
        return []
    fires: list[PredictedFire] = []
    while True:
        next_dt = cron.get_next(datetime)
        if next_dt > window_end.astimezone(tz):
            break
        fires.append(
            PredictedFire(
                schedule_name=sched.name,
                fire_time=next_dt.astimezone(UTC),
                task_name=sched.task_name,
                args=tuple(sched.args),
                kwargs=_kwargs_tuple(sched.kwargs),
                queue=sched.queue,
            )
        )
        # Defensive cap so a truly malformed expression that fires
        # every nanosecond can't OOM the process.
        if len(fires) > 100_000:
            break
    return fires


def _predict_interval(
    sched: ImportedSchedule,
    window_start: datetime,
    window_end: datetime,
) -> list[PredictedFire]:
    seconds = _interval_to_seconds(sched.expression)
    if seconds <= 0:
        return []
    out: list[PredictedFire] = []
    next_dt = window_start
    while next_dt <= window_end:
        out.append(
            PredictedFire(
                schedule_name=sched.name,
                fire_time=next_dt,
                task_name=sched.task_name,
                args=tuple(sched.args),
                kwargs=_kwargs_tuple(sched.kwargs),
                queue=sched.queue,
            )
        )
        next_dt = next_dt + timedelta(seconds=seconds)
        if len(out) > 100_000:
            break
    return out


def _predict_solar(
    sched: ImportedSchedule,
    window_start: datetime,
    window_end: datetime,
) -> list[PredictedFire]:
    """Predict every solar event fire in the window.

    Solar events fire at most once per day (some events skip days
    at polar latitudes). We walk forward day-by-day asking the
    astral helper for the next occurrence after the current
    cursor; stops when the next fire would land past window_end
    or when astral can't compute (perpetual day / night).
    """
    try:
        from z4j_scheduler.tick.solar import next_solar_fire
    except ImportError:
        return []
    fires: list[PredictedFire] = []
    cursor = window_start
    while cursor < window_end:
        try:
            nxt = next_solar_fire(sched.expression, cursor)
        except (ValueError, RuntimeError):
            return []
        if nxt is None or nxt > window_end:
            break
        fires.append(
            PredictedFire(
                schedule_name=sched.name,
                fire_time=nxt,
                task_name=sched.task_name,
                args=tuple(sched.args),
                kwargs=_kwargs_tuple(sched.kwargs),
                queue=sched.queue,
            )
        )
        cursor = nxt
        if len(fires) > 100_000:
            break
    return fires


def _predict_one_shot(
    sched: ImportedSchedule,
    window_start: datetime,
    window_end: datetime,
) -> list[PredictedFire]:
    try:
        target = datetime.fromisoformat(sched.expression)
    except ValueError:
        return []
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    if window_start <= target <= window_end:
        return [
            PredictedFire(
                schedule_name=sched.name,
                fire_time=target.astimezone(UTC),
                task_name=sched.task_name,
                args=tuple(sched.args),
                kwargs=_kwargs_tuple(sched.kwargs),
                queue=sched.queue,
            )
        ]
    return []


def _resolve_tz(name: str):
    """Resolve a timezone name to a tzinfo. Falls through to UTC.

    Resolution goes through :func:`packaged_zoneinfo`, the same
    release-pinned ``tzdata`` wheel the tick engine reads, NOT through
    bare ``ZoneInfo`` -- which searches the host's ``/usr/share/zoneinfo``
    first and only falls back to the wheel.

    The two sources genuinely disagree. The shipped image
    (``python:3.14-slim-trixie``) carries IANA 2026b while the pinned
    wheel is 2026a. Measured in that exact pairing, sweeping every
    available zone at six-hour resolution across 2020-2035, they differ
    on exactly ONE: ``America/Vancouver``, from 2026-11-01, where Canada
    drops the autumn fall-back.

    Reading the host tzdb here made this comparator predict fires an hour
    away from the engine it exists to check, for that zone -- a
    divergence report caused by the comparator rather than by the import
    it is auditing. Timezone misconfiguration is one of the four
    divergence classes named in this module's docstring, so getting it
    wrong here is a false result on the tool's own headline case.

    A note for whoever re-derives this, because it has been stated
    wrongly more than once. The answer depends on WHICH PAIRING you
    measure, and on the pin. Host-against-wheel (this paragraph) is not
    the same set as wheel-against-wheel: 2026.1 against 2026.3 is seven
    zones, which is the justification for the tzdata correction this
    release carries. Re-measure rather than trusting either number.

    The UTC fallback below is unchanged and deliberate: an unparseable
    zone is reported by the importer's earlier pass, and this function
    does not double-warn.
    """
    if not name or name == "UTC":
        return UTC
    try:
        from z4j_scheduler.tick._runtime import packaged_zoneinfo

        return packaged_zoneinfo(name)
    except Exception:
        return UTC


def _interval_to_seconds(expression: str) -> int:
    """Parse interval expressions to seconds.

    Mirrors :func:`z4j_scheduler.exporters.celery._interval_to_seconds`
    so the importer + comparator + exporter agree on the same
    vocabulary. Bare integer = seconds.
    """
    s = expression.strip()
    if s.endswith("ms"):
        return max(1, int(s[:-2]) // 1000)
    if s.endswith("s"):
        return int(s[:-1])
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    return int(s)


# =====================================================================
# Comparison + report
# =====================================================================


@dataclass(frozen=True, slots=True)
class FireDivergence:
    """One fire that does not match between source + target.

    ``kind`` is one of:

    - ``"only_source"`` - source predicted a fire the target won't.
    - ``"only_target"`` - target predicted a fire the source won't.
    - ``"args_diverge"`` - both sides fire at the same time but
      with different args / kwargs / queue / task_name. Both fires
      are present in the divergence so the operator can see both
      sides.
    """

    kind: str
    schedule_name: str
    fire_time: datetime
    source: PredictedFire | None
    target: PredictedFire | None


@dataclass(frozen=True, slots=True)
class ShadowComparisonReport:
    """Result of comparing the source + target fire predictions."""

    window_start: datetime
    window_end: datetime
    source_label: str
    target_label: str
    source_fire_count: int
    target_fire_count: int
    matched: int
    divergences: list[FireDivergence] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True iff zero divergences. The operator's go/no-go signal."""
        return len(self.divergences) == 0


def compare_predicted_fires(
    *,
    source: list[PredictedFire],
    target: list[PredictedFire],
    window_start: datetime,
    window_end: datetime,
    source_label: str = "celery-beat",
    target_label: str = "z4j-scheduler",
) -> ShadowComparisonReport:
    """Bucket every predicted fire as matched / source-only / target-only.

    Matching is by (schedule_name, fire_time) and the args/kwargs/
    queue/task_name must agree exactly for a true match. A fire
    that lines up on time but differs in payload is recorded as an
    ``args_diverge`` divergence so the operator sees the timing was
    correct but the importer dropped data.
    """
    source_by_key: dict[tuple, PredictedFire] = {f.key: f for f in source}
    target_by_key: dict[tuple, PredictedFire] = {f.key: f for f in target}

    divergences: list[FireDivergence] = []
    matched = 0

    # Source side - check each fire against the target's matching key.
    for key, src_fire in source_by_key.items():
        tgt_fire = target_by_key.get(key)
        if tgt_fire is None:
            divergences.append(
                FireDivergence(
                    kind="only_source",
                    schedule_name=src_fire.schedule_name,
                    fire_time=src_fire.fire_time,
                    source=src_fire,
                    target=None,
                )
            )
            continue
        if (
            src_fire.task_name == tgt_fire.task_name
            and src_fire.args == tgt_fire.args
            and src_fire.kwargs == tgt_fire.kwargs
            and src_fire.queue == tgt_fire.queue
        ):
            matched += 1
        else:
            divergences.append(
                FireDivergence(
                    kind="args_diverge",
                    schedule_name=src_fire.schedule_name,
                    fire_time=src_fire.fire_time,
                    source=src_fire,
                    target=tgt_fire,
                )
            )

    # Target side - any keys that source didn't have at all.
    for key, tgt_fire in target_by_key.items():
        if key in source_by_key:
            continue
        divergences.append(
            FireDivergence(
                kind="only_target",
                schedule_name=tgt_fire.schedule_name,
                fire_time=tgt_fire.fire_time,
                source=None,
                target=tgt_fire,
            )
        )

    # Sort by (fire_time, schedule_name) for stable output.
    divergences.sort(key=lambda d: (d.fire_time, d.schedule_name, d.kind))

    return ShadowComparisonReport(
        window_start=window_start,
        window_end=window_end,
        source_label=source_label,
        target_label=target_label,
        source_fire_count=len(source),
        target_fire_count=len(target),
        matched=matched,
        divergences=divergences,
    )


# =====================================================================
# Report rendering
# =====================================================================


def render_report(
    report: ShadowComparisonReport,
    *,
    max_divergences: int = 50,
) -> str:
    """Render a markdown-style report for stdout.

    Caps the divergence detail at ``max_divergences`` so a wildly
    divergent run does not flood the operator's terminal. The
    counts at the top still include the full set; the truncated
    rows note ``... and N more``.
    """
    delta = report.window_end - report.window_start
    lines = [
        "z4j-scheduler shadow-mode comparison",
        "=" * 50,
        f"Window:   {report.window_start.isoformat()} → {report.window_end.isoformat()} "
        f"({_humanize_timedelta(delta)})",
        f"Source:   {report.source_label} ({report.source_fire_count} predicted fires)",
        f"Target:   {report.target_label} ({report.target_fire_count} predicted fires)",
        f"Matched:  {report.matched}",
        f"Diverge:  {len(report.divergences)}",
        "",
    ]
    if report.ok:
        lines.append("OK, both sides predict identical fires for the window.")
        lines.append("")
        lines.append("Safe to flip the canonical scheduler.")
        return "\n".join(lines) + "\n"

    lines.append(
        f"DIVERGENCE, flip is NOT safe yet. First "
        f"{min(max_divergences, len(report.divergences))} "
        f"of {len(report.divergences)}:"
    )
    lines.append("")
    for div in report.divergences[:max_divergences]:
        lines.extend(_render_divergence(div, report))
        lines.append("")
    if len(report.divergences) > max_divergences:
        lines.append(
            f"... and {len(report.divergences) - max_divergences} more"
            f" (re-run with --report-out to dump the full list).",
        )
    return "\n".join(lines) + "\n"


def _render_divergence(
    div: FireDivergence,
    report: ShadowComparisonReport,
) -> list[str]:
    header = (
        f"  [{div.kind.upper().replace('_', ' ')}] {div.schedule_name} "
        f"@ {div.fire_time.isoformat()}"
    )
    if div.kind == "only_source":
        return [
            header,
            f"    only {report.source_label} would fire it; "
            f"{report.target_label} dropped this schedule.",
            f"    task={div.source.task_name if div.source else '?'}",
        ]
    if div.kind == "only_target":
        return [
            header,
            f"    only {report.target_label} would fire it; {report.source_label} did not.",
            f"    task={div.target.task_name if div.target else '?'}",
        ]
    # args_diverge - both sides fire, payload differs
    src = div.source
    tgt = div.target
    out = [header, "    same fire time, different payload:"]
    if src and tgt and src.task_name != tgt.task_name:
        out.append(f"    task:  {src.task_name!r}  vs  {tgt.task_name!r}")
    if src and tgt and src.args != tgt.args:
        out.append(f"    args:  {list(src.args)!r}  vs  {list(tgt.args)!r}")
    if src and tgt and src.kwargs != tgt.kwargs:
        out.append(f"    kwargs:{dict(src.kwargs)!r}  vs  {dict(tgt.kwargs)!r}")
    if src and tgt and src.queue != tgt.queue:
        out.append(f"    queue: {src.queue!r}  vs  {tgt.queue!r}")
    return out


def _humanize_timedelta(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m"
    if total < 86400:
        return f"{total / 3600:.1f}h"
    return f"{total / 86400:.1f}d"


__all__ = [
    "FireDivergence",
    "PredictedFire",
    "ShadowComparisonReport",
    "compare_predicted_fires",
    "parse_duration",
    "predict_fires",
    "render_report",
]
