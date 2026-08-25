"""Tests for the shadow-mode predicted-fire comparator.

The §17.1 promise: ``--verify --duration 24h`` predicts every fire
each side would emit and reports divergence. The comparator's
correctness matters because operators flip the canonical scheduler
based on its output - a false-clean report means a real divergence
went to production.

We exercise:

- Duration parsing (the user-facing input).
- Predicted-fire generation for cron / interval / one_shot.
- Three divergence shapes: only-source, only-target, args-mismatch.
- The OK path (zero divergences) so the report's go/no-go signal
  flips correctly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from z4j_scheduler.importers._core import ImportedSchedule
from z4j_scheduler.verify.shadow_comparator import (
    PredictedFire,
    _resolve_tz,
    compare_predicted_fires,
    parse_duration,
    predict_fires,
    render_report,
)

# =====================================================================
# Duration parsing
# =====================================================================


class TestParseDuration:
    @pytest.mark.parametrize(
        "value,expected_seconds",
        [
            ("30s", 30),
            ("5m", 300),
            ("2h", 7200),
            ("1d", 86400),
            ("24h", 86400),
            ("7d", 7 * 86400),
            ("60", 60),  # bare number = seconds
            ("0.5h", 1800),
        ],
    )
    def test_accepts_units(self, value: str, expected_seconds: int) -> None:
        assert parse_duration(value).total_seconds() == expected_seconds

    @pytest.mark.parametrize("bad", ["", "abc", "5x", "h", "5 hours", "-5h"])
    def test_rejects_garbage(self, bad: str) -> None:
        with pytest.raises(ValueError):
            parse_duration(bad)


# =====================================================================
# Prediction
# =====================================================================


def _imported(
    *,
    name: str,
    kind: str,
    expression: str,
    is_enabled: bool = True,
    args: list | None = None,
    kwargs: dict | None = None,
    queue: str | None = None,
    timezone_: str = "UTC",
) -> ImportedSchedule:
    return ImportedSchedule(
        project_slug="p",
        name=name,
        engine="celery",
        kind=kind,  # type: ignore[arg-type]
        expression=expression,
        task_name=f"app.tasks.{name.replace('-', '_')}",
        timezone=timezone_,
        queue=queue,
        args=args or [],
        kwargs=kwargs or {},
        is_enabled=is_enabled,
        source="declarative:test",
    )


class TestPredictFires:
    def test_cron_predicts_every_hour_in_window(self) -> None:
        sched = _imported(name="hourly", kind="cron", expression="0 * * * *")
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        # 6-hour window starting at 12:00:00 UTC. Cron "0 * * * *"
        # fires at 13:00, 14:00, 15:00, 16:00, 17:00, 18:00 = 6 fires.
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=6),
        )
        assert len(fires) == 6
        assert fires[0].fire_time == datetime(
            2026,
            4,
            27,
            13,
            0,
            0,
            tzinfo=UTC,
        )
        assert all(f.task_name == "app.tasks.hourly" for f in fires)

    def test_interval_predicts_evenly_spaced_fires(self) -> None:
        sched = _imported(
            name="poll",
            kind="interval",
            expression="60s",
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        # 5-minute window with a 60-second interval:
        # fires at +0, +60, +120, +180, +240, +300 = 6 fires.
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(minutes=5),
        )
        assert len(fires) == 6
        assert fires[1].fire_time == start + timedelta(seconds=60)

    def test_one_shot_inside_window_fires_once(self) -> None:
        target = datetime(2026, 4, 27, 15, 30, 0, tzinfo=UTC)
        sched = _imported(
            name="once",
            kind="one_shot",
            expression=target.isoformat(),
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=6),
        )
        assert len(fires) == 1
        assert fires[0].fire_time == target

    def test_one_shot_outside_window_emits_nothing(self) -> None:
        # Target is past the window end - no fires expected. Catches
        # the "operator ran --verify after the deadline" edge case.
        target = datetime(2027, 1, 1, 0, 0, 0, tzinfo=UTC)
        sched = _imported(
            name="future",
            kind="one_shot",
            expression=target.isoformat(),
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=6),
        )
        assert fires == []

    def test_disabled_schedule_emits_nothing(self) -> None:
        # Disabled schedules should be invisible to BOTH sides of
        # the comparator, otherwise the report would flag every
        # disabled row as "would fire on source but not target."
        sched = _imported(
            name="off",
            kind="cron",
            expression="0 * * * *",
            is_enabled=False,
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=24),
        )
        assert fires == []

    def test_unparseable_cron_emits_nothing_silently(self) -> None:
        # Defensive: a malformed expression that slipped past the
        # importer should not raise from inside the comparator.
        sched = _imported(name="bad", kind="cron", expression="not a cron")
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=24),
        )
        assert fires == []

    def test_cron_carries_args_and_kwargs_into_fire(self) -> None:
        # Args / kwargs round-trip into the predicted fire so the
        # comparator can detect importer-side data drops.
        sched = _imported(
            name="report",
            kind="cron",
            expression="0 * * * *",
            args=["weekly", 7],
            kwargs={"format": "pdf", "redact": True},
            queue="critical",
        )
        start = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(hours=2),
        )
        assert len(fires) >= 1
        f = fires[0]
        assert f.args == ("weekly", 7)
        assert f.queue == "critical"
        # kwargs is stored as a sorted tuple of (k, v) pairs for
        # hashability + deterministic comparison.
        assert dict(f.kwargs) == {"format": "pdf", "redact": True}


# =====================================================================
# Comparison
# =====================================================================


def _fire(
    name: str,
    *,
    when: datetime,
    task: str = "app.tasks.t",
    args: tuple = (),
    kwargs: dict | None = None,
    queue: str | None = None,
) -> PredictedFire:
    return PredictedFire(
        schedule_name=name,
        fire_time=when,
        task_name=task,
        args=args,
        kwargs=tuple(sorted((kwargs or {}).items())),
        queue=queue,
    )


class TestCompareFires:
    def _start(self) -> datetime:
        return datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)

    def test_identical_lists_report_ok(self) -> None:
        t = self._start()
        fires = [_fire("a", when=t), _fire("b", when=t + timedelta(minutes=5))]
        report = compare_predicted_fires(
            source=fires,
            target=list(fires),
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        assert report.ok is True
        assert report.matched == 2
        assert report.divergences == []

    def test_only_source_fire_reported(self) -> None:
        t = self._start()
        report = compare_predicted_fires(
            source=[_fire("orphan-source", when=t)],
            target=[],
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        assert not report.ok
        assert len(report.divergences) == 1
        d = report.divergences[0]
        assert d.kind == "only_source"
        assert d.schedule_name == "orphan-source"
        assert d.target is None

    def test_only_target_fire_reported(self) -> None:
        t = self._start()
        report = compare_predicted_fires(
            source=[],
            target=[_fire("orphan-target", when=t)],
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        assert len(report.divergences) == 1
        d = report.divergences[0]
        assert d.kind == "only_target"
        assert d.source is None

    def test_args_diverge_when_payload_differs(self) -> None:
        # Same name + same fire time, different args. The comparator
        # must NOT count this as matched - the operator's task would
        # receive different data on each side.
        t = self._start()
        report = compare_predicted_fires(
            source=[_fire("p", when=t, args=("a",))],
            target=[_fire("p", when=t, args=("b",))],
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        assert report.matched == 0
        assert len(report.divergences) == 1
        assert report.divergences[0].kind == "args_diverge"

    def test_queue_diverge_flagged(self) -> None:
        t = self._start()
        report = compare_predicted_fires(
            source=[_fire("p", when=t, queue="default")],
            target=[_fire("p", when=t, queue="critical")],
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        assert report.matched == 0
        assert report.divergences[0].kind == "args_diverge"

    def test_divergences_sorted_by_time(self) -> None:
        # Stable output is important - the operator runs `--verify`
        # in CI and the output should be diff-friendly across runs.
        t = self._start()
        fires_src = [
            _fire("z", when=t + timedelta(hours=2)),
            _fire("a", when=t + timedelta(hours=1)),
        ]
        report = compare_predicted_fires(
            source=fires_src,
            target=[],
            window_start=t,
            window_end=t + timedelta(hours=3),
        )
        # Two divergences, sorted by fire_time ascending.
        times = [d.fire_time for d in report.divergences]
        assert times == sorted(times)


# =====================================================================
# Report rendering
# =====================================================================


class TestRenderReport:
    def _start(self) -> datetime:
        return datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)

    def test_ok_report_says_safe_to_flip(self) -> None:
        t = self._start()
        fires = [_fire("a", when=t)]
        report = compare_predicted_fires(
            source=fires,
            target=list(fires),
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        out = render_report(report)
        assert "OK" in out
        assert "Safe to flip" in out
        # No DIVERGENCE banner on a clean run.
        assert "DIVERGENCE" not in out

    def test_diverging_report_says_not_safe(self) -> None:
        t = self._start()
        report = compare_predicted_fires(
            source=[_fire("p", when=t)],
            target=[],
            window_start=t,
            window_end=t + timedelta(hours=1),
        )
        out = render_report(report)
        assert "DIVERGENCE" in out
        assert "NOT safe" in out
        assert "ONLY SOURCE" in out
        assert "p" in out

    def test_truncates_at_max_divergences(self) -> None:
        # 200 divergent rows; renderer should cap at the configured
        # ceiling and report the remainder count so the operator's
        # terminal isn't flooded.
        t = self._start()
        src = [_fire(f"sched-{i:03d}", when=t + timedelta(minutes=i)) for i in range(200)]
        report = compare_predicted_fires(
            source=src,
            target=[],
            window_start=t,
            window_end=t + timedelta(hours=4),
        )
        out = render_report(report, max_divergences=5)
        assert "... and 195 more" in out


class TestTimezoneSourceMatchesTheEngine:
    """The comparator must read the same tzdb as the engine it audits.

    ``_resolve_tz`` used bare ``ZoneInfo``, which searches the host's
    ``/usr/share/zoneinfo`` before the release-pinned ``tzdata`` wheel.
    The tick engine reads the wheel only, via ``packaged_zoneinfo``. On
    any host whose system tzdb differs from the pin -- Debian trixie
    ships IANA 2026b against the 2026a pin, disagreeing on exactly one
    zone (``America/Vancouver``, from 2026-11-01; measured at six-hour
    resolution across 2020-2035) -- the comparator predicted fires an hour
    off the engine there and blamed the import. That set moves whenever
    the pin moves, so re-measure rather than trusting this number.

    Timezone misconfiguration is one of the four divergence classes this
    comparator advertises, so a tz-source mismatch is a false result on
    its headline case.
    """

    def test_resolves_through_the_pinned_wheel_not_the_host_tzdb(self) -> None:
        # Identity, not equality: packaged_zoneinfo is lru_cached, so a
        # resolution that went through the wheel returns the very same
        # object. A bare ZoneInfo() would return a different instance
        # built from the host tree, failing this even when the two
        # tzdbs happen to agree on offsets -- which is the point, since
        # they agree for most zones and most dates.
        from z4j_scheduler.tick._runtime import packaged_zoneinfo
        from z4j_scheduler.verify.shadow_comparator import _resolve_tz

        for zone in ("America/Vancouver", "Africa/Casablanca", "Europe/Berlin"):
            assert _resolve_tz(zone) is packaged_zoneinfo(zone), (
                f"{zone} did not resolve through the pinned tzdata wheel; "
                "the comparator and the tick engine would disagree"
            )

    def test_unknown_zone_still_falls_back_to_utc(self) -> None:
        # The fallback is deliberate and documented: the importer's
        # earlier pass reports an unparseable zone, and this function
        # does not double-warn. Pinned so the tz-source fix above is
        # not read as licence to start raising here.
        assert _resolve_tz("Not/AZone") is UTC
        assert _resolve_tz("") is UTC
        assert _resolve_tz("UTC") is UTC
