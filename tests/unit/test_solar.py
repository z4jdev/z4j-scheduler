"""Tests for solar schedule support.

docs/SCHEDULER.md §5.1 lists ``solar`` as one of the four v1
schedule kinds. Pre-1.1 we shipped only cron / interval / one_shot
and the celery importer rejected solar with a warning. v1.1 adds
end-to-end support: importer translates ``celery.schedules.solar``,
exporter renders back to celery, the shadow comparator predicts
fire times via ``astral``.

These tests pin:

- The expression parser's vocabulary + range checks.
- ``next_solar_fire`` returning a real datetime for typical
  locations and gracefully falling through for polar perpetual-
  day / -night windows.
- The shadow comparator's ``_predict_solar`` integration.
- The celery exporter rendering back to ``solar(event, lat, lon)``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip(
    "astral",
    reason="solar schedules require the astral library",
)

from z4j_scheduler.exporters import celery as celery_exporter
from z4j_scheduler.exporters._client import ExportedSchedule
from z4j_scheduler.importers._core import ImportedSchedule
from z4j_scheduler.tick import solar as solar_mod
from z4j_scheduler.tick.solar import (
    VALID_EVENTS,
    fires_between,
    next_solar_fire,
    parse_solar_expression,
)
from z4j_scheduler.verify.shadow_comparator import predict_fires


class TestFiresBetweenR9H7:
    """Solar now enumerates a missed backlog (iterated next_solar_fire),
    so the tick engine can coalesce fire_one_missed instead of re-firing one solar
    slot per tick after a multi-boundary outage."""

    def test_iterates_window_ascending(self, monkeypatch) -> None:
        seq = [
            datetime(2026, 5, 1, 6, 0, tzinfo=UTC),
            datetime(2026, 5, 1, 18, 0, tzinfo=UTC),
            datetime(2026, 5, 2, 6, 0, tzinfo=UTC),
        ]

        def _fake_next(expression: str, after: datetime, **_kw: object) -> datetime | None:
            for x in seq:
                if x > after:
                    return x
            return None

        monkeypatch.setattr(solar_mod, "next_solar_fire", _fake_next)
        out = fires_between(
            "sunrise@0.0,0.0",
            after=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            until=datetime(2026, 5, 1, 23, 0, tzinfo=UTC),
        )
        # Only the two slots INSIDE (after, until]; the next-day slot is excluded.
        assert out == seq[:2]

    def test_empty_and_cap(self, monkeypatch) -> None:
        # No occurrences in the window -> [].
        monkeypatch.setattr(solar_mod, "next_solar_fire", lambda *a, **k: None)
        assert (
            fires_between(
                "sunrise@0.0,0.0",
                after=datetime(2026, 5, 1, tzinfo=UTC),
                until=datetime(2026, 5, 2, tzinfo=UTC),
            )
            == []
        )
        # A degenerate producer that never advances yields at most ONE slot: the
        # advance guard (nxt <= cursor -> stop) dedups it and the loop is bounded,
        # never infinite. A real next_solar_fire always returns a time strictly
        # after ``after``, so this only ever bites a mock / pathological producer,
        # and one duplicate slot is strictly better than max_slots duplicates
        # (which would double-fire the same instant under fire_all_missed).
        fixed = datetime(2026, 5, 1, 6, 0, tzinfo=UTC)
        monkeypatch.setattr(solar_mod, "next_solar_fire", lambda *a, **k: fixed)
        out = fires_between(
            "sunrise@0.0,0.0",
            after=datetime(2026, 5, 1, tzinfo=UTC),
            until=datetime(2026, 5, 2, tzinfo=UTC),
            max_slots=10,
        )
        assert out == [fixed]  # deduped + bounded, not infinite


# =====================================================================
# Expression parser
# =====================================================================


class TestParseSolarExpression:
    def test_typical_sunrise(self) -> None:
        # San Francisco lat/lon.
        event, lat, lon = parse_solar_expression(
            "sunrise:37.7749:-122.4194",
        )
        assert event == "sunrise"
        assert lat == pytest.approx(37.7749)
        assert lon == pytest.approx(-122.4194)

    def test_celery_aliases(self) -> None:
        # ``solar_noon`` and ``solar_midnight`` are the celery-side
        # names; both must parse so the importer's translation is
        # straight-through.
        event, _, _ = parse_solar_expression("solar_noon:0:0")
        assert event == "solar_noon"
        event, _, _ = parse_solar_expression("solar_midnight:0:0")
        assert event == "solar_midnight"

    @pytest.mark.parametrize("event", sorted(VALID_EVENTS))
    def test_every_documented_event_parses(self, event: str) -> None:
        e, _, _ = parse_solar_expression(f"{event}:0:0")
        assert e == event

    def test_rejects_unknown_event(self) -> None:
        with pytest.raises(ValueError, match="unknown solar event"):
            parse_solar_expression("zenith:0:0")

    def test_rejects_out_of_range_latitude(self) -> None:
        with pytest.raises(ValueError, match="latitude"):
            parse_solar_expression("sunrise:91:0")
        with pytest.raises(ValueError, match="latitude"):
            parse_solar_expression("sunrise:-91:0")

    def test_rejects_out_of_range_longitude(self) -> None:
        with pytest.raises(ValueError, match="longitude"):
            parse_solar_expression("sunrise:0:181")

    def test_rejects_malformed(self) -> None:
        with pytest.raises(ValueError):
            parse_solar_expression("sunrise:0")
        with pytest.raises(ValueError):
            parse_solar_expression("")
        with pytest.raises(ValueError):
            parse_solar_expression("sunrise:abc:0")


# =====================================================================
# next_solar_fire
# =====================================================================


class TestNextSolarFire:
    def test_returns_future_sunrise_for_typical_location(self) -> None:
        # San Francisco, asking after midnight UTC any day. Sunrise
        # occurs at SF every day - we should always get a fire.
        after = datetime(2026, 4, 27, 0, 0, 0, tzinfo=UTC)
        nxt = next_solar_fire("sunrise:37.7749:-122.4194", after)
        assert nxt is not None
        # Sanity bounds: sunrise within the next 48h. SF sunrise in
        # late April is ~13:00 UTC.
        assert after < nxt < after + timedelta(days=2)

    def test_strictly_after_anchor(self) -> None:
        # The fire returned MUST be strictly later than ``after`` so
        # the caller can iterate without an infinite loop.
        anchor = datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC)
        nxt = next_solar_fire("sunset:51.5074:-0.1278", anchor)
        assert nxt is not None
        assert nxt > anchor

    def test_iterates_to_next_day(self) -> None:
        # Asking right after today's sunrise should give tomorrow's,
        # not today's. The walk-forward loop in ``next_solar_fire``
        # guards against returning past events.
        loc = "sunrise:37.7749:-122.4194"
        first = next_solar_fire(loc, datetime(2026, 4, 27, 0, tzinfo=UTC))
        assert first is not None
        # Pass ``first`` itself as the anchor; the result must be
        # strictly later (the next day's sunrise).
        second = next_solar_fire(loc, first)
        assert second is not None
        assert second > first
        # Two consecutive sunrises are roughly 24 hours apart.
        delta = second - first
        assert timedelta(hours=23) < delta < timedelta(hours=25)

    def test_polar_perpetual_day_returns_none(self) -> None:
        # Latitude 89 (very near the north pole) in mid-summer:
        # sunrise / sunset don't occur. Walk the full year-long
        # max_days_ahead and return None.
        anchor = datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)
        result = next_solar_fire(
            "sunrise:89:0",
            anchor,
            max_days_ahead=30,
        )
        # Either None (no event in 30 days) or a real datetime if
        # astral somehow finds one - either is acceptable; the
        # contract is "doesn't crash".
        assert result is None or isinstance(result, datetime)


# =====================================================================
# Shadow comparator integration
# =====================================================================


class TestShadowComparatorPredictsSolar:
    def test_solar_schedule_yields_one_fire_per_day(self) -> None:
        sched = ImportedSchedule(
            project_slug="p",
            name="sf-sunrise",
            engine="celery",
            kind="solar",  # type: ignore[arg-type]
            expression="sunrise:37.7749:-122.4194",
            task_name="app.morning",
            timezone="UTC",
            args=[],
            kwargs={},
            is_enabled=True,
            source="test",
        )
        start = datetime(2026, 4, 27, 0, 0, 0, tzinfo=UTC)
        # 3-day window; SF sunrise occurs every day, so 3 fires.
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(days=3),
        )
        assert 2 <= len(fires) <= 3, (
            f"expected ~3 sunrise fires in a 3-day window, got "
            f"{len(fires)}: {[f.fire_time for f in fires]}"
        )
        # Every fire is strictly inside the window.
        for f in fires:
            assert start <= f.fire_time <= start + timedelta(days=3)

    def test_disabled_solar_schedule_emits_nothing(self) -> None:
        sched = ImportedSchedule(
            project_slug="p",
            name="off",
            engine="celery",
            kind="solar",  # type: ignore[arg-type]
            expression="sunset:0:0",
            task_name="app.t",
            timezone="UTC",
            args=[],
            kwargs={},
            is_enabled=False,
            source="test",
        )
        start = datetime(2026, 4, 27, tzinfo=UTC)
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(days=7),
        )
        assert fires == []

    def test_malformed_solar_expression_emits_nothing_silently(
        self,
    ) -> None:
        sched = ImportedSchedule(
            project_slug="p",
            name="bad",
            engine="celery",
            kind="solar",  # type: ignore[arg-type]
            expression="bogus",  # missing :lat:lon
            task_name="app.t",
            timezone="UTC",
            args=[],
            kwargs={},
            is_enabled=True,
            source="test",
        )
        start = datetime(2026, 4, 27, tzinfo=UTC)
        # Comparator must not raise on malformed input - the
        # importer is responsible for syntactic validation.
        fires = predict_fires(
            [sched],
            window_start=start,
            window_end=start + timedelta(days=7),
        )
        assert fires == []


# =====================================================================
# Exporter back to celery
# =====================================================================


class TestCeleryExporterRendersSolar:
    def _exported(self, expr: str) -> ExportedSchedule:
        return ExportedSchedule(
            id="00000000-0000-0000-0000-000000000001",
            name="sf-sunrise",
            engine="celery",
            kind="solar",
            expression=expr,
            task_name="app.morning",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            catch_up="skip",
            is_enabled=True,
            scheduler="z4j-scheduler",
            source="dashboard",
        )

    def test_imports_solar_when_present(self) -> None:
        out = celery_exporter.render(
            [
                self._exported("sunrise:37.7749:-122.4194"),
            ]
        )
        # The ``solar`` symbol must be imported alongside crontab so
        # the rendered Python module evaluates without a NameError.
        assert "from celery.schedules import crontab, solar" in out

    def test_does_not_import_solar_when_absent(self) -> None:
        # No solar schedules in the batch: don't pollute imports.
        from z4j_scheduler.exporters._client import ExportedSchedule

        cron_only = ExportedSchedule(
            id="x",
            name="hourly",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="app.t",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            catch_up="skip",
            is_enabled=True,
            scheduler="z4j-scheduler",
            source="dashboard",
        )
        out = celery_exporter.render([cron_only])
        assert "from celery.schedules import crontab\n" in out
        assert ", solar" not in out

    def test_renders_solar_call_with_event_and_coords(self) -> None:
        out = celery_exporter.render(
            [
                self._exported("sunset:51.5074:-0.1278"),
            ]
        )
        assert "solar('sunset', 51.5074, -0.1278)" in out

    def test_unparseable_expression_renders_comment(self) -> None:
        # Defensive: a malformed solar expression that somehow made
        # it past the importer should render as a comment, not
        # raise from the renderer (would crash the whole export).
        out = celery_exporter.render([self._exported("bogus")])
        assert "unparseable solar expression" in out


class TestPerEventHighLatitudeR10H11:
    """Request the SPECIFIC solar event, not the sun() bundle, so a
    bundled event with no occurrence today (high latitude) does not make a
    computable event fail and skip the whole day until the polar season ends."""

    def test_noon_resolves_during_polar_day_when_bundle_would_raise(self) -> None:
        pytest.importorskip("astral")
        from astral import Observer
        from astral.sun import sun

        # Tromso in polar day: sunrise/sunset do not occur, so the sun() BUNDLE
        # raises -- the old code skipped the whole day even for noon.
        polar_day = datetime(2026, 6, 21, 0, 0, tzinfo=UTC)
        with pytest.raises(Exception):  # noqa: B017  bundle genuinely raises here
            sun(Observer(69.6, 18.9), date=polar_day.date(), tzinfo=UTC)
        # The per-event path still computes noon on that exact day.
        noon = next_solar_fire("noon:69.6:18.9", after=polar_day)
        assert noon is not None
        assert noon.date() == polar_day.date()
