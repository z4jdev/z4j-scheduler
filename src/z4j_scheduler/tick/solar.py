"""Solar event computation for ``kind=solar`` schedules.

docs/SCHEDULER.md §5.1 lists solar as one of the four schedule
kinds in the v1 surface. The ``expression`` field for a solar
schedule encodes ``"<event>:<latitude>:<longitude>"``, e.g.::

    "sunrise:37.7749:-122.4194"  # SF sunrise

    "sunset:51.5074:-0.1278"  # London sunset
    "dawn:-33.8688:151.2093"  # Sydney astronomical dawn

The seven supported events match astral's vocabulary plus the two
celery-beat aliases (``solar_noon`` / ``solar_midnight``):

    - ``dawn`` (astronomical dawn)
    - ``sunrise``
    - ``noon`` (solar noon, sun at meridian)
    - ``solar_noon`` (alias for ``noon``)
    - ``sunset``
    - ``dusk`` (astronomical dusk)
    - ``midnight`` / ``solar_midnight`` (sun at antimeridian)

Latitude is in [-90, 90]; longitude in [-180, 180]. Polar locations
(|lat| > ~66.5) can have days where the requested event does not
occur (sun never rises in winter / never sets in summer); we
return ``None`` from :func:`next_solar_fire` in that case and the
caller skips the fire. The astral library raises a custom error
in this scenario which we catch and translate.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Final

# Lazy-import astral so a base scheduler install without solar
# schedules doesn't pay the library load cost. The error message
# is operator-actionable.
try:
    from astral import LocationInfo, Observer
    from astral.sun import midnight as _astral_midnight
    from astral.sun import sun

    _ASTRAL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in degraded mode
    LocationInfo = None  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]
    sun = None  # type: ignore[assignment]
    _astral_midnight = None  # type: ignore[assignment]
    _ASTRAL_AVAILABLE = False


# Map our event vocabulary to either an astral.sun.sun() dict key
# OR a sentinel for the midnight events (which astral exposes via a
# separate ``astral.sun.midnight()`` function rather than the day
# dict). The five "in the daylight bundle" events come from the
# dict; midnight + solar_midnight resolve to the dedicated
# function. Discovered Apr 2026 by a Hypothesis property test that
# tried ``midnight`` and crashed with ``KeyError`` against the dict
# - the original alias map assumed astral exposed it as a dict key.
_DAY_DICT_KEYS: Final[dict[str, str]] = {
    "dawn": "dawn",
    "sunrise": "sunrise",
    "noon": "noon",
    "solar_noon": "noon",
    "sunset": "sunset",
    "dusk": "dusk",
}
_MIDNIGHT_EVENTS: Final[frozenset[str]] = frozenset(
    {
        "midnight",
        "solar_midnight",
    }
)


VALID_EVENTS = frozenset(_DAY_DICT_KEYS.keys()) | _MIDNIGHT_EVENTS


def parse_solar_expression(expression: str) -> tuple[str, float, float]:
    """Parse ``"event:lat:lon"`` into its components.

    Raises :class:`ValueError` with a clear message on any error -
    the importers + dashboard form pipe these straight to the
    operator so the message has to be useful.
    """
    if not expression or expression.count(":") != 2:
        raise ValueError(
            f"solar expression must be 'event:lat:lon'; got {expression!r}",
        )
    event, lat_s, lon_s = expression.split(":", 2)
    event = event.strip().lower()
    if event not in VALID_EVENTS:
        raise ValueError(
            f"unknown solar event {event!r}; must be one of {sorted(VALID_EVENTS)}",
        )
    try:
        lat = float(lat_s)
        lon = float(lon_s)
    except ValueError as exc:
        raise ValueError(
            f"latitude / longitude must be floats; got {lat_s!r}, {lon_s!r}",
        ) from exc
    if not -90.0 <= lat <= 90.0:
        raise ValueError(
            f"latitude {lat} out of range [-90, 90]",
        )
    if not -180.0 <= lon <= 180.0:
        raise ValueError(
            f"longitude {lon} out of range [-180, 180]",
        )
    return event, lat, lon


def next_solar_fire(
    expression: str,
    after: datetime,
    *,
    max_days_ahead: int = 365,
) -> datetime | None:
    """Compute the next UTC instant the named solar event occurs.

    ``after`` is the lower bound (exclusive). Walks forward day by
    day computing the event time at the given location until one
    is strictly after ``after``. Returns ``None`` if no occurrence
    happens within ``max_days_ahead`` days (polar regions during
    perpetual day / night).

    Astral computes events as datetime objects with the requested
    timezone; we convert to UTC so the caller (tick engine,
    shadow comparator) stays in one timezone vocabulary.
    """
    if not _ASTRAL_AVAILABLE:
        raise RuntimeError(
            "solar schedules require the `astral` library; "
            "install with `pip install astral` or "
            "`pip install z4j-scheduler[solar]`",
        )
    event_key, lat, lon = parse_solar_expression(expression)
    is_midnight_event = event_key in _MIDNIGHT_EVENTS
    if not is_midnight_event:
        astral_key = _DAY_DICT_KEYS[event_key]

    # Anchor at the date of ``after`` and walk forward.
    cursor: date = after.astimezone(UTC).date()
    observer = Observer(latitude=lat, longitude=lon)

    for _ in range(max_days_ahead):
        try:
            if is_midnight_event:
                # astral.sun.midnight is a dedicated function -
                # it's the moment the sun is at the antimeridian
                # (lowest point), not the wall-clock 00:00.
                candidate = _astral_midnight(
                    observer,
                    date=cursor,
                    tzinfo=UTC,
                )
            else:
                day_events = sun(observer, date=cursor, tzinfo=UTC)
                candidate = day_events[astral_key]
        except Exception:
            # Polar latitudes can raise ``ValueError`` ("Sun is
            # always below the horizon"). Skip the day; the
            # outer loop tries tomorrow. Eventually ``max_days_ahead``
            # bounds the work.
            cursor = cursor + timedelta(days=1)
            continue
        # Astral occasionally returns the previous day's event
        # when ``cursor`` is right at midnight UTC and the
        # location's wall clock is the previous day. Defensive:
        # only accept candidates strictly after ``after``.
        if candidate > after:
            return candidate
        cursor = cursor + timedelta(days=1)
    return None


__all__ = [
    "VALID_EVENTS",
    "next_solar_fire",
    "parse_solar_expression",
]
