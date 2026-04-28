"""structlog configuration matching z4j-brain's setup.

Configured once at process startup based on
``Z4J_SCHEDULER_LOG_LEVEL`` and ``Z4J_SCHEDULER_LOG_JSON``.

In production (``log_json=True``), emits one JSON object per log
entry with ISO timestamp, level, logger name, event message, and
any bound contextual fields. Pipes cleanly into log aggregators.

In development (``log_json=False``), emits a colourised
console-friendly format for grep-by-eye.
"""

from __future__ import annotations

import logging
import sys

import structlog

from z4j_scheduler.settings import Settings

#: Module-level guard so :func:`configure_logging` is idempotent
#: across the typical "main + tests both call it" pattern.
_configured: bool = False


def configure_logging(settings: Settings) -> None:
    """Configure structlog + stdlib logging from Settings.

    Idempotent - safe to call multiple times. The first call wins;
    subsequent calls are no-ops. Tests override by monkeypatching
    :data:`_configured` to ``False`` then re-calling.
    """
    global _configured  # noqa: PLW0603 - module-level guard
    if _configured:
        return

    level_value = getattr(logging, settings.log_level, logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_json:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level_value),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
    # Mirror to stdlib so libraries that use logging (grpcio, asyncpg)
    # show up in the same stream.
    logging.basicConfig(level=level_value, stream=sys.stderr)

    _configured = True


def reset_for_tests() -> None:
    """Reset the configuration guard so a test can re-configure.

    Production code never calls this. Tests use it to verify the
    JSON vs console branches of :func:`configure_logging` without
    leaking state across test cases.
    """
    global _configured  # noqa: PLW0603 - test helper
    _configured = False


__all__ = ["configure_logging", "reset_for_tests"]
