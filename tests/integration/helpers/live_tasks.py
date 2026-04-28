"""Shared task definitions for live-broker e2e tests.

Lives in a real module (not test-local) so workers can import the
task body across processes / queues. Counter state is per-import,
so each test that uses the live module gets a fresh counter via
``reset_counter()``.
"""

from __future__ import annotations

_state: dict[str, int] = {"n": 0}


def reset_counter() -> None:
    _state["n"] = 0


def get_counter() -> int:
    return _state["n"]


def rq_live_task(value: int = 1) -> int:
    """RQ-importable task body."""
    _state["n"] += value
    return _state["n"]


async def arq_live_task(ctx, value: int = 1) -> int:  # noqa: ANN001, ARG001
    """arq-importable task body. ``ctx`` is the arq worker ctx."""
    _state["n"] += value
    return _state["n"]
