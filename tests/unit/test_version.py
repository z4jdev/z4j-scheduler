"""Smoke test - verifies the package is importable and version is set.

This is the first test that should pass on a fresh checkout, before
any Phase 1 implementation lands. If this fails, the scaffold is
broken.
"""

from __future__ import annotations

from z4j_scheduler import __version__


def test_version_is_a_nonempty_string() -> None:
    assert isinstance(__version__, str)
    assert len(__version__) > 0
    assert __version__[0].isdigit()
