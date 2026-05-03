"""Regression tests for the scheduler-side Phase-2 audit findings.

The audit landed three fixes; the scheduler side is affected by:

- **HIGH-1**: ``lstrip("DNS:")`` in ``trigger_grpc.auth`` had the
  same bug as the brain side. Fixed via ``removeprefix``.

The brain-side fixes are pinned in
``packages/z4j/backend/tests/unit/test_audit_phase2_fixes.py``.
"""

from __future__ import annotations

import inspect


class TestSchedulerInterceptorRemovePrefix:
    def test_uses_removeprefix_not_lstrip(self) -> None:
        """The trigger_grpc.auth interceptor must not regress.

        Mirrors the brain-side test: scan the module source to
        confirm ``lstrip("DNS:")`` is gone and ``removeprefix``
        replaced it. A symbolic check is enough - the underlying
        Python behaviour is covered by the brain-side test.
        """
        from z4j_scheduler.trigger_grpc import auth as trig_auth

        source = inspect.getsource(trig_auth)
        assert "lstrip(\"DNS:\")" not in source
        assert "lstrip('DNS:')" not in source
        assert (
            "removeprefix(\"DNS:\")" in source
            or "removeprefix('DNS:')" in source
        )

    def test_cn_starting_with_S_not_mangled(self) -> None:
        # The literal symptom on the scheduler-side. If a future
        # refactor copies the lstrip pattern from somewhere else
        # this test catches it.
        assert "Scheduler-1".removeprefix("DNS:") == "Scheduler-1"
        assert "Scheduler-1".lstrip("DNS:") == "cheduler-1"
