"""Tests for ``z4j_scheduler.trigger_grpc.auth._enforce_cn``.

The brain calls the scheduler over a separate mTLS gRPC service when
an operator clicks "Trigger now" in the dashboard. The scheduler-side
interceptor mirrors brain's allow-list check, so the same
bytes-vs-str ``AuthContext`` regression that bit the brain side in
the Apr 2026 e2e run can bite this side too if the dual-key lookup
gets "cleaned up." These tests pin both shapes so a future refactor
trips the suite, not production.
"""

from __future__ import annotations

import pytest

# Mirror the brain-side import-skip - the trigger gRPC server is
# behind an opt-in extra, but the auth helper is unconditional pure
# Python and is safe to test without grpcio actually running.
pytest.importorskip("grpc")

from z4j_scheduler.trigger_grpc.auth import _enforce_cn  # noqa: E402


class _Ctx:
    """Minimal ServicerContext stand-in for the interceptor tests."""

    aborted: bool = False
    abort_msg: str | None = None

    def __init__(self, ctx_data: dict) -> None:
        self._data = ctx_data

    def auth_context(self) -> dict:
        return self._data

    async def abort(self, code, msg) -> None:  # noqa: ANN001, D401
        self.aborted = True
        self.abort_msg = msg
        # Real grpc abort raises; mirror so the abort path is observable
        # by surrounding code.
        raise RuntimeError("aborted")


@pytest.mark.asyncio
class TestEnforceCnAuthContextShape:
    async def test_str_keyed_context_accepts_known_cn(self) -> None:
        ctx = _Ctx({"x509_common_name": [b"z4j-brain"]})
        await _enforce_cn(ctx, frozenset({"z4j-brain"}))  # type: ignore[arg-type]
        assert not ctx.aborted

    async def test_bytes_keyed_context_accepts_known_cn(self) -> None:
        ctx = _Ctx({b"x509_common_name": [b"z4j-brain"]})
        await _enforce_cn(ctx, frozenset({"z4j-brain"}))  # type: ignore[arg-type]
        assert not ctx.aborted

    async def test_san_dns_prefix_stripped(self) -> None:
        ctx = _Ctx({"x509_subject_alternative_name": [b"DNS:z4j-brain"]})
        await _enforce_cn(ctx, frozenset({"z4j-brain"}))  # type: ignore[arg-type]
        assert not ctx.aborted

    async def test_unknown_cn_aborts(self) -> None:
        ctx = _Ctx({"x509_common_name": [b"intruder"]})
        with pytest.raises(RuntimeError, match="aborted"):
            await _enforce_cn(ctx, frozenset({"z4j-brain"}))  # type: ignore[arg-type]
        assert ctx.aborted
        assert ctx.abort_msg

    async def test_lstrip_dns_does_not_corrupt_d_prefixed_cn(self) -> None:
        # Regression for the lstrip-vs-removeprefix bug. If the impl
        # ever regresses to ``c.lstrip("DNS:")`` a CN like ``Drone-1``
        # becomes ``rone-1`` and silently fails the allow-list. This
        # test makes that regression loud.
        ctx = _Ctx({"x509_common_name": [b"Drone-1"]})
        await _enforce_cn(ctx, frozenset({"Drone-1"}))  # type: ignore[arg-type]
        assert not ctx.aborted

    async def test_empty_allowlist_aborts_when_no_cn(self) -> None:
        # Defensive: an operator who configures an allow-list but
        # presents a cert with no CN/SAN should NOT be silently
        # accepted. The empty-allow-list branch (trust-the-CA) is
        # tested at the SchedulerAllowlistInterceptor level - this
        # test exercises the bottom-of-the-stack helper with a
        # populated allow-list.
        ctx = _Ctx({})  # empty auth context
        with pytest.raises(RuntimeError, match="aborted"):
            await _enforce_cn(ctx, frozenset({"z4j-brain"}))  # type: ignore[arg-type]
        assert ctx.aborted
