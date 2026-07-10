"""mTLS interceptor for the scheduler-side TriggerSchedule server.

Mirrors :class:`z4j_brain.scheduler_grpc.auth.SchedulerAllowlistInterceptor`
but on the scheduler side: TLS validates the brain's client cert
against the operator-supplied CA bundle, and this interceptor adds
an application-layer CN allow-list check so a stolen cert minted
for a different service can't issue triggers.

Empty allow-list = "trust the CA" (any CA-validated cert is
accepted). Populate ``Z4J_SCHEDULER_TRIGGER_GRPC_ALLOWED_CNS`` to
add the second check.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

import grpc

logger = logging.getLogger("z4j.scheduler.trigger_grpc.auth")


class TriggerAllowlistInterceptor(grpc.aio.ServerInterceptor):
    """Reject TriggerSchedule calls whose client cert CN is unknown."""

    def __init__(self, *, allowed_cns: tuple[str, ...]) -> None:
        self._allowed = frozenset(allowed_cns)

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails],
            Awaitable[grpc.RpcMethodHandler],
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        if not self._allowed:
            return await continuation(handler_call_details)

        original = await continuation(handler_call_details)
        if original is None:
            return original
        return _wrap(original, self._allowed)


def _wrap(
    handler: grpc.RpcMethodHandler,
    allowed: frozenset[str],
) -> grpc.RpcMethodHandler:
    """Wrap unary-unary so each call validates the peer CN."""
    if handler.unary_unary is None:
        # Stream RPCs not used by TriggerSchedule. Pass through.
        return handler
    original_fn = handler.unary_unary

    async def wrapped(request: Any, context: grpc.aio.ServicerContext) -> Any:
        await _enforce_cn(context, allowed)
        return await original_fn(request, context)

    return grpc.unary_unary_rpc_method_handler(
        wrapped,
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


async def _enforce_cn(
    context: grpc.aio.ServicerContext,
    allowed: frozenset[str],
) -> None:
    """Abort the call with PERMISSION_DENIED if the peer CN is unknown."""
    auth_ctx = context.auth_context()
    # gRPC's AuthContext key naming has shifted across versions
    # (bytes-keyed up through ~1.5x, str-keyed in 1.6x+ on grpc.aio).
    # Look up under both shapes so the interceptor works across the
    # supported range. Mirrors the brain-side fix from Apr 2026.
    cn_candidates: set[str] = set()

    def _entries(key: str) -> list:
        return list(auth_ctx.get(key, [])) + list(
            auth_ctx.get(key.encode(), []),
        )

    for entry in _entries("x509_subject_alternative_name"):
        try:
            cn_candidates.add(
                entry.decode() if isinstance(entry, bytes) else str(entry),
            )
        except UnicodeDecodeError:
            continue

    for entry in _entries("x509_common_name"):
        try:
            cn_candidates.add(
                entry.decode() if isinstance(entry, bytes) else str(entry),
            )
        except UnicodeDecodeError:
            continue

    # ``removeprefix`` (NOT ``lstrip``) - lstrip strips a SET of
    # characters and would silently corrupt CNs starting with D/N/S
    # or colon. Strip the SAN general-name prefixes the brain-side
    # ``_normalise_cn`` strips (DNS:/IP:/URI:/email:), not just DNS:,
    # so an IP-SAN cert (``IP:10.0.0.5``) matches a bare ``10.0.0.5``
    # in the allow-list instead of being silently rejected.
    def _bare_cn(cn: str) -> str:
        for prefix in ("DNS:", "IP:", "URI:", "email:"):
            if cn.startswith(prefix):
                return cn.removeprefix(prefix).strip()
        return cn.strip()

    normalised = {_bare_cn(c) for c in cn_candidates}

    if not normalised & allowed:
        logger.warning(
            "z4j.scheduler.trigger_grpc: rejected RPC; peer CNs %r not in allow-list",
            sorted(normalised),
        )
        await context.abort(
            grpc.StatusCode.PERMISSION_DENIED,
            "brain not authorised - CN not on allow-list",
        )


__all__ = ["TriggerAllowlistInterceptor"]
