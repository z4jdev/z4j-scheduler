"""Boundary D scheduler/Brain protocol capability validation."""

from __future__ import annotations

from z4j_scheduler.storage._models import ProtocolCapabilities

CURRENT_PROTOCOL_EPOCH = 1
CURRENT_FIRE_RESPONSE_VERSION = 1
CURRENT_CURSOR_TRANSITION_VERSION = 1
CURRENT_STABLE_SNAPSHOT_VERSION = 1
CURRENT_REVISION_WATCH_VERSION = 1
CURRENT_PER_ID_STATE_VERSION = 1
CURRENT_QUARANTINE_VERSION = 1
CURRENT_CADENCE_SEMANTICS_VERSION = 1


class ProtocolNegotiationError(RuntimeError):
    """An authenticated peer returned a partial or contradictory tuple."""


def current_capabilities(
    *,
    cadence_runtime_fingerprint: str,
) -> ProtocolCapabilities:
    """Build the one exact tuple supported by this scheduler release."""

    fingerprint = cadence_runtime_fingerprint.strip()
    if not fingerprint:
        raise ValueError("cadence runtime fingerprint must be non-empty")
    return ProtocolCapabilities(
        protocol_epoch=CURRENT_PROTOCOL_EPOCH,
        fire_response_version=CURRENT_FIRE_RESPONSE_VERSION,
        cursor_transition_version=CURRENT_CURSOR_TRANSITION_VERSION,
        stable_snapshot_version=CURRENT_STABLE_SNAPSHOT_VERSION,
        revision_watch_version=CURRENT_REVISION_WATCH_VERSION,
        per_id_state_version=CURRENT_PER_ID_STATE_VERSION,
        quarantine_version=CURRENT_QUARANTINE_VERSION,
        cadence_semantics_version=CURRENT_CADENCE_SEMANTICS_VERSION,
        cadence_runtime_fingerprint=fingerprint,
    )


def require_exact_current(
    *,
    selected: ProtocolCapabilities,
    expected: ProtocolCapabilities,
    ping_protocol_epoch: int,
) -> None:
    """Reject every non-exact current negotiation result.

    Legacy selection is deliberately outside this function because it requires
    two transport facts together: Ping epoch zero *and* negotiation returning
    gRPC ``UNIMPLEMENTED``. A method response containing zero is not legacy.
    """

    if ping_protocol_epoch != expected.protocol_epoch:
        raise ProtocolNegotiationError(
            "Ping protocol epoch contradicts the required current epoch",
        )
    numeric = (
        selected.protocol_epoch,
        selected.fire_response_version,
        selected.cursor_transition_version,
        selected.stable_snapshot_version,
        selected.revision_watch_version,
        selected.per_id_state_version,
        selected.quarantine_version,
        selected.cadence_semantics_version,
    )
    if any(value <= 0 for value in numeric):
        raise ProtocolNegotiationError(
            "current protocol capabilities must all be nonzero",
        )
    if selected != expected:
        raise ProtocolNegotiationError(
            "Brain selected an unsupported scheduler protocol tuple",
        )


__all__ = [
    "CURRENT_CADENCE_SEMANTICS_VERSION",
    "CURRENT_PROTOCOL_EPOCH",
    "ProtocolNegotiationError",
    "current_capabilities",
    "require_exact_current",
]
