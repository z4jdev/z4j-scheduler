"""Boundary D protocol negotiation is exact and fail-closed."""

from __future__ import annotations

from dataclasses import replace

import pytest
from z4j_scheduler.storage._convert import capabilities_from_pb, capabilities_to_pb
from z4j_scheduler.storage._protocol import (
    ProtocolNegotiationError,
    current_capabilities,
    require_exact_current,
)


def _expected():
    return current_capabilities(cadence_runtime_fingerprint="f" * 64)


def test_capability_wire_round_trip() -> None:
    expected = _expected()
    assert capabilities_from_pb(capabilities_to_pb(expected)) == expected


def test_exact_current_tuple_is_accepted() -> None:
    expected = _expected()
    require_exact_current(
        selected=expected,
        expected=expected,
        ping_protocol_epoch=expected.protocol_epoch,
    )


@pytest.mark.parametrize(
    ("selected", "ping_epoch", "match"),
    [
        (replace(_expected(), stable_snapshot_version=0), 1, "nonzero"),
        (replace(_expected(), revision_watch_version=2), 1, "unsupported"),
        (_expected(), 0, "Ping protocol epoch"),
        (replace(_expected(), cadence_runtime_fingerprint="other"), 1, "unsupported"),
    ],
)
def test_partial_or_contradictory_current_tuple_is_rejected(
    selected,
    ping_epoch: int,
    match: str,
) -> None:
    with pytest.raises(ProtocolNegotiationError, match=match):
        require_exact_current(
            selected=selected,
            expected=_expected(),
            ping_protocol_epoch=ping_epoch,
        )


def test_empty_local_fingerprint_cannot_be_advertised() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        current_capabilities(cadence_runtime_fingerprint="")
