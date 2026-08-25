"""Exactly ``dev`` relaxes the scheduler's posture, same as the brain.

Both gates here compared the environment against the literal production label,
which inverts the rule for every other name. A scheduler tagged ``staging`` or
``prod`` therefore:

  - skipped the metrics-auth fail-fast, so binding ``0.0.0.0`` with metrics on
    and no token served an unauthenticated ``/metrics`` carrying project
    labels, schedule names, leadership state and fire status;
  - was allowed to talk plaintext gRPC to the brain.

Both refusal messages already told the reader to set ``dev``, so the code
disagreed with its own error text. The brain had the identical defect and it was
found by an external reviewer; the guard written to prevent a recurrence scanned
only the brain package, so these two survived it. They are behavioural tests
because the source guard, now widened to every package, is the backstop rather
than the proof.
"""

from __future__ import annotations

import pytest
from z4j_scheduler.settings import Settings

#: Enough to construct. The gates under test are separate validators.
BASE = {"brain_grpc_url": "brain:7701", "brain_rest_url": "https://brain:7700"}
#: Satisfies the channel validator so the metrics gate is what is being read.
TLS = {"tls_cert": "/t/c.pem", "tls_key": "/t/k.pem", "tls_ca": "/t/ca.pem"}

NON_DEV = ["production", "staging", "prod", "prod-eu", "qa", "development", "test", ""]


def test_dev_may_serve_metrics_without_a_token() -> None:
    """The relaxation still exists, so a laptop run is not broken by this."""
    settings = Settings(
        **BASE,
        **TLS,
        environment="dev",
        metrics_enabled=True,
        bind_host="0.0.0.0",
        metrics_auth_token=None,
    )
    assert settings.is_dev is True


@pytest.mark.parametrize("environment", NON_DEV)
def test_no_other_label_may_serve_unauthenticated_metrics(environment: str) -> None:
    """A public bind with metrics on and no token has to be refused."""
    with pytest.raises(ValueError, match="metrics"):
        Settings(
            **BASE,
            **TLS,
            environment=environment,
            metrics_enabled=True,
            bind_host="0.0.0.0",
            metrics_auth_token=None,
        )


def test_dev_may_opt_into_plaintext_grpc() -> None:
    Settings(**BASE, environment="dev", insecure_grpc=True, metrics_enabled=False)


@pytest.mark.parametrize("environment", NON_DEV)
def test_no_other_label_may_opt_into_plaintext_grpc(environment: str) -> None:
    """Plaintext on the schedule-control channel is a dev-only trade-off."""
    with pytest.raises(ValueError, match="insecure_grpc"):
        Settings(
            **BASE,
            environment=environment,
            insecure_grpc=True,
            metrics_enabled=False,
        )


@pytest.mark.parametrize("near_miss", ["DEV", "Dev", " dev ", "dev ", "	dev"])
def test_only_the_exact_string_relaxes_not_a_case_or_space_variant(near_miss: str) -> None:
    """The published guarantee is the exact string, so the code must be exact.

    This test previously asserted the opposite. It certified ``DEV``, ``Dev``
    and ``" dev "`` as relaxing, because the comparisons being replaced called
    ``.strip().lower()`` and I carried that tolerance forward. Meanwhile the
    field description and the release notes both said "the exact string dev"
    and "mirrors the brain", and the brain compares exactly. So the scheduler
    relaxed its metrics authentication and its gRPC transport for spellings the
    documentation promised were production, and the test made that permanent.

    Narrowing this does refuse a deployment that used to start. That is in the
    changelog, and it is the safe direction: the alternative is a security
    posture that depends on how somebody capitalised an environment variable.
    """
    # Metrics disabled so the metrics validator, which now refuses these, does
    # not stop construction before is_dev can be read. That refusal is the
    # point and is asserted separately above; here the predicate itself is
    # under test.
    settings = Settings(**BASE, **TLS, environment=near_miss, metrics_enabled=False)
    assert settings.is_dev is False


def test_the_exact_string_still_relaxes() -> None:
    assert Settings(**BASE, **TLS, environment="dev", metrics_enabled=False).is_dev is True


@pytest.mark.parametrize("near_miss", ["DEV", "Dev", " dev "])
def test_a_case_variant_is_refused_by_the_gates_not_merely_by_the_predicate(
    near_miss: str,
) -> None:
    """The consequence, not just the predicate: these no longer relax anything."""
    with pytest.raises(ValueError, match="insecure_grpc"):
        Settings(**BASE, environment=near_miss, insecure_grpc=True, metrics_enabled=False)
