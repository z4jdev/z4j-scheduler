"""Leader election - ensures only one scheduler instance dispatches per project.

Two implementations:

- :class:`SingleInstanceLeaderGate` - always returns True. The right
  choice for solo deployments where exactly one scheduler process
  ever runs. Adds zero infrastructure dependencies.
- :class:`~z4j_scheduler.leader.postgres.PostgresAdvisoryLockLeaderGate`
  - HA-capable, asyncpg-backed. Each instance races for the same
  ``pg_try_advisory_lock``; the lock holder is leader. Failover
  in 1-3s on a clean leader death (TCP RST), 30-60s under hard
  network partition (waits for kernel keepalive).

The :class:`~z4j_scheduler.tick.engine.TickEngine` consumes any
implementation via the :class:`~z4j_scheduler.tick.engine.LeaderGate`
Protocol - just an ``is_leader(project_id) -> bool`` method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from uuid import UUID


class SingleInstanceLeaderGate:
    """Always-true leader gate for single-instance deployments.

    Default mode. Operators run exactly one scheduler
    process per stack and rely on their process supervisor (systemd,
    Docker, k8s) to restart it on failure. Schedule misses during
    the restart window are handled per the schedule's ``catch_up``
    policy.

    HA deployments select a Postgres-backed gate from
    :mod:`z4j_scheduler.leader.postgres`.
    """

    def is_leader(self, project_id: UUID) -> bool:
        """Always True. The single instance leads every project."""
        return True


__all__ = ["SingleInstanceLeaderGate"]
