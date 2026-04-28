"""Postgres advisory lock leader gate.

asyncpg-based implementation of leader election via
``pg_try_advisory_lock(<namespace>, <project_id_hash>)``.

Public API (Phase 3 - HA milestone):

    class LeaderGate:
        async def start(): ...
        async def stop(): ...
        def is_leader(project_id: UUID) -> bool: ...
        async def projects(): ...

The gate maintains one long-lived asyncpg connection that holds
all the project-namespaced advisory locks. If the connection drops,
Postgres releases the locks automatically and standby instances
will pick them up on their next poll.

Failure modes (per ``docs/SCHEDULER.md §14``):

- Postgres unreachable: gate returns False for every project,
  scheduler keeps ticking but never dispatches
- Connection drop mid-flight: locks released, standby takes over
  within ~10s
- Two instances both think they're leader: impossible per
  Postgres advisory lock semantics; if it happens, bigger problems
"""

from __future__ import annotations

# Phase 3 implementation: asyncpg pool, advisory lock loop,
# is_leader query, graceful release on shutdown.
