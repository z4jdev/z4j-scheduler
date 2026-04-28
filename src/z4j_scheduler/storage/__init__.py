"""Schedule storage + brain gRPC client.

Owns the connection to brain, the in-memory schedule cache, and the
WatchSchedules stream consumer. Single source of truth for "what
schedules does this scheduler tick."

Modules:

- :mod:`~z4j_scheduler.storage.brain_client` - gRPC channel + RPCs
- :mod:`~z4j_scheduler.storage.cache` - in-memory schedule cache
- :mod:`~z4j_scheduler.storage.watch` - WatchSchedules stream consumer

See ``docs/SCHEDULER.md §7.2`` for the architectural breakdown.
"""
