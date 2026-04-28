"""Tick engine - decides when each schedule should fire.

The asyncio loop that:

1. Reads the next-due schedule from the cache
2. Sleeps until that schedule's ``next_fire_at``
3. Checks the leader gate
4. If leader: hands the schedule to the dispatch module
5. Computes the new ``next_fire_at`` based on the schedule's kind
6. Loops

Submodules:

- :mod:`~z4j_scheduler.tick.engine` - main asyncio loop
- :mod:`~z4j_scheduler.tick.cron` - croniter wrapper, DST-aware
- :mod:`~z4j_scheduler.tick.interval` - simple interval next-fire
- :mod:`~z4j_scheduler.tick.one_shot` - one-shot semantics + auto-disable
- :mod:`~z4j_scheduler.tick.catch_up` - skip / fire_one_missed / fire_all_missed

Tick accuracy targets per ``docs/SCHEDULER.md §23``:

- p50: +/- 100 ms
- p99: +/- 500 ms (under sustained 100 fires/sec load)
"""
