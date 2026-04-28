# Changelog

All notable changes to `z4j-scheduler` will be documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Solar schedule support (kind="solar").** Closes the §5.1 v1
  surface gap. Expression encodes `"<event>:<lat>:<lon>"` where
  event is one of `dawn` / `sunrise` / `noon` / `solar_noon` /
  `sunset` / `dusk` / `midnight` / `solar_midnight`. Backed by
  the `astral` library (added as a runtime dep, ~50 KB pure
  Python). The celery importer now translates
  `celery.schedules.solar(...)` (both static `app.conf.beat_schedule`
  config AND `django_celery_beat.PeriodicTask.solar` rows)
  instead of skipping with a warning. The celery exporter renders
  back to `solar('event', lat, lon)` and conditionally imports
  the `solar` symbol. The shadow comparator's `_predict_solar`
  walks day-by-day, returning no fires for polar
  perpetual-day / -night windows. New module:
  `z4j_scheduler.tick.solar` with `parse_solar_expression()` +
  `next_solar_fire()` helpers. Pinned by
  `tests/unit/test_solar.py` (25 tests).
- **24-hour shadow-mode fire comparator** (`import --verify
  --duration 24h`). Closes the §17.1 spec promise. Predicts
  every fire each side would emit over the window and reports
  divergence in three buckets: `only_source`, `only_target`,
  `args_diverge`. Catches importer translation bugs (dropped
  kwarg, mis-translated cron field, lost timezone) at verify
  time, not in production. Output ends with the operator's
  go/no-go signal: *"Safe to flip the canonical scheduler."*
  New module: `z4j_scheduler.verify.shadow_comparator`. Pinned
  by `tests/unit/test_shadow_comparator.py` (30 tests).
- **Head-to-head benchmark vs celery-beat.** Closes a §29 GA
  exit criterion. Runnable as
  `python -m tests.benchmarks.bench_celery_beat_compare`;
  committed JSON report at
  `tests/benchmarks/results/celery_beat_compare.json`. Measures
  three metrics on identical workloads: per-call next-fire
  computation cost (where celery's hand-tuned crontab beats
  croniter 4-8×), per-tick due-list cost at 100/1k/10k
  schedules (where z4j is 1.5× faster), and RSS at 10k
  schedules (where z4j is 10× lighter at 634 B/schedule vs
  6377 B/schedule). Six regression tests pin the report shape
  so an upstream library upgrade can't silently invalidate the
  published numbers.

### Fixed

- **Memory benchmark returned `-1.0 MB` on Windows.** The
  `bench_phase5._rss_mb()` helper tried `psutil` then
  `resource`; both modules are absent from a clean Windows
  Python install, leaving the memory targets in §23 reported
  as a sentinel that masqueraded as "well under target." Now
  prefers `psutil` if installed, falls back to `resource` on
  Unix, `/proc/self/status` on Linux without `resource`, and a
  Windows `ctypes` `GetProcessMemoryInfo` call as a last
  resort. The `ctypes` path needed explicit `argtypes` /
  `restype` declarations - without them the 64-bit pseudo-handle
  from `GetCurrentProcess()` truncated to 32 bits and the call
  failed silently with `ERROR_INVALID_HANDLE` (6). Real
  numbers now: idle 39.7 MB, 10k schedules 52.6 MB - both
  beat §23 targets by 2-5.7×.

## [1.1.0] - 2026-04-27

> **First PyPI release.** `z4j-scheduler` joins the v1.1.x ecosystem
> baseline alongside `z4j-core` 1.1.0, `z4j-brain` 1.1.0, and the
> `z4j` umbrella 1.1.0. From this version forward `z4j-scheduler`
> patches in the v1.1.x line upgrade and downgrade cleanly to / from
> any other v1.1.x version per the brain-side `docs/MIGRATIONS.md`
> contract. The 0.x scaffolding never shipped to PyPI; this entry is
> the consolidated initial release.

### Fixed

- **mTLS interceptor accepted bytes-keyed AuthContext only.** The
  brain-side `SchedulerAllowlistInterceptor` and the scheduler-side
  trigger-gRPC mirror both looked up the peer cert under
  `auth_ctx.get(b"x509_common_name", [])`. grpc.aio 1.6+ returns
  the same logical entries under str keys, so every CN check
  silently returned `[]` and the embedded sidecar's auto-minted
  `scheduler-embedded` client cert was rejected as
  `peer CNs []`. Both interceptors now look up under both shapes
  and decode bytes/str values defensively. Pinned by
  `tests/unit/test_trigger_grpc_auth.py::TestEnforceCnAuthContextShape`.
- **Exporters emitted JSON literals where Python literals were
  expected.** `exporters/celery.py`, `exporters/rq.py`, and
  `exporters/apscheduler.py` used `json.dumps()` for args / kwargs,
  emitting `true` / `false` / `null` rather than `True` / `False` /
  `None`. Any operator pasting the rendered output into a Python
  module hit `NameError: name 'true' is not defined`. Extracted a
  shared `py_repr()` helper in `exporters/_client.py`; all three
  Python-target exporters now produce valid Python source.
  Round-trip pinned by `tests/unit/test_exporters.py` —
  `TestCeleryExecRoundTrip`, `TestRqExecRoundTrip`,
  `TestApsExecRoundTrip` actually `exec()` the rendered output and
  assert the resulting structure matches the source schedule.

### Added

- **Defensive periodic full re-sync (15-min default).** The
  `WatchStream` now spawns a parallel timer that runs
  `_full_sync()` on a fixed cadence even when the watch event
  stream is healthy. Catches missed DELETE events that the
  reconnect-only sync (Phase 2) trusted the stream to deliver.
  Cadence: `Z4J_SCHEDULER_RECONCILE_INTERVAL_SECONDS=900` (set to
  0 to disable). Idempotent under `_sync_lock` so on-reconnect sync
  + periodic timer never race. Pinned by
  `tests/unit/test_watch.py::TestPeriodicResync` and
  `TestFullSyncDeleteSweep`.
- **Foundation** per [`docs/SCHEDULER.md`](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md) v0.3
- Package metadata + workspace registration
- gRPC `.proto` contract for brain ↔ scheduler communication
- Module skeleton for storage / tick / leader / dispatch / api / importers / observability
- `pydantic-settings` Settings class with all `Z4J_SCHEDULER_*` env vars
- Migration importers wired into the CLI (`z4j-scheduler import --from <tool>`):
  celery beat config, django-celery-beat `PeriodicTask` table,
  rq-scheduler Redis sorted set, APScheduler jobstores (3.x + 4.x),
  and system crontab files. `--dry-run` prints JSONL for review;
  the live path POSTs to brain's `schedules:import` endpoint.
- Brain-side `SchedulerService` (`packages/z4j-brain/backend/.../scheduler_grpc/`):
  `ListSchedules`, `WatchSchedules` (poll-based diff), `FireSchedule`,
  `AcknowledgeFireResult`, `Ping`. Wires into brain's lifespan and
  is gated by `Z4J_SCHEDULER_GRPC_ENABLED`. mTLS with optional CN
  allow-list; operator mints client certs via `z4j mint-scheduler-cert`.

### Notes

This is the foundation commit. Implementation lands across the
following phases per the spec:

- Phase 0: design + benchmark harness (current - in progress)
- Phase 1: single-instance core (cron + interval + one-shot, gRPC
  client, dispatch, /health /ready /metrics, brain-side
  SchedulerService)
- Phase 2: production hardening + framework helpers + migration tools
- Phase 3: HA + leader election (Postgres advisory locks)
- Phase 4: public release

## [0.1.0] - unreleased

Initial scaffold. Not published.
