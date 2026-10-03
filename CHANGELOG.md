# Changelog

## 1.12.0 (2026-10-03)

* Report watch-stream health where operators look. The
  `z4j_scheduler_watch_healthy{project}` gauge follows the watch stream's
  health transitions; `/ready` answers 503 with `watch_unhealthy` once the
  stream has been down longer than `on_time_grace_seconds` and 200 again
  after recovery; `/info` reports `watch_stream_healthy`.
* Emit the two metrics that were declared and never set:
  `z4j_scheduler_schedules_loaded` follows the cache after every sync and
  membership change, and `z4j_scheduler_grpc_calls_total{method,status}`
  counts every RPC. A dropped project retires its `is_leader` series.
* Hold a mid-flight cadence mismatch instead of quarantining it. A
  `FIRE_CADENCE_SEMANTICS_MISMATCH` answered during a retry holds the
  schedule locally and asks for renegotiation; only a mismatch that survives
  an agreed renegotiation is the durable quarantine, and the dispatcher
  withholds transient retries while the watch stream is down so a retry
  never reaches an unnegotiated brain.
* Drain fires on shutdown. Shutdown stops admitting slots, awaits in-flight
  dispatches up to `fire_timeout_seconds`, and only then releases the leader
  gate and closes the channel, which the module docstring had claimed since
  1.4.
* Tombstone pressure that pauses a project's cadence logs a WARNING naming
  the project and count and requests an immediate snapshot resync instead of
  waiting for the periodic one.
* A single leader backend bound off loopback outside `dev` logs once that it
  is not an election.
* The default `on_time_grace_seconds` moves from 5 to 30 (the maximum stays
  300): a brain restart takes seconds, and a slot that comes due inside that
  window is late, not missed. The two fallbacks that still named five
  seconds say thirty.
* The watch stream's reconnect penalty, which never reset so that any later
  drop cost up to thirty seconds, clears once the stream has been healthy for
  one reconcile interval; a flap shorter than that keeps the penalty, so a
  flapping brain still backs off.
* `import --from` and `export --to` accept huey, arq, taskiq and dramatiq;
  the modules existed, were tested, and were reachable from nothing. Huey,
  arq and taskiq imports take `--huey-app`, `--arq-settings` and
  `--taskiq-broker` to locate the app, settings class or broker; `--from
  dramatiq`, which has no scheduler store, prints migration guidance and
  exits 2, and `--to dramatiq` renders guidance only. The optional extras
  `huey-import`, `arq-import` and `taskiq-import` carry the adapters' engine
  floors.
* The watch outage clock no longer restarts on a stream the brain refused or
  dropped before its first frame. A stream that delivered nothing and lasted
  less than the reconnect ceiling hands the outage its original start back,
  so a brain that lists schedules but refuses every Watch trips `/ready`
  with `watch_unhealthy` after the grace; before, each reconnect's
  successful full sync reset the clock and readiness never tripped.
* `z4j_scheduler_schedules_loaded` moves one count per membership-changing
  event instead of walking the whole cache on each; a ten-thousand-row
  import streamed as events no longer costs the watch task a minute of CPU.
* arq counts weekdays from Monday and cron from Sunday. The arq importer now
  shifts integer weekdays by one on the way in and the exporter shifts them
  back, so `weekday=0` is Monday on both sides; it imported as Sunday and
  cron Monday exported as arq Tuesday. The exporter also expands a cron
  weekday range into an arq set.
* The taskiq importer carries a label's `cron_offset` as the schedule
  timezone and the exporter emits the schedule timezone as `cron_offset`.
  Both dropped it, so a `Europe/Berlin` schedule imported as UTC and
  exported without a zone.
* One handler on the root logger renders every record, stdlib or structlog,
  so under `log_json` the whole `z4j.scheduler` tree is JSON; the leader,
  watch and tick lines came out as stdlib text beside the JSON.
* The serving line of a trigger server with an empty CN allow-list is a
  WARNING naming the open CA, as the docs promised; it restated the open
  state at INFO.
* `export` with the brain unreachable prints a one-line refusal naming the
  brain URL and exits 2 instead of an httpx traceback.
* Version references removed from the `check`, `status` and `restart` help
  text.
* Remove `leader/pg_advisory.py`, a seven-line stub with no importer.
* Type annotations across the package so that mypy strict gates it beside
  z4j-core, and an import-linter contract that scheduler source never
  imports the brain. No behaviour changed.

## 1.11.0 (2026-09-10)

* Stop starting new catch-up slots when the watch becomes unhealthy, leadership
  is lost, or shutdown begins. Preserve already accepted progress and recover
  the remaining backlog after conditions permit. Queued `skip` schedules do
  not consume their cursor while watch health is lost or shutdown is underway.
* Propagate unexpected dispatch-worker recovery failures to the existing tick
  supervisor, which backs off and exits after repeated failures. Cancellation
  is preserved and queued in-flight markers are released.
* Verify committed-response-loss recovery over real mTLS gRPC: retrying or
  replacing the scheduler client reuses the durable command or buffered fire.
  These tests cover acceptance, not broker execution or exactly-once effects.
* Bound PostgreSQL election calls with deadlines and clear local leadership
  when an operation stalls. Maintain per-project cache counts instead of
  scanning every schedule on each fire. Bound detailed metric retention and
  wire live tick drift, iteration and watch reconnection observations.
* Correct the Celery calendar comparison to use equivalent time windows. Local
  component benchmarks do not establish production capacity or a general
  performance ranking against another scheduler.

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* A schedule slot the leader had already seen as due is no longer discarded
  because of the scheduler's own dispatch latency. The on-time classification
  was recomputed on every attempt against a moving clock, so a slot judged
  on-time at the first attempt could exceed the grace by the time a retry ran,
  and under `catch_up="skip"` the retry then advanced past the slot and recorded
  it as fired without ever dispatching it. The classification is now frozen once
  a slot is judged on-time, so a failed dispatch cannot change what the slot is.
  The freeze expires after fifteen minutes, three times the dispatch backoff
  cap: holding it is right for the seconds a retry takes and wrong for the hours
  an outage takes, and without a bound a nightly job whose brain was down
  overnight would have run the next morning. A slot that genuinely elapsed while
  the scheduler was not running is unaffected either way: `catch_up` still
  governs it, and `skip` still discards it.
* Slots dropped by a `catch_up` policy are no longer silent. Each discarding
  pass logs a warning naming the schedule and the dropped occurrences, and
  increments `z4j_scheduler_slots_discarded_total`, so a schedule that has
  stopped producing work is visible rather than invisible.
* The on-time grace is configurable as `on_time_grace_seconds` (default 5s).
  The line between ordinary jitter and a real miss depends on a deployment's own
  dispatch latency, and the promotion-scoped grace used at failover derives from
  this value, so it moves with it.
* An unexpected exception in one tick iteration no longer stops the scheduler.
  The tick loop runs as a child task of a task group, so an escaping exception
  tore down the watch stream, the metrics server and the process along with
  scheduling, for every project. The loop now absorbs the failure, logs it with
  its traceback, counts it in
  `z4j_scheduler_engine_iteration_failures_total`, and backs off before the
  next pass. After ten consecutive failures with no success between them it
  gives up and surfaces the fault, so a scheduler that can never make progress
  does not sit there looking healthy.

## 1.9.0 (2026-08-25)

* Raise the protobuf runtime floor to 6.33.5, the first release that
  closes CVE-2026-0994 while remaining above the committed gencode's
  6.31.1 import minimum.

**Breaking for non-dev environments that relied on the old gating.** Two
security checks compared `Z4J_SCHEDULER_ENVIRONMENT` against the literal
`production`, so every other label took the relaxed branch. A scheduler tagged
`staging` skipped the metrics-auth fail-fast entirely, serving an
unauthenticated `/metrics` with project labels, schedule names, leadership
state and fire status; one tagged `prod` or `test` could talk plaintext gRPC to
the brain on the schedule-control channel.

Both now relax only for the exact string `dev`, matching the brain and matching
what their own refusal messages already told operators to set. **If you run with
`Z4J_SCHEDULER_ENVIRONMENT` set to `test`, `staging` or anything other than
`dev`, and you rely on `Z4J_SCHEDULER_INSECURE_GRPC=true` or on serving metrics
without a token, the scheduler will now refuse to start.** Either set
`Z4J_SCHEDULER_ENVIRONMENT=dev` to acknowledge the trade-off, or supply the mTLS
bundle and a metrics token. The field description advertised the old contract
and has been corrected.

* A paused schedule is now genuinely held. The hold folds into the enabled projection the scheduler reads, so a paused schedule stops ticking rather than being refused at fire time and retried under back-off.
* A delayed stop response no longer disables a newer scheduler state indefinitely.
* Snapshot cache, tick engine and trigger-gRPC handling updated for the schedule-control changes.

## 1.8.0 (2026-07-23)

* `fire_one_missed` / catch-up no longer dispatches the entire missed backlog on recovery (a duplicate-side-effects storm); interval catch-up now coalesces the missed window and the `fire_all_missed` drain is bounded and honors stop / disable mid-drain.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-11)

* Brain-side misfire detection, per-operator fire attribution, and a `z4j_scheduler_fire_variance_seconds` histogram.
* `z4j-scheduler info` is a real command: it queries the running service's `/info` endpoint and prints version, instance id, uptime, readiness, per-subsystem health, and loaded-schedule count, with `--json` for scripting (previously a stub that exited 2).
* Fixed a Postgres leader-election release-path `TypeError` (a structlog-style kwarg on a stdlib logger) that could abort cleanup before the local held flag cleared, leaving a stale-leader belief.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.6.5 (2026-05-26)

Security hardening (round-3 audit, R3-L1).

- `metrics_enabled` setting is now honored. Pre-1.6.5 the toggle existed but nothing read it, so the `/metrics` route was mounted regardless and operators who set `Z4J_SCHEDULER_METRICS_ENABLED=false` still got a 200 with the full Prometheus snapshot. The route is now conditionally mounted; when disabled, `/metrics` returns 404.
- Production fail-safe: the scheduler refuses to start when `environment=production`, `bind_host` is not a loopback address, `metrics_enabled=true`, and no `metrics_auth_token` is configured. The validator lists three valid resolutions (bind to loopback, set a token, or disable metrics) so operators are not left guessing.
- No behavioral change for development environments or for production deployments that already bound metrics to loopback or set an auth token.

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: engine-agnostic dynamic scheduler. One service drives Celery, RQ, Dramatiq, Huey, arq, and TaskIQ from one place. Live editing, HMAC audit, HA-ready.
