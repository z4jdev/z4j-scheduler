# Changelog

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
