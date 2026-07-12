# Changelog

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
