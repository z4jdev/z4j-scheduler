# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

The engine-agnostic dynamic scheduler for [z4j](https://z4j.com).

One service drives Celery, RQ, Dramatiq, Huey, arq, and TaskIQ from
a single dashboard. Schedules live in z4j's database, you edit
them live without restarting anything, every change made through
z4j is recorded in an HMAC-chained audit log, and importers +
exporters keep the door open in either direction. This is the canonical scheduler when you
want one place to manage cron / interval / one-shot / solar
schedules across mixed engines.

## Compatibility

Python 3.11+. PostgreSQL 17+ for shared-database HA (SQLite is supported
for single-node brain deployments). The scheduler sends each prepared fire
to the brain, which routes it to a connected Celery, RQ, Dramatiq, Huey,
arq, or TaskIQ agent; engine packages do not need to be installed beside the
scheduler service.

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What makes z4j-scheduler different

z4j-scheduler is a deliberate alternative to in-language schedulers
like celery-beat, rq-scheduler, and APScheduler. The differences
that matter day to day:

- **Engine-agnostic.** One scheduler service for every supported
  Python task engine. A project running Celery for legacy services
  and arq for a FastAPI rewrite uses the same scheduler for both,
  with one dashboard and one audit trail.
- **Live editing.** Schedules live in the brain's database.
  Create, edit, pause, resume, rename, and delete from the dashboard
  or REST API without restarting the scheduler service.
- **HMAC-chained audit log.** Every schedule mutation that goes
  through z4j records the actor, change, and time (plus the source IP
  for request-originated changes) in an
  HMAC-chained audit trail alongside the brain's other audit rows,
  and the database refuses a schedule change that arrives without a
  fresh revision and a matching change-log entry. That covers the
  dashboard, the API, config, and any adapter, including an older
  one. It does not cover a database role writing those tables
  directly, which can supply the revision and the change-log entry
  itself; guard those credentials accordingly.
- **HA-ready.** Multiple scheduler instances can run against the
  same Postgres database. Depending on the configured backend, Postgres
  advisory locks elect one global leader or one leader per project; followers
  stay warm. Failover latency follows the configured leader heartbeat, and
  slots that age during a handoff are handled by the per-schedule catch-up
  policy.
- **Migration tooling.** The CLI imports celery-beat (static and
  django-celery-beat), rq-scheduler, APScheduler SQLAlchemy jobstores, and system
  crontab. It exports reviewable Celery, RQ, APScheduler, or crontab
  configuration. Generated output is advisory and must be reviewed and tested
  against the target scheduler by the operator. This is not a
  lossless rollback: target formats cannot represent every z4j schedule kind
  or policy, and unsupported rows render as comments for manual handling.

## What it ships

| Capability | Notes |
|---|---|
| Schedule kinds | cron, interval, one-shot, solar (sunrise / sunset / dawn / dusk / noon / midnight at a given lat / lon) |
| Live editing | dashboard and REST API require no restart; declarative config reconciles when a configured framework startup hook or helper/CLI invokes it |
| Engine fan-out | Celery, RQ, Dramatiq, Huey, arq, TaskIQ |
| Importers | celery / django-celery-beat / rq-scheduler / apscheduler / cron |
| Exporters | celery / rq / apscheduler / cron; generated output is advisory, and target limitations can require manual handling |
| HA leader election | Postgres advisory locks; global or per-project leadership, with warm followers |
| Audit log | every mutation through z4j is HMAC-chained; the database refuses a schedule change with no matching change-log entry, though a role holding direct write access to those tables can supply both |
| Catch-up policy | per-schedule: skip, fire one missed, fire all missed |
| Timezones | IANA zones validated at the boundary; DST fall-back fold fixed (no double-fires); spring-forward gap handled |
| Trigger surface | brain validates and dispatches operator-triggered fires directly; manual fires do not advance the cadence cursor |

## Install

Standalone (recommended for production):

```bash
pip install z4j-scheduler
export Z4J_SCHEDULER_BRAIN_GRPC_URL=brain.internal:7701
export Z4J_SCHEDULER_BRAIN_REST_URL=https://brain.internal
export Z4J_SCHEDULER_TLS_CERT=/etc/z4j/scheduler.crt
export Z4J_SCHEDULER_TLS_KEY=/etc/z4j/scheduler.key
export Z4J_SCHEDULER_TLS_CA=/etc/z4j/ca.crt
export Z4J_SCHEDULER_BIND_HOST=127.0.0.1
z4j-scheduler serve
```

On the brain host, install `z4j[scheduler-grpc]` and enable the brain's mTLS
scheduler listener. A separate scheduler installation does not add the gRPC
runtime to the brain environment. The loopback operational bind above keeps
`/metrics` local to that host; a non-loopback production bind also requires a
metrics auth token or explicitly disabled metrics.

Embedded inside z4j (recommended for homelab and small teams):

```bash
pip install z4j z4j-scheduler
# then enable in brain settings: Z4J_EMBEDDED_SCHEDULER=true
```

With the flag on, the brain spawns and supervises a
`z4j-scheduler serve` subprocess for you, no separate deployment
unit to manage.

Migrate existing schedules in:

```bash
pip install 'z4j-scheduler[celery-import]'
z4j-scheduler import \
  --from celery \
  --celery-app myapp.celery:app \
  --project myproject \
  --brain-url https://brain.example.com \
  --api-token "$Z4J_SCHEDULER_BRAIN_API_TOKEN"
```

The other importer subcommands follow the same shape (`--from rq`,
`--from apscheduler`, `--from django-celery-beat`, etc.). Add
`--dry-run` to print the parsed schedules as JSONL for review
instead of writing them to the brain. `--verify` also implies dry-run; a normal
run with neither flag pushes the imported schedules immediately.

## When to choose z4j-scheduler

You probably want it if:

- You run more than one Python task engine and want one schedule
  surface across all of them.
- An auditor or a security review asks who paused the nightly
  billing job last Tuesday and you don't have a clean answer.
- You want to edit schedules without restarting a static-config scheduler.
- You want HA scheduling without standing up a second control
  plane.
- You're considering a one-time migration from celery-beat /
  rq-scheduler / APScheduler and want reviewable import and export tooling while
  accepting the target format's limitations.

You probably don't need it if:

- You run a single engine (typically Celery), have no compliance
  pressure, and the in-language scheduler already meets your needs.
  Stay where you are; we ship z4j-celerybeat / z4j-rqscheduler /
  z4j-apscheduler as adapters that surface those schedules in the z4j
  dashboard and expose the controls each backend supports, without replacing
  the native scheduler or taking ownership of its cadence.

## Documentation

Full docs at [z4j.dev/scheduler/](https://z4j.dev/scheduler/).
The migration guide at
[z4j.dev/scheduler/migrating-from-celery-beat/](https://z4j.dev/scheduler/migrating-from-celery-beat/)
walks the importer + dashboard verification path step by step.

## License

Apache-2.0, see [LICENSE](LICENSE). `z4j-scheduler` is independently
installable from the AGPL-licensed `z4j` server distribution; consult the
license terms for the obligations that apply to your deployment.
The accepted `Z4J_SCHEDULER_PRO_LICENSE_KEY` setting is a reserved, inert
compatibility field; setting it enables no feature.

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-scheduler/
- Issues: https://github.com/z4jdev/z4j-scheduler/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
