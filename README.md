# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg?v=1.7.0)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg?v=1.7.0)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg?v=1.7.0)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

The engine-agnostic dynamic scheduler for [z4j](https://z4j.com).

One service drives Celery, RQ, Dramatiq, Huey, arq, and TaskIQ from
a single dashboard. Schedules live in z4j's database, you edit
them live without restarting anything, every change is recorded in
an HMAC-chained audit log, and importers + exporters keep the door
open in either direction. This is the canonical scheduler when you
want one place to manage cron / interval / one-shot / solar
schedules across mixed engines.

## Compatibility

Python 3.11+. PostgreSQL 17+ for shared-database HA (SQLite supported for single-node deployments). Fans out to whichever engines you install on the same host (Celery, RQ, Dramatiq, Huey, arq, TaskIQ); each engine extra carries its own upstream version floor.

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What makes z4j-scheduler different

z4j-scheduler is a deliberate alternative to in-language schedulers
like celery-beat, rq-scheduler, and APScheduler. The differences
that matter day to day:

- **Engine-agnostic.** One scheduler service for every supported
  Python task engine. A project running Celery for legacy services
  and arq for a FastAPI rewrite uses the same scheduler for both,
  with one dashboard and one audit trail.
- **Live editing.** Schedules live in z4j's Postgres database.
  Create, edit, pause, resume, rename, and delete from the dashboard
  or REST API without a daemon restart. celery-beat needs a
  beat-process restart for static-config edits, rq-scheduler stores
  schedules in Redis but has no editing UI, APScheduler edits are
  in-process.
- **HMAC-chained audit log.** Every schedule mutation (who, what,
  when, from which IP) is recorded in a tamper-evident audit chain
  alongside the brain's other audit rows. celery-beat keeps no
  record. django-celery-beat keeps a partial record only if you
  wired up django-auditlog.
- **HA-ready.** Multiple scheduler instances can run against the
  same Postgres database. Postgres advisory locks elect one leader
  to tick; followers stay warm. Rolling restarts and leader
  failovers are seconds, not minutes, with no missed-fire window
  beyond the per-schedule catch-up policy.
- **Reversible by design.** Importers cover celery-beat (static and
  django-celery-beat), rq-scheduler, APScheduler jobstores, Huey
  `@periodic_task`, arq cron, taskiq schedule sources, and system
  crontab. Exporters write any schedule back to those same formats.
  Round-trip integrity is pinned by tests so you can leave whenever
  you want; the schedule definitions are yours, not ours.

## What it ships

| Capability | Notes |
|---|---|
| Schedule kinds | cron, interval, one-shot, solar (sunrise / sunset / dawn / dusk / noon / midnight at a given lat / lon) |
| Live editing | dashboard, REST API, declarative config; no restart required |
| Engine fan-out | Celery, RQ, Dramatiq, Huey, arq, TaskIQ |
| Importers | celery / django-celery-beat / rq-scheduler / apscheduler / huey-periodic / arq-cron / taskiq-scheduler / cron |
| Exporters | every native format above; round-trip integrity tested |
| HA leader election | Postgres advisory lock, followers stay warm |
| Audit log | every mutation HMAC-chained, tamper-evident |
| Catch-up policy | per-schedule: skip, fire one missed, fire all missed |
| Timezones | IANA zones validated at the boundary; DST fall-back fold fixed (no double-fires); spring-forward gap handled |
| Trigger surface | brain dispatches `trigger_now` over a private gRPC channel so the scheduler's last-fire cache stays consistent |

## Install

Standalone (recommended for production):

```bash
pip install z4j-scheduler
z4j-scheduler serve
```

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
instead of writing them to the brain; without it, imports are
pushed immediately.

## When to choose z4j-scheduler

You probably want it if:

- You run more than one Python task engine and want one schedule
  surface across all of them.
- An auditor or a security review asks who paused the nightly
  billing job last Tuesday and you don't have a clean answer.
- You're tired of restarting celery-beat to change a cron expression.
- You want HA scheduling without standing up a second control
  plane.
- You're considering a one-time migration from celery-beat /
  rq-scheduler / APScheduler and want a reversible path.

You probably don't need it if:

- You run a single engine (typically Celery), have no compliance
  pressure, and the in-language scheduler already meets your needs.
  Stay where you are; we ship z4j-celerybeat / z4j-rqscheduler /
  z4j-apscheduler as observation-only adapters that surface those
  schedules in the z4j dashboard without taking ownership.

## Documentation

Full docs at [z4j.dev/scheduler/](https://z4j.dev/scheduler/).
The migration guide at
[z4j.dev/scheduler/migrating-from-celery-beat/](https://z4j.dev/scheduler/migrating-from-celery-beat/)
walks the importer + dashboard verification path step by step.

## License

Apache-2.0, see [LICENSE](LICENSE). Importing z4j-scheduler does
not affect your application's licensing. z4j (server +
dashboard + API) is AGPL v3, isolated in its own process; the
scheduler is separate.

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-scheduler/
- Issues: https://github.com/z4jdev/z4j-scheduler/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
