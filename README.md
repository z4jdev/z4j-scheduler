# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

Engine-agnostic dynamic scheduler for [z4j](https://z4j.com).

A standalone scheduler companion process that fires schedules into any
z4j-supported queue engine (Celery, RQ, Dramatiq, Huey, arq, TaskIQ).
Use it as the canonical scheduler when you want one place to manage
cron / interval / one-shot / solar schedules across multiple engines —
or when your stack has no upstream scheduler (e.g. plain Dramatiq).

## What it ships

- **Schedule kinds** — cron, interval, one-shot, solar
  (sunrise / sunset / dawn / dusk / noon / midnight at a given lat/lon)
- **Migration importers** — bring existing schedules from celery-beat,
  django-celery-beat, rq-scheduler, APScheduler, or `/etc/crontab` into
  z4j with `z4j-scheduler import --from <source>`
- **Migration exporters** — go the other way too, for staged
  migrations or rollbacks
- **gRPC trigger surface** — the brain dispatches `trigger_now` over
  a private gRPC channel so the scheduler's last-fire cache stays
  consistent (no double-fires on the next tick)
- **Distributed locks** — Postgres advisory locks; safe to run multiple
  scheduler replicas behind a load balancer
- **Catch-up policy** — per-schedule choice between *fire missed
  occurrences* and *skip and continue* after downtime

## Install

```bash
pip install z4j-scheduler
z4j-scheduler serve
```

Or as a sidecar in your app's deployment:

```bash
# import existing celery-beat schedules into z4j first:
pip install z4j-scheduler[celery-import]
z4j-scheduler import --from celery --celery-app myapp.celery:app \
  --project myproject --brain-url https://brain.example.com \
  --api-token "$Z4J_SCHEDULER_BRAIN_API_TOKEN"
```

## Documentation

Full docs at [z4j.dev/scheduler/](https://z4j.dev/scheduler/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-scheduler/
- Issues: https://github.com/z4jdev/z4j-scheduler/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
