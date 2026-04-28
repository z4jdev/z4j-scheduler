# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

**License:** Apache 2.0

The modern Python scheduler in the z4j stack. Engine-agnostic,
dynamic-CRUD, HA-ready. Replaces celery-beat, rq-scheduler, and
APScheduler with a single tool that fires scheduled tasks at
**any** Python task queue (Celery, RQ, Dramatiq, arq, taskiq,
huey) via the existing z4j agent network.

z4j-scheduler is **not standalone**. It requires
[z4j-brain](https://github.com/z4jdev/z4j-brain) and is structurally
part of the z4j stack.

| Resource | Link |
|---|---|
| Project home | [z4j.com](https://z4j.com) |
| Full documentation | [z4j.dev/schedulers/z4j-scheduler/](https://z4j.dev/schedulers/z4j-scheduler/) |
| Source | [github.com/z4jdev/z4j-scheduler](https://github.com/z4jdev/z4j-scheduler) |
| Issues | [github.com/z4jdev/z4j-scheduler/issues](https://github.com/z4jdev/z4j-scheduler/issues) |

---

## Why migrate off celery-beat

In every conversation with operators running Python task
infrastructure in production, the same six wishes come up:

1. Change a schedule without a restart, propagate within a second.
2. Run more than one instance for HA without duplicate fires.
3. Manage schedules from a real UI, not Django admin.
4. See an audit trail of who changed what schedule when.
5. Schedule into multiple engines without wiring three separate
   scheduler tools.
6. Get alerted when a schedule fails to fire or fires but the
   task fails.

z4j-scheduler delivers all six.

| Concern | celery-beat | django-celery-beat | **z4j-scheduler** |
|---|---|---|---|
| Engine support | celery only | celery only | **6 engines** (Celery, RQ, Dramatiq, Huey, arq, taskiq) |
| Dynamic CRUD | restart needed | 5 to 30s poll | **under 100ms via LISTEN/NOTIFY** |
| HA / leader election | no | no | **yes (Postgres advisory lock)** |
| Audit trail | no | no | **HMAC chain via z4j-brain** |
| Sub-second propagation | n/a | no | **yes** |
| 10k+ schedules | degrades | degrades | **yes (1M tested, 495 MB RSS)** |
| DST-correct | partial | partial | **yes (croniter + zoneinfo)** |
| Catch-up policy per schedule | no | no | **yes (skip / fire_one / fire_all)** |
| Schedule kinds | cron, interval, clocked | same | **cron, interval, one_shot, solar** |
| Real CRUD UI | no | django-admin | **yes (z4j-brain dashboard)** |
| Migration tools | n/a | n/a | **yes** (4 importers + reverse export + 24h shadow comparator) |
| Native notifications | no | no | **yes** (in-app, email, Slack, Telegram, webhook) |

The trade-off: z4j-scheduler adds about 10 to 30ms per fire vs
celery-beat's direct broker write because it goes through one extra
hop (scheduler to brain to agent to engine). For minute-resolution
and coarser schedules this is invisible. For sub-second workloads,
use the engine's native scheduler.

---

## Measured numbers (v1.1)

Numbers below are from a single Windows laptop with WSL-2-backed
Docker; production hardware typically beats these.

### Against the v1 GA targets

| Metric | Target | Measured | Margin |
|---|---|---|---|
| Memory at idle | < 80 MB | **39.7 MB** | 2.0x headroom |
| Memory at 10k schedules | < 300 MB | **52.6 MB** (about 634 B / schedule) | 5.7x headroom |
| Memory at 100k schedules | (n/a, beyond target) | **93.6 MB** | well in budget |
| Memory at 1M schedules | (n/a) | **495.6 MB** | fits in a 1 GB container |
| Startup time | < 2 s | **102.7 ms** | 19.5x headroom |
| Tick accuracy p50 | +/- 100 ms | **5.4 ms** | 18x headroom |
| Tick accuracy p99 | +/- 500 ms | **160.2 ms** | 3.1x headroom |
| Fires per second | 100/s sustained | **120/s** | met |
| HA failover time | < 10 s | **130 ms** | 76x headroom |

### Head-to-head vs celery-beat

Same workload (5 typical cron expressions and scaling tests)
measured against `celery.schedules.crontab` running in-process.

| Workload | z4j-scheduler | celery-beat | Verdict |
|---|---|---|---|
| Single next-fire compute (p50, every-minute) | 41 us | 11 us | celery 3.6x faster |
| Single next-fire compute (p99, every-5-min) | 147 us | 18 us | celery 8x faster |
| Per-tick due-list scan at 100 schedules | 1.4 ms | 2.1 ms | **z4j 1.5x faster** |
| Per-tick due-list scan at 10k schedules | 131 ms | 200 ms | **z4j 1.5x faster** |
| RSS at 10k schedules in memory | **6 MB** (634 B/sched) | 60 MB (6377 B/sched) | **z4j 10x lighter** |

Honest mixed result: celery's hand-tuned `crontab` class beats
croniter on the per-call next-fire computation, but
z4j-scheduler's per-tick and memory profile is strictly better at
operational scale. Replacing celery-beat trades about 30 us of
per-fire compute for about 10x less RAM and about 33% faster ticks.

---

## Install

```bash
pip install z4j-scheduler
# or via the umbrella with extras:
pip install z4j[scheduler]
```

## Run

```bash
z4j-scheduler serve \
  --brain-grpc-url brain:7701 \
  --brain-rest-url http://brain:7700 \
  --tls-cert /certs/scheduler.crt \
  --tls-key /certs/scheduler.key \
  --tls-ca /certs/brain-ca.crt
```

Full configuration reference (every env var, every flag) lives at
[z4j.dev/schedulers/z4j-scheduler/](https://z4j.dev/schedulers/z4j-scheduler/).

For the **single-container homelab deploy**, set
`Z4J_EMBEDDED_SCHEDULER=true` on the brain image and skip running
a separate scheduler process. Brain spawns z4j-scheduler as a
supervised subprocess with auto-minted loopback mTLS PKI.

For **Kubernetes**, see the deployment guide at
[z4j.dev/schedulers/z4j-scheduler/](https://z4j.dev/schedulers/z4j-scheduler/).

## Requirements

- z4j-brain v1.1+ with the SchedulerService gRPC endpoint enabled
- Postgres 17+ (shared with brain), required only for HA via
  advisory locks; single-instance deployments work without it
- Python 3.13+

---

## Migration from celery-beat

The full operator playbook lives at
[z4j.dev/schedulers/z4j-scheduler/](https://z4j.dev/schedulers/z4j-scheduler/).
Quick version:

```bash
# 1. Read-only diff: see what would be imported.
z4j-scheduler import \
    --from django-celery-beat \
    --django-settings myapp.settings \
    --project acme-prod \
    --verify

# 2. Shadow-mode fire prediction over the next 24 hours.
#    Reports per-fire divergence in timing, args, kwargs, queue.
#    Verdict line says "Safe to flip" iff zero divergences.
z4j-scheduler import \
    --from django-celery-beat \
    --django-settings myapp.settings \
    --project acme-prod \
    --verify --duration 24h

# 3. Apply.
z4j-scheduler import \
    --from django-celery-beat \
    --django-settings myapp.settings \
    --project acme-prod

# 4. Stop celery beat. Start z4j-scheduler. Done.
```

### Reverse migration (back-out plan)

Adoption is reversible. The export tool generates a Python module
you paste into your Django settings to revert:

```bash
z4j-scheduler export --to celery --project acme-prod \
    --output beat_schedule.py
```

Targets: `celery`, `rq`, `apscheduler`, `cron`.

### Importers supported

- `--from celery`: celery app's static `app.conf.beat_schedule`
- `--from django-celery-beat`: `PeriodicTask` ORM table
- `--from rq`: rq-scheduler Redis sorted set
- `--from apscheduler`: APScheduler jobstores (3.x and 4.x)
- `--from cron`: `/etc/crontab` and friends (system cron files)

All five include `--dry-run` (print JSONL) and `--verify` (diff
against brain). Solar schedules (rare in celery-beat) round-trip
through the importer and exporter.

---

## Schedule kinds (v1.1)

- **cron**: any standard 5-field crontab string
- **interval**: `30s`, `5m`, `2h`, `1d` (or bare integer seconds)
- **one_shot**: fire once at an ISO-8601 timestamp
- **solar**: `event:lat:lon` for sunrise, sunset, dawn, dusk,
  noon, solar_noon, midnight, solar_midnight at a given location.
  Backed by [`astral`](https://pypi.org/project/astral/); polar
  perpetual-day windows return no fire.

Per-schedule **catch-up policy** (skip / fire_one_missed /
fire_all_missed) is honored at recovery time.

---

## Project status

Engineering scope of v1 GA is closed. Every architectural
deliverable is shipped; the four "in the wild" criteria (5+
external production deployments, 90-day soak, public benchmark
write-up, first paying inquiry) are calendar-bound on adoption.

If you want zero risk, wait for those references. If you want to
be one of the early production users, the migration tools and
rollback plan are built. You can be running it in production by
end of week.

Open an issue at
[github.com/z4jdev/z4j-scheduler/issues](https://github.com/z4jdev/z4j-scheduler/issues)
with your migration story.

---

## See also

- [z4j.com](https://z4j.com): project home
- [z4j.dev/schedulers/z4j-scheduler/](https://z4j.dev/schedulers/z4j-scheduler/): full documentation, migration playbook, configuration reference
- [z4jdev/z4j-celerybeat](https://github.com/z4jdev/z4j-celerybeat): the *adapter* for an existing celery-beat process (coexistence path; z4j-scheduler is the *replacement* path)
- [z4jdev/z4j-brain](https://github.com/z4jdev/z4j-brain): the brain this scheduler integrates with
- [z4jdev/z4j-core](https://github.com/z4jdev/z4j-core): shared models and protocols
