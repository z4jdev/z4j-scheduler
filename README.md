# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

**License:** Apache 2.0
**Status:** v1.1.x — engineering scope of v1 GA closed. See
[docs/SCHEDULER.md §29](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md#29-success-criteria-for-v1-ga)
for the GA criteria status.

The modern Python scheduler in the z4j stack. Engine-agnostic,
dynamic-CRUD, HA-ready. Replaces celery-beat / rq-scheduler /
APScheduler with a single tool that fires scheduled tasks at
**any** Python task queue (Celery, RQ, Dramatiq, arq, taskiq,
huey) via the existing z4j agent network.

z4j-scheduler is **not standalone** — it requires
[z4j-brain](https://github.com/z4jdev/z4j-brain). It is
structurally part of the z4j stack.

---

## Why migrate off celery-beat

In every conversation with operators running Python task
infrastructure in production, the same six wishes come up
([SCHEDULER.md §3.2](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md#32-what-operators-actually-want)):

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
| Engine support | celery only | celery only | **6 engines** (Celery / RQ / Dramatiq / Huey / arq / taskiq) |
| Dynamic CRUD | restart needed | 5-30s poll | **<100ms via LISTEN/NOTIFY** |
| HA / leader election | no | no | **yes (Postgres advisory lock)** |
| Audit trail | no | no | **HMAC chain via z4j-brain** |
| Sub-second propagation | n/a | no | **yes** |
| 10k+ schedules | degrades | degrades | **yes (1M tested, 495 MB RSS)** |
| DST-correct | partial | partial | **yes (croniter + zoneinfo)** |
| Catch-up policy per schedule | no | no | **yes (skip / fire_one / fire_all)** |
| Schedule kinds | cron + interval + clocked | same | **cron + interval + one_shot + solar** |
| Real CRUD UI | no | django-admin | **yes (z4j-brain dashboard)** |
| Migration tools provided | n/a | n/a | **yes** (4 importers + reverse export + 24h shadow comparator) |
| Native notification integration | no | no | **yes** (in-app + email + Slack + Telegram + webhook) |

The trade-off: z4j-scheduler adds ~10-30ms per fire vs celery-beat's
direct broker write because it goes through one extra hop
(scheduler → brain → agent → engine). For minute-resolution and
coarser schedules this is invisible. For sub-second workloads, use
the engine's native scheduler.

---

## Measured numbers (v1.1)

Benchmark harness:
[`tests/benchmarks/bench_phase5.py`](https://github.com/z4jdev/z4j-scheduler/blob/main/tests/benchmarks/bench_phase5.py)
+ [`bench_celery_beat_compare.py`](https://github.com/z4jdev/z4j-scheduler/blob/main/tests/benchmarks/bench_celery_beat_compare.py).
Numbers below are from a single Windows laptop with
WSL-2-backed Docker; production hardware typically beats these.

### Against the [SCHEDULER.md §23](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md#23-performance-targets) targets

| Metric | Target | Measured | Margin |
|---|---|---|---|
| Memory at idle | < 80 MB | **39.7 MB** | 2.0× headroom |
| Memory @ 10k schedules | < 300 MB | **52.6 MB** (~634 B / schedule) | 5.7× headroom |
| Memory @ 100k schedules | (n/a — beyond §23) | **93.6 MB** | well in budget |
| Memory @ 1M schedules | (n/a) | **495.6 MB** | fits in a 1 GB container |
| Startup time | < 2 s | **102.7 ms** | 19.5× headroom |
| Tick accuracy p50 | ± 100 ms | **5.4 ms** | 18× headroom |
| Tick accuracy p99 | ± 500 ms | **160.2 ms** | 3.1× headroom |
| Fires per second | 100/s sustained | **120/s** | ✅ |
| HA failover time | < 10 s | **130 ms** | 76× headroom |

### Head-to-head vs celery-beat

Same workload (5 typical cron expressions + scaling tests)
measured against `celery.schedules.crontab` running in-process.

| Workload | z4j-scheduler | celery-beat | Verdict |
|---|---|---|---|
| Single next-fire compute (p50, every-minute) | 41 µs | 11 µs | celery 3.6× faster |
| Single next-fire compute (p99, every-5-min) | 147 µs | 18 µs | celery 8× faster |
| Per-tick due-list scan @ 100 schedules | 1.4 ms | 2.1 ms | **z4j 1.5× faster** |
| Per-tick due-list scan @ 10k schedules | 131 ms | 200 ms | **z4j 1.5× faster** |
| RSS at 10k schedules in memory | **6 MB** (634 B/sched) | 60 MB (6377 B/sched) | **z4j 10× lighter** |

Honest mixed result: celery's hand-tuned `crontab` class beats
croniter on the per-call next-fire computation, but
z4j-scheduler's per-tick + memory profile is strictly better at
operational scale. Replacing celery-beat trades ~30 µs of
per-fire compute for ~10× less RAM and ~33% faster ticks.

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

Or via env vars — see
[docs/SCHEDULER.md §20](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md#20-configuration-reference---every-env-var)
for the full configuration reference.

For the **single-container homelab deploy**, set
`Z4J_EMBEDDED_SCHEDULER=true` on the brain image and skip running
a separate scheduler process — brain spawns z4j-scheduler as a
supervised subprocess with auto-minted loopback mTLS PKI.

For **Kubernetes**, use the Helm chart at
[`deploy/helm/z4j-scheduler/`](https://github.com/z4jdev/z4j/tree/main/deploy/helm/z4j-scheduler)
or the plain manifest at
[`deploy/kubernetes/z4j-scheduler.yaml`](https://github.com/z4jdev/z4j/tree/main/deploy/kubernetes/z4j-scheduler.yaml).

## Requirements

- z4j-brain v1.1+ with the SchedulerService gRPC endpoint enabled
- Postgres 17+ (shared with brain) — required only for HA via
  advisory locks; single-instance deployments work without it
- Python 3.13+

---

## Migration from celery-beat

The full operator playbook is at
[docs/MIGRATION_FROM_CELERY_BEAT.md](https://github.com/z4jdev/z4j/blob/main/docs/MIGRATION_FROM_CELERY_BEAT.md).
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

Targets: `celery` / `rq` / `apscheduler` / `cron`.

### Importers supported

- `--from celery` — celery app's static `app.conf.beat_schedule`
- `--from django-celery-beat` — `PeriodicTask` ORM table
- `--from rq` — rq-scheduler Redis sorted set
- `--from apscheduler` — APScheduler jobstores (3.x + 4.x)
- `--from cron` — `/etc/crontab` and friends (system cron files)

All five include `--dry-run` (print JSONL) and `--verify` (diff
against brain). Solar schedules (rare in celery-beat) round-trip
through the importer + exporter.

---

## Schedule kinds (v1.1)

- **cron** — any standard 5-field crontab string
- **interval** — `30s` / `5m` / `2h` / `1d` (or bare integer seconds)
- **one_shot** — fire once at an ISO-8601 timestamp
- **solar** — `event:lat:lon` for sunrise / sunset / dawn / dusk /
  noon / solar_noon / midnight / solar_midnight at a given location.
  Backed by [`astral`](https://pypi.org/project/astral/); polar
  perpetual-day windows return no fire.

Per-schedule **catch-up policy** (skip / fire_one_missed /
fire_all_missed) honored at recovery time.

---

## Project status

Engineering scope of v1 GA is closed. See
[docs/SCHEDULER.md §29](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md#29-success-criteria-for-v1-ga)
for the criteria status — every architectural deliverable is
shipped; the four "in the wild" criteria (5+ external production
deployments, 90-day soak, public benchmark write-up, first paying
inquiry) are calendar-bound on adoption.

If you want zero risk, wait for those references. If you want to
be one of the early production users, the migration tools and
rollback plan are built — you can be running it in production by
end of week.

Open an issue at
[github.com/z4jdev/z4j-scheduler](https://github.com/z4jdev/z4j-scheduler/issues)
with your migration story.

---

## See also

- [`docs/SCHEDULER.md`](https://github.com/z4jdev/z4j/blob/main/docs/SCHEDULER.md) — full specification
- [`docs/MIGRATION_FROM_CELERY_BEAT.md`](https://github.com/z4jdev/z4j/blob/main/docs/MIGRATION_FROM_CELERY_BEAT.md) — operator cutover playbook
- [`packages/z4j-celerybeat/`](https://github.com/z4jdev/z4j-celerybeat) — the *adapter* for an existing celery-beat process (coexistence path; z4j-scheduler is the *replacement* path)
- [`packages/z4j-brain/`](https://github.com/z4jdev/z4j-brain) — the brain this scheduler integrates with
- [`packages/z4j-core/`](https://github.com/z4jdev/z4j-core) — the shared models and protocols
