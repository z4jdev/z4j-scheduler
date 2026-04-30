# z4j-scheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-scheduler.svg)](https://pypi.org/project/z4j-scheduler/)
[![License](https://img.shields.io/pypi/l/z4j-scheduler.svg)](https://github.com/z4jdev/z4j-scheduler/blob/main/LICENSE)

Engine-agnostic dynamic scheduler for [z4j](https://z4j.com).

A standalone scheduler companion process that fires schedules
into any z4j-supported queue engine (Celery, RQ, Dramatiq,
Huey, arq, TaskIQ). Use it as the canonical scheduler when you
want one place to manage cron / interval / one-shot / solar
schedules across multiple engines.

## Install

```bash
pip install z4j-scheduler
z4j-scheduler serve
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
