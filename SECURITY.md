# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in
`z4j-scheduler`, **do not open a public GitHub issue**. Email
`security@z4j.com` instead.

We follow the [disclose.io](https://disclose.io) baseline:

- Initial acknowledgement within **72 hours**.
- Coordinated disclosure timeline agreed before public release.
- Credit in the release notes (unless you prefer to remain anonymous).

PGP key and the full disclosure policy live in the
[z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Supported versions

Only the latest minor release receives security fixes. See
[CHANGELOG.md](CHANGELOG.md) for the current version.

## Security-critical surface

`z4j-scheduler` is a standalone service that decides when work
fires, so its surface is operational rather than user-facing:

- **Brain API credentials**: the scheduler authenticates to the
  brain with an API token (`Z4J_SCHEDULER_BRAIN_API_TOKEN`). The
  token grants schedule read/fire capability; treat it like any
  other service credential and rotate it via the brain.
- **Trigger gRPC endpoint**: when enabled, the on-demand trigger
  channel requires mutual TLS (`require_client_auth`) plus a
  certificate common-name allowlist; there is no plaintext or
  anonymous mode.
- **Leader election**: HA deployments coordinate through Postgres
  advisory locks. Anyone with write access to that database can
  influence leadership, so the scheduler's database role should be
  scoped to its own database/schema.
- **Importers**: `z4j-scheduler import` reads schedule definitions
  from operator-supplied sources (Celery apps, RQ, APScheduler,
  django-celery-beat) and pushes them to the brain. Importing means
  executing the source project's import path, so run importers only
  against code you already trust and review with `--dry-run` first.

A vulnerability in any of the above is treated as release-blocking.
Issues in the brain's own authentication, RBAC, or audit trail
belong to the `z4j` package's policy.
