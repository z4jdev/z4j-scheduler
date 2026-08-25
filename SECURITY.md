# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in
`z4j-scheduler`, **do not open a public GitHub issue**. Email
`security@z4j.com` instead.

We acknowledge reports within **48 hours**, provide a preliminary assessment
within **5 business days**, and target fixes within **30 days** (**7 days** for
confirmed critical issues). Reporting timelines, safe harbor,
supported-version policy, and published advisories are maintained in the
[canonical z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Security-critical surface

`z4j-scheduler` is a standalone service that decides when work
fires, so its surface is operational rather than user-facing:

- **Scheduler-to-brain fire channel**: production-shaped deployments submit
  prepared fires to the brain over gRPC using a client certificate, private
  key, and CA bundle. Protect that material and use the brain's
  certificate-identity controls; a credential accepted by the brain can speak
  this privileged scheduler protocol. An explicit development-only exception,
  `Z4J_SCHEDULER_INSECURE_GRPC=true` with environment exactly `dev`, opens a
  plaintext channel and must remain on a trusted local network.
- **Brain API credentials**: importer, exporter, and schedule-management CLI
  commands can read a brain API key from
  `Z4J_SCHEDULER_BRAIN_API_TOKEN`. It is an ordinary scoped z4j API key, not
  a scheduler-specific read/fire credential. Grant only the scopes the
  command needs and rotate it via the brain. Outside the explicit insecure
  development mode above, the scheduler service's hot path authenticates over
  gRPC with mutual TLS instead.
- **Trigger gRPC endpoint**: when enabled, the on-demand trigger
  channel requires mutual TLS (`require_client_auth`); there is no plaintext
  or anonymous mode. By default it trusts any client certificate signed by
  the configured CA. Populate `Z4J_SCHEDULER_TRIGGER_GRPC_ALLOWED_CNS` for a
  certificate-name allowlist, and set
  `Z4J_SCHEDULER_TRIGGER_GRPC_REQUIRE_ALLOWLIST=true` to refuse startup when
  that list is empty.
- **Leader election**: HA deployments coordinate through Postgres
  advisory locks. A role does not need table-write permission to contend for
  a known advisory-lock key; any client permitted to connect and invoke the
  advisory-lock functions can influence leadership. Keep database access and
  credentials private rather than treating schema permissions as this
  boundary.
- **Operational HTTP listener**: `/health`, `/ready`, and redacted `/info` are
  operational endpoints. `/metrics` includes project and schedule labels. In
  every environment except the exact value `dev`, startup refuses enabled
  metrics on a non-loopback bind without an auth token; keep the listener on
  loopback, configure the token, or disable metrics.
- **Importers**: `z4j-scheduler import` reads schedule definitions
  from operator-supplied sources and pushes them to the brain. The Celery and
  django-celery-beat paths execute the configured Python import path. The RQ
  and APScheduler paths use their upstream clients to inspect native job
  records; those records can contain pickle-backed payloads, so reading an
  attacker-writable Redis database or APScheduler jobstore can execute code in
  the importer process. The crontab path parses text. Treat source code,
  source stores, and their credentials as trusted. `--dry-run` prevents writes
  to the brain; it does not sandbox or avoid loading the source.

A vulnerability in any of the above is treated as release-blocking.
Issues in the brain's own authentication, RBAC, or audit trail
belong to the `z4j` package's policy.
