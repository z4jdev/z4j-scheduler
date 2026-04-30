# Changelog

All notable changes to this package are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2026-05-15

**Aligned with the z4j 1.3.0 ecosystem reset.**

z4j 1.3.0 is a clean-slate reset of the 1.x ecosystem. All prior
1.x versions on PyPI (1.0.x, 1.1.x, 1.2.x) are yanked — they
remain installable by exact pin but `pip install` no longer
selects them.

### Changed

- Bumped to 1.3.0 to match the rest of the z4j wave.
- `z4j-core` and `z4j-bare` dependency floors bumped to `>=1.3.0,<2`.

### Compatibility

- Compatible with `z4j-core>=1.3.0,<2` and `z4j-bare>=1.3.0,<2`.
- The engine integration surface (Celery / RQ / arq / dramatiq /
  huey / taskiq / apscheduler / scheduler) is unchanged from
  1.2.x — adapter callers don't need code changes.

### See also

- `CHANGELOG-1.x-legacy.md` for the 1.0/1.1/1.2 release history.

## [Unreleased]
