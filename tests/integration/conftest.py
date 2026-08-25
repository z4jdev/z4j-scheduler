"""Fail-closed service fixtures for the scheduler PostgreSQL E2E tests."""

from __future__ import annotations

import contextlib
import os
import stat
from collections.abc import Generator, Iterator
from pathlib import Path

import pytest

_INTEGRATION_ROOT = Path(__file__).resolve().parent
_REQUIRED_INTEGRATION = os.environ.get("Z4J_REQUIRE_INTEGRATION") == "1"
_REQUIRED_POSTGRES_MODULE_NAMES = frozenset(
    {
        "test_leader_postgres_e2e.py",
        "test_watch_listen_e2e.py",
    }
)
_REQUIRED_POSTGRES_MODULES = frozenset(
    (_INTEGRATION_ROOT / name).resolve() for name in _REQUIRED_POSTGRES_MODULE_NAMES
)


def _is_required_postgres_item(item: pytest.Item) -> bool:
    return item.path.resolve() in _REQUIRED_POSTGRES_MODULES


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark the two real-PostgreSQL modules before ``-m`` deselection."""
    for item in items:
        if _is_required_postgres_item(item):
            item.add_marker(pytest.mark.integration)


def pytest_collection_finish(session: pytest.Session) -> None:
    """A required run must collect at least one test from both PG modules."""
    if not _REQUIRED_INTEGRATION:
        return
    collected_modules = {
        item.path.resolve() for item in session.items if _is_required_postgres_item(item)
    }
    missing_modules = sorted(path.name for path in _REQUIRED_POSTGRES_MODULES - collected_modules)
    if missing_modules:
        pytest.exit(
            "required scheduler PostgreSQL integration suite selected no tests from: "
            + ", ".join(missing_modules),
            returncode=4,
        )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[object],
) -> Generator[None, object, None]:
    """Turn every skip in the required PG modules into a test failure."""
    del call
    outcome = yield
    report = outcome.get_result()  # type: ignore[attr-defined]
    if not _REQUIRED_INTEGRATION or not _is_required_postgres_item(item) or not report.skipped:
        return
    original_reason = str(report.longrepr)
    report.outcome = "failed"
    report.longrepr = (
        str(item.path),
        item.location[1] + 1,
        "required scheduler PostgreSQL integration test skipped during "
        f"{report.when}: {original_reason}",
    )


def _normalise_postgres_url(raw: str) -> str:
    for scheme in (
        "postgresql+psycopg2://",
        "postgresql+psycopg://",
        "postgresql+asyncpg://",
    ):
        if raw.startswith(scheme):
            return raw.replace(scheme, "postgresql://", 1)
    return raw


def _postgres_container_class():
    reason = (
        "testcontainers is not installed; run `uv sync --all-extras` or set Z4J_TEST_POSTGRES_URL"
    )
    try:
        try:  # testcontainers moved this module and deprecates the old path
            from testcontainers.community.postgres import PostgresContainer
        except ImportError:
            from testcontainers.postgres import PostgresContainer
    except ModuleNotFoundError as exc:
        if exc.name not in {
            "testcontainers",
            "testcontainers.community",
            "testcontainers.community.postgres",
            "testcontainers.postgres",
        }:
            raise
        if _REQUIRED_INTEGRATION:
            raise pytest.UsageError(
                f"required scheduler PostgreSQL integration suite unavailable: {reason}"
            ) from exc
        pytest.skip(reason)
    return PostgresContainer


def _unavailable_local_docker_socket() -> str | None:
    """Explain a missing local Docker transport without opening docker-py.

    docker-py can leave its Unix socket for cyclic garbage collection when a
    connection attempt fails before a client is fully constructed. Pytest's
    strict unraisable-warning gate then reports that third-party socket leak
    against an unrelated later test. A local Unix transport is cheap to
    validate without constructing the client; non-Unix transports remain the
    container library's responsibility.
    """
    docker_host = os.environ.get("DOCKER_HOST")
    if not docker_host:
        socket_path = Path("/var/run/docker.sock")
    elif docker_host.startswith("unix://"):
        socket_path = Path(docker_host.removeprefix("unix://"))
    else:
        return None

    try:
        mode = socket_path.stat().st_mode
    except OSError as exc:
        return f"Docker Unix socket {socket_path} is unavailable: {exc}"
    if not stat.S_ISSOCK(mode):
        return f"Docker Unix transport {socket_path} is not a socket"
    return None


@pytest.fixture(scope="module")
def postgres_url() -> Iterator[str]:
    """Yield a bare asyncpg URL from the shared service or testcontainers."""
    shared_url = os.environ.get("Z4J_TEST_POSTGRES_URL")
    if shared_url:
        yield _normalise_postgres_url(shared_url)
        return

    postgres_container_class = _postgres_container_class()
    unavailable = _unavailable_local_docker_socket()
    if unavailable is not None:
        if _REQUIRED_INTEGRATION:
            pytest.fail(
                f"required scheduler PostgreSQL integration service unavailable: {unavailable}"
            )
        pytest.skip(f"could not start Postgres container: {unavailable}")

    try:
        container = postgres_container_class(
            "postgres:18.6@sha256:06cad38a5d9f5d24b4d83d86def30795d5e4b757fedbf5281172b576dedcd941"
        )
        container.start()
    except Exception as exc:
        if _REQUIRED_INTEGRATION:
            pytest.fail(f"required scheduler PostgreSQL integration service unavailable: {exc}")
        pytest.skip(f"could not start Postgres container: {exc}")
    try:
        yield _normalise_postgres_url(container.get_connection_url())
    finally:
        with contextlib.suppress(Exception):
            container.stop()
