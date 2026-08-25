"""End-to-end test for declarative reconciliation.

Spins up a real brain in-process, exercises the full reconcile()
flow including:

1. Initial reconcile → all schedules inserted.
2. Re-reconcile with same dict → unchanged=N (idempotency).
3. Reconcile with one schedule renamed → old deleted + new
   inserted (replace_for_source semantic).
4. Reconcile with empty list → all rows for this source deleted.
5. Other sources untouched (per-source scoping).
"""

from __future__ import annotations

import secrets
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

pytest.importorskip("z4j_brain")

from httpx import ASGITransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool
from z4j_brain.auth.passwords import PasswordHasher
from z4j_brain.auth.sessions import SessionCookieCodec, cookie_name
from z4j_brain.main import create_app
from z4j_brain.persistence import models  # noqa: F401
from z4j_brain.persistence.base import Base
from z4j_brain.persistence.enums import ScheduleKind
from z4j_brain.persistence.models import (
    Project,
    Schedule,
    Session,
    User,
)
from z4j_brain.settings import Settings as BrainSettings
from z4j_scheduler.declarative import ScheduleSpec, reconcile

# =====================================================================
# Brain fixtures
# =====================================================================


@pytest.fixture
def brain_settings() -> BrainSettings:
    return BrainSettings(
        database_url="sqlite+aiosqlite:///:memory:",
        secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        session_secret=secrets.token_urlsafe(48),  # type: ignore[arg-type]
        environment="dev",
        log_json=False,
        argon2_time_cost=1,
        argon2_memory_cost=8192,
        login_min_duration_ms=10,
        registry_backend="local",
        metrics_public=True,
        disable_spa_fallback=True,
    )


@pytest.fixture
async def brain_app(brain_settings: BrainSettings):
    engine = create_async_engine(
        brain_settings.database_url,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    app = create_app(brain_settings, engine=engine)
    yield app
    await engine.dispose()


@pytest.fixture
async def admin_seed(brain_settings: BrainSettings, brain_app):
    """Seed an admin user + project. Returns (project_id, csrf, session_id)."""
    db = brain_app.state.db
    hasher = PasswordHasher(brain_settings)
    project_id = uuid.uuid4()
    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    csrf = secrets.token_urlsafe(32)

    async with db.session() as s:
        s.add_all(
            [
                Project(id=project_id, slug="declarative-test", name="DT"),
                User(
                    id=user_id,
                    email=f"u-{uuid.uuid4().hex[:8]}@example.com",
                    password_hash=hasher.hash("correct horse battery staple 9"),
                    is_admin=True,
                    is_active=True,
                ),
            ],
        )
        # These models carry scalar foreign-key IDs rather than ORM
        # relationships, so SQLAlchemy cannot infer that the User must be
        # inserted before its Session when both are pending in one flush.
        await s.flush()
        s.add(
            Session(
                id=session_id,
                user_id=user_id,
                csrf_token=csrf,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                ip_at_issue="127.0.0.1",
                user_agent_at_issue="test",
            ),
        )
        await s.commit()

    return {
        "project_id": project_id,
        "session_id": session_id,
        "csrf": csrf,
    }


@pytest.fixture
async def reconcile_via_asgi(
    brain_app,
    brain_settings: BrainSettings,
    admin_seed: dict,
    monkeypatch,
):
    """Patch httpx.AsyncClient so reconcile() talks to the in-proc app.

    The reconciler's HTTP path uses ``httpx.AsyncClient(...).post(...)``;
    we point that at an ASGITransport bound to the test brain app
    so the test exercises the real route + real DB without
    standing up uvicorn or a network port.
    """
    from z4j_brain.auth.csrf import csrf_cookie_name

    transport = ASGITransport(app=brain_app)

    real_client_cls = httpx.AsyncClient

    def _client_factory(*args, **kwargs):
        # Inject the ASGI transport + the admin's session cookie so
        # the brain treats the call as authenticated.
        kwargs["transport"] = transport
        kwargs["base_url"] = "http://testserver"
        codec = SessionCookieCodec(brain_settings)
        client = real_client_cls(*args, **kwargs)
        client.cookies.set(
            cookie_name(environment=brain_settings.environment),
            codec.encode(admin_seed["session_id"]),
        )
        client.cookies.set(
            csrf_cookie_name(environment=brain_settings.environment),
            admin_seed["csrf"],
        )
        # Also set the X-CSRF-Token header for double-submit.
        client.headers["X-CSRF-Token"] = admin_seed["csrf"]
        return client

    monkeypatch.setattr("httpx.AsyncClient", _client_factory)
    return admin_seed


# =====================================================================
# Tests
# =====================================================================


def _spec(name: str, *, expression: str = "0 * * * *") -> ScheduleSpec:
    return ScheduleSpec(
        name=name,
        engine="celery",
        kind="cron",
        expression=expression,
        task_name=f"myapp.tasks.{name}",
    )


class TestInitialReconcile:
    @pytest.mark.asyncio
    async def test_first_run_inserts_everything(
        self,
        brain_app,
        reconcile_via_asgi,
    ) -> None:
        summary = await reconcile(
            schedules=[_spec("hourly"), _spec("daily", expression="0 0 * * *")],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        assert summary["inserted"] == 2
        assert summary["updated"] == 0
        assert summary["unchanged"] == 0
        assert summary["deleted"] == 0
        assert summary["failed"] == 0

        async with brain_app.state.db.session() as s:
            rows = (await s.execute(select(Schedule))).scalars().all()
        assert {r.name for r in rows} == {"hourly", "daily"}
        assert all(r.source == "declarative_django" for r in rows)


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_rerun_is_unchanged(
        self,
        reconcile_via_asgi,
    ) -> None:
        specs = [_spec("hourly"), _spec("daily", expression="0 0 * * *")]
        await reconcile(
            schedules=specs,
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        # Run again - same content.
        summary = await reconcile(
            schedules=specs,
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        assert summary["unchanged"] == 2
        assert summary["inserted"] == 0
        assert summary["updated"] == 0
        assert summary["deleted"] == 0


class TestReplaceSemantics:
    @pytest.mark.asyncio
    async def test_renaming_a_schedule_is_delete_plus_insert(
        self,
        brain_app,
        reconcile_via_asgi,
    ) -> None:
        # Initial: hourly + daily.
        await reconcile(
            schedules=[_spec("hourly"), _spec("daily", expression="0 0 * * *")],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        # Rename "daily" → "nightly".
        summary = await reconcile(
            schedules=[
                _spec("hourly"),
                _spec("nightly", expression="0 0 * * *"),
            ],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        assert summary["inserted"] == 1  # nightly
        assert summary["unchanged"] == 1  # hourly
        assert summary["deleted"] == 1  # daily

        async with brain_app.state.db.session() as s:
            rows = (await s.execute(select(Schedule))).scalars().all()
        assert {r.name for r in rows} == {"hourly", "nightly"}

    @pytest.mark.asyncio
    async def test_empty_dict_deletes_all_rows_for_source(
        self,
        brain_app,
        reconcile_via_asgi,
    ) -> None:
        await reconcile(
            schedules=[_spec("hourly"), _spec("daily", expression="0 0 * * *")],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        # Empty-out the source.
        summary = await reconcile(
            schedules=[],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        assert summary["deleted"] == 2

        async with brain_app.state.db.session() as s:
            rows = (await s.execute(select(Schedule))).scalars().all()
        assert rows == []


class TestSourceIsolation:
    @pytest.mark.asyncio
    async def test_other_source_untouched(
        self,
        brain_app,
        admin_seed: dict,
        reconcile_via_asgi,
    ) -> None:
        # Insert a row from a different source directly. Then run
        # the declarative reconciler with an empty dict for our
        # source. The other source's row must NOT be deleted.
        async with brain_app.state.db.session() as s:
            s.add(
                Schedule(
                    project_id=admin_seed["project_id"],
                    engine="celery",
                    scheduler="z4j-scheduler",
                    name="dashboard-managed",
                    task_name="t.t",
                    kind=ScheduleKind.CRON,
                    expression="0 * * * *",
                    timezone="UTC",
                    args=[],
                    kwargs={},
                    is_enabled=True,
                    source="dashboard",
                ),
            )
            await s.commit()

        summary = await reconcile(
            schedules=[],
            project="declarative-test",
            source="declarative_django",
            brain_url="http://testserver",
        )
        # Nothing deleted - the row's source is "dashboard", not
        # the source we asked to replace.
        assert summary["deleted"] == 0

        async with brain_app.state.db.session() as s:
            rows = (await s.execute(select(Schedule))).scalars().all()
        assert {r.name for r in rows} == {"dashboard-managed"}
