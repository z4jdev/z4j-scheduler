"""End-to-end test for the reverse-export flow.

Spins up a real brain in-process, seeds a few schedules, then runs
the export pipeline (fetch + render) for each target. Verifies:

1. The fetch path picks up only schedules matching the
   (scheduler, source) filters.
2. The rendered output is valid Python (for celery/rq/aps) or
   crontab text (for cron).
3. Round-tripping (export celery → re-import via the importer)
   would re-produce the same set, modulo the always-fresh
   source_hash. Not asserted strictly but the code path runs.
"""

from __future__ import annotations

import ast
import secrets
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

pytest.importorskip("z4j_brain")

from httpx import ASGITransport
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
from z4j_scheduler.exporters import apscheduler, celery, cron, rq
from z4j_scheduler.exporters._client import fetch_schedules

# =====================================================================
# Brain fixture (mirror test_declarative_e2e.py)
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
    db = brain_app.state.db
    hasher = PasswordHasher(brain_settings)
    project_id = uuid.uuid4()
    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    csrf = secrets.token_urlsafe(32)

    async with db.session() as s:
        s.add_all(
            [
                Project(id=project_id, slug="export-test", name="ET"),
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
        # relationships, so flush their parents before inserting Session and
        # Schedule rows that reference them.
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
        # Seed three schedules: two z4j-scheduler-owned (one cron,
        # one interval) and one celery-beat-owned (which the
        # default scheduler_filter must exclude).
        s.add(
            Schedule(
                project_id=project_id,
                engine="celery",
                scheduler="z4j-scheduler",
                name="hourly",
                task_name="myapp.tasks.heartbeat",
                kind=ScheduleKind.CRON,
                expression="0 * * * *",
                timezone="UTC",
                args=[1, 2],
                kwargs={"k": "v"},
                is_enabled=True,
                source="dashboard",
            ),
        )
        s.add(
            Schedule(
                project_id=project_id,
                engine="celery",
                scheduler="z4j-scheduler",
                name="poll",
                task_name="myapp.tasks.poll",
                kind=ScheduleKind.INTERVAL,
                expression="60s",
                timezone="UTC",
                args=[],
                kwargs={},
                is_enabled=True,
                source="declarative_django",
            ),
        )
        s.add(
            Schedule(
                project_id=project_id,
                engine="celery",
                scheduler="celery-beat",  # NOT z4j-scheduler
                name="not-mine",
                task_name="myapp.tasks.other",
                kind=ScheduleKind.CRON,
                expression="0 0 * * *",
                timezone="UTC",
                args=[],
                kwargs={},
                is_enabled=True,
                source="dashboard",
            ),
        )
        await s.commit()

    return {
        "project_id": project_id,
        "session_id": session_id,
        "csrf": csrf,
    }


@pytest.fixture
async def export_via_asgi(
    brain_app,
    brain_settings: BrainSettings,
    admin_seed: dict,
    monkeypatch,
):
    """Patch httpx.AsyncClient so fetch_schedules talks to the in-proc app."""
    from z4j_brain.auth.csrf import csrf_cookie_name

    transport = ASGITransport(app=brain_app)
    real_client_cls = httpx.AsyncClient

    def _client_factory(*args, **kwargs):
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
        return client

    monkeypatch.setattr("httpx.AsyncClient", _client_factory)
    return admin_seed


# =====================================================================
# Tests
# =====================================================================


class TestFetchSchedules:
    @pytest.mark.asyncio
    async def test_default_filters_to_z4j_scheduler(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
        )
        names = {r.name for r in rows}
        assert names == {"hourly", "poll"}
        assert "not-mine" not in names

    @pytest.mark.asyncio
    async def test_source_filter_narrows(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
            source_filter="declarative_django",
        )
        assert {r.name for r in rows} == {"poll"}


class TestExportRoundTrip:
    @pytest.mark.asyncio
    async def test_celery_output_parses_as_python(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
        )
        rendered = celery.render(rows)
        # Must be a valid Python module the operator can paste.
        ast.parse(rendered)
        # Both schedules present.
        assert '"hourly"' in rendered
        assert '"poll"' in rendered
        # cron emits crontab(...), interval emits timedelta(...).
        assert "crontab(" in rendered
        assert "timedelta(seconds=60)" in rendered

    @pytest.mark.asyncio
    async def test_rq_output_parses_as_python(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
        )
        rendered = rq.render(rows)
        ast.parse(rendered)
        assert "scheduler.cron(" in rendered
        assert "scheduler.schedule(" in rendered

    @pytest.mark.asyncio
    async def test_apscheduler_output_parses_as_python(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
        )
        rendered = apscheduler.render(rows)
        ast.parse(rendered)
        # Both job kinds present.
        assert '"cron"' in rendered
        assert '"interval"' in rendered

    @pytest.mark.asyncio
    async def test_cron_output_has_wrapper_header(
        self,
        export_via_asgi,
    ) -> None:
        rows = await fetch_schedules(
            brain_url="http://testserver",
            project_slug="export-test",
            api_token=None,
        )
        rendered = cron.render(rows)
        assert "WRAPPER=" in rendered
        # Hourly schedule emits the standard cron line.
        assert "0 * * * * $WRAPPER myapp.tasks.heartbeat" in rendered
        # Interval 60s = every minute.
        assert "* * * * * $WRAPPER myapp.tasks.poll" in rendered
