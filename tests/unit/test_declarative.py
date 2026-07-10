"""Unit tests for the declarative reconciler.

The HTTP path is exercised against a fake httpx.AsyncClient. The
real round-trip to a brain (and the replace-for-source delete
semantics) is covered by the brain-side test_schedules_crud.py.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from z4j_scheduler.declarative import ScheduleSpec, reconcile, reconcile_sync
from z4j_scheduler.declarative._reconciler import _to_imported

# =====================================================================
# ScheduleSpec → ImportedSchedule conversion
# =====================================================================


class TestToImported:
    def test_required_fields_round_trip(self) -> None:
        spec = ScheduleSpec(
            name="hourly",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="myapp.tasks.heartbeat",
        )
        imp = _to_imported(spec, project="acme", source="declarative_django")
        assert imp.name == "hourly"
        assert imp.project_slug == "acme"
        assert imp.source == "declarative_django"
        assert imp.timezone == "UTC"
        assert imp.catch_up == "skip"
        assert imp.is_enabled is True

    def test_optional_overrides_carry_through(self) -> None:
        spec = ScheduleSpec(
            name="poll",
            engine="celery",
            kind="interval",
            expression="60s",
            task_name="t",
            timezone="America/New_York",
            queue="orders",
            args=[1, 2, 3],
            kwargs={"k": "v"},
            catch_up="fire_one_missed",
            is_enabled=False,
        )
        imp = _to_imported(spec, project="acme", source="src")
        assert imp.timezone == "America/New_York"
        assert imp.queue == "orders"
        assert imp.args == [1, 2, 3]
        assert imp.kwargs == {"k": "v"}
        assert imp.catch_up == "fire_one_missed"
        assert imp.is_enabled is False


# =====================================================================
# Dict-form input
# =====================================================================


class TestDictInput:
    @pytest.mark.asyncio
    async def test_dict_with_matching_key_works(self) -> None:
        # Verify the dict-form is accepted when key matches name.
        # Use a fake httpx so we don't actually hit the network.
        spec = ScheduleSpec(
            name="hourly",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with _fake_httpx_returning(
            {"inserted": 1, "updated": 0, "unchanged": 0, "failed": 0, "deleted": 0},
        ) as recorded:
            await reconcile(
                schedules={"hourly": spec},
                project="acme",
                source="src",
                brain_url="http://brain",
            )
        assert recorded["calls"] == 1

    @pytest.mark.asyncio
    async def test_dict_with_mismatched_key_rejected(self) -> None:
        spec = ScheduleSpec(
            name="actual-name",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with pytest.raises(ValueError, match="dict key"):
            await reconcile(
                schedules={"different-key": spec},
                project="acme",
                source="src",
                brain_url="http://brain",
            )


# =====================================================================
# Reconcile sends correct wire format
# =====================================================================


class TestWireFormat:
    @pytest.mark.asyncio
    async def test_sends_replace_for_source_mode(self) -> None:
        spec = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with _fake_httpx_returning(
            {"inserted": 1, "updated": 0, "unchanged": 0, "failed": 0, "deleted": 0},
        ) as recorded:
            await reconcile(
                schedules=[spec],
                project="acme",
                source="declarative_django",
                brain_url="http://brain",
                api_token="bearer-tkn",
            )
        # Verify the payload's mode + source_filter.
        body = recorded["last_body"]
        assert body["mode"] == "replace_for_source"
        assert body["source_filter"] == "declarative_django"
        assert len(body["schedules"]) == 1
        # Bearer token forwarded to Authorization header.
        headers = recorded["last_headers"]
        assert headers["Authorization"] == "Bearer bearer-tkn"

    @pytest.mark.asyncio
    async def test_url_targets_import_endpoint(self) -> None:
        spec = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with _fake_httpx_returning(
            {"inserted": 1, "updated": 0, "unchanged": 0, "failed": 0, "deleted": 0},
        ) as recorded:
            await reconcile(
                schedules=[spec],
                project="acme",
                source="src",
                brain_url="http://brain.example.com:7700",
            )
        assert recorded["last_url"] == (
            "http://brain.example.com:7700/api/v1/projects/acme/schedules:import"
        )

    @pytest.mark.asyncio
    async def test_404_response_raises_clear_error(self) -> None:
        spec = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with (
            _fake_httpx_returning(
                response_status=404,
                response_json={},
            ),
            pytest.raises(RuntimeError, match="schedules:import"),
        ):
            await reconcile(
                schedules=[spec],
                project="acme",
                source="src",
                brain_url="http://brain",
            )


# =====================================================================
# Idempotency hash stability
# =====================================================================


class TestHashStability:
    def test_same_spec_same_hash(self) -> None:
        a = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        b = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        ia = _to_imported(a, project="p", source="s")
        ib = _to_imported(b, project="p", source="s")
        # Source hash matches → re-reconcile is a no-op.
        assert ia.compute_hash() == ib.compute_hash()

    def test_changing_expression_changes_hash(self) -> None:
        a = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        b = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="*/15 * * * *",
            task_name="t",
        )
        ia = _to_imported(a, project="p", source="s")
        ib = _to_imported(b, project="p", source="s")
        assert ia.compute_hash() != ib.compute_hash()


# =====================================================================
# reconcile_sync
# =====================================================================


class TestReconcileSync:
    def test_runs_event_loop(self) -> None:
        spec = ScheduleSpec(
            name="x",
            engine="celery",
            kind="cron",
            expression="0 * * * *",
            task_name="t",
        )
        with _fake_httpx_returning(
            {"inserted": 1, "updated": 0, "unchanged": 0, "failed": 0, "deleted": 0},
        ):
            summary = reconcile_sync(
                schedules=[spec],
                project="acme",
                source="src",
                brain_url="http://brain",
            )
        assert summary["inserted"] == 1


# =====================================================================
# Helpers
# =====================================================================


class _FakeResponse:
    def __init__(self, *, status: int, body: dict) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> dict:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 404:
            raise RuntimeError(f"http {self.status_code}")


def _fake_httpx_returning(
    response_json: dict | None = None,
    *,
    response_status: int = 200,
):
    """Patch httpx.AsyncClient with a recording fake.

    Returns a context manager whose ``__enter__`` value is a dict
    we accumulate call metadata into so tests can assert on it.
    """
    body = response_json or {}
    recorded: dict = {"calls": 0, "last_url": None, "last_body": None, "last_headers": None}

    class _Client:
        def __init__(self, *_args, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, json=None, headers=None):
            recorded["calls"] += 1
            recorded["last_url"] = url
            recorded["last_body"] = json
            recorded["last_headers"] = headers or {}
            return _FakeResponse(status=response_status, body=body)

    class _CM:
        def __enter__(self):
            self._patcher = patch("httpx.AsyncClient", _Client)
            self._patcher.start()
            return recorded

        def __exit__(self, *args):
            self._patcher.stop()

    return _CM()
