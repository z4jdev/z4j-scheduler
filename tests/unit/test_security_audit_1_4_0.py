"""Regression tests for the 1.4.0 security audit fixes (scheduler side).

Brain-side counterparts live in z4j-brain's
``test_security_audit_1_4_0.py``.

- S002: TriggerSchedule idempotency cache
- S004: trigger_grpc_require_allowlist startup-fail opt-in (symmetric to brain)

See ``RELEASE-1.4.0-SECURITY-AUDIT.md`` for the original audit
narrative and ``RELEASE-1.4.0-PLAN.md §4.7`` for the policy.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

# =====================================================================
# S002 -- TriggerSchedule idempotency cache
# =====================================================================


class TestS002IdempotencyCache:
    """Audit fix S002: ``TriggerScheduleServicer`` honors
    ``request.idempotency_key`` for dedup.

    Pre-fix the servicer logged the key but always called
    ``trigger_now`` which generated a fresh ``uuid4`` per call. Brain's
    ``TriggerScheduleClient`` retries exactly once on UNAVAILABLE /
    DEADLINE_EXCEEDED -- the case where the first call succeeded but
    the response was lost -- so a transient gRPC error caused a
    duplicate fire. For idempotent tasks: harmless. For non-idempotent
    (charge customer, send email, fire webhook): real bug.
    """

    @pytest.mark.asyncio
    async def test_repeated_call_with_same_key_returns_cached_command_id(
        self,
    ) -> None:
        from z4j_scheduler.leader import SingleInstanceLeaderGate
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage._models import FireResult
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            TriggerScheduleServicer,
        )

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        # Track invocation count + return distinct command ids on
        # each call so we can prove the cache was hit (only one
        # invocation; response for second call uses the FIRST
        # command_id, not a fresh one).
        call_count = 0
        first_cmd_id = uuid.uuid4()
        second_cmd_id = uuid.uuid4()

        dispatcher = MagicMock()

        async def _trigger_now(*, schedule_id, **_kwargs):
            nonlocal call_count
            call_count += 1
            cmd = first_cmd_id if call_count == 1 else second_cmd_id
            return FireResult(
                command_id=cmd,
                error_code=None,
                error_message=None,
                buffered=False,
            )

        dispatcher.trigger_now = _trigger_now

        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=SingleInstanceLeaderGate(),
        )

        request = pb.TriggerScheduleRequest(
            schedule_id=str(sid),
            user_id="",
            idempotency_key="op-123:click-456",
        )

        first = await servicer.TriggerSchedule(request, MagicMock())
        second = await servicer.TriggerSchedule(request, MagicMock())

        assert first.command_id == str(first_cmd_id)
        assert second.command_id == str(first_cmd_id), (
            "second call with same idempotency_key must return the "
            "FIRST call's command_id, not a fresh fire"
        )
        assert call_count == 1, (
            "trigger_now must be invoked exactly once across two "
            "requests with the same idempotency_key"
        )

    @pytest.mark.asyncio
    async def test_empty_idempotency_key_does_not_dedup(self) -> None:
        """Legacy callers with empty key bypass the cache.

        Empty key = "no idempotency requested". Two calls fire two
        distinct commands, preserving the pre-1.4.0 behavior for
        any client that hasn't been updated to send a key.
        """
        from z4j_scheduler.leader import SingleInstanceLeaderGate
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage._models import FireResult
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            TriggerScheduleServicer,
        )

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        call_count = 0
        dispatcher = MagicMock()

        async def _trigger_now(*, schedule_id, **_kwargs):
            nonlocal call_count
            call_count += 1
            return FireResult(
                command_id=uuid.uuid4(),
                error_code=None,
                error_message=None,
                buffered=False,
            )

        dispatcher.trigger_now = _trigger_now

        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=SingleInstanceLeaderGate(),
        )
        request = pb.TriggerScheduleRequest(
            schedule_id=str(sid),
            user_id="",
            idempotency_key="",
        )
        await servicer.TriggerSchedule(request, MagicMock())
        await servicer.TriggerSchedule(request, MagicMock())
        assert call_count == 2, (
            "empty idempotency_key must NOT dedup -- legacy clients "
            "preserve pre-1.4.0 fire-every-time behavior"
        )

    @pytest.mark.asyncio
    async def test_distinct_keys_do_not_collide(self) -> None:
        from z4j_scheduler.leader import SingleInstanceLeaderGate
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage._models import FireResult
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            TriggerScheduleServicer,
        )

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        call_count = 0
        dispatcher = MagicMock()

        async def _trigger_now(*, schedule_id, **_kwargs):
            nonlocal call_count
            call_count += 1
            return FireResult(
                command_id=uuid.uuid4(),
                error_code=None,
                error_message=None,
                buffered=False,
            )

        dispatcher.trigger_now = _trigger_now

        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=SingleInstanceLeaderGate(),
        )
        await servicer.TriggerSchedule(
            pb.TriggerScheduleRequest(
                schedule_id=str(sid),
                user_id="",
                idempotency_key="A",
            ),
            MagicMock(),
        )
        await servicer.TriggerSchedule(
            pb.TriggerScheduleRequest(
                schedule_id=str(sid),
                user_id="",
                idempotency_key="B",
            ),
            MagicMock(),
        )
        assert call_count == 2, "distinct idempotency_keys must result in distinct fires"

    @pytest.mark.asyncio
    async def test_expired_entries_get_swept(self) -> None:
        """After the TTL elapses, the same key fires fresh.

        We push the cache forward by stuffing an expired entry
        directly, then issue a new call and assert the dispatcher
        is invoked (cache miss, fresh fire).
        """
        from z4j_scheduler.leader import SingleInstanceLeaderGate
        from z4j_scheduler.proto import scheduler_pb2 as pb
        from z4j_scheduler.storage._models import FireResult
        from z4j_scheduler.storage.cache import ScheduleCache
        from z4j_scheduler.tick._entry import ScheduleEntry
        from z4j_scheduler.trigger_grpc.handlers import (
            _IDEM_TTL_SECONDS,
            TriggerScheduleServicer,
        )

        cache = ScheduleCache()
        sid = uuid.uuid4()
        pid = uuid.uuid4()
        await cache.upsert(
            ScheduleEntry(
                id=sid,
                project_id=pid,
                kind="cron",
                expression="0 * * * *",
                timezone="UTC",
                is_enabled=True,
                catch_up="skip",
                anchor_at=datetime(2026, 1, 1, tzinfo=UTC),
                last_fire_at=None,
                name="x",
            ),
        )

        call_count = 0
        dispatcher = MagicMock()

        async def _trigger_now(*, schedule_id, **_kwargs):
            nonlocal call_count
            call_count += 1
            return FireResult(
                command_id=uuid.uuid4(),
                error_code=None,
                error_message=None,
                buffered=False,
            )

        dispatcher.trigger_now = _trigger_now

        servicer = TriggerScheduleServicer(
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=SingleInstanceLeaderGate(),
        )

        # Pre-stuff the cache with an entry that's older than the
        # TTL so the next call sweeps it and fires fresh.
        idem_key = "expired-key"
        cache_key = (str(sid), idem_key)
        servicer._idem_cache[cache_key] = (
            time.monotonic() - _IDEM_TTL_SECONDS - 1.0,
            "stale-command-id",
        )

        await servicer.TriggerSchedule(
            pb.TriggerScheduleRequest(
                schedule_id=str(sid),
                user_id="",
                idempotency_key=idem_key,
            ),
            MagicMock(),
        )
        assert call_count == 1, "expired cache entry must NOT short-circuit the call"


# =====================================================================
# S004 -- scheduler-side trigger_grpc_require_allowlist
# =====================================================================


class TestS004TriggerRequireAllowlist:
    """Audit fix S004 (scheduler side): the trigger gRPC server
    refuses to start when ``trigger_grpc_require_allowlist=true`` AND
    the allow-list is empty.

    Symmetric to the brain-side fix; mirrors the same operator
    fail-closed opt-in but for the brain -> scheduler direction.
    """

    def test_default_is_false(self, tmp_path) -> None:
        from z4j_scheduler.settings import Settings

        cert_path = tmp_path / "client.crt"
        key_path = tmp_path / "client.key"
        ca_path = tmp_path / "ca.crt"
        for p in (cert_path, key_path, ca_path):
            p.write_bytes(b"unused")

        s = Settings(
            brain_grpc_url="brain:7701",
            brain_rest_url="http://brain:7700",
            brain_api_token="x" * 16,
            projects="acme",
            tls_cert=cert_path,
            tls_key=key_path,
            tls_ca=ca_path,
        )
        assert s.trigger_grpc_require_allowlist is False

    @pytest.mark.asyncio
    async def test_start_raises_when_required_and_empty(
        self,
        tmp_path,
    ) -> None:
        from z4j_scheduler.settings import Settings
        from z4j_scheduler.trigger_grpc.server import TriggerGrpcServer

        # Touch fake TLS material so the cert-loader doesn't fail
        # before our require-allowlist guard fires. The strict guard
        # runs BEFORE TLS loading by design (so a misconfigured
        # operator gets the right error instead of a confusing TLS
        # one), so empty content is fine here.
        cert_path = tmp_path / "srv.crt"
        key_path = tmp_path / "srv.key"
        ca_path = tmp_path / "ca.crt"
        for p in (cert_path, key_path, ca_path):
            p.write_bytes(b"unused-strict-guard-fires-first")

        s = Settings(
            brain_grpc_url="brain:7701",
            brain_rest_url="http://brain:7700",
            brain_api_token="x" * 16,
            projects="acme",
            tls_cert=cert_path,
            tls_key=key_path,
            tls_ca=ca_path,
            trigger_grpc_enabled=True,
            trigger_grpc_bind_host="127.0.0.1",
            trigger_grpc_bind_port=0,
            trigger_grpc_tls_cert=cert_path,
            trigger_grpc_tls_key=key_path,
            trigger_grpc_tls_ca=ca_path,
            trigger_grpc_allowed_cns=[],
            trigger_grpc_require_allowlist=True,
        )

        cache = MagicMock()
        dispatcher = MagicMock()
        leader_gate = MagicMock()
        server = TriggerGrpcServer(
            settings=s,
            cache=cache,
            dispatcher=dispatcher,
            leader_gate=leader_gate,
        )
        with pytest.raises(RuntimeError, match="require_allowlist"):
            await server.start()
