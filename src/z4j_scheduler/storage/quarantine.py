"""Independent retry queue for durable Boundary D quarantine reports."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from z4j_scheduler.storage._protocol import CURRENT_PROTOCOL_EPOCH

logger = logging.getLogger("z4j.scheduler.quarantine")

if TYPE_CHECKING:
    from uuid import UUID

    from z4j_scheduler.storage.brain_client import BrainClient
    from z4j_scheduler.storage.cache import LocalQuarantine, ScheduleCache
    from z4j_scheduler.tick._entry import ScheduleEntry


@dataclass(frozen=True, slots=True)
class PendingQuarantineReport:
    schedule_id: UUID
    project_id: UUID
    control_token: UUID
    reason_code: str
    detail: str
    minimum_observed_revision: int


class QuarantineReporter:
    """Coalesce reports and refresh stale/not-found outcomes safely."""

    def __init__(
        self,
        *,
        client: BrainClient,
        cache: ScheduleCache,
        retry_interval_seconds: float = 1.0,
    ) -> None:
        if retry_interval_seconds <= 0:
            raise ValueError("quarantine retry interval must be positive")
        self._client = client
        self._cache = cache
        self._pending: dict[tuple[UUID, UUID], PendingQuarantineReport] = {}
        self._retry_interval_seconds = retry_interval_seconds
        self._stop_event = asyncio.Event()

    def enqueue(
        self,
        *,
        entry: ScheduleEntry,
        quarantine: LocalQuarantine,
    ) -> bool:
        """Queue one current generation; duplicate `(id, token)` is a no-op."""

        if entry.control_token is None or entry.schedule_revision <= 0:
            return False
        key = (entry.id, entry.control_token)
        self._pending.setdefault(
            key,
            PendingQuarantineReport(
                schedule_id=entry.id,
                project_id=entry.project_id,
                control_token=entry.control_token,
                reason_code=quarantine.code,
                detail=quarantine.detail,
                minimum_observed_revision=entry.schedule_revision,
            ),
        )
        return True

    def __len__(self) -> int:
        return len(self._pending)

    async def run(self) -> None:
        """Retry pending reports independently of cadence dispatch."""

        while not self._stop_event.is_set():
            if self._pending:
                await self.flush_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._retry_interval_seconds,
                )
            except TimeoutError:
                continue

    async def stop(self) -> None:
        """Wake and stop the independent retry loop."""

        self._stop_event.set()

    async def flush_once(self) -> None:
        """Try each pending generation once; transport failures remain queued."""

        for key, report in list(self._pending.items()):
            try:
                result = await self._client.quarantine_schedule(
                    project_id=report.project_id,
                    schedule_id=report.schedule_id,
                    observed_control_token=report.control_token,
                    reason_code=report.reason_code,
                    detail=report.detail,
                    scheduler_protocol_epoch=CURRENT_PROTOCOL_EPOCH,
                )
                if result.outcome in {"applied", "already_applied"}:
                    # The local latch remains until Watch installs the committed
                    # disabled row. Only report delivery is complete.
                    self._pending.pop(key, None)
                    continue
                if result.outcome == "not_found":
                    await self._cache.apply_observed_absence(
                        schedule_id=report.schedule_id,
                        project_id=report.project_id,
                        observed_revision=result.observed_revision,
                    )
                    self._pending.pop(key, None)
                    continue

                observation = await self._client.get_schedule_state(
                    project_id=report.project_id,
                    schedule_id=report.schedule_id,
                    minimum_observed_revision=max(
                        report.minimum_observed_revision,
                        result.observed_revision,
                    ),
                )
                if observation.schedule is None:
                    await self._cache.apply_observed_absence(
                        schedule_id=observation.schedule_id,
                        project_id=observation.project_id,
                        observed_revision=observation.observed_revision,
                    )
                else:
                    await self._cache.apply_watch_update(observation.schedule)
                self._pending.pop(key, None)
            except Exception:
                # Independent best-effort retry: a locally broken definition
                # stays latched even while Brain or the refresh path is down.
                logger.warning(
                    "quarantine report/refresh failed for schedule_id=%s; retaining local latch",
                    report.schedule_id,
                    exc_info=True,
                )
                continue


__all__ = ["PendingQuarantineReport", "QuarantineReporter"]
