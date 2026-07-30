"""Strict application of Boundary D revision-ordered Watch frames."""

from __future__ import annotations

from typing import TYPE_CHECKING

from z4j_scheduler.storage._models import ScannedThrough, ScheduleChange
from z4j_scheduler.storage.cache import ScheduleProtocolError

if TYPE_CHECKING:
    from uuid import UUID

    from z4j_scheduler.storage.cache import ScheduleCache

WATCH_FORMAT_VERSION = 1


class OrderedWatchApplier:
    """Apply frames and advance only after each frame is fully accepted."""

    def __init__(
        self,
        *,
        cache: ScheduleCache,
        project_id: UUID | None,
        after_revision: int,
    ) -> None:
        if after_revision < 0:
            raise ValueError("Watch cursor cannot be negative")
        self._cache = cache
        self._project_id = project_id
        self._cursor = after_revision

    @property
    def cursor(self) -> int:
        """Last completely applied event or checkpoint revision."""

        return self._cursor

    async def apply(self, frame: ScheduleChange | ScannedThrough) -> None:
        """Apply one strictly increasing frame or fail the channel closed."""

        if frame.revision <= self._cursor:
            raise ScheduleProtocolError(
                "schedule Watch revision did not increase",
            )
        if isinstance(frame, ScannedThrough):
            if frame.server_revision <= 0 or frame.revision > frame.server_revision:
                raise ScheduleProtocolError(
                    "schedule Watch checkpoint exceeds server revision",
                )
            # Deliberately no cache watermark mutation: this frame proves only
            # that irrelevant global revisions were examined.
            self._cursor = frame.revision
            return

        if self._project_id is not None and frame.project_id != self._project_id:
            raise ScheduleProtocolError(
                "schedule Watch event escaped its requested project",
            )
        if frame.kind == "upsert":
            if frame.schedule is None or frame.deleted_id is not None:
                raise ScheduleProtocolError("malformed schedule upsert envelope")
            if (
                frame.schedule.project_id != frame.project_id
                or frame.schedule.schedule_revision != frame.revision
            ):
                raise ScheduleProtocolError(
                    "schedule upsert payload does not match its envelope",
                )
            await self._cache.apply_watch_update(frame.schedule)
        elif frame.kind == "tombstone":
            if frame.schedule is not None or frame.deleted_id is None:
                raise ScheduleProtocolError("malformed schedule tombstone envelope")
            await self._cache.apply_tombstone(
                schedule_id=frame.deleted_id,
                project_id=frame.project_id,
                revision=frame.revision,
            )
        else:  # pragma: no cover - Literal guard for untyped callers
            raise ScheduleProtocolError("unknown schedule Watch change kind")
        self._cursor = frame.revision


__all__ = ["WATCH_FORMAT_VERSION", "OrderedWatchApplier"]
