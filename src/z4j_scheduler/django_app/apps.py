"""Django ``AppConfig`` for the z4j-scheduler integration.

The minimal contract: register the app so ``manage.py
z4j_schedules ...`` is discoverable. The optional contract:
auto-reconcile on web-worker startup when
``settings.Z4J_SCHEDULES_AUTO_RECONCILE`` is True.

Auto-reconcile is OFF by default. Most operators prefer to call
``manage.py z4j_schedules sync`` from a deploy hook (one shot,
deterministic) instead of paying the network round-trip on every
worker boot. The flag is offered for sites that already reconcile
other state on startup and want this to fit the same pattern.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("z4j.scheduler.django_app")


try:
    from django.apps import AppConfig
except ImportError:  # pragma: no cover - module is no-op outside Django
    AppConfig = object  # type: ignore[misc,assignment]


class Z4JSchedulerConfig(AppConfig):  # type: ignore[misc]
    name = "z4j_scheduler.django_app"
    label = "z4j_scheduler"
    verbose_name = "z4j-scheduler integration"

    def ready(self) -> None:  # noqa: D401 - Django convention
        """Optional: fire reconcile on app boot when opted in.

        Toggle via ``settings.Z4J_SCHEDULES_AUTO_RECONCILE = True``.
        Failures are caught + logged; never raised so a brain outage
        cannot prevent Django from starting.
        """
        try:
            from django.conf import settings  # noqa: PLC0415
        except ImportError:
            return

        if not getattr(settings, "Z4J_SCHEDULES_AUTO_RECONCILE", False):
            return

        # Defer import so a Django-only operator who hasn't installed
        # the reconcile path's deps doesn't fail to load Django.
        from z4j_scheduler.declarative.frameworks.django import (  # noqa: PLC0415
            reconcile_from_settings,
        )

        try:
            reconcile_from_settings()
        except Exception:  # noqa: BLE001
            logger.exception(
                "z4j.scheduler.django_app: auto-reconcile crashed "
                "(non-fatal; Django boot continues)",
            )
