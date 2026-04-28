"""Verification helpers for the migration cutover.

Two complementary checks operators can run before flipping their
canonical scheduler from celery-beat (or rq-scheduler / APScheduler /
cron) to z4j-scheduler:

- ``import --verify`` (existing, in :mod:`z4j_scheduler.cli`):
  compares the source-of-truth dict against brain's current schedule
  state and prints an INSERT / UPDATE / UNCHANGED / DELETE diff.
  Catches configuration drift at re-import time.

- ``import --verify --duration <window>`` (new, lives here): walks
  the next ``window`` of time and predicts every fire each side
  would issue. Reports timing divergence and any fires only one
  side would emit. Catches importer translation bugs and timezone
  misconfigurations BEFORE the operator actually swaps the
  scheduler. This is the §17.1 promise from
  ``docs/SCHEDULER.md``.
"""

from z4j_scheduler.verify.shadow_comparator import (
    FireDivergence,
    PredictedFire,
    ShadowComparisonReport,
    compare_predicted_fires,
    parse_duration,
    predict_fires,
    render_report,
)

__all__ = [
    "FireDivergence",
    "PredictedFire",
    "ShadowComparisonReport",
    "compare_predicted_fires",
    "parse_duration",
    "predict_fires",
    "render_report",
]
