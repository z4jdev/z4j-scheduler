"""Verification helpers for schedule predictions.

The CLI currently exposes one operator check before a scheduler cutover:

- ``import --verify`` (existing, in :mod:`z4j_scheduler.cli`):
  compares the source-of-truth dict against brain's current schedule
  state and prints an INSERT / UPDATE / UNCHANGED / DELETE diff.
  Catches configuration drift at re-import time.

The lower-level helpers in this package can compare two independently
constructed prediction lists. The import CLI does not yet have an independent
source-side oracle, so ``import --verify --duration`` fails closed instead of
self-comparing one list and reporting a false-safe cutover.
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
