"""Regression tests for the head-to-head bench harness.

The bench is the §29 GA exit criterion's defensible-numbers
generator. If it silently breaks (e.g. an upstream celery upgrade
changes the ``crontab`` constructor signature), our published
README numbers go stale without anyone noticing. These tests pin
the contract of each measurement function.

We do NOT pin specific timing numbers - those are machine-dependent
and would flap. We only pin SHAPE: each function returns the
expected dict keys, the celery side gracefully degrades when
celery isn't importable, the renderer produces non-empty output.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from tests.benchmarks.bench_celery_beat_compare import (
    bench_memory_per_schedule,
    bench_next_fire_cost,
    bench_tick_at_scale,
    render_summary,
)


class TestBenchNextFireCost:
    def test_returns_per_cron_keyed_dict(self) -> None:
        # Use a tiny iteration count to keep the test fast.
        out = bench_next_fire_cost(iterations=10)
        assert out["iterations"] == 10
        # Five canonical schedule shapes are documented in the
        # bench module - if any go missing the README table loses
        # a row without warning.
        assert set(out["per_cron"].keys()) == {
            "every-minute", "every-5-min", "hourly",
            "daily-3am", "weekly-mon",
        }
        # Every entry has at least the z4j metrics. The celery
        # metrics are conditional on celery being importable.
        for cron in out["per_cron"].values():
            assert "z4j_us_p50" in cron
            assert "z4j_us_p99" in cron
            assert cron["z4j_us_p50"] >= 0
            assert cron["z4j_us_p99"] >= cron["z4j_us_p50"]


class TestBenchTickAtScale:
    def test_three_scale_points(self) -> None:
        out = bench_tick_at_scale()
        # 100 / 1k / 10k - dropping any of these silently would
        # remove information from the published table.
        assert set(out["per_scale"].keys()) == {"100", "1000", "10000"}
        for n_str, r in out["per_scale"].items():
            assert r["schedules"] == int(n_str)
            assert r["z4j_tick_ms"] >= 0
            # Due-count is non-negative and bounded by schedules.
            assert 0 <= r["z4j_due_count"] <= r["schedules"]


class TestBenchMemoryPerSchedule:
    def test_returns_z4j_total_and_per_schedule(self) -> None:
        out = bench_memory_per_schedule()
        assert out["schedules"] == 10_000
        assert "z4j_total_mb" in out
        assert out["z4j_total_mb"] >= 0
        # bytes_per_schedule is None only when the total measured
        # zero growth - skip the assert in that case (CI sandboxes
        # with very small RSS noise can hit this).
        if out.get("z4j_bytes_per_schedule") is not None:
            assert out["z4j_bytes_per_schedule"] > 0


class TestRenderSummary:
    def test_renders_all_three_metrics(self) -> None:
        # Minimal report shape that should render cleanly with
        # both halves present.
        report = {
            "generated": "2026-04-27T00:00:00+00:00",
            "celery_available": True,
            "next_fire_cost": {
                "iterations": 100,
                "per_cron": {
                    "hourly": {
                        "expression": "0 * * * *",
                        "z4j_us_p50": 50.0,
                        "z4j_us_p99": 100.0,
                        "celery_us_p50": 10.0,
                        "celery_us_p99": 20.0,
                    },
                },
            },
            "tick_at_scale": {
                "per_scale": {
                    "100": {
                        "schedules": 100,
                        "z4j_tick_ms": 1.0,
                        "z4j_due_count": 100,
                        "celery_tick_ms": 2.0,
                        "celery_due_count": 100,
                    },
                },
            },
            "memory_per_schedule": {
                "schedules": 10_000,
                "z4j_total_mb": 6.0,
                "z4j_bytes_per_schedule": 600.0,
                "celery_total_mb": 60.0,
                "celery_bytes_per_schedule": 6000.0,
            },
        }
        out = render_summary(report)
        # The three section headers must all appear so the
        # README screenshot doesn't lose a section.
        assert "1. Next-fire computation cost" in out
        assert "2. Per-tick due-list cost" in out
        assert "3. Memory footprint" in out
        # Both schedulers represented.
        assert "z4j-scheduler" in out
        assert "celery-beat" in out
        # The verdict line shows up when celery is enabled.
        assert "Geomean" in out

    def test_renders_skip_message_when_celery_missing(self) -> None:
        report = {
            "generated": "2026-04-27T00:00:00+00:00",
            "celery_available": False,
            "next_fire_cost": {
                "iterations": 100,
                "per_cron": {
                    "hourly": {
                        "expression": "0 * * * *",
                        "z4j_us_p50": 50.0,
                        "z4j_us_p99": 100.0,
                    },
                },
            },
            "tick_at_scale": {
                "per_scale": {
                    "100": {
                        "schedules": 100,
                        "z4j_tick_ms": 1.0,
                        "z4j_due_count": 100,
                    },
                },
            },
            "memory_per_schedule": {
                "schedules": 10_000,
                "z4j_total_mb": 6.0,
                "z4j_bytes_per_schedule": 600.0,
            },
        }
        out = render_summary(report)
        # The (skip) marker stands in for missing celery numbers.
        assert "(skip)" in out
        # Operator-facing pointer to enable celery side.
        assert "pip install celery" in out.lower() or "Re-run" in out


class TestPersistedReportShape:
    """The committed JSON report must round-trip cleanly.

    The published README cites this file. If a refactor changes
    the JSON schema without updating the README, the docs go
    stale silently.
    """

    def test_committed_report_parses(self) -> None:
        path = Path(__file__).parent.parent / "benchmarks" / "results" / "celery_beat_compare.json"
        if not path.exists():
            pytest.skip(
                "committed report missing; run "
                "`python -m tests.benchmarks.bench_celery_beat_compare` first",
            )
        report = json.loads(path.read_text())
        # Top-level shape that the README + the renderer rely on.
        assert "generated" in report
        assert "celery_available" in report
        assert "next_fire_cost" in report
        assert "tick_at_scale" in report
        assert "memory_per_schedule" in report
