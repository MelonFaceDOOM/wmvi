"""Tests for LLM benchmark comparison metrics."""

from __future__ import annotations

import pytest

from apps.claim_extractor.labeler_lab import eval_metrics
from apps.claim_extractor.labeler_lab.eval_metrics import compare_to_llm_baseline, eval_predictions


def test_eval_predictions_basic() -> None:
    m = eval_predictions([0.0, 1.0], [0.1, 0.9])
    assert m["n"] == 2
    assert m["mae"] == pytest.approx(0.1)
    assert m["rmse"] is not None


def test_compare_to_llm_baseline_beats_llm() -> None:
    manual = [0.2, 0.8, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
    ridge = [0.25, 0.75, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
    llm = [0.6, 0.1, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
    cmp = compare_to_llm_baseline(manual, ridge, llm, min_eval_for_beats=10)
    assert cmp["ridge_vs_manual"]["n"] == 10
    assert cmp["llm_vs_manual"]["n"] == 10
    assert cmp["ridge_vs_manual"]["mae"] < cmp["llm_vs_manual"]["mae"]
    assert cmp["beats_llm"] is True


def test_compare_skips_invalid_llm_scores() -> None:
    manual = [0.5, 0.5]
    ridge = [0.5, 0.5]
    llm = [None, 0.9]
    cmp = compare_to_llm_baseline(manual, ridge, llm, min_eval_for_beats=2)
    assert cmp["llm_vs_manual"]["n"] == 1
    assert cmp["n_llm_invalid"] == 1


def test_metrics_comparison_rows() -> None:
    ridge = {"mae": 0.2, "rmse": 0.25, "pearson": 0.5}
    llm = {"mae": 0.1, "rmse": 0.15, "pearson": 0.8}
    rows = eval_metrics.metrics_comparison_rows(ridge, llm)
    assert len(rows) == 4
    assert {r["model"] for r in rows} == {"Ridge", "LLM"}


def test_scatter_rows_per_row() -> None:
    per_row = [
        {"y_manual": 0.2, "y_ridge": 0.3, "y_llm": 0.25},
        {"y_manual": 0.8, "y_ridge": 0.7, "y_llm": None},
    ]
    ridge_pts, llm_pts = eval_metrics.scatter_rows_per_row(per_row)
    assert len(ridge_pts) == 2
    assert len(llm_pts) == 1
    assert ridge_pts[0]["manual"] == 0.2
