"""Compare prediction vectors to gold for the labeler lab."""

from __future__ import annotations

import math
from typing import Any

METRIC_HELP: dict[str, str] = {
    "mae": (
        "**MAE (mean absolute error)** — average |prediction − your label| on the 0–1 scale. "
        "Lower is better. MAE ≈ 0.16 means predictions are typically off by about 0.16."
    ),
    "rmse": (
        "**RMSE (root mean squared error)** — like MAE but large mistakes count more. "
        "Lower is better; often slightly above MAE when a few rows are badly wrong."
    ),
    "pearson": (
        "**Pearson r** — correlation between predictions and your labels (−1 to 1). "
        "Higher is better for ranking agreement (high vs low scores). "
        "Does not measure calibration: predictions can track your order but still be shifted."
    ),
    "n": "Number of eval rows included in this metric block.",
}

BEATS_LLM_HELP = (
    "Ridge **beats LLM** when Ridge MAE is lower than LLM MAE **and** Ridge Pearson ≥ LLM Pearson "
    "(requires ≥10 valid LLM scores on eval)."
)


def _pearson(x: list[float], y: list[float]) -> float | None:
    if len(x) < 2:
        return None
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    denx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    deny = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if denx < 1e-12 or deny < 1e-12:
        return None
    return num / (denx * deny)


def eval_predictions(y_true: list[float], y_pred: list[float]) -> dict[str, Any]:
    if len(y_true) != len(y_pred) or not y_true:
        return {"n": 0, "mae": None, "rmse": None, "pearson": None}
    abs_errs = [abs(a - b) for a, b in zip(y_true, y_pred)]
    sq_errs = [(a - b) ** 2 for a, b in zip(y_true, y_pred)]
    mae = sum(abs_errs) / len(abs_errs)
    rmse = math.sqrt(sum(sq_errs) / len(sq_errs))
    return {
        "n": len(y_true),
        "mae": mae,
        "rmse": rmse,
        "pearson": _pearson(y_true, y_pred),
    }


def format_metric(value: float | None, *, decimals: int = 3) -> str:
    if value is None:
        return "—"
    return f"{float(value):.{decimals}f}"


def metrics_comparison_rows(
    ridge: dict[str, Any],
    llm: dict[str, Any],
) -> list[dict[str, Any]]:
    """Rows for a grouped bar chart: model × metric → value."""
    rows: list[dict[str, Any]] = []
    for metric_key, label in (("mae", "MAE"), ("rmse", "RMSE")):
        rv = ridge.get(metric_key)
        lv = llm.get(metric_key)
        if rv is not None:
            rows.append({"metric": label, "value": float(rv), "model": "Ridge"})
        if lv is not None:
            rows.append({"metric": label, "value": float(lv), "model": "LLM"})
    return rows


def pearson_comparison_rows(ridge: dict[str, Any], llm: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if ridge.get("pearson") is not None:
        rows.append({"model": "Ridge", "pearson": float(ridge["pearson"])})
    if llm.get("pearson") is not None:
        rows.append({"model": "LLM", "pearson": float(llm["pearson"])})
    return rows


def scatter_rows_per_row(per_row: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Manual gold (x) vs prediction (y) points for Ridge and LLM scatter charts."""
    ridge_pts: list[dict[str, Any]] = []
    llm_pts: list[dict[str, Any]] = []
    for row in per_row:
        gold = float(row["y_manual"])
        ridge_pts.append({"manual": gold, "predicted": float(row["y_ridge"])})
        llm_v = row.get("y_llm")
        if llm_v is not None:
            llm_pts.append({"manual": gold, "predicted": float(llm_v)})
    return ridge_pts, llm_pts


def abs_error_histogram_rows(per_row: list[dict[str, Any]], *, bins: int = 10) -> list[dict[str, Any]]:
    """Bucket absolute errors for Ridge vs LLM overlay histogram via bar chart."""
    if not per_row:
        return []
    edges = [i / bins for i in range(bins + 1)]

    def bucket(err: float) -> str:
        idx = min(int(err * bins), bins - 1)
        lo = edges[idx]
        hi = edges[idx + 1]
        return f"{lo:.1f}–{hi:.1f}"

    counts: dict[tuple[str, str], int] = {}
    labels = [f"{edges[i]:.1f}–{edges[i + 1]:.1f}" for i in range(bins)]
    for row in per_row:
        rb = bucket(float(row["ridge_abs_err"]))
        key = ("Ridge", rb)
        counts[key] = counts.get(key, 0) + 1
        llm_err = row.get("llm_abs_err")
        if llm_err is not None:
            lb = bucket(float(llm_err))
            key = ("LLM", lb)
            counts[key] = counts.get(key, 0) + 1
    out: list[dict[str, Any]] = []
    for label in labels:
        for model in ("Ridge", "LLM"):
            out.append({"error_bin": label, "count": counts.get((model, label), 0), "model": model})
    return out


def compare_to_llm_baseline(
    y_manual: list[float],
    y_ridge: list[float],
    y_llm: list[float | None],
    *,
    min_eval_for_beats: int = 10,
) -> dict[str, Any]:
    """
    Compare Ridge and LLM predictions against manual gold on the same eval rows.

    ``y_llm`` may contain None for rows where the LLM score was missing or invalid.
    """
    if len(y_manual) != len(y_ridge) or len(y_manual) != len(y_llm):
        raise ValueError("y_manual, y_ridge, and y_llm must have the same length")

    ridge = eval_predictions(y_manual, y_ridge)

    llm_manual: list[float] = []
    llm_pred: list[float] = []
    per_row: list[dict[str, Any]] = []
    for i, (gold, ridge_v, llm_v) in enumerate(zip(y_manual, y_ridge, y_llm)):
        row: dict[str, Any] = {
            "index": i,
            "y_manual": gold,
            "y_ridge": ridge_v,
            "ridge_abs_err": abs(gold - ridge_v),
            "y_llm": llm_v,
        }
        if llm_v is not None:
            llm_manual.append(gold)
            llm_pred.append(llm_v)
            row["llm_abs_err"] = abs(gold - llm_v)
        else:
            row["llm_abs_err"] = None
        per_row.append(row)

    llm = eval_predictions(llm_manual, llm_pred) if llm_manual else {"n": 0, "mae": None, "rmse": None, "pearson": None}

    beats_llm: bool | None = None
    if (
        ridge.get("n", 0) >= min_eval_for_beats
        and llm.get("n", 0) >= min_eval_for_beats
        and ridge.get("mae") is not None
        and llm.get("mae") is not None
    ):
        mae_beats = float(ridge["mae"]) < float(llm["mae"])
        r_ridge = ridge.get("pearson")
        r_llm = llm.get("pearson")
        if r_ridge is not None and r_llm is not None:
            beats_llm = mae_beats and float(r_ridge) >= float(r_llm)
        else:
            beats_llm = mae_beats

    return {
        "ridge_vs_manual": ridge,
        "llm_vs_manual": llm,
        "n_llm_valid": llm.get("n", 0),
        "n_llm_invalid": len(y_llm) - llm.get("n", 0),
        "beats_llm": beats_llm,
        "per_row": per_row,
    }
