"""Compare prediction vectors to gold for the labeler lab."""

from __future__ import annotations

import math
from typing import Any


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
