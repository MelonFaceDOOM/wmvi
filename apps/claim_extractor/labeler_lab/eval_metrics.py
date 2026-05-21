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
