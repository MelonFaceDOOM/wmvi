"""Agreement metrics for gold / agent / model evaluation."""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence


def _as_float_list(xs: Sequence[Any]) -> list[float]:
    return [float(x) for x in xs]


def cohen_kappa(y_true: Sequence[Any], y_pred: Sequence[Any]) -> float | None:
    """Cohen's kappa for categorical labels (stringified)."""
    yt = [str(x) for x in y_true]
    yp = [str(x) for x in y_pred]
    if len(yt) != len(yp) or not yt:
        return None
    labels = sorted(set(yt) | set(yp))
    n = len(yt)
    # Observed agreement
    po = sum(1 for a, b in zip(yt, yp) if a == b) / n
    # Expected agreement
    ct = Counter(yt)
    cp = Counter(yp)
    pe = sum((ct[lab] / n) * (cp[lab] / n) for lab in labels)
    if abs(1.0 - pe) < 1e-12:
        return 1.0 if abs(po - 1.0) < 1e-12 else 0.0
    return float((po - pe) / (1.0 - pe))


def confusion_counts(
    y_true: Sequence[Any], y_pred: Sequence[Any]
) -> dict[str, dict[str, int]]:
    yt = [str(x) for x in y_true]
    yp = [str(x) for x in y_pred]
    labels = sorted(set(yt) | set(yp))
    grid = {a: {b: 0 for b in labels} for a in labels}
    for a, b in zip(yt, yp):
        grid[a][b] += 1
    return grid


def per_class_prf(
    y_true: Sequence[Any], y_pred: Sequence[Any]
) -> dict[str, dict[str, float | int]]:
    yt = [str(x) for x in y_true]
    yp = [str(x) for x in y_pred]
    labels = sorted(set(yt) | set(yp))
    out: dict[str, dict[str, float | int]] = {}
    for lab in labels:
        tp = sum(1 for a, b in zip(yt, yp) if a == lab and b == lab)
        fp = sum(1 for a, b in zip(yt, yp) if a != lab and b == lab)
        fn = sum(1 for a, b in zip(yt, yp) if a == lab and b != lab)
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
        out[lab] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": float(prec),
            "recall": float(rec),
            "f1": float(f1),
            "support": tp + fn,
        }
    return out


def binary_agreement(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    *,
    threshold: float = 0.5,
) -> dict[str, Any]:
    yt = [1 if float(v) >= threshold else 0 for v in y_true]
    # Predictions may already be hard labels or scores
    yp_raw = _as_float_list(y_pred)
    yp = [1 if v >= threshold else 0 for v in yp_raw]
    n = len(yt)
    if n == 0:
        return {"n": 0, "accuracy": None, "kappa": None, "per_class": {}, "confusion": {}}
    acc = sum(1 for a, b in zip(yt, yp) if a == b) / n
    return {
        "n": n,
        "threshold": threshold,
        "accuracy": float(acc),
        "kappa": cohen_kappa(yt, yp),
        "per_class": per_class_prf(yt, yp),
        "confusion": confusion_counts(yt, yp),
    }


def _nearest_bucket(v: float, buckets: Sequence[float]) -> float:
    return min(buckets, key=lambda b: abs(float(b) - float(v)))


def discrete_float_agreement(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    *,
    buckets: Sequence[float] | None = None,
) -> dict[str, Any]:
    yt = _as_float_list(y_true)
    yp = _as_float_list(y_pred)
    n = len(yt)
    if n == 0:
        return {
            "n": 0,
            "exact_agreement": None,
            "adjacent_agreement": None,
            "mae": None,
            "kappa": None,
        }
    if buckets is None:
        buckets = sorted({round(v, 4) for v in yt} | {0.0, 0.25, 0.5, 0.75, 1.0})
    buckets = sorted(float(b) for b in buckets)
    yt_b = [_nearest_bucket(v, buckets) for v in yt]
    yp_b = [_nearest_bucket(v, buckets) for v in yp]
    exact = sum(1 for a, b in zip(yt_b, yp_b) if abs(a - b) < 1e-9) / n
    # Adjacent: within one bucket step on the sorted bucket list
    idx = {b: i for i, b in enumerate(buckets)}
    adjacent = (
        sum(1 for a, b in zip(yt_b, yp_b) if abs(idx[a] - idx[b]) <= 1) / n
        if buckets
        else exact
    )
    mae = sum(abs(a - b) for a, b in zip(yt, yp)) / n
    # Quadratic weighted kappa on bucket indices
    k = quadratic_weighted_kappa(
        [idx[a] for a in yt_b],
        [idx[b] for b in yp_b],
        n_classes=len(buckets),
    )
    return {
        "n": n,
        "buckets": buckets,
        "exact_agreement": float(exact),
        "adjacent_agreement": float(adjacent),
        "mae": float(mae),
        "kappa": k,
        "cohen_kappa_exact": cohen_kappa(yt_b, yp_b),
        "per_class": per_class_prf(yt_b, yp_b),
        "confusion": confusion_counts(yt_b, yp_b),
    }


def quadratic_weighted_kappa(
    y_true_idx: Sequence[int],
    y_pred_idx: Sequence[int],
    *,
    n_classes: int,
) -> float | None:
    n = len(y_true_idx)
    if n == 0 or n_classes < 2:
        return None
    o = [[0 for _ in range(n_classes)] for _ in range(n_classes)]
    for a, b in zip(y_true_idx, y_pred_idx):
        o[int(a)][int(b)] += 1
    hist_t = [sum(o[i][j] for j in range(n_classes)) for i in range(n_classes)]
    hist_p = [sum(o[i][j] for i in range(n_classes)) for j in range(n_classes)]
    e = [[hist_t[i] * hist_p[j] / n for j in range(n_classes)] for i in range(n_classes)]
    denom = (n_classes - 1) ** 2
    num = 0.0
    den = 0.0
    for i in range(n_classes):
        for j in range(n_classes):
            w = ((i - j) ** 2) / denom
            num += w * o[i][j]
            den += w * e[i][j]
    if den <= 1e-12:
        return 1.0 if num <= 1e-12 else 0.0
    return float(1.0 - num / den)


def agreement_metrics(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    *,
    value_type: str,
    threshold: float = 0.5,
    buckets: Sequence[float] | None = None,
) -> dict[str, Any]:
    if value_type == "binary":
        return {"value_type": "binary", **binary_agreement(y_true, y_pred, threshold=threshold)}
    return {
        "value_type": "float",
        **discrete_float_agreement(y_true, y_pred, buckets=buckets),
    }
