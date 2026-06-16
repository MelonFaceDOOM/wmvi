"""Claim-set precision/recall/F1 from LLM-judge alignment output."""

from __future__ import annotations

from collections import Counter
from typing import Any


def claim_prf(*, matched: int, missed: int, extra: int) -> dict[str, float | None]:
    """Precision/recall/F1 from alignment counts."""
    if matched == 0 and missed == 0 and extra == 0:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    precision = matched / (matched + extra) if (matched + extra) > 0 else (1.0 if missed == 0 else 0.0)
    recall = matched / (matched + missed) if (matched + missed) > 0 else (1.0 if extra == 0 else 0.0)
    if precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return {"precision": precision, "recall": recall, "f1": f1}


def prf_from_alignment(alignment: dict[str, Any]) -> dict[str, float | None]:
    matched = len(alignment.get("matched") or [])
    missed = len(alignment.get("missed") or [])
    extra = len(alignment.get("extra") or [])
    return claim_prf(matched=matched, missed=missed, extra=extra)


def aggregate_per_post(per_post: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Aggregate per-post PRF rows.

    Each row: {task_id, precision, recall, f1, alignment?}
    """
    if not per_post:
        return {
            "n_posts": 0,
            "macro_precision": None,
            "macro_recall": None,
            "macro_f1": None,
            "micro_precision": None,
            "micro_recall": None,
            "micro_f1": None,
            "issue_categories": {},
        }

    macro_p = [r["precision"] for r in per_post if r.get("precision") is not None]
    macro_r = [r["recall"] for r in per_post if r.get("recall") is not None]
    macro_f = [r["f1"] for r in per_post if r.get("f1") is not None]

    total_matched = 0
    total_missed = 0
    total_extra = 0
    issue_counter: Counter[str] = Counter()
    for r in per_post:
        al = r.get("alignment") or {}
        total_matched += len(al.get("matched") or [])
        total_missed += len(al.get("missed") or [])
        total_extra += len(al.get("extra") or [])
        for item in al.get("missed") or []:
            if isinstance(item, dict) and item.get("issue_category"):
                issue_counter[str(item["issue_category"])] += 1
        for item in al.get("extra") or []:
            if isinstance(item, dict) and item.get("issue_category"):
                issue_counter[str(item["issue_category"])] += 1

    micro = claim_prf(matched=total_matched, missed=total_missed, extra=total_extra)

    def _avg(vals: list[float]) -> float | None:
        return sum(vals) / len(vals) if vals else None

    return {
        "n_posts": len(per_post),
        "macro_precision": _avg(macro_p),
        "macro_recall": _avg(macro_r),
        "macro_f1": _avg(macro_f),
        "micro_precision": micro["precision"],
        "micro_recall": micro["recall"],
        "micro_f1": micro["f1"],
        "issue_categories": dict(issue_counter),
    }
