"""Stable train/eval split assignment for manual labels."""

from __future__ import annotations

import hashlib


def assign_label_split(
    task_id: str,
    claim_index: int,
    *,
    eval_frac: float,
    seed: int,
) -> str:
    """
    Deterministic split from (seed, task_id, claim_index).

    Same inputs always yield the same split; across many labels roughly ``eval_frac``
    land in eval (unlike re-seeding RNG on every save, which repeats one draw).
    """
    ev = max(0.0, min(1.0, float(eval_frac)))
    if ev <= 0.0:
        return "train"
    if ev >= 1.0:
        return "eval"
    payload = f"split|{seed}|{task_id}|{claim_index}".encode("utf-8")
    bucket = int(hashlib.sha256(payload).hexdigest()[:8], 16) / 0xFFFFFFFF
    return "eval" if bucket < ev else "train"
