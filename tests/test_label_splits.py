"""Tests for stable label split assignment."""

from __future__ import annotations

from apps.claim_extractor.labeler_lab.splits import assign_label_split


def test_assign_label_split_stable() -> None:
    a = assign_label_split("t1", 0, eval_frac=0.2, seed=42)
    b = assign_label_split("t1", 0, eval_frac=0.2, seed=42)
    assert a == b


def test_assign_label_split_mixes_with_default_frac() -> None:
    labels = [
        assign_label_split(f"t{i}", i, eval_frac=0.2, seed=42) for i in range(200)
    ]
    n_eval = sum(1 for x in labels if x == "eval")
    assert 20 <= n_eval <= 60


def test_seed_42_first_draw_was_train_only_bug() -> None:
    """Old bug: random.seed(42); random.random() -> 0.639, always train at 0.2 frac."""
    assert assign_label_split("any", 0, eval_frac=0.2, seed=42) in ("train", "eval")
    evals = sum(
        1
        for i in range(100)
        if assign_label_split(f"id-{i}", i, eval_frac=0.2, seed=42) == "eval"
    )
    assert evals > 0
