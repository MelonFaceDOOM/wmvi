"""Tests for NLI subgroup assignment (heuristic backend)."""

from __future__ import annotations

from pathlib import Path

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims.keys import claim_key
from apps.claims.nli import assign_subgroups, heuristic_contradiction, run_nli_step
from apps.claims.pipeline import Ctx


def test_heuristic_identical_low():
    assert heuristic_contradiction("vaccines work", "vaccines work") < 0.2


def test_assign_subgroups_keeps_near_duplicates_together():
    k1, k2 = claim_key("a"), claim_key("b")
    texts = {
        k1: "vaccines are safe and effective",
        k2: "vaccines are safe and effective today",
    }
    out = assign_subgroups(cluster_of={k1: 0, k2: 0}, texts=texts, threshold=0.85)
    assert out[k1] == out[k2]


def test_run_nli_step_writes_annotation(tmp_path: Path):
    root = tmp_path / "toy"
    root.mkdir()
    k1, k2 = claim_key("hello world"), claim_key("goodbye moon")
    claims_io.write_json(
        root / "groups.json",
        {
            "source_hash": "h",
            "groups": [
                {"claim_key": k1, "claim_text": "hello world"},
                {"claim_key": k2, "claim_text": "goodbye moon"},
            ],
        },
    )
    ann_mod.write_annotation(
        root, "cluster_labels", {k1: 0, k2: 0}, producer="test", source_hash="h"
    )

    from types import SimpleNamespace

    ctx = Ctx(
        corpus=SimpleNamespace(slug="toy", root=root, groups=root / "groups.json"),  # type: ignore[arg-type]
        force=True,
    )
    result = run_nli_step(ctx, output_annotation="nli_subgroup")
    loaded = ann_mod.read_annotation(root, "nli_subgroup")
    assert loaded.meta.count == 2
    assert result.n_subgroups >= 1
    assert result.annotation.name == "nli_subgroup"
