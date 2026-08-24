"""Massage resume + schema filtering (mocked LLM)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from apps.claims.demo.catalog import load_names, save_membership, save_names
from apps.claims.demo.massage import name_leaves, name_narratives, reassign_leaves


def _write_hierarchy(tmp: Path) -> Path:
    exp = tmp / "exp"
    exp.mkdir()
    hier = {
        "narratives": [
            {
                "narrative_id": 1,
                "n_leaves": 2,
                "size": 4,
                "leaves": [
                    {
                        "leaf_id": 10,
                        "size": 2,
                        "medoid_claim_text": "MMR does not cause autism.",
                        "sample_claim_texts": ["MMR does not cause autism.", "The MMR vaccine is not linked to autism."],
                    },
                    {
                        "leaf_id": 11,
                        "size": 2,
                        "medoid_claim_text": "Vitamin A treats measles.",
                        "sample_claim_texts": ["Vitamin A treats measles."],
                    },
                ],
            }
        ]
    }
    (exp / "hierarchy_test.json").write_text(__import__("json").dumps(hier), encoding="utf-8")
    np.save(exp / "leaf_labels_test.npy", np.array([10, 10, 11, 11], dtype=int))
    np.save(exp / "narrative_labels_test.npy", np.array([1, 1, 1, 1], dtype=int))
    return exp


def test_name_narratives_and_leaf_resume(tmp_path: Path) -> None:
    exp = _write_hierarchy(tmp_path)

    def complete_nar(**kwargs):
        return {
            "narratives": [
                {"id": 1, "title": "MMR and treatment", "blurb": "Autism + vitamin A"},
                {"id": 99, "title": "ghost", "blurb": "drop me"},
            ]
        }

    out = name_narratives(exp, complete=complete_nar)
    assert out["n_narratives"] == 1
    names = load_names(exp)
    assert names["narratives"][0]["title"] == "MMR and treatment"

    calls = {"n": 0}

    def complete_leaf(**kwargs):
        calls["n"] += 1
        user = kwargs["user"]
        rows = []
        if "id=10" in user:
            rows.append({"id": 10, "title": "MMR not autism", "blurb": "safety"})
        if "id=11" in user:
            rows.append({"id": 11, "title": "Vitamin A", "blurb": "treatment"})
        return {"leaves": rows}

    first = name_leaves(exp, batch_size=1, limit=1, complete=complete_leaf)
    assert first["n_new"] == 1
    assert calls["n"] == 1
    second = name_leaves(exp, batch_size=10, complete=complete_leaf)
    assert second["n_new"] == 1
    assert second["n_leaves"] == 2
    assert calls["n"] == 2
    third = name_leaves(exp, complete=complete_leaf)
    assert third["n_new"] == 0
    assert calls["n"] == 2


def test_reassign_overrides(tmp_path: Path) -> None:
    exp = _write_hierarchy(tmp_path)
    save_names(
        exp,
        {
            "narratives": [{"id": 1, "title": "Main", "blurb": ""}],
            "leaves": [
                {"id": 10, "title": "MMR", "blurb": ""},
                {"id": 11, "title": "VitA", "blurb": ""},
            ],
        },
    )

    def complete_asg(**kwargs):
        return {
            "assignments": [
                {"leaf_id": 10, "narrative_id": 1},
                {"leaf_id": 11, "narrative_id": -1},
            ]
        }

    out = reassign_leaves(exp, complete=complete_asg)
    assert out["n_overrides"] == 1
    again = reassign_leaves(exp, complete=lambda **k: {"assignments": []})
    assert again["n_written"] == 2


def test_resolve_bundle_bare_name(tmp_path: Path, monkeypatch) -> None:
    from apps.claims.demo import BUNDLE_FILE
    from apps.claims.demo import catalog as cat

    exp = tmp_path / "exp"
    exp.mkdir()
    packed = exp / BUNDLE_FILE
    packed.write_bytes(b"sqlite")
    monkeypatch.setattr(cat, "DEFAULT_EXP_DIR", exp)
    found = cat.resolve_bundle_path(BUNDLE_FILE)
    assert found == packed
    missing = cat.resolve_bundle_path("nope.sqlite")
    assert missing.name == "nope.sqlite"
    assert not missing.is_file()
