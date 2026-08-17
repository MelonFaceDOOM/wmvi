"""Tests for keys, annotations, and selections."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from apps.claims import annotations as ann_mod
from apps.claims import selections as sel_mod
from apps.claims.keys import claim_key, make_row_id
from apps.claims.grouping import group as grouping


def test_claim_key_stable_and_normalized():
    a = claim_key("Vaccines cause autism.")
    b = claim_key("  vaccines   cause autism. ")
    assert a == b
    assert len(a) == 16
    assert make_row_id("t1", 2) == "t1:2"


def test_bundle_emits_claim_key(tmp_path: Path):
    posts = {
        "posts": [
            {
                "post_id": 1,
                "platform": "reddit_submission",
                "text": "body",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "text": "chunk",
                        "task_id": "t1",
                        "claim_extraction_disposition": "success",
                        "claims": [{"claim": "Foo bar."}],
                    }
                ],
            }
        ]
    }
    path = tmp_path / "claims.json"
    path.write_text(json.dumps(posts), encoding="utf-8")
    bundle = grouping.run(path)
    d = grouping.bundle_to_dict(bundle)
    assert d["groups"][0]["claim_key"] == claim_key("Foo bar.")


def test_annotation_roundtrip(tmp_path: Path):
    root = tmp_path / "corpus"
    root.mkdir()
    values = {claim_key("a"): 0.1, claim_key("b"): 0.9}
    written = ann_mod.write_annotation(
        root,
        "stance",
        values,
        producer="test",
        model="ridge-v1",
        params={"alpha": 1.0},
        source_hash="abc",
    )
    assert written.meta.count == 2
    loaded = ann_mod.read_annotation(root, "stance")
    assert loaded.values == values
    assert loaded.meta.producer == "test"
    assert loaded.meta.model == "ridge-v1"
    assert loaded.meta.source_hash == "abc"

    metas = ann_mod.list_annotations(root)
    assert [m.name for m in metas] == ["stance"]

    with pytest.raises(FileExistsError):
        ann_mod.write_annotation(root, "stance", values, producer="x")

    ann_mod.write_annotation(root, "stance", {claim_key("c"): 0.5}, producer="x", force=True)
    assert ann_mod.read_annotation(root, "stance").values == {claim_key("c"): 0.5}

    ann_mod.remove_annotation(root, "stance")
    assert ann_mod.list_annotations(root) == []


def test_join_into_groups_group_scope(tmp_path: Path):
    groups = [
        {"group_id": 0, "claim_key": claim_key("hello"), "claim_text": "hello"},
        {"group_id": 1, "claim_text": "world"},  # no claim_key — computed
    ]
    ann = ann_mod.Annotation(
        meta=ann_mod.AnnotationMeta(name="stance", scope="group", producer="t"),
        values={claim_key("hello"): 0.2, claim_key("world"): 0.8},
    )
    joined = ann_mod.join_into_groups(groups, ann)
    assert joined[0]["stance"] == 0.2
    assert joined[1]["stance"] == 0.8


def test_selection_from_threshold_and_row_indices(tmp_path: Path):
    root = tmp_path / "corpus"
    root.mkdir()
    k_anti = claim_key("anti")
    k_neu = claim_key("neu")
    k_pro = claim_key("pro")
    ann = ann_mod.write_annotation(
        root,
        "stance",
        {k_anti: 0.1, k_neu: 0.5, k_pro: 0.9},
        producer="test",
    )
    anti = sel_mod.from_threshold(ann, name="anti", high=0.33)
    neu = sel_mod.from_threshold(ann, name="neutral", low=0.33, high=0.66)
    pro = sel_mod.from_threshold(ann, name="pro", low=0.66)
    assert set(anti.keys) == {k_anti}
    assert set(neu.keys) == {k_neu}
    assert set(pro.keys) == {k_pro}

    sel_mod.write_selection(root, anti)
    loaded = sel_mod.read_selection(root, "anti")
    assert loaded.keys == anti.keys

    index = {
        "claim_keys": [k_anti, k_neu, k_pro],
        "claim_texts": ["anti", "neu", "pro"],
        "groups": [
            {"claim_key": k_anti, "claim_text": "anti"},
            {"claim_key": k_neu, "claim_text": "neu"},
            {"claim_key": k_pro, "claim_text": "pro"},
        ],
    }
    vectors = np.arange(9, dtype=np.float32).reshape(3, 3)
    sub, sub_index, rows = sel_mod.subset_vectors(vectors, index, anti)
    assert rows == [0]
    assert sub.shape == (1, 3)
    assert sub_index["selection"] == "anti"
    assert sub_index["claim_keys"] == [k_anti]
    assert sub_index["parent_row_indices"] == [0]


def test_annotation_is_fresh(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    assert not ann_mod.annotation_is_fresh(root, "x", source_hash="h")
    ann_mod.write_annotation(root, "x", {"a": 1}, producer="t", source_hash="h1")
    assert ann_mod.annotation_is_fresh(root, "x", source_hash="h1")
    assert not ann_mod.annotation_is_fresh(root, "x", source_hash="h2")
