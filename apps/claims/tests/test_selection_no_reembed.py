"""Tests proving selection subsets vectors without re-embedding."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from apps.claims import annotations as ann_mod
from apps.claims import selections as sel_mod
from apps.claims.keys import claim_key


def test_stance_split_and_cluster_selection_no_reembed(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    keys = [claim_key(f"claim {i}") for i in range(6)]
    stance = {keys[0]: 0.1, keys[1]: 0.2, keys[2]: 0.5, keys[3]: 0.55, keys[4]: 0.9, keys[5]: 0.95}
    ann = ann_mod.write_annotation(root, "stance", stance, producer="test")
    anti = sel_mod.from_threshold(ann, name="anti", high=0.33)
    pro = sel_mod.from_threshold(ann, name="pro", low=0.66)
    assert len(anti.keys) == 2
    assert len(pro.keys) == 2

    # Fake run: 6 vectors
    vectors = np.random.randn(6, 4).astype(np.float32)
    index = {
        "claim_keys": keys,
        "claim_texts": [f"claim {i}" for i in range(6)],
        "groups": [{"claim_key": k, "claim_text": f"claim {i}"} for i, k in enumerate(keys)],
        "source_hash": "h",
    }
    anti_vecs, anti_idx, anti_rows = sel_mod.subset_vectors(vectors, index, anti)
    pro_vecs, pro_idx, pro_rows = sel_mod.subset_vectors(vectors, index, pro)

    # Same underlying array memory path — subset is a view/copy of existing vectors
    assert anti_vecs.shape == (2, 4)
    assert pro_vecs.shape == (2, 4)
    assert anti_rows == [0, 1]
    assert pro_rows == [4, 5]
    np.testing.assert_array_equal(anti_vecs, vectors[anti_rows])
    np.testing.assert_array_equal(pro_vecs, vectors[pro_rows])
    assert anti_idx["selection"] == "anti"
    assert pro_idx["selection"] == "pro"
    # No new embedding artifact written — only in-memory subset
    assert not list(root.glob("**/vectors.npy"))
