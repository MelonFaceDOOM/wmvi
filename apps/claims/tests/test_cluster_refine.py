"""Tests for recursive 2-means split and greedy LLM merge."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import numpy as np
import pytest

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import keys as keys_mod
from apps.claims.clustering import browse as cluster_browse
from apps.claims.clustering.browse import (
    BrowseBundle,
    ClusterOutput,
    ClusterRow,
    SelectionHint,
)
from apps.claims.clustering.refine import (
    UnionFind,
    estimate_merge_prompts,
    greedy_merge,
    remap_labels,
    sibling_out_dir,
    split_labels,
)


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def _blob(center: list[float], n: int, jitter: float = 0.02) -> np.ndarray:
    rng = np.random.default_rng(0)
    c = np.asarray(center, dtype=np.float32)
    noise = rng.normal(0, jitter, size=(n, c.size)).astype(np.float32)
    return noise + c


def _write_run(
    run_dir: Path,
    texts: list[str],
    sources: list[list[dict]],
    vectors: np.ndarray | None = None,
) -> list[str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    keys = [keys_mod.claim_key(t) for t in texts]
    groups = []
    for i, text in enumerate(texts):
        groups.append(
            {
                "group_id": i,
                "claim_key": keys[i],
                "claim_text": text,
                "count": len(sources[i]),
                "sources": sources[i],
            }
        )
    index = {
        "model_id": "dummy",
        "claim_keys": keys,
        "claim_texts": texts,
        "groups": groups,
        "source_hash": "h",
    }
    claims_io.write_json(run_dir / "index.json", index)
    if vectors is None:
        arr = np.eye(len(texts), dtype=np.float32)
    else:
        arr = np.asarray(vectors, dtype=np.float32)
    np.save(run_dir / "vectors.npy", arr)
    return keys


def test_split_labels_three_blobs_become_three_ids():
    texts = [f"claim {i}" for i in range(9)]
    vectors = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3),
            _blob([0.0, 1.0, 0.0], 3),
            _blob([0.0, 0.0, 1.0], 3),
        ]
    )
    labels = np.zeros(9, dtype=int)
    out, meta = split_labels(vectors, labels, texts, min_gain=0.0, min_child_n=2)
    assert meta["n_parent"] == 1
    assert meta["n_terminals"] == 3
    assert sorted({int(x) for x in out.tolist()}) == [0, 1, 2]
    counts = [int((out == i).sum()) for i in range(3)]
    assert sorted(counts) == [3, 3, 3]


def test_split_labels_n2_stays_one_id():
    texts = ["a", "b"]
    vectors = np.eye(2, dtype=np.float32)
    labels = np.zeros(2, dtype=int)
    out, meta = split_labels(vectors, labels, texts, min_gain=0.0, min_child_n=2)
    assert meta["n_terminals"] == 1
    assert set(out.tolist()) == {0}


def test_sibling_out_dir(tmp_path: Path):
    d = tmp_path / "cluster_abc"
    d.mkdir()
    assert sibling_out_dir(d, "_split") == tmp_path / "cluster_abc_split"
    f = d / "result_abc.json"
    f.write_text("{}", encoding="utf-8")
    assert sibling_out_dir(f, "_merge") == tmp_path / "cluster_abc_merge"


def test_remap_union_find():
    uf = UnionFind([0, 1, 2, 3])
    uf.union(0, 1)
    uf.union(2, 3)
    labels = np.asarray([0, 1, 2, 3], dtype=int)
    out, dense = remap_labels(labels, uf.roots())
    assert len(set(out.tolist())) == 2
    assert out[0] == out[1]
    assert out[2] == out[3]
    assert out[0] != out[2]
    assert len(dense) == 2


def _pair_bundle() -> BrowseBundle:
    """Two close pairs of one-point clusters (0~1, 2~3)."""
    vectors = np.asarray(
        [
            [1.0, 0.0],
            [0.98, 0.2],
            [0.0, 1.0],
            [0.2, 0.98],
        ],
        dtype=np.float32,
    )
    labels = np.asarray([0, 1, 2, 3], dtype=int)
    texts = ["alpha", "alpha twin", "beta", "beta twin"]
    output = ClusterOutput(
        directory=Path("."),
        meta_path=None,
        payload={"run_dir": "runs/x/y"},
        labels_path=Path("labels_x.npy"),
        run_dir=None,
        n_selected=None,
        hint=SelectionHint(raw=None),
    )
    clusters = []
    for cid in range(4):
        clusters.append(
            ClusterRow(
                cluster_id=cid,
                size=1,
                mean_intra_cosine=1.0,
                medoid_text=texts[cid],
                medoid_idx=cid,
            )
        )
    cluster_browse.attach_leaf_neighbors(clusters, vectors)
    return BrowseBundle(
        output=output,
        index={"claim_texts": texts, "groups": [{"claim_text": t, "count": 1, "sources": []} for t in texts]},
        labels=labels,
        vectors=vectors,
        corpus=None,
        claims_path=None,
        applied_selection=None,
        applied_filters=(),
        clusters=clusters,
    )


def test_estimate_merge_prompt_bounds():
    bundle = _pair_bundle()
    est = estimate_merge_prompts(bundle, nn_threshold=0.8, neighbor_k=15)
    assert est["n_terminals"] == 4
    assert est["n_seeds_max"] == 4
    assert est["n_seeds_min"] == 2
    assert est["n_seeds_min"] <= est["n_seeds_max"]


def test_greedy_merge_fake_llm():
    bundle = _pair_bundle()

    def merge_fn(seed_id: int, user: str, blocks: list[dict]) -> list[int]:
        if not blocks:
            return []
        return [int(blocks[0]["id"])]

    roots, rows, n_prompts = greedy_merge(
        bundle, nn_threshold=0.8, neighbor_k=15, merge_fn=merge_fn
    )
    assert n_prompts >= 1
    labels, _ = remap_labels(bundle.labels, roots)
    assert len(set(int(x) for x in labels.tolist())) == 2
    assert any("merge_ids" in r for r in rows)


def test_greedy_merge_resume_skips_logged_seed(tmp_path: Path):
    bundle = _pair_bundle()
    log_path = tmp_path / "merge_log.jsonl"
    seed0 = next(c for c in bundle.clusters if c.cluster_id == 0)
    nn = int(seed0.nn_id) if seed0.nn_id is not None else 1
    log_path.write_text(
        json.dumps({"seed_id": 0, "merge_ids": [nn], "nn_id": nn, "nn_cosine": seed0.nn_cosine})
        + "\n",
        encoding="utf-8",
    )
    calls: list[int] = []

    def merge_fn(seed_id: int, user: str, blocks: list[dict]) -> list[int]:
        calls.append(seed_id)
        return [int(blocks[0]["id"])] if blocks else []

    from apps.claims.clustering.refine import read_jsonl

    roots, _rows, n_prompts = greedy_merge(
        bundle,
        nn_threshold=0.8,
        neighbor_k=15,
        merge_fn=merge_fn,
        log_path=log_path,
        resume_log=read_jsonl(log_path),
    )
    assert 0 not in calls
    labels, _ = remap_labels(bundle.labels, roots)
    assert labels[0] == labels[nn]


def test_cluster_split_cli_writes_sibling(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = [f"claim {i}" for i in range(9)]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(9)]
    vectors = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3),
            _blob([0.0, 1.0, 0.0], 3),
            _blob([0.0, 0.0, 1.0], 3),
        ]
    )
    _write_run(corp.run_dir("m"), texts, sources, vectors=vectors)
    exp = corp.experiment_dir("m", "cluster_abc")
    exp.mkdir(parents=True)
    np.save(exp / "labels_abc.npy", np.zeros(9, dtype=int))
    claims_io.write_json(
        exp / "result_abc.json",
        {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans", "n_clusters": 1, "n_clusters_frac": 0.02},
    )
    from apps.claims.cli.cluster_refine_cmd import cmd_cluster_split

    rc = cmd_cluster_split(Namespace(from_path=exp, out_dir=None, min_gain=0.0, min_child_n=2, seed=0))
    assert rc == 0
    out = exp.parent / "cluster_abc_split"
    assert (out / "labels_abc.npy").is_file()
    payload = claims_io.read_json(out / "result_abc.json")
    assert payload["split"]["n_terminals"] == 3
    assert payload["n_clusters"] == 3
    assert payload["n_clusters_frac"] == 0.02
    labels = np.load(out / "labels_abc.npy")
    assert labels.shape == (9,)
    assert len(set(int(x) for x in labels.tolist())) == 3


def test_cluster_merge_estimate_only(isolated_data: Path, capsys):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = ["a", "b", "c", "d"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(4)]
    vectors = np.asarray(
        [[1.0, 0.0], [0.98, 0.2], [0.0, 1.0], [0.2, 0.98]],
        dtype=np.float32,
    )
    _write_run(corp.run_dir("m"), texts, sources, vectors=vectors)
    exp = corp.experiment_dir("m", "cluster_abc_split")
    exp.mkdir(parents=True)
    np.save(exp / "labels_abc.npy", np.asarray([0, 1, 2, 3], dtype=int))
    claims_io.write_json(exp / "result_abc.json", {"run_dir": str(corp.run_dir("m")), "n_clusters": 4})
    from apps.claims.cli.cluster_refine_cmd import cmd_cluster_merge

    rc = cmd_cluster_merge(
        Namespace(
            from_path=exp,
            estimate_only=True,
            nn_threshold=0.8,
            neighbor_k=15,
            out_dir=None,
            model="gpt-5.4-mini",
            log=None,
        )
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["estimate"]["n_terminals"] == 4
    assert out["estimate"]["n_seeds_min"] <= out["estimate"]["n_seeds_max"]


def test_cluster_merge_missing_api_key(isolated_data: Path, monkeypatch: pytest.MonkeyPatch, capsys):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = ["a", "b"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(2)]
    _write_run(corp.run_dir("m"), texts, sources)
    exp = corp.experiment_dir("m", "cluster_x_split")
    exp.mkdir(parents=True)
    np.save(exp / "labels_x.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(exp / "result_x.json", {"run_dir": str(corp.run_dir("m")), "n_clusters": 2})
    monkeypatch.setattr(
        "nlp.claim_extraction.clients.load_openai_config",
        lambda: (_ for _ in ()).throw(
            RuntimeError("Missing PERSONAL_OPENAI_API_KEY in environment.")
        ),
    )
    from apps.claims.cli.cluster_refine_cmd import cmd_cluster_merge

    rc = cmd_cluster_merge(
        Namespace(
            from_path=exp,
            estimate_only=False,
            nn_threshold=0.8,
            neighbor_k=15,
            out_dir=None,
            model="gpt-5.4-mini",
            log=None,
        )
    )
    assert rc == 1
    err = json.loads(capsys.readouterr().out)
    assert "PERSONAL_OPENAI_API_KEY" in err["error"]
