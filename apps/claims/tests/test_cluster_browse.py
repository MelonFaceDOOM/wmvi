"""Tests for the read-only cluster browser loaders."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import keys as keys_mod
from apps.claims import selections as sel_mod
from apps.claims.clustering import browse as cluster_browse


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def _write_claims(path: Path, posts: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"posts": posts}), encoding="utf-8")


def _post(task_id: str, text: str, claim: str, **extra) -> dict:
    alignment = extra.pop("alignment", None)
    claims = extra.pop("claims", None)
    if claims is None:
        obj: dict = {"claim": claim}
        if alignment is not None:
            obj["claim_vaccine_alignment_score"] = alignment
        claims = [obj]
    post = {
        "post_id": extra.pop("post_id", task_id.split(":")[0]),
        "platform": extra.pop("platform", "reddit_submission"),
        "url": extra.pop("url", "https://example.test/p"),
        "created_at_ts": extra.pop("created_at_ts", "2024-01-01T00:00:00+00:00"),
        "primary_metric": extra.pop("primary_metric", 7),
        "reddit_submission_title": extra.pop("reddit_submission_title", "A title"),
        "text": text,
        "chunks": [
            {
                "chunk_index": 0,
                "text": text,
                "task_id": task_id,
                "claim_extraction_disposition": "success",
                "claims": claims,
            }
        ],
    }
    post.update(extra)
    return post


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


def test_parse_selection_label():
    h = cluster_browse.parse_selection_label("selection:standalone_ok")
    assert h.selection == "standalone_ok"
    assert h.filter_annotations == ()
    h2 = cluster_browse.parse_selection_label("filter:stance+selection:anti")
    assert h2.selection == "anti"
    assert h2.filter_annotations == ("stance",)
    assert cluster_browse.parse_selection_label(None).selection is None


def test_infer_corpus_from_run_dir():
    p = Path("/tmp/apps/claims/data/runs/measles_bal/bge-large")
    assert cluster_browse.infer_corpus_from_run_dir(p) == ("measles_bal", "bge-large")
    win = r"C:\projects\wmvi\apps\claims\data\runs\measles2\qwen3-emb-8b"
    assert cluster_browse.infer_corpus_from_run_dir(win) == ("measles2", "qwen3-emb-8b")
    assert cluster_browse.portable_run_dir_str(win) == "runs/measles2/qwen3-emb-8b"
    assert cluster_browse.infer_corpus_from_run_dir("runs/measles2/qwen3-emb-8b") == (
        "measles2",
        "qwen3-emb-8b",
    )


def test_resolve_run_dir_from_windows_path(isolated_data: Path):
    corpus_mod.create_corpus("measles2")
    corp = corpus_mod.get_corpus("measles2")
    texts = ["one claim", "two claim"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(2)]
    _write_run(corp.run_dir("qwen3-emb-8b"), texts, sources)
    exp = corp.experiment_dir("qwen3-emb-8b", "cluster_default_x")
    exp.mkdir(parents=True)
    np.save(exp / "labels_x.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(
        exp / "result_x.json",
        {"run_dir": r"C:\projects\wmvi\apps\claims\data\runs\measles2\qwen3-emb-8b"},
    )
    bundle = cluster_browse.load_browse_bundle(exp)
    assert bundle.corpus == "measles2"
    assert bundle.labels.shape == (2,)


def test_browse_with_selection(isolated_data: Path):
    corpus_mod.create_corpus("measles_bal")
    corp = corpus_mod.get_corpus("measles_bal")
    texts = ["MMR causes autism.", "Measles is airborne.", "Vitamin A treats measles."]
    sources = [
        [{"task_id": "p1:0", "claim_index": 0, "row_id": "p1:0:0"}],
        [{"task_id": "p2:0", "claim_index": 0, "row_id": "p2:0:0"}],
        [{"task_id": "p3:0", "claim_index": 0, "row_id": "p3:0:0"}],
    ]
    keys = _write_run(corp.run_dir("bge"), texts, sources)
    _write_claims(
        corp.claims,
        [
            _post("p1:0", "chunk one about mmr", texts[0], post_id="p1", primary_metric=12),
            _post("p2:0", "chunk two airborne", texts[1], post_id="p2", platform="youtube_comment"),
            _post("p3:0", "chunk three vit a", texts[2], post_id="p3"),
        ],
    )
    sel_mod.write_selection(
        corp.root,
        sel_mod.Selection(name="standalone_ok", scope="group", keys=[keys[0], keys[1]]),
    )

    exp = corp.experiment_dir("bge", "cluster_standalone_ok_x")
    exp.mkdir(parents=True)
    np.save(exp / "labels_abc.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(
        exp / "result_abc.json",
        {
            "run_dir": str(corp.run_dir("bge")),
            "selection": "selection:standalone_ok",
            "n_selected": 2,
            "algorithm": "kmeans",
        },
    )

    discovered = cluster_browse.discover_cluster_output(exp)
    assert discovered.hint.selection == "standalone_ok"

    bundle = cluster_browse.load_browse_bundle(exp)
    assert bundle.labels.shape == (2,)
    assert bundle.applied_selection == "standalone_ok"
    assert bundle.corpus == "measles_bal"
    members = cluster_browse.members_for_cluster(bundle, 0)
    assert len(members) == 1
    assert members[0].claim_text == texts[0]
    occ = cluster_browse.load_occurrence_index(corp.claims)
    hits = cluster_browse.occurrences_for_member(members[0], occ)
    assert hits[0]["found"] is True
    assert hits[0]["platform"] == "reddit_submission"
    assert hits[0]["primary_metric"] == 12
    assert "mmr" in hits[0]["chunk_text"]


def test_rejects_hierarchy_experiment_dir(isolated_data: Path):
    corpus_mod.create_corpus("measles_bal")
    corp = corpus_mod.get_corpus("measles_bal")
    texts = ["a claim", "b claim"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(2)]
    _write_run(corp.run_dir("bge"), texts, sources)
    exp = corp.experiment_dir("bge", "hier")
    exp.mkdir(parents=True)
    np.save(exp / "leaf_labels_x.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(exp / "hierarchy_x.json", {"run_dir": str(corp.run_dir("bge"))})
    with pytest.raises(ValueError, match="hierarchy"):
        cluster_browse.load_browse_bundle(exp)


def test_labels_mismatch_without_selection(isolated_data: Path):
    corpus_mod.create_corpus("measles_bal")
    corp = corpus_mod.get_corpus("measles_bal")
    texts = ["a claim", "b claim", "c claim"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(3)]
    _write_run(corp.run_dir("bge"), texts, sources)
    exp = corp.experiment_dir("bge", "cluster_short")
    exp.mkdir(parents=True)
    np.save(exp / "labels_x.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(exp / "result_x.json", {"run_dir": str(corp.run_dir("bge"))})
    with pytest.raises(ValueError, match="labels length"):
        cluster_browse.load_browse_bundle(exp)


def test_live_filter_requires_repass(isolated_data: Path):
    corpus_mod.create_corpus("measles_bal")
    corp = corpus_mod.get_corpus("measles_bal")
    texts = ["a claim", "b claim"]
    sources = [[{"task_id": "t0:0", "claim_index": 0, "row_id": "t0:0:0"}] for _ in texts]
    _write_run(corp.run_dir("bge"), texts, sources)
    exp = corp.experiment_dir("bge", "cluster_filt")
    exp.mkdir(parents=True)
    np.save(exp / "labels_z.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(
        exp / "result_z.json",
        {"run_dir": str(corp.run_dir("bge")), "selection": "filter:stance", "algorithm": "kmeans"},
    )
    with pytest.raises(ValueError, match="live --filter"):
        cluster_browse.load_browse_bundle(exp)


def test_flat_cluster_output(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = ["one", "two", "three"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(3)]
    _write_run(corp.run_dir("m"), texts, sources)
    exp = corp.experiment_dir("m", "cluster_x")
    exp.mkdir(parents=True)
    np.save(exp / "labels_q.npy", np.asarray([0, 0, 1], dtype=int))
    claims_io.write_json(exp / "result_q.json", {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans"})
    bundle = cluster_browse.load_browse_bundle(exp)
    assert {c.cluster_id: c.size for c in bundle.clusters} == {0: 2, 1: 1}
    assert all(c.mean_intra_cosine is not None for c in bundle.clusters)
    assert bundle.clusters[0].medoid_text in {"one", "two"}
    assert len(cluster_browse.members_for_cluster(bundle, 0)) == 2


def test_cluster_browse_cli_requires_from_or_labels(capsys):
    from argparse import Namespace

    from apps.claims.cli.cluster_browse_cmd import cmd_cluster_browse

    rc = cmd_cluster_browse(Namespace(from_path=None, labels=None))
    assert rc == 1
    assert "Provide --from" in capsys.readouterr().out


def test_parse_app_args_keeps_from_path():
    from apps.claims.cluster_browser_app import parse_app_args

    args = parse_app_args(["--from", "apps/claims/data/exp"])
    assert args.from_path == Path("apps/claims/data/exp")


def _unit(v) -> np.ndarray:
    a = np.asarray(v, dtype=np.float32)
    return a / np.linalg.norm(a)


def _blob(center: list[float], n: int, jitter: float = 0.04) -> np.ndarray:
    rng = np.random.default_rng(0)
    c = np.asarray(center, dtype=np.float32)
    rows = []
    for _ in range(n):
        rows.append(_unit(c + rng.normal(0, jitter, size=c.shape).astype(np.float32)))
    return np.stack(rows)


def test_n_clusters_from_frac():
    from apps.claims.cli.cluster_cmd import apply_n_clusters_frac, n_clusters_from_frac

    assert n_clusters_from_frac(100, 0.02) == 2
    assert n_clusters_from_frac(93871, 0.02) == 1877
    params = apply_n_clusters_frac(
        {"n_clusters": 800, "reduce": "none"}, n=100, frac=0.02, algorithm="kmeans"
    )
    assert params["n_clusters"] == 2
    with pytest.raises(ValueError, match="does not apply"):
        apply_n_clusters_frac({}, n=100, frac=0.02, algorithm="dbscan")


def test_leaf_nn_and_min_n_filter(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = [f"claim {i}" for i in range(9)]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(9)]
    vectors = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3),
            _blob([0.92, 0.39, 0.0], 3),
            _blob([0.0, 0.0, 1.0], 3),
        ]
    )
    _write_run(corp.run_dir("m"), texts, sources, vectors=vectors)
    exp = corp.experiment_dir("m", "cluster_nn")
    exp.mkdir(parents=True)
    np.save(exp / "labels_q.npy", np.asarray([0, 0, 0, 1, 1, 1, 2, 2, 2], dtype=int))
    claims_io.write_json(exp / "result_q.json", {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans"})
    bundle = cluster_browse.load_browse_bundle(exp)
    by_id = {c.cluster_id: c for c in bundle.clusters}
    assert by_id[0].nn_id == 1
    assert by_id[1].nn_id == 0
    assert by_id[0].nn_cosine is not None and by_id[2].nn_cosine is not None
    assert by_id[0].nn_cosine > by_id[2].nn_cosine
    ranked = cluster_browse.neighbors_for_cluster(bundle, 0, k=2)
    assert ranked[0][0].cluster_id == 1
    tight = cluster_browse.filter_sort_clusters(bundle.clusters, min_n=3, sort_by="intra")
    assert {c.cluster_id for c in tight} == {0, 1, 2}
    tiny = cluster_browse.filter_sort_clusters(bundle.clusters, min_n=10, sort_by="n")
    assert tiny == []


def test_twomeans_split_and_member_paint(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = [f"claim {i}" for i in range(6)]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(6)]
    vectors = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3, jitter=0.02),
            _blob([0.0, 1.0, 0.0], 3, jitter=0.02),
        ]
    )
    _write_run(corp.run_dir("m"), texts, sources, vectors=vectors)
    exp = corp.experiment_dir("m", "cluster_split")
    exp.mkdir(parents=True)
    np.save(exp / "labels_q.npy", np.asarray([0, 0, 0, 0, 0, 0], dtype=int))
    claims_io.write_json(exp / "result_q.json", {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans"})
    bundle = cluster_browse.load_browse_bundle(exp)
    row = bundle.clusters[0]
    assert row.size == 6
    assert row.split_gain is not None
    assert row.split_gain > 0
    assert row.child_sizes is not None
    assert sorted(row.child_sizes) == [3, 3]
    members = cluster_browse.members_for_cluster(bundle, 0)
    assert len(members) == 6
    children = {m.twomeans_child for m in members}
    assert children == {0, 1}
    cosines = [m.cosine_to_medoid for m in members]
    assert all(c is not None for c in cosines)
    assert cosines == sorted(cosines, reverse=True)


def test_twomeans_skipped_for_tiny_leaves(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = ["one", "two", "three"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(3)]
    _write_run(corp.run_dir("m"), texts, sources)
    exp = corp.experiment_dir("m", "cluster_tiny")
    exp.mkdir(parents=True)
    np.save(exp / "labels_q.npy", np.asarray([0, 0, 1], dtype=int))
    claims_io.write_json(exp / "result_q.json", {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans"})
    bundle = cluster_browse.load_browse_bundle(exp)
    by_id = {c.cluster_id: c for c in bundle.clusters}
    assert by_id[0].size == 2
    assert by_id[0].split_gain is None
    assert by_id[1].size == 1
    assert by_id[1].mean_intra_cosine == 1.0
    assert by_id[1].split_gain is None
    kept = cluster_browse.filter_sort_clusters(bundle.clusters, min_n=3, sort_by="intra")
    assert kept == []
    kept2 = cluster_browse.filter_sort_clusters(bundle.clusters, min_n=2, sort_by="n")
    assert [c.cluster_id for c in kept2] == [0]


def test_recursive_twomeans_separates_blobs():
    texts = [f"claim {i}" for i in range(9)]
    vectors = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3, jitter=0.02),
            _blob([0.0, 1.0, 0.0], 3, jitter=0.02),
            _blob([0.0, 0.0, 1.0], 3, jitter=0.02),
        ]
    )
    idx = np.arange(9)
    tree = cluster_browse.recursive_twomeans(vectors, idx, texts, min_gain=0.0, min_child_n=2)
    stats = cluster_browse.split_tree_stats(tree)
    assert stats["n_splits"] >= 2
    assert stats["n_terminals"] == 3
    sizes = sorted(t.size for t in stats["terminals"])
    assert sizes == [3, 3, 3]
    paths = {t.path for t in stats["terminals"]}
    assert "root" not in paths
    pre = list(cluster_browse.iter_split_preorder(tree))
    assert pre[0].path == "root"
    assert pre[0].children is not None


def test_recursive_twomeans_respects_min_gain_and_small_n():
    texts = ["a", "b"]
    vectors = np.eye(2, dtype=np.float32)
    tree = cluster_browse.recursive_twomeans(vectors, np.arange(2), texts, min_gain=0.0)
    assert tree.children is None
    assert tree.stop_reason == "n<3"

    texts6 = [f"c{i}" for i in range(6)]
    vectors6 = np.vstack(
        [
            _blob([1.0, 0.0, 0.0], 3, jitter=0.02),
            _blob([0.0, 1.0, 0.0], 3, jitter=0.02),
        ]
    )
    blocked = cluster_browse.recursive_twomeans(
        vectors6, np.arange(6), texts6, min_gain=10.0
    )
    assert blocked.children is None
    assert "gain" in blocked.stop_reason


def test_leaf_alignment_from_extract_scores(isolated_data: Path):
    corpus_mod.create_corpus("c")
    corp = corpus_mod.get_corpus("c")
    texts = ["anti claim", "also anti", "pro claim"]
    sources = [
        [
            {"task_id": "t0:0", "claim_index": 0, "row_id": "t0:0:0"},
            {"task_id": "t0b:0", "claim_index": 0, "row_id": "t0b:0:0"},
        ],
        [{"task_id": "t1:0", "claim_index": 0, "row_id": "t1:0:0"}],
        [{"task_id": "t2:0", "claim_index": 0, "row_id": "t2:0:0"}],
    ]
    _write_run(corp.run_dir("m"), texts, sources)
    _write_claims(
        corp.claims,
        [
            _post("t0:0", "chunk anti a", texts[0], alignment=0.0),
            _post("t0b:0", "chunk anti a2", texts[0], alignment=0.5),
            _post("t1:0", "chunk anti b", texts[1], alignment=0.25),
            _post("t2:0", "chunk pro", texts[2], alignment=1.0),
        ],
    )
    exp = corp.experiment_dir("m", "cluster_align")
    exp.mkdir(parents=True)
    np.save(exp / "labels_q.npy", np.asarray([0, 0, 1], dtype=int))
    claims_io.write_json(exp / "result_q.json", {"run_dir": str(corp.run_dir("m")), "algorithm": "kmeans"})
    bundle = cluster_browse.load_browse_bundle(exp)
    occ = cluster_browse.load_occurrence_index(corp.claims)
    cluster_browse.attach_leaf_alignment(bundle, occ)
    by_id = {c.cluster_id: c for c in bundle.clusters}
    # group 0: mean(0.0, 0.5) = 0.25; group 1: 0.25 → leaf 0 mean 0.25
    assert by_id[0].mean_alignment == 0.25
    assert by_id[0].frac_anti == 1.0
    assert by_id[1].mean_alignment == 1.0
    assert by_id[1].frac_anti == 0.0
    anti = cluster_browse.filter_sort_clusters(
        bundle.clusters, min_n=1, sort_by="alignment", max_alignment=0.33
    )
    assert [c.cluster_id for c in anti] == [0]
    members = cluster_browse.members_for_cluster(bundle, 0)
    aligns = {m.claim_text: m.alignment for m in members}
    assert aligns["anti claim"] == 0.25
    assert aligns["also anti"] == 0.25
