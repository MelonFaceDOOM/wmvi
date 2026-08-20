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
                "claims": [{"claim": claim}],
            }
        ],
    }
    post.update(extra)
    return post


def _write_run(run_dir: Path, texts: list[str], sources: list[list[dict]]) -> list[str]:
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
    np.save(run_dir / "vectors.npy", np.eye(len(texts), dtype=np.float32))
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


def test_hierarchy_browse_with_selection(isolated_data: Path):
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

    exp = corp.experiment_dir("bge", "hierarchy_standalone_ok_x")
    exp.mkdir(parents=True)
    # labels aligned to selection subset (index order of selected keys: 0, 1)
    np.save(exp / "leaf_labels_abc.npy", np.asarray([0, 1], dtype=int))
    np.save(exp / "narrative_labels_abc.npy", np.asarray([7, 7], dtype=int))
    claims_io.write_json(
        exp / "hierarchy_abc.json",
        {
            "run_dir": str(corp.run_dir("bge")),
            "selection": "selection:standalone_ok",
            "n_selected": 2,
            "preset": "default",
            "narratives": [
                {
                    "narrative_id": 7,
                    "n_leaves": 2,
                    "size": 2,
                    "leaves": [
                        {
                            "leaf_id": 0,
                            "size": 1,
                            "mean_intra_cosine": 0.91,
                            "medoid_idx": 0,
                            "medoid_claim_text": texts[0],
                            "sample_claim_texts": [texts[0]],
                        },
                        {
                            "leaf_id": 1,
                            "size": 1,
                            "mean_intra_cosine": 0.88,
                            "medoid_idx": 1,
                            "medoid_claim_text": texts[1],
                            "sample_claim_texts": [texts[1]],
                        },
                    ],
                }
            ],
        },
    )

    discovered = cluster_browse.discover_cluster_output(exp)
    assert discovered.kind == "hierarchy"
    assert discovered.hint.selection == "standalone_ok"

    bundle = cluster_browse.load_browse_bundle(exp)
    assert bundle.labels.shape == (2,)
    assert bundle.applied_selection == "standalone_ok"
    assert bundle.corpus == "measles_bal"
    assert len(bundle.narratives or []) == 1
    assert bundle.narratives[0].cluster_id == 7
    members = cluster_browse.members_for_cluster(bundle, 0, level="leaf")
    assert len(members) == 1
    assert members[0].claim_text == texts[0]
    occ = cluster_browse.load_occurrence_index(corp.claims)
    hits = cluster_browse.occurrences_for_member(members[0], occ)
    assert hits[0]["found"] is True
    assert hits[0]["platform"] == "reddit_submission"
    assert hits[0]["primary_metric"] == 12
    assert "mmr" in hits[0]["chunk_text"]


def test_labels_mismatch_without_selection(isolated_data: Path):
    corpus_mod.create_corpus("measles_bal")
    corp = corpus_mod.get_corpus("measles_bal")
    texts = ["a claim", "b claim", "c claim"]
    sources = [[{"task_id": f"t{i}:0", "claim_index": 0, "row_id": f"t{i}:0:0"}] for i in range(3)]
    _write_run(corp.run_dir("bge"), texts, sources)
    exp = corp.experiment_dir("bge", "hier")
    exp.mkdir(parents=True)
    np.save(exp / "leaf_labels_x.npy", np.asarray([0, 1], dtype=int))
    claims_io.write_json(exp / "hierarchy_x.json", {"run_dir": str(corp.run_dir("bge"))})
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
    assert bundle.output.kind == "flat"
    assert bundle.narratives is None
    assert {c.cluster_id: c.size for c in bundle.clusters} == {0: 2, 1: 1}
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
