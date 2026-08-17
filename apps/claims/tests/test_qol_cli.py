"""Tests for QoL CLI features (status, models, copy-posts, runs, presets, embed force)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import models as models_mod
from apps.claims.__main__ import main
from apps.claims.clustering.presets import get_hierarchy_preset


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def test_corpus_status_and_copy_posts(isolated_data: Path, tmp_path: Path):
    src = tmp_path / "posts_for_term.json"
    src.write_text(
        json.dumps({"terms": ["measles"], "posts": [{"post_id": 1, "text": "a"}, {"post_id": 2, "text": "b"}]}),
        encoding="utf-8",
    )
    assert main(["corpus", "copy-posts", "--name", "measles", "--from", str(src), "--create"]) == 0
    assert main(["corpus", "status", "--name", "measles"]) == 0
    st = corpus_mod.get_corpus("measles").status()
    assert st["posts"]["exists"] is True
    assert st["posts"]["post_count"] == 2
    # refuse overwrite
    assert main(["corpus", "copy-posts", "--name", "measles", "--from", str(src)]) == 1
    assert main(["corpus", "copy-posts", "--name", "measles", "--from", str(src), "--force"]) == 0


def test_model_register_and_resolve(isolated_data: Path, tmp_path: Path):
    fake = tmp_path / "fake_model"
    fake.mkdir()
    (fake / "config.json").write_text("{}", encoding="utf-8")
    assert main(["model", "register", "--path", str(fake), "--tag", "bge-large"]) == 0
    assert main(["model", "list"]) == 0
    resolved = models_mod.resolve_model("bge-large")
    assert Path(resolved).exists()
    assert main(["model", "resolve", "--model", "bge-large"]) == 0


def test_runs_list_filter(isolated_data: Path):
    corpus_mod.create_corpus("covid")
    run = claims_io.runs_dir() / "covid" / "bge"
    run.mkdir(parents=True)
    claims_io.write_json(run / "metrics.json", {"claim_count": 10, "vector_dim": 1024})
    other = claims_io.runs_dir() / "measles" / "bge"
    other.mkdir(parents=True)
    assert main(["runs", "list", "--corpus", "covid"]) == 0
    # smoke only — output on stdout


def test_hierarchy_preset_default():
    p = get_hierarchy_preset("default")
    assert p["leaf_algorithm"] == "kmeans"
    assert p["leaf_params"]["n_clusters"] == 800
    assert p["narrative_params"]["n_clusters"] == 25


def test_embed_refuses_overwrite(isolated_data: Path, tmp_path: Path):
    groups = {
        "source_hash": "abc",
        "source_path": "x",
        "source_claim_count": 1,
        "claim_count": 1,
        "groups": [{"group_id": 0, "claim_text": "hi", "count": 1, "sources": []}],
    }
    gpath = tmp_path / "groups.json"
    gpath.write_text(json.dumps(groups), encoding="utf-8")
    run = claims_io.runs_dir() / "t" / "m"
    run.mkdir(parents=True)
    (run / "vectors.npy").write_bytes(b"x")
    # Will fail before loading model because run exists
    rc = main(
        [
            "embed",
            "--groups",
            str(gpath),
            "--model",
            "dummy",
            "--run-name",
            "t/m",
        ]
    )
    assert rc == 1
