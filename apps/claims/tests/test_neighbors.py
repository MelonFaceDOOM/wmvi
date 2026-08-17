"""Tests for neighbors CLI and triplet_neighbors helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims.__main__ import main
from apps.claims.embedding.triplet_neighbors import (
    format_neighbors_list,
    neighbors_for_claim_index,
    neighbors_for_query_text,
    neighbors_for_query_vector,
)


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def _write_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True)
    vectors = np.array(
        [
            [1.0, 0.0],
            [0.9, 0.1],
            [0.0, 1.0],
            [0.1, 0.9],
        ],
        dtype=np.float32,
    )
    # L2-normalize so dots ≈ cosine
    vectors = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    np.save(run_dir / "vectors.npy", vectors)
    (run_dir / "index.json").write_text(
        json.dumps(
            {
                "claim_texts": ["anchor", "close", "far", "also_far"],
                "model_id": "dummy-model",
                "query_instruction": "",
                "normalize": True,
            }
        ),
        encoding="utf-8",
    )


def test_neighbors_for_claim_index_excludes_self():
    vectors = np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]], dtype=np.float32)
    texts = ["anchor", "close", "far"]
    neighbors = neighbors_for_claim_index(0, vectors=vectors, claim_texts=texts, top_k=2)
    assert len(neighbors) == 2
    assert all(i != 0 for i, _, _ in neighbors)
    assert neighbors[0][0] == 1


def test_neighbors_for_query_vector_excludes_exact_text():
    vectors = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    texts = ["q", "other"]
    out = neighbors_for_query_vector(
        np.array([1.0, 0.0], dtype=np.float32),
        vectors=vectors,
        claim_texts=texts,
        top_k=2,
        exclude_text="q",
    )
    assert len(out) == 1
    assert out[0][0] == 1


def test_neighbors_for_query_text_uses_embed_query():
    vectors = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    texts = ["a", "b"]
    with patch(
        "apps.claims.clustering.query_eval.embed_query",
        return_value=np.array([0.0, 1.0], dtype=np.float32),
    ) as mock_embed:
        out = neighbors_for_query_text(
            "hello",
            vectors=vectors,
            claim_texts=texts,
            model_id="m",
            top_k=1,
        )
    mock_embed.assert_called_once()
    assert out[0][0] == 1


def test_format_neighbors_list():
    out = format_neighbors_list([(3, 0.88, "claim one")])
    assert "score=0.8800" in out
    assert "claim one" in out


def test_cli_neighbors_claim_index(isolated_data: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    _write_run(run_dir)

    assert (
        main(
            [
                "neighbors",
                "--corpus",
                "measles",
                "--model-tag",
                "bge-large",
                "--claim-index",
                "0",
                "--top-k",
                "2",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out.strip().splitlines()[0]
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["mode"] == "claim_index"
    assert payload["n_results"] == 1
    assert payload["results"][0]["anchor"]["idx"] == 0
    assert len(payload["results"][0]["neighbors"]) == 2
    assert all(n["idx"] != 0 for n in payload["results"][0]["neighbors"])


def test_cli_neighbors_sample_and_human(isolated_data: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    _write_run(run_dir)

    assert (
        main(
            [
                "neighbors",
                "--corpus",
                "measles",
                "--model-tag",
                "bge-large",
                "--sample",
                "2",
                "--seed",
                "0",
                "--top-k",
                "1",
                "--human",
            ]
        )
        == 0
    )
    captured = capsys.readouterr().out
    payload = json.loads(captured.splitlines()[0])
    assert payload["mode"] == "sample"
    assert payload["n_results"] == 2
    assert "Anchor [idx=" in captured
    assert "Neighbors:" in captured


def test_cli_neighbors_text(isolated_data: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    _write_run(run_dir)

    with patch(
        "apps.claims.clustering.query_eval.embed_query",
        return_value=np.array([1.0, 0.0], dtype=np.float32),
    ):
        assert (
            main(
                [
                    "neighbors",
                    "--run-dir",
                    str(run_dir),
                    "--text",
                    "query text",
                    "--top-k",
                    "2",
                ]
            )
            == 0
        )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["mode"] == "text"
    assert payload["results"][0]["anchor"]["idx"] is None
    assert payload["results"][0]["anchor"]["text"] == "query text"
    assert payload["results"][0]["neighbors"]


def test_cli_neighbors_bad_index(isolated_data: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    _write_run(run_dir)
    assert (
        main(
            [
                "neighbors",
                "--corpus",
                "measles",
                "--model-tag",
                "bge-large",
                "--claim-index",
                "99",
            ]
        )
        == 1
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert "error" in payload
