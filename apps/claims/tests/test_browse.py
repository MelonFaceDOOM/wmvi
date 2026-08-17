"""Tests for browse CLI and claim_sample helpers."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from apps.claims import claim_sample
from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import keys as keys_mod
from apps.claims.__main__ import main


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def _write_run(run_dir: Path, texts: list[str] | None = None) -> list[str]:
    run_dir.mkdir(parents=True)
    texts = texts or ["anchor", "close", "far", "also_far"]
    keys = [keys_mod.claim_key(t) for t in texts]
    vectors = np.eye(len(texts), dtype=np.float32)
    if vectors.shape[1] < 2:
        vectors = np.ones((len(texts), 2), dtype=np.float32)
    np.save(run_dir / "vectors.npy", vectors)
    (run_dir / "index.json").write_text(
        json.dumps(
            {
                "claim_texts": texts,
                "claim_keys": keys,
                "model_id": "dummy-model",
            }
        ),
        encoding="utf-8",
    )
    return keys


def test_load_exclude_jsonl_objects(tmp_path: Path):
    p = tmp_path / "done.jsonl"
    p.write_text(
        json.dumps({"claim_key": "abc", "label": "yes"}) + "\n"
        + json.dumps({"k": "def"}) + "\n",
        encoding="utf-8",
    )
    assert claim_sample.load_exclude_claim_keys(p) == {"abc", "def"}


def test_load_exclude_plain_keys(tmp_path: Path):
    p = tmp_path / "keys.txt"
    p.write_text("aaa\n# comment\nbbb\n", encoding="utf-8")
    assert claim_sample.load_exclude_claim_keys(p) == {"aaa", "bbb"}


def test_load_exclude_json_list(tmp_path: Path):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps(["x", "y"]), encoding="utf-8")
    assert claim_sample.load_exclude_claim_keys(p) == {"x", "y"}


def test_sample_claim_indices_excludes_keys():
    texts = ["a", "b", "c", "d"]
    keys = [keys_mod.claim_key(t) for t in texts]
    excl = {keys[0], keys[1]}
    picked = claim_sample.sample_claim_indices(
        texts, n=2, seed=0, claim_keys=keys, exclude_keys=excl
    )
    assert len(picked) == 2
    assert all(keys[i] not in excl for i in picked)


def _write_groups(corpus_name: str, texts: list[str]) -> list[str]:
    corpus = corpus_mod.create_corpus(corpus_name)
    keys = [keys_mod.claim_key(t) for t in texts]
    payload = {
        "source_path": "test",
        "source_hash": "abc",
        "source_claim_count": len(texts),
        "claim_count": len(texts),
        "groups": [
            {
                "group_id": i,
                "claim_key": keys[i],
                "claim_text": texts[i],
                "count": 1,
                "sources": [],
            }
            for i in range(len(texts))
        ],
    }
    claims_io.write_json(corpus.groups, payload)
    return keys


def test_cli_browse_sample_and_exclude(isolated_data: Path, tmp_path: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    keys = _write_run(run_dir)
    excl = tmp_path / "done.jsonl"
    excl.write_text(
        json.dumps({"claim_key": keys[0], "label": "no"}) + "\n"
        + json.dumps({"claim_key": keys[1], "label": "yes"}) + "\n",
        encoding="utf-8",
    )

    assert (
        main(
            [
                "browse",
                "--corpus",
                "measles",
                "--model-tag",
                "bge-large",
                "--sample",
                "2",
                "--seed",
                "0",
                "--exclude",
                str(excl),
                "--human",
            ]
        )
        == 0
    )
    captured = capsys.readouterr().out
    payload = json.loads(captured.splitlines()[0])
    assert payload["ok"] is True
    assert payload["source"] == "run"
    assert payload["run_dir"] is not None
    assert payload["groups_path"] is None
    assert payload["n_excluded"] == 2
    assert payload["n_returned"] == 2
    returned_keys = {c["claim_key"] for c in payload["claims"]}
    assert keys[0] not in returned_keys
    assert keys[1] not in returned_keys
    assert "key=" in captured


def test_cli_browse_from_groups_without_run(isolated_data: Path, tmp_path: Path, capsys):
    texts = ["alpha claim", "beta claim", "gamma claim", "delta claim"]
    keys = _write_groups("resp", texts)
    excl = tmp_path / "labels.jsonl"
    excl.write_text(
        json.dumps({"claim_key": keys[0], "value": 1, "claim_text": texts[0]}) + "\n"
        + json.dumps({"claim_key": keys[1], "value": 0, "claim_text": texts[1]}) + "\n",
        encoding="utf-8",
    )

    assert (
        main(
            [
                "browse",
                "--corpus",
                "resp",
                "--sample",
                "2",
                "--seed",
                "0",
                "--exclude",
                str(excl),
                "--human",
            ]
        )
        == 0
    )
    captured = capsys.readouterr().out
    payload = json.loads(captured.splitlines()[0])
    assert payload["ok"] is True
    assert payload["source"] == "groups"
    assert payload["run_dir"] is None
    assert payload["groups_path"] is not None
    assert payload["n_excluded"] == 2
    assert payload["n_returned"] == 2
    returned_keys = {c["claim_key"] for c in payload["claims"]}
    assert keys[0] not in returned_keys
    assert keys[1] not in returned_keys
    assert returned_keys <= set(keys[2:])


def test_cli_browse_missing_groups_errors(isolated_data: Path, capsys):
    corpus_mod.create_corpus("emptycorp")
    assert main(["browse", "--corpus", "emptycorp", "--sample", "1"]) == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert "error" in payload
    assert "groups.json" in payload["error"]


def test_cli_browse_where_annotation_filters(isolated_data: Path, capsys):
    from apps.claims import annotations as ann_mod

    texts = ["low claim", "mid claim", "high claim", "also high"]
    keys = _write_groups("resp", texts)
    corpus = corpus_mod.get_corpus("resp")
    ann_mod.write_annotation(
        corpus.root,
        "stance",
        {
            keys[0]: 0.1,
            keys[1]: 0.5,
            keys[2]: 0.9,
            keys[3]: 0.95,
        },
        producer="test",
        scope="group",
        value_type="float",
        source_hash="abc",
    )

    assert (
        main(
            [
                "browse",
                "--corpus",
                "resp",
                "--sample",
                "10",
                "--seed",
                "0",
                "--where-annotation",
                "stance",
                "--low",
                "0.875",
                "--high",
                "1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["n_pool"] == 2
    assert payload["n_returned"] == 2
    assert payload["filter"]["annotation"] == "stance"
    assert {c["claim_key"] for c in payload["claims"]} == {keys[2], keys[3]}

    assert (
        main(
            [
                "browse",
                "--corpus",
                "resp",
                "--sample",
                "10",
                "--seed",
                "0",
                "--where-annotation",
                "stance",
                "--low",
                "0",
                "--high",
                "0.2",
            ]
        )
        == 0
    )
    payload_lo = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload_lo["n_pool"] == 1
    assert payload_lo["claims"][0]["claim_key"] == keys[0]


def test_cli_browse_filter_and_quality_stance(isolated_data: Path, capsys):
    from apps.claims import annotations as ann_mod

    texts = ["junk anti", "good anti", "good pro", "junk pro"]
    keys = _write_groups("resp", texts)
    corpus = corpus_mod.get_corpus("resp")
    ann_mod.write_annotation(
        corpus.root,
        "quality",
        {keys[0]: 0.1, keys[1]: 0.9, keys[2]: 0.85, keys[3]: 0.2},
        producer="test",
        value_type="float",
    )
    ann_mod.write_annotation(
        corpus.root,
        "stance",
        {keys[0]: 0.2, keys[1]: 0.2, keys[2]: 0.9, keys[3]: 0.95},
        producer="test",
        value_type="float",
    )

    assert (
        main(
            [
                "browse",
                "--corpus",
                "resp",
                "--sample",
                "10",
                "--seed",
                "0",
                "--filter",
                "quality:low=0.5",
                "--filter",
                "stance:low=0.875,high=1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["n_pool"] == 1
    assert payload["filter"]["op"] == "and"
    assert payload["claims"][0]["claim_key"] == keys[2]


def test_cli_browse_filter_and_selection(isolated_data: Path, capsys):
    """Regression: resolve_keys_for_args AND-s --filter with --selection."""
    from apps.claims import annotations as ann_mod
    from apps.claims import selections as sel_mod

    texts = ["a", "b", "c", "d"]
    keys = _write_groups("resp", texts)
    corpus = corpus_mod.get_corpus("resp")
    ann_mod.write_annotation(
        corpus.root,
        "standalone",
        {keys[0]: 1.0, keys[1]: 1.0, keys[2]: 0.0, keys[3]: 1.0},
        producer="test",
        value_type="binary",
    )
    sel_mod.write_selection(
        corpus.root,
        sel_mod.Selection(
            name="keep_ab",
            scope="group",
            keys=[keys[0], keys[1]],
            from_annotation=None,
            predicate={},
            created_at="2026-01-01T00:00:00Z",
        ),
    )

    assert (
        main(
            [
                "browse",
                "--corpus",
                "resp",
                "--sample",
                "10",
                "--seed",
                "0",
                "--filter",
                "standalone:eq=1",
                "--selection",
                "keep_ab",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["n_pool"] == 2
    assert {c["claim_key"] for c in payload["claims"]} == {keys[0], keys[1]}
    assert payload["filter"]["selection"] == "keep_ab"
    assert payload["filter"]["annotation"] == "standalone"


def test_cli_neighbors_sample_exclude(isolated_data: Path, tmp_path: Path, capsys):
    corpus_mod.create_corpus("measles")
    run_dir = claims_io.runs_dir() / "measles" / "bge-large"
    keys = _write_run(run_dir)
    excl = tmp_path / "done.txt"
    excl.write_text(keys[0] + "\n", encoding="utf-8")

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
                "1",
                "--top-k",
                "1",
                "--exclude",
                str(excl),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["n_excluded"] == 1
    assert all(r["anchor"]["claim_key"] != keys[0] for r in payload["results"])
