"""Tests for corpus create/list and path resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims.__main__ import main


@pytest.fixture()
def isolated_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def test_create_and_list(isolated_data: Path):
    assert main(["corpus", "create", "--name", "measles", "--notes", "measles dump"]) == 0
    corpus = corpus_mod.get_corpus("measles")
    assert corpus.root.is_dir()
    assert corpus.notes.is_file()
    assert "measles" in corpus.notes.read_text(encoding="utf-8").casefold()

    assert main(["corpus", "list"]) == 0
    rows = corpus_mod.list_corpora()
    assert len(rows) == 1
    assert rows[0]["slug"] == "measles"
    assert rows[0]["posts"]["exists"] is False


def test_create_rejects_bad_slug(isolated_data: Path):
    assert main(["corpus", "create", "--name", "Measles!"]) == 1
    assert main(["corpus", "create", "--name", "measles"]) == 0
    assert main(["corpus", "create", "--name", "measles"]) == 1  # exists


def test_group_with_corpus(isolated_data: Path):
    assert main(["corpus", "create", "--name", "covid"]) == 0
    corpus = corpus_mod.get_corpus("covid")
    posts = {
        "posts": [
            {
                "task_id": "1",
                "claim_extraction_status": "success",
                "claim_extraction_output": {"claims": [{"claim": "X"}, {"claim": "x"}]},
            }
        ]
    }
    corpus.claims.write_text(json.dumps(posts), encoding="utf-8")
    assert main(["group", "--corpus", "covid"]) == 0
    assert corpus.groups.is_file()
    data = json.loads(corpus.groups.read_text(encoding="utf-8"))
    assert data["claim_count"] == 1


def test_run_name_and_experiment_paths(isolated_data: Path):
    corpus = corpus_mod.create_corpus("mpox")
    assert corpus.run_name("bge-large") == "mpox__bge-large"
    assert corpus.run_dir("bge-large") == claims_io.runs_dir() / "mpox__bge-large"
    exp = corpus.experiment_dir("bge-large", "hier_k800")
    assert exp == claims_io.experiments_dir() / "mpox__bge-large" / "hier_k800"
    assert corpus_mod.model_tag_from_path("/models/bge-large-500trips") == "bge-large-500trips"


def test_corpus_seed_dry_run(isolated_data: Path):
    assert (
        main(
            [
                "corpus",
                "seed",
                "--name",
                "measles",
                "--create",
                "--terms",
                "measles",
                "mmr vaccine",
                "--since",
                "2024-01-01",
                "--until",
                "2025-01-01",
                "--dry-run",
            ]
        )
        == 0
    )
    corpus = corpus_mod.get_corpus("measles")
    assert corpus.root.is_dir()
    assert not corpus.posts.is_file()  # dry-run does not write


def test_corpus_seed_mocked_fetch(isolated_data: Path, monkeypatch: pytest.MonkeyPatch):
    def fake_fetch(**kwargs):
        out = Path(kwargs["out_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text('{"posts":[{"post_id":1,"text":"x"}],"post_count":1}\n', encoding="utf-8")
        return {
            "out": str(out),
            "post_count": 1,
            "matched_post_count": 1,
            "terms": list(kwargs["terms"]),
            "since": kwargs["since"].isoformat() if kwargs.get("since") else None,
            "until": kwargs["until"].isoformat() if kwargs.get("until") else None,
            "use_prod": bool(kwargs.get("use_prod")),
        }

    monkeypatch.setattr(
        "scripts.get_posts_for_search_term.fetch_and_write",
        fake_fetch,
    )
    assert (
        main(
            [
                "corpus",
                "seed",
                "--name",
                "covid",
                "--create",
                "--terms",
                "covid",
                "--since",
                "2020-01-01",
                "--until",
                "2021-01-01",
            ]
        )
        == 0
    )
    corpus = corpus_mod.get_corpus("covid")
    assert corpus.posts.is_file()
    assert "Seeded" in corpus.notes.read_text(encoding="utf-8")
