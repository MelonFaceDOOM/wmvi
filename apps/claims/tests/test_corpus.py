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
                "post_id": 1,
                "platform": "reddit_submission",
                "text": "body",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "text": "chunk",
                        "task_id": "1",
                        "claim_extraction_disposition": "success",
                        "claims": [{"claim": "X"}, {"claim": "x"}],
                    }
                ],
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
    assert corpus.run_name("bge-large") == "mpox/bge-large"
    assert corpus.run_dir("bge-large") == claims_io.runs_dir() / "mpox" / "bge-large"
    exp = corpus.experiment_dir("bge-large", "hier_k800")
    assert exp == claims_io.clustering_experiments_dir() / "mpox" / "bge-large" / "hier_k800"
    assert corpus_mod.model_tag_from_path("/models/bge-large-500trips") == "bge-large-500trips"


def test_layout_path_helpers(isolated_data: Path):
    assert claims_io.corpora_dir() == claims_io.data_root() / "corpora"
    assert claims_io.fixtures_dir() == claims_io.data_root() / "fixtures"
    assert claims_io.registered_models_dir() == claims_io.data_root() / "models" / "registered"
    assert claims_io.clustering_experiments_dir() == claims_io.data_root() / "experiments" / "clustering"
    claims_io.ensure_data_dirs()
    assert claims_io.registered_models_dir().is_dir()
    assert claims_io.corpora_dir().is_dir()
    corpus = corpus_mod.create_corpus("layout")
    assert corpus.root == claims_io.corpora_dir() / "layout"

def test_corpus_import_claims(isolated_data: Path, tmp_path: Path):
    src = tmp_path / "nested.json"
    src.write_text(
        json.dumps(
            {
                "posts": [
                    {
                        "post_id": 1,
                        "chunks": [
                            {
                                "task_id": "1:0",
                                "claim_extraction_disposition": "success",
                                "claims": [{"claim": "Hello"}],
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert (
        main(
            [
                "corpus",
                "import-claims",
                "--name",
                "measles",
                "--create",
                "--from",
                str(src),
            ]
        )
        == 0
    )
    corpus = corpus_mod.get_corpus("measles")
    assert corpus.claims.is_file()
    status = corpus.status()
    assert status["claims"]["exists"] is True
    assert status["claims"]["claim_count"] == 1
    assert status["stages"]["claims"] is True
    assert status["stages"]["grouped"] is False
    assert status["stages"]["embedded"] is False
    assert status["stages"]["clustered"] is False
    assert status["stages"]["n_runs"] == 0
    assert status["stages"]["n_experiments"] == 0
    assert status["stages"]["claim_count"] == 1
    assert "Imported claims" in corpus.notes.read_text(encoding="utf-8")


def test_corpus_status_stages_after_group(isolated_data: Path, capsys):
    assert main(["corpus", "create", "--name", "toy"]) == 0
    corpus = corpus_mod.get_corpus("toy")
    corpus.claims.write_text(
        json.dumps(
            {
                "posts": [
                    {
                        "post_id": 1,
                        "chunks": [
                            {
                                "task_id": "t1",
                                "claim_extraction_disposition": "success",
                                "claims": [{"claim": "A claim"}],
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert main(["group", "--corpus", "toy"]) == 0
    assert main(["corpus", "status", "--name", "toy", "--human"]) == 0
    out = capsys.readouterr().out
    status = corpus.status()
    assert status["stages"]["claims"] is True
    assert status["stages"]["grouped"] is True
    assert status["stages"]["group_count"] == 1
    assert "grouped:   yes" in out
    assert "embedded:  no" in out
    assert "clustered: no" in out

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


def test_corpus_derive_reddit_claim_balance(isolated_data: Path, capsys):
    """Downsample reddit posts so reddit claims ≈ other claims."""
    assert main(["corpus", "create", "--name", "src"]) == 0
    corpus = corpus_mod.get_corpus("src")
    posts = []
    # 4 other posts × 2 claims = 8 other claims
    for i in range(4):
        posts.append(
            {
                "post_id": 100 + i,
                "platform": "telegram_post",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "task_id": f"{100 + i}:0",
                        "claims": [
                            {"claim": f"other claim {i}a"},
                            {"claim": f"other claim {i}b"},
                        ],
                    }
                ],
            }
        )
    # 10 reddit posts × 2 claims = 20 reddit claims (will keep ~8)
    for i in range(10):
        posts.append(
            {
                "post_id": 200 + i,
                "platform": "reddit_comment",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "task_id": f"{200 + i}:0",
                        "claims": [
                            {"claim": f"reddit claim {i}a"},
                            {"claim": f"reddit claim {i}b"},
                        ],
                    }
                ],
            }
        )
    claims_io.write_json(corpus.claims, {"posts": posts, "post_count": len(posts)})
    capsys.readouterr()  # drop create/status JSON

    assert (
        main(
            [
                "corpus",
                "derive",
                "--from",
                "src",
                "--name",
                "src_bal",
                "--seed",
                "0",
                "--group",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert payload["ok"] is True
    assert payload["other_claims"] == 8
    assert abs(payload["reddit_claims_kept"] - 8) <= 2
    assert payload["grouped"] is True
    dst = corpus_mod.get_corpus("src_bal")
    assert dst.claims.is_file()
    assert dst.groups.is_file()
    derived = claims_io.read_json(dst.claims)
    assert derived["derived"]["from_corpus"] == "src"
    assert derived["derived"]["method"] == "reddit_downsample_to_other_claims"
    # No non-reddit posts dropped
    out_posts = derived["posts"]
    assert sum(1 for p in out_posts if p["platform"] == "telegram_post") == 4
    assert "Derived" in dst.notes.read_text(encoding="utf-8")
