"""Smoke test for get_posts_extract_upload with mocked fetch/extract/upload."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from nlp.claim_extraction.batch import format_duration, format_eta
from scripts.get_posts_extract_upload import main, run_pipeline, run_smoke


def test_eta_from_completed_ratio():
    assert format_duration(0) == "0s"
    assert format_duration(45) == "45s"
    assert format_duration(90) == "1m 30s"
    assert format_duration(3661) == "1h 01m"
    assert format_eta(done=0, total=100, elapsed_s=10) == "unknown"
    assert format_eta(done=25, total=100, elapsed_s=100) == "5m 00s"
    assert format_eta(done=100, total=100, elapsed_s=50) == "0s"
    assert format_eta(done=10, total=None, elapsed_s=10) == "unknown"


def _sample_post(post_id: int) -> dict:
    return {
        "post_id": post_id,
        "platform": "reddit_submission",
        "text": "Measles can kill. Vaccination prevents severe outcomes.",
        "hits": [
            {
                "term_id": 1,
                "term_name": "measles",
                "match_start": 0,
                "match_end": 7,
            }
        ],
        "reddit_submission_title": "Title",
    }


def _fake_extract(**kwargs):
    import json

    chunks = json.loads(kwargs["posts_path"].read_text(encoding="utf-8"))
    rows = []
    for row in chunks["posts"]:
        rows.append(
            {
                **row,
                "task_id": f"{row['source_post_id']}:{row['sentence_boundary_chunk_index']}",
                "claim_extraction_disposition": "success",
                "claim_extraction_status": "success",
                "claim_extraction_output": {"claims": [{"claim": "Measles can cause death."}]},
                "claim_extraction_error": None,
            }
        )
        if kwargs.get("n_posts") == 1:
            break
    out = kwargs["out_path"]
    out.write_text(
        json.dumps({"posts": rows, "post_count": len(rows)}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_run_pipeline_mocked(tmp_path: Path):
    def fake_batches(*args, **kwargs):
        terms = args[0] if args else kwargs["terms"]
        assert terms == ["measles"]
        yield [_sample_post(1)]

    out = tmp_path / "nested.json"
    with (
        patch("scripts.get_posts_extract_upload.iter_post_batches", side_effect=fake_batches),
        patch("scripts.get_posts_extract_upload.extract_run", side_effect=_fake_extract),
        patch(
            "scripts.get_posts_extract_upload.matching_post_ids",
            return_value=[1],
        ),
    ):
        summary = run_pipeline(terms=["measles"], out_path=out, keep_work=True)

    assert out.is_file()
    assert summary["post_count"] == 1
    assert summary["chunk_count"] >= 1
    assert summary["claim_count"] >= 1
    assert summary["model"]
    work = tmp_path / ".nested_work" / "posts.json"
    assert work.is_file()
    import json

    saved = json.loads(work.read_text(encoding="utf-8"))
    assert saved["posts"][0]["post_id"] == 1


def test_run_pipeline_extracts_each_page(tmp_path: Path):
    """Each database slice is extracted on its own; pages are not concatenated first."""
    calls: list[int] = []

    def fake_batches(*args, **kwargs):
        yield [_sample_post(1)]
        yield [_sample_post(2)]

    def fake_extract(**kwargs):
        import json

        chunks = json.loads(kwargs["posts_path"].read_text(encoding="utf-8"))
        calls.append(chunks["post_count"])
        _fake_extract(**kwargs)

    out = tmp_path / "nested.json"
    with (
        patch("scripts.get_posts_extract_upload.iter_post_batches", side_effect=fake_batches),
        patch("scripts.get_posts_extract_upload.extract_run", side_effect=fake_extract),
        patch(
            "scripts.get_posts_extract_upload.matching_post_ids",
            return_value=[1, 2],
        ),
    ):
        summary = run_pipeline(terms=["measles"], out_path=out)

    import json

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert [p["post_id"] for p in payload["posts"]] == [1, 2]
    assert payload["post_count"] == 2
    assert summary["post_count"] == 2
    assert len(calls) == 2
    assert all(n >= 1 for n in calls)
    assert payload["chunk_count"] == summary["chunk_count"]
    assert payload["claim_count"] == summary["claim_count"]


def test_main_count_only():
    with patch(
        "scripts.get_posts_extract_upload.run_count_only",
        return_value={"matched_post_count": 42, "terms": ["measles"]},
    ) as count_fn:
        rc = main(["--terms", "measles", "--count-only", "--prod"])
    assert rc == 0
    count_fn.assert_called_once()
    assert count_fn.call_args.kwargs["use_prod"] is True


def test_run_smoke_mocked():
    posts = [
        {
            "post_id": 9,
            "platform": "reddit_submission",
            "text": "Measles can kill. Vaccination prevents severe outcomes.",
            "hits": [
                {
                    "term_id": 1,
                    "term_name": "measles",
                    "match_start": 0,
                    "match_end": 7,
                }
            ],
            "reddit_submission_title": "Title",
        }
    ]

    def fake_fetch(**kwargs):
        assert kwargs.get("limit") == 1
        out = kwargs["out_path"]
        from scripts.get_posts_for_search_term import write_posts_json

        write_posts_json(out, posts, terms=kwargs["terms"])
        return {"post_count": 1, "matched_post_count": 99, "terms": kwargs["terms"]}

    def fake_extract(**kwargs):
        import json

        assert kwargs.get("n_posts") == 1
        chunks = json.loads(kwargs["posts_path"].read_text(encoding="utf-8"))
        row = chunks["posts"][0]
        out = kwargs["out_path"]
        out.write_text(
            json.dumps(
                {
                    "posts": [
                        {
                            **row,
                            "task_id": f"{row['source_post_id']}:{row['sentence_boundary_chunk_index']}",
                            "claim_extraction_disposition": "success",
                            "claim_extraction_status": "success",
                            "claim_extraction_output": {
                                "claims": [{"claim": "Measles can cause death."}]
                            },
                            "claim_extraction_error": None,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

    with (
        patch("scripts.get_posts_extract_upload.fetch_and_write", side_effect=fake_fetch),
        patch("scripts.get_posts_extract_upload.extract_run", side_effect=fake_extract),
    ):
        summary = run_smoke(terms=["measles"])

    assert summary["ok"] is True
    assert summary["mode"] == "smoke"
    assert summary["sample"]["post_id"] == 9
    assert summary["sample"]["chunk"]["claims"][0]["claim"] == "Measles can cause death."
