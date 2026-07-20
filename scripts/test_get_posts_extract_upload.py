"""Smoke test for get_posts_extract_upload with mocked fetch/extract/upload."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from scripts.get_posts_extract_upload import main, run_pipeline, run_smoke


def test_run_pipeline_mocked(tmp_path: Path):
    posts = [
        {
            "post_id": 1,
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
        out = kwargs["out_path"]
        out.parent.mkdir(parents=True, exist_ok=True)
        from scripts.get_posts_for_search_term import write_posts_json

        write_posts_json(out, posts, terms=kwargs["terms"], since=kwargs.get("since"), until=kwargs.get("until"))
        return {"post_count": 1, "matched_post_count": 1, "terms": kwargs["terms"]}

    def fake_extract(**kwargs):
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

    out = tmp_path / "nested.json"
    with (
        patch("scripts.get_posts_extract_upload.fetch_and_write", side_effect=fake_fetch),
        patch("scripts.get_posts_extract_upload.extract_run", side_effect=fake_extract),
    ):
        summary = run_pipeline(terms=["measles"], out_path=out, keep_work=True)

    assert out.is_file()
    assert summary["post_count"] == 1
    assert summary["chunk_count"] >= 1
    assert summary["claim_count"] >= 1
    assert summary["model"]


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
