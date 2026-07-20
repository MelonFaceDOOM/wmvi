"""Tests for punct→trim→explode prep and nest rebuild."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from nlp.claim_extraction.nest import nest_posts_chunks_claims, write_nested_json
from nlp.claim_extraction.prep import iter_chunk_rows, prepare_and_explode, prepare_post


def _post(*, post_id: int = 1, text: str, hits: list | None = None) -> dict:
    return {
        "post_id": post_id,
        "platform": "reddit_submission",
        "text": text,
        "hits": hits
        or [
            {
                "term_id": 1,
                "term_name": "measles",
                "match_start": text.lower().index("measles"),
                "match_end": text.lower().index("measles") + len("measles"),
            }
        ],
        "reddit_submission_title": "Test",
    }


def test_prepare_post_trims_without_punct_when_well_punctuated():
    text = (
        "Measles is highly contagious. Vaccination prevents severe outcomes. "
        "Public health agencies recommend the MMR schedule for children."
    )
    row = prepare_post(_post(text=text))
    assert row["punctuation_restored"] is False
    assert isinstance(row["sentence_boundary_chunks"], list)
    assert row["sentence_boundary_chunk_count"] >= 1
    assert all(isinstance(c, str) and c.strip() for c in row["sentence_boundary_chunks"])


def test_iter_chunk_rows_sets_source_ids():
    text = "Measles outbreaks are rising. Vaccines reduce risk of severe disease."
    prepared = prepare_post(_post(post_id=42, text=text))
    rows = iter_chunk_rows([prepared])
    assert rows
    for i, row in enumerate(rows):
        assert row["source_post_id"] == 42
        assert row["sentence_boundary_chunk_index"] == i
        assert "sentence_boundary_chunks" not in row
        assert row["text"].strip()


def test_prepare_and_explode_with_mocked_punct():
    text = "measles vaccine safety discussion without much punctuation maybe"
    # Force punct path without loading the real model.
    with (
        patch("nlp.claim_extraction.prep.needs_punctuation", return_value=True),
        patch(
            "nlp.claim_extraction.prep.restore_punctuation",
            return_value=("Measles vaccine safety discussion. Without much punctuation, maybe.", True),
        ),
        patch(
            "nlp.claim_extraction.prep.remap_hits_to_text",
            side_effect=lambda _o, _n, hits: hits,
        ),
    ):
        prepared, chunks = prepare_and_explode([_post(text=text)])
    assert prepared[0]["punctuation_restored"] is True
    assert prepared[0]["text_punct"].startswith("Measles")
    assert len(chunks) >= 1


def test_nest_maps_claims_to_chunks(tmp_path: Path):
    text = "Measles can kill. Vaccination prevents severe outcomes."
    prepared, chunk_rows = prepare_and_explode([_post(post_id=7, text=text)])
    assert chunk_rows
    extract_rows = []
    for row in chunk_rows:
        extract_rows.append(
            {
                **row,
                "task_id": f"{row['source_post_id']}:{row['sentence_boundary_chunk_index']}",
                "claim_extraction_disposition": "success",
                "claim_extraction_output": {
                    "claims": [{"claim": f"Claim for chunk {row['sentence_boundary_chunk_index']}"}]
                },
            }
        )
    nested = nest_posts_chunks_claims(
        prepared,
        extract_rows,
        terms=["measles"],
        since="2024-01-01T00:00:00+00:00",
        until="2025-01-01T00:00:00+00:00",
    )
    assert nested["post_count"] == 1
    assert nested["chunk_count"] == len(chunk_rows)
    assert nested["claim_count"] == len(chunk_rows)
    assert nested["posts"][0]["post_id"] == 7
    assert nested["posts"][0]["chunks"][0]["claims"][0]["claim"].startswith("Claim for chunk")

    out = tmp_path / "nested.json"
    write_nested_json(out, nested)
    assert out.is_file()
