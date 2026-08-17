"""Unit tests for shared claim-extraction text/schema helpers."""

from __future__ import annotations

import json

import pytest

from nlp.claim_extraction.schema import (
    parse_claims_alignment_output,
    parse_claims_only_output,
    parse_claims_with_scores_output,
)
from nlp.claim_extraction.scores import snap_alignment_score
from nlp.claim_extraction.text import format_input_text, stable_task_id


def test_stable_task_id_prefers_explicit() -> None:
    assert stable_task_id({"task_id": "abc", "post_id": "x"}) == "abc"


def test_stable_task_id_chunk() -> None:
    assert stable_task_id({"source_post_id": "p1", "sentence_boundary_chunk_index": 2}) == "p1:2"


def test_format_input_text_reddit() -> None:
    row = {"platform": "reddit_submission", "reddit_submission_title": "Hello"}
    assert "Submission title: Hello" in format_input_text(row, "body")
    assert format_input_text(row, "body").endswith("body")


def test_parse_claims_only_ok() -> None:
    raw = json.dumps({"claims": [{"claim": "Vaccines work."}]})
    out = parse_claims_only_output(raw)
    assert out["claims"][0]["claim"] == "Vaccines work."


def test_parse_claims_only_rejects_bad() -> None:
    with pytest.raises(ValueError):
        parse_claims_only_output(json.dumps({"claims": [{"claim": 1}]}))


def test_parse_claims_with_scores_requires_scores() -> None:
    raw = json.dumps({"claims": [{"claim": "x"}]})
    with pytest.raises(ValueError):
        parse_claims_with_scores_output(raw)


def test_snap_alignment_score() -> None:
    assert snap_alignment_score(0.0) == 0.0
    assert snap_alignment_score(0.51) == 0.5
    assert snap_alignment_score(0.7) == 0.75
    assert snap_alignment_score(1) == 1.0


def test_parse_claims_alignment_snaps_and_requires_score() -> None:
    raw = json.dumps(
        {"claims": [{"claim": "MMR vaccination does not cause autism.", "claim_vaccine_alignment_score": 0.9}]}
    )
    out = parse_claims_alignment_output(raw)
    assert out["claims"][0]["claim_vaccine_alignment_score"] == 1.0
    with pytest.raises(ValueError):
        parse_claims_alignment_output(json.dumps({"claims": [{"claim": "x"}]}))


def test_load_openai_config_strips_crlf(monkeypatch: pytest.MonkeyPatch) -> None:
    from nlp.claim_extraction.clients import load_openai_config

    monkeypatch.setenv("PERSONAL_OPENAI_API_KEY", "sk-test-key\r")
    monkeypatch.setenv("PERSONAL_OPENAI_BASE_URL", "https://api.openai.com/v1\r")
    cfg = load_openai_config()
    assert cfg.key == "sk-test-key"
    assert cfg.base_url == "https://api.openai.com/v1"
