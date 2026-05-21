"""Tests for structured scoring inputs."""

from __future__ import annotations

from apps.claim_extractor.model_common import ClaimRecord
from apps.claim_extractor.scoring_inputs import (
    context_text_for_post_row,
    structured_input_for_field,
)


def test_context_text_prefers_coref_then_plain_text() -> None:
    row = {"text_coreference_resolved": "  coref body  ", "text": "plain"}
    assert context_text_for_post_row(row) == "  coref body  "
    row2 = {"text": "only plain"}
    assert context_text_for_post_row(row2) == "only plain"


def test_structured_claim_alignment_claim_only() -> None:
    rec = ClaimRecord(
        task_id="t1",
        claim_index=0,
        post_row={"text": "ctx should not appear in claim-only field"},
        claim={"claim": "Vaccines save lives."},
        input_text=None,
    )
    s = structured_input_for_field("claim_vaccine_alignment_score", rec)
    assert s.startswith("[CLAIM]\n")
    assert "Vaccines save lives." in s
    assert "[TEXT]" not in s


def test_structured_other_fields_include_text_block() -> None:
    rec = ClaimRecord(
        task_id="t1",
        claim_index=0,
        post_row={"text_coreference_resolved": "Author context here."},
        claim={"claim": "The claim text."},
        input_text=None,
    )
    s = structured_input_for_field("author_claim_agreement_score", rec)
    assert "[CLAIM]\nThe claim text.\n" in s
    assert "[TEXT]\nAuthor context here.\n" in s
