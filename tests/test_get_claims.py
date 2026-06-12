from __future__ import annotations

import json

import pytest

from apps.claim_extractor.extraction_core import parse_claims_with_scores_output
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES


def _valid_claim() -> dict:
    return {
        "claim": "x",
        **{k: 0.5 for k in SCORE_FIELD_NAMES},
    }


def test_parse_valid_scores() -> None:
    raw = json.dumps({"claims": [_valid_claim()]})
    out = parse_claims_with_scores_output(raw)
    assert len(out["claims"]) == 1


def test_parse_rejects_out_of_range() -> None:
    c = _valid_claim()
    c["claim_vaccine_alignment_score"] = 1.5
    raw = json.dumps({"claims": [c]})
    with pytest.raises(ValueError, match="claim_vaccine_alignment_score"):
        parse_claims_with_scores_output(raw)


def test_parse_rejects_missing_field() -> None:
    c = {"claim": "x", **{k: 0.5 for k in SCORE_FIELD_NAMES if k != "attribution_authority_score"}}
    raw = json.dumps({"claims": [c]})
    with pytest.raises(ValueError):
        parse_claims_with_scores_output(raw)
