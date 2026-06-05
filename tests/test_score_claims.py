"""Tests for score_claims batch merge."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from apps.claim_extractor.score_claims import score_posts


def test_score_posts_writes_pred_keys(tmp_path: Path) -> None:
    posts = [
        {
            "task_id": "t1",
            "claim_extraction_status": "success",
            "claim_extraction_output": {
                "claims": [
                    {
                        "claim": "Vaccines work.",
                        "claim_vaccine_alignment_score": 0.9,
                    }
                ]
            },
            "text_coreference_resolved": "Body text.",
        }
    ]

    mock_predictor = MagicMock()
    mock_predictor.predict_scores.return_value = [0.42]

    with patch("apps.claim_extractor.score_claims._load_predictors") as _:
        n = score_posts(posts, {"claim_vaccine_alignment_score": mock_predictor})
    assert n == 1
    claim = posts[0]["claim_extraction_output"]["claims"][0]
    assert claim["pred_claim_vaccine_alignment_score"] == 0.42
    mock_predictor.predict_scores.assert_called_once()


def test_score_posts_skips_non_success() -> None:
    posts = [{"claim_extraction_status": "failed"}]
    mock_predictor = MagicMock()
    n = score_posts(posts, {"claim_vaccine_alignment_score": mock_predictor})
    assert n == 0
    mock_predictor.predict_scores.assert_not_called()
