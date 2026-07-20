"""Tests for apps.claims.grouping."""

from __future__ import annotations

import json
from pathlib import Path

from apps.claims.grouping import group as grouping


def test_collapse_duplicate_claims(tmp_path: Path):
    posts = {
        "posts": [
            {
                "task_id": "t1",
                "claim_extraction_status": "success",
                "claim_extraction_output": {
                    "claims": [
                        {"claim": "Vaccines cause autism."},
                        {"claim": "vaccines cause autism."},
                    ]
                },
            },
            {
                "task_id": "t2",
                "claim_extraction_status": "success",
                "claim_extraction_output": {
                    "claims": [{"claim": "Vaccines cause autism."}]
                },
            },
            {
                "task_id": "t3",
                "claim_extraction_status": "failed",
                "claim_extraction_output": {"claims": [{"claim": "ignored"}]},
            },
        ]
    }
    path = tmp_path / "claims.json"
    path.write_text(json.dumps(posts), encoding="utf-8")
    bundle = grouping.run(path)
    assert bundle.claim_count == 1
    assert bundle.source_claim_count == 3
    assert bundle.groups[0].count == 3

    out = tmp_path / "groups.json"
    out.write_text(json.dumps(grouping.bundle_to_dict(bundle)), encoding="utf-8")
    loaded = grouping.load_groups_json(out)
    assert loaded.claim_count == 1
    assert loaded.groups[0].claim_text.lower().startswith("vaccines")
