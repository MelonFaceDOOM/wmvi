"""Tests for apps.claims.grouping."""

from __future__ import annotations

import json
from pathlib import Path

from apps.claims.grouping import group as grouping


def _nested(*, task_id: str, claims: list[dict], disposition: str = "success") -> dict:
    return {
        "post_id": task_id,
        "platform": "reddit_submission",
        "text": "body",
        "chunks": [
            {
                "chunk_index": 0,
                "text": "chunk",
                "task_id": task_id,
                "claim_extraction_disposition": disposition,
                "claims": claims,
            }
        ],
    }


def test_collapse_duplicate_claims(tmp_path: Path):
    posts = {
        "posts": [
            _nested(
                task_id="t1",
                claims=[
                    {"claim": "Vaccines cause autism."},
                    {"claim": "vaccines cause autism."},
                ],
            ),
            _nested(
                task_id="t2",
                claims=[{"claim": "Vaccines cause autism."}],
            ),
            _nested(
                task_id="t3",
                claims=[{"claim": "ignored"}],
                disposition="terminal_failure",
            ),
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
