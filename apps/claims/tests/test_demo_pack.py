"""Sqlite packer fixture."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np

from apps.claims.demo.catalog import save_names
from apps.claims.demo.pack import pack_bundle


def test_pack_tiny_bundle(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    exp.mkdir()
    hier = {
        "narratives": [
            {
                "narrative_id": 1,
                "n_leaves": 2,
                "size": 2,
                "leaves": [
                    {
                        "leaf_id": 0,
                        "size": 1,
                        "medoid_claim_text": "MMR is safe.",
                        "sample_claim_texts": ["MMR is safe."],
                    },
                    {
                        "leaf_id": 1,
                        "size": 1,
                        "medoid_claim_text": "Measles is mild.",
                        "sample_claim_texts": ["Measles is mild."],
                    },
                ],
            }
        ]
    }
    (exp / "hierarchy_x.json").write_text(json.dumps(hier), encoding="utf-8")
    np.save(exp / "leaf_labels_x.npy", np.array([0, 1], dtype=int))
    np.save(exp / "narrative_labels_x.npy", np.array([1, 1], dtype=int))
    save_names(
        exp,
        {
            "narratives": [{"id": 1, "title": "Safety vs mildness", "blurb": "two takes"}],
            "leaves": [
                {"id": 0, "title": "MMR is safe", "blurb": ""},
                {"id": 1, "title": "Measles is mild", "blurb": ""},
            ],
        },
    )

    run = tmp_path / "run"
    run.mkdir()
    index = {
        "claim_keys": ["a", "b"],
        "claim_texts": ["MMR is safe.", "Measles is mild."],
        "groups": [
            {
                "group_id": 0,
                "claim_key": "a",
                "claim_text": "MMR is safe.",
                "count": 1,
                "sources": [{"task_id": "p1:0", "claim_index": 0, "row_id": "p1:0:0"}],
            },
            {
                "group_id": 1,
                "claim_key": "b",
                "claim_text": "Measles is mild.",
                "count": 1,
                "sources": [{"task_id": "p2:0", "claim_index": 0, "row_id": "p2:0:0"}],
            },
        ],
    }
    (run / "index.json").write_text(json.dumps(index), encoding="utf-8")

    claims = {
        "posts": [
            {
                "post_id": "p1",
                "platform": "reddit_submission",
                "created_at_ts": "2025-01-08T00:00:00+00:00",
                "url": "https://example.com/1",
                "text": "Sponsor intro that should not appear in the snippet. " * 8 + "MMR is safe.",
                "chunks": [
                    {
                        "task_id": "p1:0",
                        "chunk_index": 0,
                        "text": "Full chunk body: MMR is safe and does not cause autism according to large studies.",
                        "claims": [{"claim": "MMR is safe.", "claim_vaccine_alignment_score": 1.0}],
                    }
                ],
            },
            {
                "post_id": "p2",
                "platform": "youtube_comment",
                "created_at_ts": "2025-02-01T00:00:00+00:00",
                "url": "https://example.com/2",
                "text": "Measles is mild.",
                "chunks": [
                    {
                        "task_id": "p2:0",
                        "chunk_index": 0,
                        "claims": [{"claim": "Measles is mild.", "claim_vaccine_alignment_score": 0.25}],
                    }
                ],
            },
        ]
    }
    claims_path = tmp_path / "claims.json"
    claims_path.write_text(json.dumps(claims), encoding="utf-8")
    out = tmp_path / "demo.sqlite"
    result = pack_bundle(exp_dir=exp, claims_path=claims_path, run_dir=run, out_path=out)
    assert result["n_claims"] == 2
    assert result["n_occurrences"] == 2
    conn = sqlite3.connect(out)
    nars = conn.execute("SELECT id, title, n_occurrences, n_anti FROM narratives").fetchall()
    assert nars[0][1] == "Safety vs mildness"
    assert nars[0][2] == 2
    assert nars[0][3] == 1
    anti = conn.execute("SELECT COUNT(*) FROM occurrences WHERE alignment <= 0.25").fetchone()[0]
    assert anti == 1
    weeks = conn.execute("SELECT DISTINCT week FROM occurrences ORDER BY week").fetchall()
    assert weeks[0][0].startswith("2025-W")
    snip = conn.execute(
        "SELECT snippet FROM occurrences WHERE post_id = 'p1'"
    ).fetchone()[0]
    assert snip.startswith("Full chunk body:")
    assert "Sponsor intro" not in snip
    fallback = conn.execute(
        "SELECT snippet FROM occurrences WHERE post_id = 'p2'"
    ).fetchone()[0]
    assert fallback == "Measles is mild."
    conn.close()
