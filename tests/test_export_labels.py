"""Tests for export_labels JSONL exporter."""

from __future__ import annotations

import json
from pathlib import Path

from apps.claim_extractor.export_labels import export_labels
from apps.claim_extractor.labeler_lab import db, standard_heads


def test_export_labels_manual_only(tmp_path: Path) -> None:
    posts = [
        {
            "task_id": "t1",
            "post_id": 99,
            "platform": "reddit_comment",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Vaccines work.", "claim_vaccine_alignment_score": 0.9}]},
            "text_coreference_resolved": "Body.",
        }
    ]
    conn = db.connect(tmp_path / "lab.sqlite")
    db.init_schema(conn)
    standard_heads.create_standard_heads(conn)
    head = db.get_head_by_name(conn, "claim_vaccine_alignment_score")
    assert head is not None
    db.upsert_label(conn, head_id=head.id, task_id="t1", claim_index=0, y=0.8, split="train")
    rows = export_labels(posts, conn)
    conn.close()
    assert len(rows) == 1
    row = rows[0]
    assert row["label_source"] == "manual"
    assert row["claim_vaccine_alignment_score"] == 0.8
    assert row["claim"] == "Vaccines work."
    assert "author_claim_agreement_score" not in row
    # LLM score must not appear in export
    assert row.get("claim_vaccine_alignment_score") == 0.8  # manual only
    serialized = json.dumps(row)
    assert "0.9" not in serialized
