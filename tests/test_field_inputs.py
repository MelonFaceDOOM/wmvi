"""Tests for field_inputs and standard head input wiring."""

from __future__ import annotations

from apps.claim_extractor.labeler_lab.field_inputs import build_input_for_head
from apps.claim_extractor.labeler_lab.standard_heads import create_standard_heads
from apps.claim_extractor.labeler_lab import db
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES


def test_build_input_for_standard_field() -> None:
    post = {"text_coreference_resolved": "Context body."}
    claim = {"claim": "Vaccines are safe."}
    txt = build_input_for_head(
        score_field_name="claim_vaccine_alignment_score",
        input_var_keys=[],
        post_row=post,
        claim_dict=claim,
    )
    assert txt.startswith("[CLAIM]\n")
    assert "Vaccines are safe." in txt
    assert "[TEXT]" not in txt


def test_build_input_for_context_field() -> None:
    post = {"text_coreference_resolved": "Author context."}
    claim = {"claim": "Some claim."}
    txt = build_input_for_head(
        score_field_name="author_claim_agreement_score",
        input_var_keys=[],
        post_row=post,
        claim_dict=claim,
    )
    assert "[CLAIM]" in txt
    assert "[TEXT]" in txt
    assert "Author context." in txt


def test_create_standard_heads(tmp_path) -> None:
    conn = db.connect(tmp_path / "lab.sqlite")
    db.init_schema(conn)
    created, skipped = create_standard_heads(conn)
    assert len(created) == len(SCORE_FIELD_NAMES)
    assert skipped == []
    created2, skipped2 = create_standard_heads(conn)
    assert created2 == []
    assert len(skipped2) == len(SCORE_FIELD_NAMES)
    heads = db.list_heads(conn)
    assert len(heads) == len(SCORE_FIELD_NAMES)
    for h in heads:
        assert h.score_field_name in SCORE_FIELD_NAMES
    conn.close()


def test_fetch_labels_sorted_and_delete(tmp_path) -> None:
    conn = db.connect(tmp_path / "lab.sqlite")
    db.init_schema(conn)
    hid = db.create_head(conn, "test_head", [], score_field_name="claim_vaccine_alignment_score")
    db.upsert_label(conn, head_id=hid, task_id="a", claim_index=0, y=0.9, split="train")
    db.upsert_label(conn, head_id=hid, task_id="b", claim_index=0, y=0.1, split="eval")
    db.upsert_label(conn, head_id=hid, task_id="c", claim_index=1, y=0.5, split="train")

    asc = db.fetch_labels_sorted(conn, hid, descending=False)
    assert [r["y"] for r in asc] == [0.1, 0.5, 0.9]

    desc = db.fetch_labels_sorted(conn, hid, descending=True)
    assert [r["y"] for r in desc] == [0.9, 0.5, 0.1]

    eval_only = db.fetch_labels_sorted(conn, hid, "eval", descending=False)
    assert len(eval_only) == 1
    assert eval_only[0]["y"] == 0.1

    assert db.delete_label(conn, hid, "b", 0) is True
    assert len(db.fetch_labels_sorted(conn, hid)) == 2
    conn.close()
