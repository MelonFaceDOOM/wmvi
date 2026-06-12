"""Tests for claim text deduplication (labeler lab)."""

from __future__ import annotations

from apps.claim_extractor.claim_normalize import normalize_claim_text
from apps.claim_extractor.labeler_lab import claims_data


def test_normalize_claim_text_whitespace_and_case() -> None:
    assert normalize_claim_text("  Vaccines  Are Safe  ") == "vaccines are safe"
    assert normalize_claim_text("Hello.") == "hello"


def test_build_claim_dedup_groups_two_posts_same_claim() -> None:
    posts = [
        {
            "task_id": "t1",
            "claim_extraction_status": "success",
            "claim_extraction_output": {
                "claims": [{"claim": "Vaccines work.", "claim_vaccine_alignment_score": 0.9}]
            },
        },
        {
            "task_id": "t2",
            "claim_extraction_status": "success",
            "claim_extraction_output": {
                "claims": [{"claim": "vaccines work.", "claim_vaccine_alignment_score": 0.8}]
            },
        },
    ]
    groups, key_to_group = claims_data.build_claim_dedup_groups(posts)
    assert len(groups) == 1
    g = groups[0]
    assert g.canonical_task_id == "t1"
    assert g.aliases == [("t2", 0)]
    assert g.occurrences == 2
    assert key_to_group[("t2", 0)] is g


def test_iter_unique_claims_for_labeling() -> None:
    posts = [
        {
            "task_id": "a",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Same."}]},
        },
        {
            "task_id": "b",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Same"}]},
        },
        {
            "task_id": "c",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Different."}]},
        },
    ]
    rows = list(claims_data.iter_unique_claims_for_labeling(posts))
    assert len(rows) == 2
    tids = {tid for _p, _c, tid, _i in rows}
    assert tids == {"a", "c"}


def test_labeled_norm_keys_alias_counts() -> None:
    posts = [
        {
            "task_id": "t1",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Dup claim"}]},
        },
        {
            "task_id": "t2",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Dup claim"}]},
        },
    ]
    groups, _ = claims_data.build_claim_dedup_groups(posts)
    labeled = {("t2", 0)}  # alias labeled, not canonical
    norms = claims_data.labeled_norm_keys(groups, labeled)
    assert len(norms) == 1


def test_shuffle_label_queue_stable() -> None:
    queue = [
        ({"platform": "a"}, {"claim": "1"}, f"t{i}", 0)
        for i in range(20)
    ]
    a = claims_data.shuffle_label_queue(queue, seed=42, head_id=1)
    b = claims_data.shuffle_label_queue(queue, seed=42, head_id=1)
    assert [x[2] for x in a] == [x[2] for x in b]
    file_order = [x[2] for x in queue]
    shuffled_order = [x[2] for x in a]
    assert shuffled_order != file_order


def test_shuffle_label_queue_differs_by_head() -> None:
    queue = [
        ({"platform": "a"}, {"claim": "1"}, f"t{i}", 0)
        for i in range(20)
    ]
    a = claims_data.shuffle_label_queue(queue, seed=42, head_id=1)
    b = claims_data.shuffle_label_queue(queue, seed=42, head_id=2)
    assert [x[2] for x in a] != [x[2] for x in b]


def test_dedupe_alignment_training_xy() -> None:
    posts = [
        {
            "task_id": "t1",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "Vaccines work."}]},
        },
        {
            "task_id": "t2",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "vaccines work."}]},
        },
    ]
    labeled_rows = [
        {"task_id": "t1", "claim_index": 0, "y": 0.8, "created_at": "2025-01-01"},
        {"task_id": "t2", "claim_index": 0, "y": 0.8, "created_at": "2025-01-02"},
    ]
    texts, ys, warnings = claims_data.dedupe_alignment_training_xy(
        posts,
        labeled_rows,
        input_var_keys=[],
        score_field_name="claim_vaccine_alignment_score",
    )
    assert len(texts) == 1
    assert len(ys) == 1
    assert ys[0] == 0.8
    assert warnings == []
