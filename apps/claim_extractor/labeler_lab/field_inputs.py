"""Build model-facing input strings for Ridge heads (standard score fields or generic vars)."""

from __future__ import annotations

from typing import Any

from apps.claim_extractor.labeler_lab.text_builder import build_structured_input
from apps.claim_extractor.model_common import ClaimRecord, stable_task_id
from apps.claim_extractor.scoring_inputs import structured_input_for_field


def claim_record_from_row(
    post_row: dict[str, Any],
    claim_dict: dict[str, Any],
    *,
    task_id: str | None = None,
    claim_index: int = 0,
) -> ClaimRecord:
    tid = task_id or str(post_row.get("task_id") or stable_task_id(post_row))
    return ClaimRecord(
        task_id=tid,
        claim_index=claim_index,
        post_row=post_row,
        claim=claim_dict,
        input_text=None,
    )


def build_input_for_head(
    *,
    score_field_name: str | None,
    input_var_keys: list[str],
    post_row: dict[str, Any],
    claim_dict: dict[str, Any],
    claim_index: int = 0,
    task_id: str | None = None,
) -> str:
    if score_field_name:
        rec = claim_record_from_row(post_row, claim_dict, task_id=task_id, claim_index=claim_index)
        return structured_input_for_field(score_field_name, rec)
    return build_structured_input(input_var_keys, post_row, claim_dict)
