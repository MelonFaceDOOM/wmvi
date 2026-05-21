"""
Structured model inputs per score field: explicit [CLAIM] / [TEXT] blocks (no raw concat).

Used by export, training, and inference so formatting cannot drift.
"""

from __future__ import annotations

from typing import Any

from apps.claim_extractor.model_common import ClaimRecord, SCORE_FIELD_NAMES


def context_text_for_post_row(row: dict[str, Any]) -> str:
    """Body for [TEXT]: coref-resolved string, else plain ``text``."""
    t = row.get("text_coreference_resolved")
    if not isinstance(t, str) or not t.strip():
        t = row.get("text")
    if isinstance(t, str):
        return t
    return ""


def claim_text_from_record(rec: ClaimRecord) -> str:
    return str(rec.claim.get("claim") or "")


def structured_input_for_field(field_name: str, rec: ClaimRecord) -> str:
    """
    Model-facing string for ``field_name`` (one of ``SCORE_FIELD_NAMES``).

    - ``claim_vaccine_alignment_score``: ``[CLAIM]`` only.
    - All other fields: ``[CLAIM]`` + ``[TEXT]`` (context from post row).
    """
    if field_name not in SCORE_FIELD_NAMES:
        raise ValueError(f"Unknown score field: {field_name!r}")
    claim = claim_text_from_record(rec)
    if field_name == "claim_vaccine_alignment_score":
        return f"[CLAIM]\n{claim}\n"
    ctx = context_text_for_post_row(rec.post_row)
    return f"[CLAIM]\n{claim}\n\n[TEXT]\n{ctx}\n"
