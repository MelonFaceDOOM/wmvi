"""Load and index posts from the shared source JSON."""

from __future__ import annotations

from typing import Any

from apps.claim_extractor.model_common import stable_task_id


def extraction_status(post_row: dict[str, Any]) -> str:
    """Return ``success`` or ``failed`` for filter UI."""
    status = post_row.get("claim_extraction_status")
    if status == "success":
        out = post_row.get("claim_extraction_output")
        if isinstance(out, dict) and isinstance(out.get("claims"), list):
            return "success"
    return "failed"


def baseline_claims_from_post(post_row: dict[str, Any]) -> list | None:
    if extraction_status(post_row) != "success":
        return None
    out = post_row.get("claim_extraction_output")
    if not isinstance(out, dict):
        return None
    claims = out.get("claims")
    if isinstance(claims, list):
        return claims
    return None


def post_text(post_row: dict[str, Any]) -> str:
    t = post_row.get("text_coreference_resolved")
    if isinstance(t, str) and t.strip():
        return t
    t = post_row.get("text")
    return t if isinstance(t, str) else ""


def platform_name(post_row: dict[str, Any]) -> str:
    p = post_row.get("platform")
    return str(p) if p else "(unknown)"


def index_posts_by_task_id(posts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in posts:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("task_id") or stable_task_id(row))
        out[tid] = row
    return out


def iter_all_posts(posts: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    """Return (post_row, task_id) for every dict in posts."""
    items: list[tuple[dict[str, Any], str]] = []
    for row in posts:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("task_id") or stable_task_id(row))
        items.append((row, tid))
    return items


def claim_texts(claims: list | None) -> list[str]:
    if not claims:
        return []
    out: list[str] = []
    for c in claims:
        if isinstance(c, dict):
            text = str(c.get("claim") or "").strip()
            if text:
                out.append(text)
    return out
