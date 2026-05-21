"""Load posts JSON and index claims by (task_id, claim_index)."""

from __future__ import annotations

from typing import Any, Iterator

from apps.claim_extractor.labeler_lab.text_builder import build_structured_input
from apps.claim_extractor.model_common import stable_task_id


def index_claims_by_key(posts: list[dict[str, Any]]) -> dict[tuple[str, int], tuple[dict[str, Any], dict[str, Any]]]:
    out: dict[tuple[str, int], tuple[dict[str, Any], dict[str, Any]]] = {}
    for row in posts:
        if not isinstance(row, dict):
            continue
        if row.get("claim_extraction_status") != "success":
            continue
        outd = row.get("claim_extraction_output")
        if not isinstance(outd, dict):
            continue
        claims = outd.get("claims")
        if not isinstance(claims, list):
            continue
        tid = str(row.get("task_id") or stable_task_id(row))
        for i, c in enumerate(claims):
            if isinstance(c, dict):
                out[(tid, i)] = (row, c)
    return out


def iter_success_claims(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> Iterator[tuple[dict[str, Any], dict[str, Any], str, int]]:
    posts_seen = 0
    claims_emitted = 0
    for row in posts:
        if not isinstance(row, dict):
            continue
        if row.get("claim_extraction_status") != "success":
            continue
        outd = row.get("claim_extraction_output")
        if not isinstance(outd, dict):
            continue
        claims = outd.get("claims")
        if not isinstance(claims, list) or not claims:
            continue
        if max_posts is not None and posts_seen >= max_posts:
            break
        posts_seen += 1
        tid = str(row.get("task_id") or stable_task_id(row))
        for i, c in enumerate(claims):
            if max_claims is not None and claims_emitted >= max_claims:
                return
            if not isinstance(c, dict):
                continue
            claims_emitted += 1
            yield row, c, tid, i


def build_xy_for_labels(
    posts: list[dict[str, Any]],
    var_keys: list[str],
    labeled_rows: list[tuple[str, int, float]],
) -> tuple[list[str], list[float]]:
    idx = index_claims_by_key(posts)

    texts: list[str] = []
    ys: list[float] = []
    for tid, cidx, y in labeled_rows:
        key = (tid, cidx)
        if key not in idx:
            continue
        post_row, claim_dict = idx[key]
        texts.append(build_structured_input(var_keys, post_row, claim_dict))
        ys.append(y)
    return texts, ys
