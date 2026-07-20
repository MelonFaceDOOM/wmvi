"""Build nested posts → chunks → claims artifacts from extract rows."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nlp.claim_extraction.defaults import MODEL_NAME


def _claims_from_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    out = row.get("claim_extraction_output")
    if not isinstance(out, dict):
        return []
    claims = out.get("claims")
    if not isinstance(claims, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for c in claims:
        if isinstance(c, dict) and isinstance(c.get("claim"), str):
            cleaned.append({"claim": c["claim"]})
    return cleaned


def nest_posts_chunks_claims(
    prepared_posts: list[dict[str, Any]],
    extract_rows: list[dict[str, Any]],
    *,
    terms: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    model: str = MODEL_NAME,
    extra_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Join extract results onto prepared posts by source_post_id + chunk index."""
    by_key: dict[tuple[Any, int], dict[str, Any]] = {}
    for row in extract_rows:
        if not isinstance(row, dict):
            continue
        src = row.get("source_post_id")
        idx = row.get("sentence_boundary_chunk_index")
        if src is None or idx is None:
            continue
        by_key[(src, int(idx))] = row

    nested_posts: list[dict[str, Any]] = []
    chunk_count = 0
    claim_count = 0

    for post in prepared_posts:
        if not isinstance(post, dict):
            continue
        post_id = post.get("post_id")
        chunks_raw = post.get("sentence_boundary_chunks")
        if not isinstance(chunks_raw, list):
            chunks_raw = []

        chunk_objs: list[dict[str, Any]] = []
        for idx, chunk_text in enumerate(chunks_raw):
            if not isinstance(chunk_text, str) or not chunk_text.strip():
                continue
            row = by_key.get((post_id, idx), {})
            claims = _claims_from_row(row) if row else []
            claim_count += len(claims)
            chunk_count += 1
            chunk_objs.append(
                {
                    "chunk_index": idx,
                    "text": chunk_text,
                    "task_id": row.get("task_id") or f"{post_id}:{idx}",
                    "claim_extraction_disposition": row.get(
                        "claim_extraction_disposition", "unprocessed"
                    ),
                    "claim_extraction_error": row.get("claim_extraction_error"),
                    "claims": claims,
                }
            )

        nested = {
            k: v
            for k, v in post.items()
            if k
            not in (
                "sentence_boundary_chunks",
                "sentence_boundary_chunk_count",
                "sentence_boundary_chunk_index",
                "source_post_id",
            )
        }
        nested["punctuation_restored"] = bool(post.get("punctuation_restored"))
        nested["chunks"] = chunk_objs
        nested_posts.append(nested)

    payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "terms": list(terms or []),
        "since": since,
        "until": until,
        "model": model,
        "post_count": len(nested_posts),
        "chunk_count": chunk_count,
        "claim_count": claim_count,
        "posts": nested_posts,
    }
    if extra_meta:
        for k, v in extra_meta.items():
            if k not in payload:
                payload[k] = v
    return payload


def write_nested_json(out_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(out_path)
    return payload
