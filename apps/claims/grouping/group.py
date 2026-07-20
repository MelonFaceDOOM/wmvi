"""Collapse duplicate claim texts into groups (in-memory)."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from apps.claims.types import ClaimGroup, ClaimSource, ClaimsBundle


def normalize_claim_key(text: str) -> str:
    return " ".join(text.split()).casefold()


def compute_source_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_task_id(row: dict[str, Any]) -> str:
    tid = row.get("task_id") or row.get("id") or row.get("post_id")
    if tid is not None and str(tid).strip():
        return str(tid)
    text = str(row.get("text") or row.get("body") or "")[:80]
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def iter_success_claim_records(posts: list[dict[str, Any]]) -> Iterator[tuple[str, int, dict[str, Any], dict[str, Any]]]:
    """Yield (task_id, claim_index, post_row, claim_dict) for successful extractions."""
    for row in posts:
        if not isinstance(row, dict):
            continue
        if row.get("claim_extraction_status") != "success":
            continue
        out = row.get("claim_extraction_output")
        if not isinstance(out, dict):
            continue
        claims = out.get("claims")
        if not isinstance(claims, list) or not claims:
            continue
        tid = _stable_task_id(row)
        for i, c in enumerate(claims):
            if not isinstance(c, dict):
                continue
            yield tid, i, row, c


def collapse_claims(
    clean_posts: list[dict[str, Any]],
) -> tuple[list[ClaimGroup], dict[str, dict[str, Any]], int]:
    buckets: dict[str, dict[str, Any]] = {}
    posts_by_task_id: dict[str, dict[str, Any]] = {}
    source_claim_count = 0

    for task_id, claim_index, post_row, claim in iter_success_claim_records(clean_posts):
        text = str(claim.get("claim") or "").strip()
        if not text:
            continue
        source_claim_count += 1
        posts_by_task_id.setdefault(task_id, post_row)
        norm_key = normalize_claim_key(text)
        if norm_key not in buckets:
            buckets[norm_key] = {"spellings": Counter(), "sources": []}
        buckets[norm_key]["spellings"][text] += 1
        buckets[norm_key]["sources"].append(
            ClaimSource(
                task_id=task_id,
                claim_index=claim_index,
                row_id=f"{task_id}:{claim_index}",
            )
        )

    groups: list[ClaimGroup] = []
    for group_id, data in enumerate(buckets.values()):
        canonical = data["spellings"].most_common(1)[0][0]
        groups.append(
            ClaimGroup(
                group_id=group_id,
                claim_text=canonical,
                sources=list(data["sources"]),
            )
        )
    return groups, posts_by_task_id, source_claim_count


def group_from_posts_payload(
    payload: dict[str, Any],
    *,
    source_path: str = "",
    source_hash: str = "",
) -> ClaimsBundle:
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Expected top-level key `posts` to be a JSON array.")
    clean_posts = [x for x in posts if isinstance(x, dict)]
    groups, posts_by_task_id, source_claim_count = collapse_claims(clean_posts)
    if not groups:
        raise ValueError("No successful claims found in source file.")
    return ClaimsBundle(
        groups=groups,
        posts_by_task_id=posts_by_task_id,
        source_hash=source_hash,
        source_path=source_path,
        source_claim_count=source_claim_count,
    )


def run(claims_path: Path) -> ClaimsBundle:
    """Load posts-with-claims JSON and return a ClaimsBundle."""
    payload = json.loads(claims_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON root must be an object with a `posts` array.")
    return group_from_posts_payload(
        payload,
        source_path=str(claims_path),
        source_hash=compute_source_hash(claims_path),
    )


def bundle_to_dict(bundle: ClaimsBundle) -> dict[str, Any]:
    return {
        "source_path": bundle.source_path,
        "source_hash": bundle.source_hash,
        "source_claim_count": bundle.source_claim_count,
        "claim_count": bundle.claim_count,
        "groups": [
            {
                "group_id": g.group_id,
                "claim_text": g.claim_text,
                "count": g.count,
                "sources": [
                    {
                        "task_id": s.task_id,
                        "claim_index": s.claim_index,
                        "row_id": s.row_id,
                    }
                    for s in g.sources
                ],
            }
            for g in bundle.groups
        ],
    }


def load_groups_json(path: Path) -> ClaimsBundle:
    """Load a groups JSON written by ``bundle_to_dict`` (posts_by_task_id empty)."""
    data = json.loads(path.read_text(encoding="utf-8"))
    groups: list[ClaimGroup] = []
    for row in data.get("groups") or []:
        sources = [
            ClaimSource(
                task_id=str(s["task_id"]),
                claim_index=int(s["claim_index"]),
                row_id=str(s["row_id"]),
            )
            for s in (row.get("sources") or [])
        ]
        groups.append(
            ClaimGroup(
                group_id=int(row.get("group_id", len(groups))),
                claim_text=str(row["claim_text"]),
                sources=sources,
            )
        )
    return ClaimsBundle(
        groups=groups,
        posts_by_task_id={},
        source_hash=str(data.get("source_hash") or ""),
        source_path=str(data.get("source_path") or str(path)),
        source_claim_count=int(data.get("source_claim_count") or sum(g.count for g in groups)),
    )
