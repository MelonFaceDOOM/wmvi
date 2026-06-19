"""Load the source claims JSON and collapse duplicate claim texts into groups.

The embedding unit is claim text alone (no post context). Duplicate claims are
collapsed by a normalized key (trim + collapse whitespace + case-insensitive);
each ``ClaimGroup`` keeps every source post via ``ClaimSource`` records.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from apps.claim_extractor.model_common import iter_success_claim_records


def normalize_claim_key(text: str) -> str:
    """Normalization key for duplicate detection."""
    return " ".join(text.split()).casefold()


@dataclass(frozen=True)
class ClaimSource:
    task_id: str
    claim_index: int
    row_id: str


@dataclass
class ClaimGroup:
    group_id: int
    claim_text: str
    sources: list[ClaimSource] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.sources)


@dataclass
class ClaimsBundle:
    groups: list[ClaimGroup]
    posts_by_task_id: dict[str, dict[str, Any]]
    source_hash: str
    source_path: str
    source_claim_count: int

    @property
    def claim_count(self) -> int:
        """Unique claim groups (one vector row each after embedding)."""
        return len(self.groups)

    @property
    def post_count(self) -> int:
        return len(self.posts_by_task_id)


def compute_source_hash(path: Path) -> str:
    """Content hash of the source file (chunked so large files stay cheap-ish)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_path(path_str: str, repo_root: Path) -> Path | None:
    raw = (path_str or "").strip()
    if not raw:
        return None
    candidates = [Path(raw).expanduser()]
    if not candidates[0].is_absolute():
        candidates.append(repo_root / raw.lstrip("/"))
    for c in candidates:
        if c.is_file():
            return c
    return None


def _collapse_claims(
  clean_posts: list[dict[str, Any]],
) -> tuple[list[ClaimGroup], dict[str, dict[str, Any]], int]:
    """Return (groups, posts_by_task_id, source_claim_count)."""
    buckets: dict[str, dict[str, Any]] = {}
    posts_by_task_id: dict[str, dict[str, Any]] = {}
    source_claim_count = 0

    for rec in iter_success_claim_records(clean_posts):
        text = str(rec.claim.get("claim") or "").strip()
        if not text:
            continue
        source_claim_count += 1
        posts_by_task_id.setdefault(rec.task_id, rec.post_row)
        norm_key = normalize_claim_key(text)
        if norm_key not in buckets:
            buckets[norm_key] = {"spellings": Counter(), "sources": []}
        buckets[norm_key]["spellings"][text] += 1
        buckets[norm_key]["sources"].append(
            ClaimSource(
                task_id=rec.task_id,
                claim_index=rec.claim_index,
                row_id=f"{rec.task_id}:{rec.claim_index}",
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


def load_claims(path_str: str, *, repo_root: Path) -> tuple[ClaimsBundle | None, str | None]:
    """Return (bundle, error). Mirrors the (data, err) pattern of the other labs."""
    p = _resolve_path(path_str, repo_root)
    if p is None:
        return None, f"No file found at: {path_str!r}"
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return None, f"Could not load {p}: {e}"
    if not isinstance(payload, dict):
        return None, "JSON root must be an object with a `posts` array."
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return None, "Expected top-level key `posts` to be a JSON array."

    clean_posts = [x for x in posts if isinstance(x, dict)]
    groups, posts_by_task_id, source_claim_count = _collapse_claims(clean_posts)

    if not groups:
        return None, "No successful claims found in source file."

    bundle = ClaimsBundle(
        groups=groups,
        posts_by_task_id=posts_by_task_id,
        source_hash=compute_source_hash(p),
        source_path=str(p),
        source_claim_count=source_claim_count,
    )
    return bundle, None
