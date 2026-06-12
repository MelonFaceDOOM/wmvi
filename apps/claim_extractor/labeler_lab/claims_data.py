"""Load posts JSON and index claims by (task_id, claim_index)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Iterator

LabelQueueItem = tuple[dict[str, Any], dict[str, Any], str, int]

from apps.claim_extractor.claim_normalize import normalize_claim_text
from apps.claim_extractor.labeler_lab.field_inputs import build_input_for_head
from apps.claim_extractor.model_common import stable_task_id


@dataclass
class DedupGroup:
    norm_key: str
    claim_text: str
    canonical_task_id: str
    canonical_claim_index: int
    canonical_post_row: dict[str, Any]
    canonical_claim_dict: dict[str, Any]
    aliases: list[tuple[str, int]] = field(default_factory=list)

    @property
    def occurrences(self) -> int:
        return 1 + len(self.aliases)

    @property
    def all_keys(self) -> list[tuple[str, int]]:
        keys = [(self.canonical_task_id, self.canonical_claim_index)]
        keys.extend(self.aliases)
        return keys


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


def build_claim_dedup_groups(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> tuple[list[DedupGroup], dict[tuple[str, int], DedupGroup]]:
    """
    Group claims by normalized claim text. First occurrence in iteration order is canonical.
    """
    groups_by_key: dict[str, DedupGroup] = {}
    key_to_group: dict[tuple[str, int], DedupGroup] = {}

    for row, claim_dict, tid, idx in iter_success_claims(
        posts, max_posts=max_posts, max_claims=max_claims
    ):
        claim_text = str(claim_dict.get("claim") or "")
        norm_key = normalize_claim_text(claim_text)
        if not norm_key:
            continue
        g = groups_by_key.get(norm_key)
        if g is None:
            g = DedupGroup(
                norm_key=norm_key,
                claim_text=claim_text,
                canonical_task_id=tid,
                canonical_claim_index=idx,
                canonical_post_row=row,
                canonical_claim_dict=claim_dict,
            )
            groups_by_key[norm_key] = g
            key_to_group[(tid, idx)] = g
        else:
            g.aliases.append((tid, idx))
            key_to_group[(tid, idx)] = g

    return list(groups_by_key.values()), key_to_group


def dedup_stats(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> dict[str, int]:
    total = sum(
        1
        for _ in iter_success_claims(posts, max_posts=max_posts, max_claims=max_claims)
    )
    groups, _ = build_claim_dedup_groups(posts, max_posts=max_posts, max_claims=max_claims)
    unique = len(groups)
    return {
        "total": total,
        "unique": unique,
        "duplicate_rows": max(0, total - unique),
    }


def _label_queue_shuffle_key(
    seed: int,
    head_id: int,
    task_id: str,
    claim_index: int,
) -> int:
    payload = f"shuffle|{seed}|{head_id}|{task_id}|{claim_index}".encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest()[:16], 16)


def shuffle_label_queue(
    queue: list[LabelQueueItem],
    *,
    seed: int,
    head_id: int,
) -> list[LabelQueueItem]:
    """Stable pseudo-random order per Ridge head (same seed + head + items → same order)."""
    if len(queue) < 2:
        return list(queue)
    return sorted(
        queue,
        key=lambda item: _label_queue_shuffle_key(seed, head_id, item[2], item[3]),
    )


def iter_unique_claims_for_labeling(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> Iterator[tuple[dict[str, Any], dict[str, Any], str, int]]:
    """Yield one row per unique normalized claim text (canonical occurrence only)."""
    groups, _ = build_claim_dedup_groups(posts, max_posts=max_posts, max_claims=max_claims)
    for g in groups:
        yield (
            g.canonical_post_row,
            g.canonical_claim_dict,
            g.canonical_task_id,
            g.canonical_claim_index,
        )


def labeled_norm_keys(
    groups: list[DedupGroup],
    labeled_keys: set[tuple[str, int]],
) -> set[str]:
    """Norm keys where canonical or any alias row has a label."""
    out: set[str] = set()
    for g in groups:
        if any(k in labeled_keys for k in g.all_keys):
            out.add(g.norm_key)
    return out


def occurrence_count_for_key(
    key: tuple[str, int],
    key_to_group: dict[tuple[str, int], DedupGroup],
) -> int:
    g = key_to_group.get(key)
    return g.occurrences if g is not None else 1


def build_xy_for_labels(
    posts: list[dict[str, Any]],
    labeled_rows: list[tuple[str, int, float]],
    *,
    input_var_keys: list[str],
    score_field_name: str | None = None,
) -> tuple[list[str], list[float]]:
    idx = index_claims_by_key(posts)

    texts: list[str] = []
    ys: list[float] = []
    for tid, cidx, y in labeled_rows:
        key = (tid, cidx)
        if key not in idx:
            continue
        post_row, claim_dict = idx[key]
        texts.append(
            build_input_for_head(
                score_field_name=score_field_name,
                input_var_keys=input_var_keys,
                post_row=post_row,
                claim_dict=claim_dict,
                claim_index=cidx,
                task_id=tid,
            )
        )
        ys.append(y)
    return texts, ys


def dedupe_alignment_training_xy(
    posts: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    *,
    input_var_keys: list[str],
    score_field_name: str | None,
) -> tuple[list[str], list[float], list[str]]:
    """
    Build (text, y) pairs and collapse to one row per normalized claim text.

    ``labeled_rows`` items: task_id, claim_index, y, created_at (optional).
    On conflict, keep the row with the latest created_at.
    """
    idx = index_claims_by_key(posts)
    best: dict[str, tuple[str, float, str]] = {}
    warnings: list[str] = []

    for row in labeled_rows:
        tid = str(row["task_id"])
        cidx = int(row["claim_index"])
        y = float(row["y"])
        created = str(row.get("created_at") or "")
        hit = idx.get((tid, cidx))
        if hit is None:
            continue
        post_row, claim_dict = hit
        norm = normalize_claim_text(str(claim_dict.get("claim") or ""))
        if not norm:
            continue
        text = build_input_for_head(
            score_field_name=score_field_name,
            input_var_keys=input_var_keys,
            post_row=post_row,
            claim_dict=claim_dict,
            claim_index=cidx,
            task_id=tid,
        )
        prev = best.get(norm)
        if prev is None:
            best[norm] = (text, y, created)
        else:
            _pt, py, pcreated = prev
            if abs(py - y) > 1e-9:
                warnings.append(
                    f"Conflicting labels for same claim text (norm={norm[:40]}…): "
                    f"{py:.3f} vs {y:.3f}; keeping newer."
                )
            if created >= pcreated:
                best[norm] = (text, y, created)
            else:
                best[norm] = (prev[0], prev[1], prev[2])

    texts = [v[0] for v in best.values()]
    ys = [v[1] for v in best.values()]
    return texts, ys, warnings
