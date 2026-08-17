"""LLM triplet-anchor discovery (file-mode; writes JSON, not SQLite)."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims import io as claims_io
from apps.claims.embedding import eval_triplets as eval_mod
from apps.claims.embedding.discovery_defaults import (
    DEFAULT_DISCOVERY_USER_PROMPT,
    DISCOVERY_SYSTEM_PROMPT,
    collect_discovery_categories,
    format_discovery_categories,
    render_discovery_prompt,
)
from apps.claims.embedding.discovery_helpers import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_TOP_K_NEIGHBORS,
    DiscoveryClient,
    is_unusable_response,
    normalize_strings,
    pick_category,
    record_unusable_claim,
    sample_claim_indices,
)
from apps.claims.embedding.triplet_neighbors import (
    format_neighbors_list,
    neighbors_for_claim_index,
)
from nlp.claim_extraction.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    default_is_retryable_exception,
)
from apps.claims.types import TripletAnchor

_POOLS = ("dev", "eval", "training")


def run(
    *,
    vectors: np.ndarray,
    claim_texts: list[str],
    model: str,
    n_claims: int = 20,
    existing: list[TripletAnchor] | None = None,
    unusable_log: Path | None = None,
    out_path: Path | None = None,
    pool_weights: tuple[int, int, int] = (1, 1, 2),
) -> dict[str, Any]:
    existing = list(existing or [])
    existing_texts = [a.text.strip() for a in existing if a.text.strip()]
    log_path = unusable_log or (claims_io.fixtures_dir() / "unusable_claims.jsonl")
    rng = random.Random()

    category_text = format_discovery_categories(collect_discovery_categories(existing))
    indices = sample_claim_indices(
        claim_texts, exclude_texts=set(existing_texts), n=n_claims, rng=rng
    )
    if not indices:
        return {
            "created": 0,
            "unusable": 0,
            "skipped": 0,
            "failed": 0,
            "unusable_log": str(log_path),
            "out": str(out_path) if out_path else None,
            "n_total_anchors": len(existing),
        }

    client = DiscoveryClient(model=model, system=DISCOVERY_SYSTEM_PROMPT)
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=DEFAULT_MAX_WORKERS,
        retry_policy=RetryPolicy(max_retries=2),
        throttle_policy=ThrottlePolicy(target_requests_per_minute=60, global_429_cooldown_s=15.0),
        is_retryable=default_is_retryable_exception,
    )

    tasks: list[RequestTask] = []
    claim_by_task: dict[str, str] = {}
    for seq, claim_index in enumerate(indices, start=1):
        claim_text = claim_texts[claim_index].strip()
        neighbors = neighbors_for_claim_index(
            claim_index,
            vectors=vectors,
            claim_texts=claim_texts,
            top_k=DEFAULT_TOP_K_NEIGHBORS,
        )
        user_prompt = render_discovery_prompt(
            DEFAULT_DISCOVERY_USER_PROMPT,
            claim=claim_text,
            neighbors=format_neighbors_list(neighbors),
            categories=category_text,
            neighbor_count=DEFAULT_TOP_K_NEIGHBORS,
            claim_index=seq,
            existing_anchor_count=len(existing_texts),
        )
        task_id = str(claim_index)
        claim_by_task[task_id] = claim_text
        tasks.append(RequestTask(task_id=task_id, payload={"user_prompt": user_prompt}))

    new_anchors: list[TripletAnchor] = []
    unusable = skipped = failed = 0
    exclude = set(existing_texts)

    for api_result in requester.run(tasks):
        task_id = api_result.task_id
        claim_text = claim_by_task.get(task_id, task_id)
        if api_result.status != RequestStatus.SUCCESS or api_result.output is None:
            failed += 1
            continue
        response = api_result.output
        if is_unusable_response(response):
            record_unusable_claim(
                log_path,
                claim_text=claim_text,
                row_id=task_id,
                task_id="",
                post="",
                model=model,
            )
            unusable += 1
            continue
        positives = normalize_strings(response.get("positives"))
        negatives = normalize_strings(response.get("negatives"))
        if not positives or not negatives or claim_text in exclude:
            skipped += 1
            continue
        pool = rng.choices(list(_POOLS), weights=list(pool_weights), k=1)[0]
        new_anchors.append(
            TripletAnchor(
                id=0,
                text=claim_text,
                pool=pool,
                category=pick_category(response),
                positives=positives,
                negatives=negatives,
            )
        )
        exclude.add(claim_text)

    next_id = max((a.id for a in existing), default=0) + 1
    for a in new_anchors:
        a.id = next_id
        next_id += 1
    merged = existing + new_anchors
    if out_path is not None:
        eval_mod.dump_triplets_json(out_path, merged)

    return {
        "created": len(new_anchors),
        "unusable": unusable,
        "skipped": skipped,
        "failed": failed,
        "unusable_log": str(log_path),
        "out": str(out_path) if out_path else None,
        "n_total_anchors": len(merged),
    }
