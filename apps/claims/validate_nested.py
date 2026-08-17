"""Validate nested posts→chunks→claims JSON (corpus ``claims.json`` QA)."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from apps.claims.claims_data import chunk_is_usable


def load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object with top-level 'posts'.")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Object input JSON must contain top-level 'posts' list.")
    return payload


def summarize(payload: dict[str, Any]) -> dict[str, Any]:
    posts = [p for p in (payload.get("posts") or []) if isinstance(p, dict)]
    total_posts = len(posts)
    total_chunks = 0
    success_chunks = 0
    failed_chunks = 0
    unprocessed_chunks = 0
    empty_chunks = 0
    malformed_chunks = 0
    total_claims = 0
    claim_count_hist: Counter[int] = Counter()
    error_counter: Counter[str] = Counter()
    disposition_counter: Counter[str] = Counter()

    for post in posts:
        chunks = post.get("chunks")
        if not isinstance(chunks, list):
            continue
        for chunk in chunks:
            if not isinstance(chunk, dict):
                malformed_chunks += 1
                error_counter["malformed: non-object chunk"] += 1
                continue
            total_chunks += 1
            disp = chunk.get("claim_extraction_disposition")
            disp_key = str(disp) if disp is not None else "(missing)"
            disposition_counter[disp_key] += 1

            claims = chunk.get("claims")
            if disp == "unprocessed":
                unprocessed_chunks += 1
                continue
            if disp in ("terminal_failure", "retryable_failure"):
                failed_chunks += 1
                err = chunk.get("claim_extraction_error")
                err_text = str(err).strip() if err is not None else "unknown error"
                error_counter[err_text] += 1
                continue

            if claims is None:
                malformed_chunks += 1
                error_counter["malformed: missing claims list"] += 1
                continue
            if not isinstance(claims, list):
                malformed_chunks += 1
                error_counter["malformed: claims is not a list"] += 1
                continue

            if not claims:
                empty_chunks += 1
                if disp == "success":
                    # Success with zero claims is allowed but tracked separately.
                    success_chunks += 1
                    claim_count_hist[0] += 1
                continue

            bad_item = False
            for c in claims:
                if not isinstance(c, dict) or not isinstance(c.get("claim"), str):
                    bad_item = True
                    break
            if bad_item:
                malformed_chunks += 1
                error_counter["malformed: claim entry missing string 'claim'"] += 1
                continue

            if chunk_is_usable(chunk) or disp == "success":
                success_chunks += 1
            n_claims = len(claims)
            total_claims += n_claims
            claim_count_hist[n_claims] += 1

    over_3 = sum(v for k, v in claim_count_hist.items() if k > 3)
    return {
        "total_posts": total_posts,
        "total_chunks": total_chunks,
        "success_chunks": success_chunks,
        "failed_chunks": failed_chunks,
        "unprocessed_chunks": unprocessed_chunks,
        "empty_chunks": empty_chunks,
        "malformed_chunks": malformed_chunks,
        "total_claims": total_claims,
        "claim_count_hist": {
            "0": claim_count_hist.get(0, 0),
            "1": claim_count_hist.get(1, 0),
            "2": claim_count_hist.get(2, 0),
            "3": claim_count_hist.get(3, 0),
            ">3": over_3,
        },
        "dispositions": dict(disposition_counter.most_common()),
        "top_errors": [
            {"error": err, "count": count}
            for err, count in error_counter.most_common(15)
        ],
        # Aliases kept for older callers expecting row-oriented names.
        "total_rows": total_chunks,
        "success_rows": success_chunks,
        "failed_rows": failed_chunks,
        "malformed_rows": malformed_chunks,
    }


def run(claims_path: Path) -> dict[str, Any]:
    return summarize(load_payload(claims_path))
