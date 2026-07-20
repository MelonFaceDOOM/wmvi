"""Claim extraction from posts (network / Azure OpenAI)."""

from __future__ import annotations

from pathlib import Path

from apps.claims.extraction import get_claims as get_claims_mod
from nlp.claim_extraction.defaults import (
    DEFAULT_BATCH_COUNT,
    DEFAULT_MAX_CLAIMS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_WORKERS,
)


def run(
    *,
    posts_path: Path,
    out_path: Path,
    n_posts: int = 0,
    claims_only: bool = False,
    batch_count: int = DEFAULT_BATCH_COUNT,
    max_workers: int = DEFAULT_MAX_WORKERS,
    max_claims: int = DEFAULT_MAX_CLAIMS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> None:
    get_claims_mod.run(
        input_file=posts_path,
        out_file=out_path,
        batch_count=batch_count,
        max_workers=max_workers,
        max_claims=max_claims,
        max_retries=max_retries,
        max_tasks=0,
        n_posts=n_posts,
        claims_only=claims_only,
    )
