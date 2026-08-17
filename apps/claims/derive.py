"""Derive a platform-rebalanced nested claims corpus from a parent corpus.

Primary use: downsample Reddit posts so Reddit claim count ≈ non-Reddit claim
count, keeping all non-Reddit posts. Sampling is at the **post** level (chunks
stay intact); the balance target is measured in **claims**.
"""

from __future__ import annotations

import random
from collections import Counter
from typing import Any

from apps.claims.claims_data import count_nested_claims

REDDIT_PLATFORMS = frozenset({"reddit_comment", "reddit_submission"})


def post_claim_count(post: dict[str, Any]) -> int:
    n = 0
    for chunk in post.get("chunks") or []:
        if not isinstance(chunk, dict):
            continue
        claims = chunk.get("claims")
        if isinstance(claims, list):
            n += len(claims)
    return n


def is_reddit(post: dict[str, Any]) -> bool:
    return str(post.get("platform") or "") in REDDIT_PLATFORMS


def platform_claim_counts(posts: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for post in posts:
        if not isinstance(post, dict):
            continue
        plat = str(post.get("platform") or "unknown")
        counts[plat] += post_claim_count(post)
    return dict(counts)


def select_reddit_posts_for_claim_target(
    reddit_posts: list[dict[str, Any]],
    *,
    target_claims: int,
    seed: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Greedy random sample of Reddit posts aiming for ``target_claims`` claims.

    Posts are shuffled with ``seed``, then added in order. After each add that
    reaches/exceeds the target, drop the last post if that is closer to the
    target (unless the pool would become empty).
    """
    if target_claims <= 0 or not reddit_posts:
        return [], 0

    order = list(reddit_posts)
    rng = random.Random(int(seed))
    rng.shuffle(order)

    selected: list[dict[str, Any]] = []
    n_claims = 0
    for post in order:
        n = post_claim_count(post)
        if n_claims < target_claims:
            selected.append(post)
            n_claims += n
            if n_claims >= target_claims:
                # Prefer the closer of (with last) vs (without last)
                without = n_claims - n
                if selected and abs(without - target_claims) < abs(n_claims - target_claims):
                    selected.pop()
                    n_claims = without
                break
    return selected, n_claims


def derive_reddit_balanced_posts(
    posts: list[dict[str, Any]],
    *,
    seed: int = 0,
    target_ratio: float = 1.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Keep all non-Reddit posts; downsample Reddit so claims ≈ ``target_ratio * other``.

    ``target_ratio=1.0`` means Reddit claims ≈ sum of all other platforms' claims.
    """
    if target_ratio <= 0:
        raise ValueError("target_ratio must be > 0")

    other: list[dict[str, Any]] = []
    reddit: list[dict[str, Any]] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        if is_reddit(post):
            reddit.append(post)
        else:
            other.append(post)

    _, other_claims = count_nested_claims(other)
    _, reddit_claims_all = count_nested_claims(reddit)
    target = int(round(float(target_ratio) * other_claims))

    selected_reddit, reddit_claims_kept = select_reddit_posts_for_claim_target(
        reddit,
        target_claims=target,
        seed=seed,
    )
    out_posts = other + selected_reddit
    # Stable-ish order: non-reddit first (parent order), then selected reddit
    # in sample order. Re-shuffle combined? Keep as-is for reproducibility.

    stats = {
        "seed": int(seed),
        "target_ratio": float(target_ratio),
        "target_reddit_claims": target,
        "other_posts": len(other),
        "other_claims": other_claims,
        "reddit_posts_all": len(reddit),
        "reddit_claims_all": reddit_claims_all,
        "reddit_posts_kept": len(selected_reddit),
        "reddit_claims_kept": reddit_claims_kept,
        "posts_out": len(out_posts),
        "claims_out": other_claims + reddit_claims_kept,
        "platform_claims_in": platform_claim_counts(posts),
        "platform_claims_out": platform_claim_counts(out_posts),
    }
    return out_posts, stats


def build_derived_payload(
    parent_payload: dict[str, Any],
    derived_posts: list[dict[str, Any]],
    *,
    derived_from: str,
    stats: dict[str, Any],
) -> dict[str, Any]:
    """Copy parent meta and attach derive provenance + updated counts."""
    out = {k: v for k, v in parent_payload.items() if k != "posts"}
    post_count, claim_count = count_nested_claims(derived_posts)
    out["posts"] = derived_posts
    out["post_count"] = post_count
    out["claim_count"] = claim_count
    out["derived"] = {
        "from_corpus": derived_from,
        "method": "reddit_downsample_to_other_claims",
        **stats,
    }
    return out
