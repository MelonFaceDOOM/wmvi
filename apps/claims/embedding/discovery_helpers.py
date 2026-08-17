"""Helpers for LLM triplet-anchor discovery (no SQLite)."""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claims.embedding.discovery_defaults import (
    DISCOVERY_RESPONSE_SCHEMA,
    TRIPLET_CATEGORY_PLACEHOLDER,
)
from nlp.claim_extraction.clients import openai_structured_completion

DEFAULT_MAX_WORKERS = 4
DEFAULT_TOP_K_NEIGHBORS = 10


class DiscoveryClient:
    """RequestClient adapter for per-claim discovery LLM calls."""

    def __init__(self, *, model: str, system: str) -> None:
        self._model = model
        self._system = system

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        out = openai_structured_completion(
            model=self._model,
            system=self._system,
            user=str(payload["user_prompt"]),
            schema=DISCOVERY_RESPONSE_SCHEMA,
        )
        return out, {}


def normalize_strings(items: list[Any] | None) -> list[str]:
    out: list[str] = []
    for item in items or []:
        s = str(item).strip()
        if s:
            out.append(s)
    return out


def is_unusable_response(response: dict[str, Any]) -> bool:
    return response.get("usable_claim", True) is False


def pick_category(response: dict[str, Any]) -> str:
    cat = str(response.get("category") or "").strip()
    return cat or TRIPLET_CATEGORY_PLACEHOLDER


def record_unusable_claim(
    log_path: Path,
    *,
    claim_text: str,
    row_id: str,
    task_id: str,
    post: str,
    model: str,
) -> None:
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "id": row_id,
        "task_id": task_id,
        "post_text": post,
        "claim_text": claim_text,
    }
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def sample_claim_indices(
    claim_texts: list[str],
    *,
    exclude_texts: set[str],
    n: int,
    rng: random.Random | None = None,
) -> list[int]:
    r = rng or random.Random()
    candidates: list[int] = []
    exclude_norm = {t.strip() for t in exclude_texts}
    for i, text in enumerate(claim_texts):
        t = (text or "").strip()
        if not t or t in exclude_norm:
            continue
        candidates.append(i)
    if not candidates:
        return []
    k = min(int(n), len(candidates))
    return r.sample(candidates, k=k)
