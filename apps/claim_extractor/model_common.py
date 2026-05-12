"""
Shared types, score field names, validation, and iterators for claim labeling.

Score fields stay aligned with apps/claim_extractor/get_claims.py (CLAIMS_JSON_SCHEMA).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Protocol, Sequence

# --- Continuous scores on each claim (0.0 .. 1.0), see get_claims / prompts for semantics ---

SCORE_FIELD_NAMES: tuple[str, ...] = (
    "claim_vaccine_alignment_score",
    "author_claim_agreement_score",
    "attribution_anecdote_score",
    "attribution_authority_score",
    "attribution_common_knowledge_score",
)

# Gold column -> written prediction column on each claim dict
PRED_JSON_KEYS: dict[str, str] = {name: f"pred_{name}" for name in SCORE_FIELD_NAMES}

ATTRIBUTION_SCORE_FIELDS: frozenset[str] = frozenset(
    {
        "attribution_anecdote_score",
        "attribution_authority_score",
        "attribution_common_knowledge_score",
    }
)


class LabelField(str, Enum):
    CLAIM_VACCINE_ALIGNMENT_SCORE = "claim_vaccine_alignment_score"
    AUTHOR_CLAIM_AGREEMENT_SCORE = "author_claim_agreement_score"
    ATTRIBUTION_ANECDOTE_SCORE = "attribution_anecdote_score"
    ATTRIBUTION_AUTHORITY_SCORE = "attribution_authority_score"
    ATTRIBUTION_COMMON_KNOWLEDGE_SCORE = "attribution_common_knowledge_score"


@dataclass
class ClaimRecord:
    """One extracted claim in context of its parent post row."""

    task_id: str
    claim_index: int
    post_row: dict[str, Any]
    claim: dict[str, Any]
    #: Resolved input text used for extraction (formatted), when available.
    input_text: str | None = None


@dataclass
class SinglePrediction:
    """Prediction for one score field on one claim."""

    value: float
    confidence: float | None = None
    reason: str | None = None
    pred_model_name: str | None = None
    coerced_from_invalid: bool = False


@dataclass
class PredictionCounters:
    invalid_claim_alignment: int = 0
    invalid_author_agreement: int = 0
    invalid_attribution_anecdote: int = 0
    invalid_attribution_authority: int = 0
    invalid_attribution_common: int = 0


@dataclass
class ClaimPredictions:
    claim_vaccine_alignment_score: SinglePrediction | None = None
    author_claim_agreement_score: SinglePrediction | None = None
    attribution_anecdote_score: SinglePrediction | None = None
    attribution_authority_score: SinglePrediction | None = None
    attribution_common_knowledge_score: SinglePrediction | None = None

    counters: PredictionCounters = field(default_factory=PredictionCounters)


class LabelPredictor(Protocol):
    field: LabelField

    def predict(self, records: list[ClaimRecord], config: dict[str, Any]) -> list[SinglePrediction]:
        ...


def clamp_score_01(x: float) -> float:
    if math.isnan(x) or math.isinf(x):
        return 0.5
    return max(0.0, min(1.0, float(x)))


def parse_score_01(raw: Any) -> tuple[float | None, bool]:
    """
    Parse a value that should be a finite float in [0, 1].

    Returns (value, invalid) where value is None if missing/invalid.
    """
    if raw is None:
        return None, True
    if isinstance(raw, bool):
        return None, True
    if isinstance(raw, (int, float)):
        v = float(raw)
        if math.isnan(v) or math.isinf(v):
            return None, True
        if 0.0 <= v <= 1.0:
            return v, False
        return None, True
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None, True
        try:
            v = float(s)
        except ValueError:
            return None, True
        if math.isnan(v) or math.isinf(v):
            return None, True
        if 0.0 <= v <= 1.0:
            return v, False
        return None, True
    return None, True


def resolve_enum_choice(user_input: str, options: Sequence[str]) -> str | None:
    """
    Map free text to exactly one of ``options`` (for tests / non-interactive tools).

    - Strip input; empty -> None.
    - Case-insensitive equality to a full option -> that option.
    - Else prefix match: options where ``opt.lower().startswith(s.lower())``.
      Exactly one match -> that option; zero or multiple -> None.
    """
    s = (user_input or "").strip()
    if not s:
        return None
    low = s.lower()
    for opt in options:
        if opt.lower() == low:
            return opt
    matches = [opt for opt in options if opt.lower().startswith(low)]
    if len(matches) == 1:
        return matches[0]
    return None


# (JSON key, short description for manual labeling prompts)
MANUAL_SCORE_FIELDS: tuple[tuple[str, str], ...] = (
    ("claim_vaccine_alignment_score", "0=strongly anti-vaccine … 0.5=neutral/mixed/unclear … 1=strongly pro-vaccine"),
    ("author_claim_agreement_score", "0=author rejects claim … 0.5=unclear/reporting … 1=author supports claim"),
    ("attribution_anecdote_score", "0=none … 1=strong personal/anecdote framing (self or relations)"),
    ("attribution_authority_score", "0=none … 1=strong expert/institution/study framing"),
    ("attribution_common_knowledge_score", "0=none … 1=strong obvious/widely-known framing"),
)


def stable_task_id(row: dict[str, Any]) -> str:
    tid = row.get("task_id")
    if tid is not None and str(tid).strip():
        return str(tid)
    src = row.get("source_post_id")
    idx = row.get("sentence_boundary_chunk_index")
    if src is not None and idx is not None:
        return f"{src}:{idx}"
    return str(row.get("post_id", "unknown"))


def input_text_for_row(row: dict[str, Any]) -> str:
    """Best-effort extraction text (aligned with get_claims formatting intent)."""
    text = row.get("text_coreference_resolved")
    if not isinstance(text, str) or not text.strip():
        text = row.get("text")
    if not isinstance(text, str):
        return ""
    platform = str(row.get("platform", "unknown"))
    if platform == "reddit_submission":
        return f"Submission title: {row.get('reddit_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "reddit_comment":
        return f"Reddit comment context title: {row.get('reddit_comment_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "youtube_video":
        return f"YouTube video title: {row.get('youtube_video_title') or 'Unknown'}\n\n{text}"
    if platform == "podcast_episode":
        return f"Podcast name: {row.get('podcast_name') or 'Unknown'}\n\n{text}"
    return text


def iter_success_claim_records(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> Iterator[ClaimRecord]:
    """
    Yield ClaimRecord for posts with successful extraction and non-empty claims list.
    Optional caps apply in iteration order (not random sampling).
    """
    posts_seen = 0
    claims_emitted = 0
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
        if max_posts is not None and posts_seen >= max_posts:
            break
        posts_seen += 1
        tid = stable_task_id(row)
        itext = input_text_for_row(row) or None
        for i, c in enumerate(claims):
            if max_claims is not None and claims_emitted >= max_claims:
                return
            if not isinstance(c, dict):
                continue
            claims_emitted += 1
            yield ClaimRecord(
                task_id=tid,
                claim_index=i,
                post_row=row,
                claim=c,
                input_text=itext,
            )


def load_posts_from_claims_json(path: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load `{ ... meta, posts: [...] }` JSON; returns (payload, posts list)."""
    import json
    from pathlib import Path

    p = Path(path)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object at top level.")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Expected top-level 'posts' list.")
    cleaned = [x for x in posts if isinstance(x, dict)]
    return payload, cleaned


PredictFn = Callable[[list[ClaimRecord], dict[str, Any]], list[SinglePrediction]]
