"""
Heuristic baseline for ``author_claim_agreement_score`` (0=reject … 1=support).

Optional LLM path returns a float in [0, 1] when Azure env is configured.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from apps.claim_extractor.model_common import (
    ClaimRecord,
    LabelField,
    SinglePrediction,
    clamp_score_01,
    parse_score_01,
)

DEFAULT_CONFIG: dict[str, Any] = {
    "variant_name": "rules_v1",
    "use_llm": False,
    "llm_model": "gpt-4o-mini",
    "reject_markers": [
        "false",
        "misinformation",
        "debunk",
        "not true",
        "isn't true",
        "isn't supported",
        "no evidence",
        "debunked",
        "wrong",
        "incorrect",
    ],
    "support_markers": [
        "correct",
        "accurate",
        "true that",
        "confirmed",
        "studies confirm",
        "evidence shows",
    ],
    "reporting_verbs": [
        "claims",
        "said that",
        "argues",
        "according to",
        "reports",
        "allegedly",
        " reportedly",
    ],
    "hedge_markers": [
        "might",
        "may",
        "could",
        "possibly",
        "unclear",
        "not sure",
        "unsure",
    ],
}


def _count_any(text: str, phrases: list[str]) -> int:
    t = text.lower()
    return sum(1 for p in phrases if p.lower() in t)


def _rules_score(blob: str, cfg: dict[str, Any]) -> tuple[float, str]:
    rej = _count_any(blob, list(cfg.get("reject_markers") or []))
    sup = _count_any(blob, list(cfg.get("support_markers") or []))
    rep = _count_any(blob, list(cfg.get("reporting_verbs") or []))
    hed = _count_any(blob, list(cfg.get("hedge_markers") or []))

    if rep >= 1 and rej == 0 and sup == 0:
        return 0.5, "reporting_only"
    if hed >= 2:
        return 0.5, "heavy_hedging"
    if rej > sup and rej > 0:
        return 0.2, f"reject_dominant_{rej}_vs_{sup}"
    if sup > rej and sup > 0:
        return 0.85, f"support_dominant_{sup}_vs_{rej}"
    if re.search(r"\b(not|no|never)\s+(safe|effective|true)\b", blob.lower()):
        return 0.15, "negated_positive_claim"
    return 0.5, "no_clear_signal"


def _llm_predict(rec: ClaimRecord, cfg: dict[str, Any]) -> SinglePrediction | None:
    if os.getenv("AZURE_OPENAI_KEY") is None or os.getenv("AZURE_OPENAI_ENDPOINT") is None:
        return None
    try:
        from openai import AzureOpenAI
    except ImportError:
        return None

    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", ""),
    )
    model = str(cfg.get("llm_model") or "gpt-4o-mini")
    claim = str(rec.claim.get("claim") or "")
    ctx = rec.input_text or ""
    prompt = (
        "Rate how much the author agrees with the claim (vaccine-related), on a scale from 0 to 1.\n"
        "0 = clearly rejects or disputes the claim, 0.5 = neutral/reporting/unclear, 1 = clearly endorses it.\n"
        'Reply with JSON only: {"author_claim_agreement_score": <float 0..1>}\n\n'
        f"Claim: {claim}\n\nContext:\n{ctx[:6000]}"
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You output only compact JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    content = (resp.choices[0].message.content or "").strip()
    try:
        obj = json.loads(content)
        raw = obj.get("author_claim_agreement_score")
    except json.JSONDecodeError:
        raw = None
    val, bad = parse_score_01(raw)
    if val is None:
        val = 0.5
        bad = True
    return SinglePrediction(
        value=val,
        confidence=0.85,
        reason="llm_chat_completion",
        pred_model_name=str(cfg.get("variant_name") or "rules_v1") + "+llm",
        coerced_from_invalid=bad,
    )


def predict_one(rec: ClaimRecord, config: dict[str, Any]) -> SinglePrediction:
    cfg = {**DEFAULT_CONFIG, **config}
    blob = f"{rec.claim.get('claim') or ''}\n{rec.input_text or ''}"

    if cfg.get("use_llm"):
        llm_out = _llm_predict(rec, cfg)
        if llm_out is not None:
            return llm_out

    score, reason = _rules_score(blob, cfg)
    return SinglePrediction(
        value=clamp_score_01(score),
        confidence=0.55,
        reason=reason,
        pred_model_name=str(cfg.get("variant_name") or "rules_v1"),
        coerced_from_invalid=False,
    )


def predict(records: list[ClaimRecord], config: dict[str, Any]) -> list[SinglePrediction]:
    return [predict_one(r, config) for r in records]


field = LabelField.AUTHOR_CLAIM_AGREEMENT_SCORE
