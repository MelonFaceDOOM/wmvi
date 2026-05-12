"""
Lexical baseline for ``claim_vaccine_alignment_score`` (0=anti … 1=pro).

Keyword hits are turned into a soft pro/anti/neutral mass then mapped to [0, 1].
"""

from __future__ import annotations

from typing import Any

from apps.claim_extractor.model_common import (
    ClaimRecord,
    LabelField,
    SinglePrediction,
    clamp_score_01,
)

DEFAULT_CONFIG: dict[str, Any] = {
    "variant_name": "lexical_v1",
    "pro_keywords": [
        "safe and effective",
        "get vaccinated",
        "vaccination saves",
        "herd immunity",
        "protect yourself",
        "vaccines work",
        "i trust",
        "thank science",
        "approved vaccine",
    ],
    "anti_keywords": [
        "do not trust",
        "don't trust",
        "unsafe",
        "side effect",
        "autism",
        "microchip",
        "depopulation",
        "poison",
        "experimental",
        "refuse the shot",
        "against mandates",
        "big pharma lies",
    ],
    "neutral_keywords": [
        "studies show",
        "research suggests",
        "according to cdc",
        "rates of",
        "efficacy",
        "schedule",
    ],
    "min_margin": 0.15,
}


def _score_keywords(text: str, keywords: list[str]) -> float:
    t = text.lower()
    return sum(1.0 for kw in keywords if kw.lower() in t)


def predict_one(rec: ClaimRecord, config: dict[str, Any]) -> SinglePrediction:
    cfg = {**DEFAULT_CONFIG, **config}
    claim_text = str(rec.claim.get("claim") or "")
    blob = f"{claim_text}\n{rec.input_text or ''}"

    pro_kw = list(cfg.get("pro_keywords") or DEFAULT_CONFIG["pro_keywords"])
    anti_kw = list(cfg.get("anti_keywords") or DEFAULT_CONFIG["anti_keywords"])
    neu_kw = list(cfg.get("neutral_keywords") or DEFAULT_CONFIG["neutral_keywords"])

    sp = _score_keywords(blob, pro_kw)
    sa = _score_keywords(blob, anti_kw)
    sn = _score_keywords(blob, neu_kw)
    total = sp + sa + sn + 1e-9

    margin = max(sp, sa, sn) - sorted([sp, sa, sn])[-2] if max(sp, sa, sn) > 0 else 0.0
    min_margin = float(cfg.get("min_margin") or DEFAULT_CONFIG["min_margin"])

    if total < 1e-6:
        score = 0.5
        reason = "no_keyword_hits"
    elif margin < min_margin and max(sp, sa, sn) > 0:
        score = 0.5
        reason = f"weak_margin={margin:.3f}_sp={sp}_sa={sa}_sn={sn}"
    else:
        # Weighted blend: anti->0, neutral->0.5, pro->1
        score = (sp * 1.0 + sn * 0.5 + sa * 0.0) / total
        reason = f"weighted_sp={sp}_sa={sa}_sn={sn}"

    score = clamp_score_01(score)
    return SinglePrediction(
        value=score,
        confidence=0.55 if total >= 1e-6 else 0.2,
        reason=reason,
        pred_model_name=str(cfg.get("variant_name") or "lexical_v1"),
        coerced_from_invalid=False,
    )


def predict(records: list[ClaimRecord], config: dict[str, Any]) -> list[SinglePrediction]:
    return [predict_one(r, config) for r in records]


field = LabelField.CLAIM_VACCINE_ALIGNMENT_SCORE
