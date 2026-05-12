"""
Independent heuristic scores for anecdote / authority / common-knowledge framing.

Each score in [0, 1]; multiple can be high at once. Used via ``predict_bundle``.
"""

from __future__ import annotations

import re
from typing import Any

from apps.claim_extractor.model_common import (
    ClaimRecord,
    LabelField,
    SinglePrediction,
    clamp_score_01,
)

DEFAULT_CONFIG: dict[str, Any] = {
    "variant_name": "rules_v1",
    "authority_patterns": [
        r"\b(?:cdc|fda|who|nih|study|studies|researchers|scientists|doctor|doctors|physician)\b",
        r"\baccording to (?:the )?(?:cdc|fda|who|experts)\b",
        r"\bpublished in\b",
    ],
    "anecdote_kinship_patterns": [
        r"\bmy (?:son|daughter|child|kid|mother|father|mom|dad|parent|wife|husband|friend|neighbor)\b",
        r"\b(?:his|her) (?:son|daughter|child)\b",
    ],
    "anecdote_self_patterns": [
        r"\bi (?:had|have|got|received|took)\b",
        r"\bmy experience\b",
        r"\bin my case\b",
        r"\bwe went through\b",
    ],
    "common_knowledge_patterns": [
        r"\bit(?:'s| is) common knowledge\b",
        r"\beveryone knows\b",
        r"\bobviously\b",
        r"\bgenerally accepted\b",
        r"\bof course\b",
    ],
    "hearsay_patterns": [
        r"\bi heard\b",
        r"\bpeople say\b",
        r"\bthey say\b",
    ],
    "per_hit": 0.28,
    "per_hit_cap": 1.0,
}


def _pattern_hits(blob: str, patterns: list[str]) -> int:
    return sum(1 for p in patterns if re.search(p, blob, flags=re.IGNORECASE))


def _score_from_hits(hits: int, per: float, cap: float) -> float:
    return clamp_score_01(min(cap, per * max(0, hits)))


def _triplet_scores(blob: str, cfg: dict[str, Any]) -> tuple[float, float, float, str]:
    per = float(cfg.get("per_hit") or DEFAULT_CONFIG["per_hit"])
    cap = float(cfg.get("per_hit_cap") or DEFAULT_CONFIG["per_hit_cap"])

    auth_pats = list(cfg.get("authority_patterns") or DEFAULT_CONFIG["authority_patterns"])
    kin_pats = list(cfg.get("anecdote_kinship_patterns") or DEFAULT_CONFIG["anecdote_kinship_patterns"])
    self_pats = list(cfg.get("anecdote_self_patterns") or DEFAULT_CONFIG["anecdote_self_patterns"])
    ck_pats = list(cfg.get("common_knowledge_patterns") or DEFAULT_CONFIG["common_knowledge_patterns"])
    hear_pats = list(cfg.get("hearsay_patterns") or DEFAULT_CONFIG["hearsay_patterns"])

    auth_h = _pattern_hits(blob, auth_pats)
    anecdote_h = _pattern_hits(blob, kin_pats) + _pattern_hits(blob, self_pats)
    ck_h = _pattern_hits(blob, ck_pats) + int(0.5 * _pattern_hits(blob, hear_pats))

    a_score = _score_from_hits(anecdote_h, per, cap)
    au_score = _score_from_hits(auth_h, per, cap)
    ck_score = _score_from_hits(ck_h, per, cap)
    reason = f"anecdote_hits={anecdote_h}_authority_hits={auth_h}_common_knowledge_hits={ck_h}"
    return a_score, au_score, ck_score, reason


def predict_bundle(
    records: list[ClaimRecord], config: dict[str, Any]
) -> dict[str, list[SinglePrediction]]:
    cfg = {**DEFAULT_CONFIG, **config}
    name = str(cfg.get("variant_name") or "rules_v1")
    an: list[SinglePrediction] = []
    au: list[SinglePrediction] = []
    ck: list[SinglePrediction] = []
    for rec in records:
        blob = f"{rec.claim.get('claim') or ''}\n{rec.input_text or ''}"
        av, uv, cv, reason = _triplet_scores(blob, cfg)
        an.append(SinglePrediction(value=av, reason=reason, pred_model_name=name, confidence=0.5))
        au.append(SinglePrediction(value=uv, reason=reason, pred_model_name=name, confidence=0.5))
        ck.append(SinglePrediction(value=cv, reason=reason, pred_model_name=name, confidence=0.5))
    return {
        "attribution_anecdote_score": an,
        "attribution_authority_score": au,
        "attribution_common_knowledge_score": ck,
    }


# Legacy single-field entry: anecdote only (unused if run_label_models uses bundle).
def predict(records: list[ClaimRecord], config: dict[str, Any]) -> list[SinglePrediction]:
    return predict_bundle(records, config)["attribution_anecdote_score"]


field = LabelField.ATTRIBUTION_ANECDOTE_SCORE
