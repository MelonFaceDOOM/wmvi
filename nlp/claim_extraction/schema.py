"""JSON schemas and parsers for claim-extraction model output."""

from __future__ import annotations

import json
from typing import Any

from nlp.claim_extraction.scores import SCORE_FIELD_NAMES, parse_score_01

_SCORE_PROPS: dict[str, Any] = {
    name: {"type": "number", "minimum": 0.0, "maximum": 1.0} for name in SCORE_FIELD_NAMES
}

CLAIMS_ONLY_JSON_SCHEMA: dict[str, Any] = {
    "name": "vaccine_claim_extraction_claims_only",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "claim": {"type": "string"},
                    },
                    "required": ["claim"],
                },
            }
        },
        "required": ["claims"],
    },
}

CLAIMS_JSON_SCHEMA: dict[str, Any] = {
    "name": "vaccine_claim_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "claim": {"type": "string"},
                        **_SCORE_PROPS,
                    },
                    "required": ["claim", *SCORE_FIELD_NAMES],
                },
            }
        },
        "required": ["claims"],
    },
}


def parse_claims_only_output(content: str) -> dict[str, Any]:
    parsed = json.loads(content.strip())
    if not isinstance(parsed, dict):
        raise ValueError("model output JSON top-level is not an object")
    claims = parsed.get("claims")
    if not isinstance(claims, list):
        raise ValueError("model output missing list field 'claims'")
    for i, c in enumerate(claims):
        if not isinstance(c, dict):
            raise ValueError(f"claims[{i}] is not an object")
        if not isinstance(c.get("claim"), str):
            raise ValueError(f"claims[{i}].claim must be a string")
    return parsed


def parse_claims_with_scores_output(content: str) -> dict[str, Any]:
    parsed = json.loads(content.strip())
    if not isinstance(parsed, dict):
        raise ValueError("model output JSON top-level is not an object")
    claims = parsed.get("claims")
    if not isinstance(claims, list):
        raise ValueError("model output missing list field 'claims'")
    for i, c in enumerate(claims):
        if not isinstance(c, dict):
            raise ValueError(f"claims[{i}] is not an object")
        if not isinstance(c.get("claim"), str):
            raise ValueError(f"claims[{i}].claim must be a string")
        for key in SCORE_FIELD_NAMES:
            v, bad = parse_score_01(c.get(key))
            if v is None or bad:
                raise ValueError(f"claims[{i}].{key} must be a number in [0, 1]")
    return parsed
