"""Shared claim-extraction helpers (schemas, formatting, Azure client factory)."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any

from apps.claim_extractor.model_common import SCORE_FIELD_NAMES, parse_score_01

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


def format_input_text(row: dict[str, Any], text: str) -> str:
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


def build_azure_claims_client(
    *,
    model: str,
    system_prompt_builder: Callable[[dict[str, Any]], str],
    user_prompt_builder: Callable[[dict[str, Any]], str],
    claims_only: bool = True,
    api_key: str | None = None,
    azure_endpoint: str | None = None,
    api_version: str | None = None,
) -> Any:
    from apps.claim_extractor.api_requester import AzureClaimsClient

    key = api_key or os.getenv("AZURE_OPENAI_KEY")
    endpoint = azure_endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
    version = api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
    if not key:
        raise RuntimeError("Missing AZURE_OPENAI_KEY in environment.")
    if not endpoint:
        raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT in environment.")
    schema = CLAIMS_ONLY_JSON_SCHEMA if claims_only else CLAIMS_JSON_SCHEMA
    parser = parse_claims_only_output if claims_only else parse_claims_with_scores_output
    return AzureClaimsClient(
        api_key=key,
        azure_endpoint=endpoint,
        api_version=version,
        model=model,
        system_prompt_builder=system_prompt_builder,
        user_prompt_builder=user_prompt_builder,
        response_schema=schema,
        output_parser=parser,
    )
