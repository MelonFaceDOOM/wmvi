"""LLM backend for Prompt Lab.

Flip ``LLM_PROVIDER`` only — no UI switch.
  "openai" = personal OpenAI (PERSONAL_OPENAI_API_KEY)
  "azure"  = Azure Foundry (AZURE_OPENAI_KEY + AZURE_OPENAI_ENDPOINT)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from nlp.claim_extraction.clients import (
    azure_structured_completion,
    build_azure_claims_client,
    build_openai_claims_client,
    check_azure_connectivity,
    check_openai_connectivity,
    load_azure_config,
    load_openai_config,
    openai_structured_completion,
)
from nlp.claim_extraction.schema import (
    CLAIMS_ALIGNMENT_JSON_SCHEMA,
    CLAIMS_ONLY_JSON_SCHEMA,
    parse_claims_alignment_output,
    parse_claims_only_output,
)

Provider = Literal["openai", "azure"]

# Single switch: "openai" | "azure"
LLM_PROVIDER: Provider = "openai"


def is_azure() -> bool:
    return LLM_PROVIDER == "azure"


def provider_label() -> str:
    return "Azure OpenAI" if is_azure() else "OpenAI"


def load_config() -> Any:
    return load_azure_config() if is_azure() else load_openai_config()


def check_connectivity(model: str) -> tuple[bool, str]:
    if is_azure():
        return check_azure_connectivity(model)
    return check_openai_connectivity(model)


def prompts_request_alignment(system_prompt: str, user_prompt: str) -> bool:
    """True when the profile asks for claim_vaccine_alignment_score (next prompts)."""
    blob = f"{system_prompt}\n{user_prompt}"
    return "claim_vaccine_alignment_score" in blob


def build_claims_client(
    *,
    model: str,
    system_prompt_builder: Callable[[dict[str, Any]], str],
    user_prompt_builder: Callable[[dict[str, Any]], str],
    alignment: bool = False,
) -> Any:
    if alignment:
        schema = CLAIMS_ALIGNMENT_JSON_SCHEMA
        parser = parse_claims_alignment_output
    else:
        schema = CLAIMS_ONLY_JSON_SCHEMA
        parser = parse_claims_only_output
    kwargs: dict[str, Any] = {
        "model": model,
        "claims_only": not alignment,
        "system_prompt_builder": system_prompt_builder,
        "user_prompt_builder": user_prompt_builder,
        "response_schema": schema,
        "output_parser": parser,
    }
    if is_azure():
        return build_azure_claims_client(**kwargs)
    return build_openai_claims_client(**kwargs)


def structured_completion(
    *,
    model: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    max_completion_tokens: int = 4096,
) -> dict[str, Any]:
    fn = azure_structured_completion if is_azure() else openai_structured_completion
    return fn(
        model=model,
        system=system,
        user=user,
        schema=schema,
        max_completion_tokens=max_completion_tokens,
    )
