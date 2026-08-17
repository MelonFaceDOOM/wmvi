"""LLM client factories and connectivity checks for claim extraction."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

from nlp.claim_extraction.schema import (
    CLAIMS_JSON_SCHEMA,
    CLAIMS_ONLY_JSON_SCHEMA,
    parse_claims_only_output,
    parse_claims_with_scores_output,
)

load_dotenv()

# Chat-based connectivity pings (Azure deployments). Reasoning models (gpt-5.x,
# o-series) spend completion budget on internal reasoning, so 1 token always
# fails; 256 is plenty for a one-word ping while staying cheap.
_PING_MAX_COMPLETION_TOKENS = 256


def _ping_chat_completion(client: Any, *, model: str) -> None:
    """Issue a tiny chat completion to verify a deployment responds."""
    from openai._exceptions import APIStatusError

    messages = [{"role": "user", "content": "Reply with exactly: ok"}]
    try:
        client.chat.completions.create(
            model=model,
            messages=messages,
            max_completion_tokens=_PING_MAX_COMPLETION_TOKENS,
        )
    except APIStatusError as exc:
        status = getattr(exc, "status_code", None)
        err = str(exc).lower()
        if status == 400 and "max_completion_tokens" in err and "unsupported" in err:
            client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=_PING_MAX_COMPLETION_TOKENS,
            )
            return
        raise


def _verify_openai_model(client: Any, *, model: str) -> None:
    """Verify auth, network, and model availability without a completion."""
    client.models.retrieve(model)


@dataclass(frozen=True)
class AzureConfig:
    key: str
    endpoint: str
    api_version: str


@dataclass(frozen=True)
class OpenAIConfig:
    key: str
    base_url: str | None = None


def load_azure_config() -> AzureConfig:
    missing: list[str] = []
    key = os.getenv("AZURE_OPENAI_KEY")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    if not key:
        missing.append("AZURE_OPENAI_KEY")
    if not endpoint:
        missing.append("AZURE_OPENAI_ENDPOINT")
    if missing:
        if len(missing) == 1:
            raise RuntimeError(f"Missing {missing[0]} in environment.")
        raise RuntimeError(f"Missing {', '.join(missing)} in environment.")
    return AzureConfig(
        key=key,
        endpoint=endpoint,
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )


def check_azure_connectivity(model: str) -> tuple[bool, str]:
    """Ping Azure OpenAI with a minimal completion; returns (ok, message)."""
    try:
        cfg = load_azure_config()
    except RuntimeError as exc:
        return False, str(exc)

    try:
        from openai import AzureOpenAI
        from openai._exceptions import APIConnectionError, APIStatusError, APITimeoutError

        client = AzureOpenAI(
            api_key=cfg.key,
            azure_endpoint=cfg.endpoint,
            api_version=cfg.api_version,
            timeout=15.0,
            max_retries=0,
        )
        _ping_chat_completion(client, model=model)
        return True, f"Connected to {cfg.endpoint} as {model}"
    except APIConnectionError:
        return (
            False,
            "Cannot reach Azure OpenAI from this machine. Check network, DNS, firewall, or VPN.",
        )
    except APITimeoutError:
        return False, "Connection to Azure OpenAI timed out. Check network or AZURE_OPENAI_ENDPOINT."
    except APIStatusError as exc:
        status = getattr(exc, "status_code", None)
        if status == 401:
            return False, "Authentication failed (401). Check AZURE_OPENAI_KEY."
        if status == 404:
            return (
                False,
                f"Deployment '{model}' not found (404). Use your Azure deployment name, not the base model name.",
            )
        message = getattr(exc, "message", None) or str(exc)
        return False, f"Azure API error ({status}): {message}"
    except Exception as exc:
        return False, f"Unexpected error checking Azure connectivity: {exc}"


def load_openai_config() -> OpenAIConfig:
    key = os.getenv("PERSONAL_OPENAI_API_KEY")
    if not key:
        raise RuntimeError("Missing PERSONAL_OPENAI_API_KEY in environment.")
    base_url = os.getenv("PERSONAL_OPENAI_BASE_URL") or None
    return OpenAIConfig(key=key, base_url=base_url)


def check_openai_connectivity(model: str) -> tuple[bool, str]:
    """Ping OpenAI with a minimal completion; returns (ok, message)."""
    try:
        cfg = load_openai_config()
    except RuntimeError as exc:
        return False, str(exc)

    try:
        from openai import OpenAI
        from openai._exceptions import APIConnectionError, APIStatusError, APITimeoutError

        kwargs: dict[str, Any] = {"api_key": cfg.key, "timeout": 15.0, "max_retries": 0}
        if cfg.base_url:
            kwargs["base_url"] = cfg.base_url
        client = OpenAI(**kwargs)
        _verify_openai_model(client, model=model)
        endpoint = cfg.base_url or "https://api.openai.com/v1"
        return True, f"Connected to OpenAI ({endpoint}) as {model}"
    except APIConnectionError:
        return (
            False,
            "Cannot reach OpenAI from this machine. Check network, DNS, firewall, or VPN.",
        )
    except APITimeoutError:
        return False, "Connection to OpenAI timed out. Check network or PERSONAL_OPENAI_BASE_URL."
    except APIStatusError as exc:
        status = getattr(exc, "status_code", None)
        if status == 401:
            return False, "Authentication failed (401). Check PERSONAL_OPENAI_API_KEY."
        if status == 404:
            return (
                False,
                f"Model '{model}' not found (404). Use a model name available on your OpenAI account.",
            )
        message = getattr(exc, "message", None) or str(exc)
        return False, f"OpenAI API error ({status}): {message}"
    except Exception as exc:
        return False, f"Unexpected error checking OpenAI connectivity: {exc}"


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
    from nlp.claim_extraction.api_requester import AzureClaimsClient

    if api_key is not None and azure_endpoint is not None:
        version = api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
        key = api_key
        endpoint = azure_endpoint
    elif api_key is not None or azure_endpoint is not None or api_version is not None:
        cfg = load_azure_config()
        key = api_key if api_key is not None else cfg.key
        endpoint = azure_endpoint if azure_endpoint is not None else cfg.endpoint
        version = api_version if api_version is not None else cfg.api_version
    else:
        cfg = load_azure_config()
        key = cfg.key
        endpoint = cfg.endpoint
        version = cfg.api_version
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


def build_openai_claims_client(
    *,
    model: str,
    system_prompt_builder: Callable[[dict[str, Any]], str],
    user_prompt_builder: Callable[[dict[str, Any]], str],
    claims_only: bool = True,
    api_key: str | None = None,
    base_url: str | None = None,
) -> Any:
    from nlp.claim_extraction.api_requester import OpenAIClaimsClient

    if api_key is not None:
        key = api_key
        url = base_url
    else:
        cfg = load_openai_config()
        key = cfg.key
        url = base_url if base_url is not None else cfg.base_url
    schema = CLAIMS_ONLY_JSON_SCHEMA if claims_only else CLAIMS_JSON_SCHEMA
    parser = parse_claims_only_output if claims_only else parse_claims_with_scores_output
    return OpenAIClaimsClient(
        api_key=key,
        base_url=url,
        model=model,
        system_prompt_builder=system_prompt_builder,
        user_prompt_builder=user_prompt_builder,
        response_schema=schema,
        output_parser=parser,
    )


_DEFAULT_STRUCTURED_MAX_TOKENS = 4096


def _build_openai_sdk_client() -> Any:
    from openai import OpenAI

    cfg = load_openai_config()
    kwargs: dict[str, Any] = {"api_key": cfg.key}
    if cfg.base_url:
        kwargs["base_url"] = cfg.base_url
    return OpenAI(**kwargs)


def _build_azure_sdk_client() -> Any:
    from openai import AzureOpenAI

    cfg = load_azure_config()
    return AzureOpenAI(
        api_key=cfg.key,
        azure_endpoint=cfg.endpoint,
        api_version=cfg.api_version,
    )


def _structured_completion_with_client(
    client: Any,
    *,
    model: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    max_completion_tokens: int,
) -> dict[str, Any]:
    from nlp.claim_extraction.api_requester import _create_structured_chat_completion

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]
    resp = _create_structured_chat_completion(
        client,
        model=model,
        messages=messages,
        response_schema=schema,
        max_completion_tokens=max_completion_tokens,
    )
    if not getattr(resp, "choices", None):
        raise RuntimeError("Model response has no choices.")
    content = getattr(resp.choices[0].message, "content", None)
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Model response content is empty.")
    parsed = json.loads(content.strip())
    if not isinstance(parsed, dict):
        raise ValueError("structured completion top-level is not an object")
    return parsed


def openai_structured_completion(
    *,
    model: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    max_completion_tokens: int = _DEFAULT_STRUCTURED_MAX_TOKENS,
) -> dict[str, Any]:
    """Single OpenAI chat completion with JSON schema response."""
    return _structured_completion_with_client(
        _build_openai_sdk_client(),
        model=model,
        system=system,
        user=user,
        schema=schema,
        max_completion_tokens=max_completion_tokens,
    )


def azure_structured_completion(
    *,
    model: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    max_completion_tokens: int = _DEFAULT_STRUCTURED_MAX_TOKENS,
) -> dict[str, Any]:
    """Single Azure OpenAI chat completion with JSON schema response."""
    return _structured_completion_with_client(
        _build_azure_sdk_client(),
        model=model,
        system=system,
        user=user,
        schema=schema,
        max_completion_tokens=max_completion_tokens,
    )
