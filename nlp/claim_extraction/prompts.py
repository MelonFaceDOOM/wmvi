"""Load and render canonical claim-extraction prompt templates."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from nlp.claim_extraction.defaults import OPTIONAL_SYSTEM_PLACEHOLDERS, REQUIRED_USER_PLACEHOLDERS

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_PROMPT_PATH = PROMPTS_DIR / "extract_system.txt"
USER_PROMPT_PATH = PROMPTS_DIR / "extract_user.txt"


class PromptTemplateError(ValueError):
    """Raised when a prompt template is missing required placeholders."""


def _assert_placeholders(template: str, *, required: tuple[str, ...], label: str) -> None:
    missing = [p for p in required if p not in template]
    if missing:
        raise PromptTemplateError(
            f"{label} missing required placeholder(s): {', '.join(missing)}"
        )


@lru_cache(maxsize=1)
def load_system_template() -> str:
    text = SYSTEM_PROMPT_PATH.read_text(encoding="utf-8-sig")
    # System may omit placeholders; nothing required.
    _ = OPTIONAL_SYSTEM_PLACEHOLDERS  # documented contract
    return text


@lru_cache(maxsize=1)
def load_user_template() -> str:
    text = USER_PROMPT_PATH.read_text(encoding="utf-8-sig")
    _assert_placeholders(text, required=REQUIRED_USER_PLACEHOLDERS, label="user prompt")
    return text


def render_system(*, max_claims: int) -> str:
    template = load_system_template()
    return template.replace("{{max_claims}}", str(max_claims)).replace(
        "[[max_claims]]", str(max_claims)
    )


def render_user(text_input: str, *, max_claims: int) -> str:
    template = load_user_template()
    return (
        template.replace("{{max_claims}}", str(max_claims))
        .replace("[[max_claims]]", str(max_claims))
        .replace("{{text_input}}", text_input)
    )


def reload_templates() -> None:
    """Clear cached templates (for tests that mutate prompt files)."""
    load_system_template.cache_clear()
    load_user_template.cache_clear()
