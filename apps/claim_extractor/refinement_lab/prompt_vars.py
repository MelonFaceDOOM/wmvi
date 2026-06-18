"""Post-level prompt variables and ``{var}`` template rendering."""

from __future__ import annotations

import re
from typing import Any

from apps.claim_extractor.extraction_core import format_input_text
from apps.claim_extractor.model_common import stable_task_id
from apps.claim_extractor.refinement_lab import posts_data

_VAR_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _text_coreference_resolved(post: dict[str, Any]) -> str:
    return posts_data.post_text(post)


def _plain_text(post: dict[str, Any]) -> str:
    t = post.get("text")
    return t if isinstance(t, str) else ""


def _platform(post: dict[str, Any]) -> str:
    return posts_data.platform_name(post)


def _task_id(post: dict[str, Any]) -> str:
    return stable_task_id(post)


def _text_input(post: dict[str, Any]) -> str:
    return format_input_text(post, posts_data.post_text(post))


VAR_EXTRACTORS: dict[str, Any] = {
    "text_coreference_resolved": _text_coreference_resolved,
    "text": _plain_text,
    "platform": _platform,
    "task_id": _task_id,
    "text_input": _text_input,
}

VAR_DISPLAY_NAMES: dict[str, str] = {
    "text_coreference_resolved": "Post text (coref-resolved, else plain)",
    "text": "Post text (plain)",
    "platform": "Platform",
    "task_id": "Task ID",
    "text_input": "Formatted input (titles + body, same as get_claims)",
    "max_claims": "Max claims (from profile setting)",
}


def list_var_keys() -> list[str]:
    keys = sorted(VAR_EXTRACTORS.keys())
    keys.append("max_claims")
    return keys


def display_name(key: str) -> str:
    return VAR_DISPLAY_NAMES.get(key, key)


def build_var_map(post_row: dict[str, Any], *, max_claims: int) -> dict[str, str]:
    values = {key: str(fn(post_row)) for key, fn in VAR_EXTRACTORS.items()}
    values["max_claims"] = str(max_claims)
    return values


def render_template(template: str, values: dict[str, str]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"Unknown template variable: {{{key}}}")
        return values[key]

    return _VAR_PATTERN.sub(repl, template)


def render_profile_prompts(
    *,
    system_prompt: str,
    user_prompt: str,
    post_row: dict[str, Any],
    max_claims: int,
) -> tuple[str, str]:
    values = build_var_map(post_row, max_claims=max_claims)
    return render_template(system_prompt, values), render_template(user_prompt, values)
