"""Claim-text input formatting for Ridge heads (group-level scoring)."""

from __future__ import annotations

from typing import Any


def claim_only_input(claim_text: str) -> str:
    return f"[CLAIM]\n{claim_text}\n"


def build_structured_input(var_keys: list[str], claim_text: str, post_text: str = "") -> str:
    """Minimal structured input from ordered variable keys (CLAIM / TEXT)."""
    parts: list[str] = []
    for key in var_keys:
        k = str(key).upper()
        if k in ("CLAIM", "CLAIM_TEXT"):
            parts.append(f"[CLAIM]\n{claim_text}\n")
        elif k in ("TEXT", "CONTEXT", "POST"):
            parts.append(f"[TEXT]\n{post_text}\n")
        else:
            parts.append(f"[{key}]\n{claim_text}\n")
    return "\n".join(parts).strip() + "\n" if parts else claim_only_input(claim_text)


def build_input_for_head(
    *,
    input_var_keys: list[str] | None,
    claim_text: str,
    post_text: str = "",
    score_field_name: str | None = None,
) -> str:
    """Build model-facing string. Group scoring defaults to claim-only."""
    _ = score_field_name
    keys = list(input_var_keys or ["CLAIM"])
    if keys == ["CLAIM"] or not keys:
        return claim_only_input(claim_text)
    return build_structured_input(keys, claim_text, post_text)
