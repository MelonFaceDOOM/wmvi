"""Structured input string from ordered variable keys."""

from __future__ import annotations

from typing import Any

from apps.claim_extractor.labeler_lab.var_registry import extract_var


def build_structured_input(var_keys: list[str], post_row: dict[str, Any], claim_dict: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in var_keys:
        body = extract_var(key, post_row, claim_dict)
        parts.append(f"[{key}]\n{body}\n")
    return "\n".join(parts).strip() + "\n" if parts else ""
