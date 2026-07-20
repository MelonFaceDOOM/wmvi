"""Defaults for LLM triplet-anchor discovery (file-mode)."""

from __future__ import annotations

import re
from typing import Any

TRIPLET_CATEGORY_PLACEHOLDER = "(uncategorized)"

TRIPLET_CATEGORIES: tuple[str, ...] = (
    "Vaccine Safety",
    "Vaccine Effectiveness",
    "Disease Pathogenicity",
    "Natural Immunity",
    "Intentional Harm or Conspiracy",
    "Vaccination Policy",
    "Vaccination Recommendations",
    "Epidemiology and Transmission",
    "Population Statistics",
    "Public Trust",
    "Institutions and Actors",
    "Clinical Management",
    "Compensation and Legal Issues",
)

_VAR_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

REQUIRED_PLACEHOLDERS: tuple[str, ...] = ("claim", "neighbors", "categories")

DISCOVERY_SYSTEM_PROMPT = (
    "You are an expert at grouping vaccine- and epidemiology-related claims for "
    "embedding-model triplet training. Return only JSON matching the schema."
)

DEFAULT_DISCOVERY_USER_PROMPT = """\
You are helping build triplet-training anchors for a vaccine-claim embedding model.

Seed claim:
{claim}

Nearest neighbor claims (by embedding similarity; {neighbor_count} shown):
{neighbors}

Categories (pick the single best one for the seed claim):
{categories}

First decide whether the seed claim is usable. A claim is NOT usable if it is too
vague, generic, incomplete, or otherwise useless as a training anchor. In that case
set usable_claim=false and return empty positives, negatives, and category.

If the claim is usable, set usable_claim=true and:
- choose exactly one category from the list above
- identify neighbors that are CLEAR positives (same underlying proposition) and
  CLEAR negatives (contradictory or clearly different proposition). Only include
  high-confidence items; leave a list empty if there are no clear matches.

Return JSON with:
- usable_claim: boolean
- positives: list of neighbor claim texts (verbatim from the neighbor list)
- negatives: list of neighbor claim texts (verbatim from the neighbor list)
- category: one category name from the list (empty string when usable_claim=false)
"""

DISCOVERY_RESPONSE_SCHEMA: dict[str, Any] = {
    "name": "anchor_discovery_response",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "usable_claim": {"type": "boolean"},
            "positives": {"type": "array", "items": {"type": "string"}},
            "negatives": {"type": "array", "items": {"type": "string"}},
            "category": {"type": "string"},
        },
        "required": ["usable_claim", "positives", "negatives", "category"],
    },
}


def collect_discovery_categories(anchors: list[Any] | None = None) -> list[str]:
    out = list(TRIPLET_CATEGORIES)
    seen = set(out)
    for anchor in anchors or []:
        cat = str(getattr(anchor, "category", "") or "").strip()
        if cat and cat != TRIPLET_CATEGORY_PLACEHOLDER and cat not in seen:
            out.append(cat)
            seen.add(cat)
    return out


def format_discovery_categories(categories: list[str] | None = None) -> str:
    cats = categories if categories is not None else list(TRIPLET_CATEGORIES)
    return "\n".join(cats)


def validate_discovery_prompt(template: str) -> None:
    found = set(_VAR_PATTERN.findall(template))
    missing = set(REQUIRED_PLACEHOLDERS) - found
    if missing:
        raise ValueError(
            "Discovery prompt missing placeholders: "
            + ", ".join(f"{{{m}}}" for m in sorted(missing))
        )


def render_discovery_prompt(
    template: str,
    *,
    claim: str,
    neighbors: str,
    categories: str,
    neighbor_count: int,
    claim_index: int,
    existing_anchor_count: int = 0,
    validate_template: bool = True,
) -> str:
    if validate_template:
        validate_discovery_prompt(template)
    values: dict[str, str] = {
        "claim": claim,
        "neighbors": neighbors,
        "categories": categories,
        "neighbor_count": str(neighbor_count),
        "claim_index": str(claim_index),
    }

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key == "existing_anchor_texts":
            n = int(existing_anchor_count)
            return f"{n} existing anchor(s); list omitted (seeds exclude duplicates)"
        if key in ("families",):
            return ""
        if key not in values:
            raise ValueError(f"Unknown template variable: {{{key}}}")
        return values[key]

    return _VAR_PATTERN.sub(repl, template)
