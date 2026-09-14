"""Prompts for LLM leaf-merge (same proposition, not epi-value)."""

from __future__ import annotations

from typing import Any

MERGE_SYSTEM_PROMPT = (
    "You group vaccine- and epidemiology-related claims by underlying proposition. "
    "Return only JSON matching the schema."
)

MERGE_USER_PROMPT = """\
You are merging over-split claim groups. The seed group and each neighbor are already \
small clusters of grouped claim texts (identical wording is already collapsed).

Decide which neighbors express the SAME underlying proposition as the seed. \
A proposition is atomic but complete: keep qualifiers that belong to the same claim \
(population, vaccine, disease, mechanism, outcome, policy).

Do NOT merge when neighbors differ in:
- outcome or effect
- causal mechanism
- vaccine or disease/variant
- population or setting
- policy vs clinical vs epidemiological claim

Same topic or the same entity is not enough. If unsure, leave the neighbor out \
(default stay split).

Seed group id={seed_id}
Medoid: {seed_medoid}
Members:
{seed_members}

Neighbor groups (only these ids may appear in merge_ids):
{neighbors}

Return JSON with merge_ids: the neighbor ids that should merge into the seed. \
Empty list if none. Do not include the seed id.
"""

MERGE_RESPONSE_SCHEMA: dict[str, Any] = {
    "name": "leaf_merge_response",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "merge_ids": {"type": "array", "items": {"type": "integer"}},
        },
        "required": ["merge_ids"],
    },
}

DEFAULT_MERGE_MODEL = "gpt-5.6-luna"
DEFAULT_NN_THRESHOLD = 0.8
DEFAULT_NEIGHBOR_K = 15
DEFAULT_MEMBER_SNIPPETS = 3
