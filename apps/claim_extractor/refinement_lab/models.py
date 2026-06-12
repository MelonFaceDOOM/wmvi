"""Azure Foundry model options for prompt profiles."""

from __future__ import annotations

DEFAULT_MODEL = "gpt-5.4-mini"

SEED_MODELS: tuple[str, ...] = (
    "gpt-5.4-mini",
    "gpt-5.4",
    "gpt-4.1-mini",
    "gpt-4.1",
    "gpt-4o-mini",
    "gpt-4o",
)
