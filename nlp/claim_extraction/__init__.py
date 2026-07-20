"""Canonical claim extraction: prompts, defaults, and prep helpers.

Batch LLM I/O and ``ConcurrentApiRequester`` live in ``apps.claims.extraction``.
"""

from nlp.claim_extraction.defaults import (
    DEFAULT_BATCH_COUNT,
    DEFAULT_MAX_CLAIMS,
    DEFAULT_MAX_WORKERS,
    MODEL_NAME,
)
from nlp.claim_extraction.prompts import (
    PromptTemplateError,
    load_system_template,
    load_user_template,
    render_system,
    render_user,
)

__all__ = [
    "DEFAULT_BATCH_COUNT",
    "DEFAULT_MAX_CLAIMS",
    "DEFAULT_MAX_WORKERS",
    "MODEL_NAME",
    "PromptTemplateError",
    "load_system_template",
    "load_user_template",
    "render_system",
    "render_user",
]
