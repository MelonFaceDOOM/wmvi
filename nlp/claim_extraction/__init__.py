"""Canonical claim extraction: prompts, defaults, prep helpers, and LLM runners.

Batch posts-JSON I/O lives in ``nlp.claim_extraction.batch``; shared schemas, text
helpers, concurrent requester, and client factories live here.
"""

from nlp.claim_extraction.api_requester import (
    ConcurrentApiRequester,
    RequestResult,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    default_is_retryable_exception,
)
from nlp.claim_extraction.clients import (
    AzureConfig,
    OpenAIConfig,
    azure_structured_completion,
    build_azure_claims_client,
    build_openai_claims_client,
    check_azure_connectivity,
    check_openai_connectivity,
    load_azure_config,
    load_openai_config,
    openai_structured_completion,
)
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
from nlp.claim_extraction.schema import (
    CLAIMS_JSON_SCHEMA,
    CLAIMS_ONLY_JSON_SCHEMA,
    parse_claims_only_output,
    parse_claims_with_scores_output,
)
from nlp.claim_extraction.text import format_input_text, stable_task_id

__all__ = [
    "AzureConfig",
    "CLAIMS_JSON_SCHEMA",
    "CLAIMS_ONLY_JSON_SCHEMA",
    "ConcurrentApiRequester",
    "DEFAULT_BATCH_COUNT",
    "DEFAULT_MAX_CLAIMS",
    "DEFAULT_MAX_WORKERS",
    "MODEL_NAME",
    "OpenAIConfig",
    "PromptTemplateError",
    "RequestResult",
    "RequestStatus",
    "RequestTask",
    "RetryPolicy",
    "ThrottlePolicy",
    "azure_structured_completion",
    "build_azure_claims_client",
    "build_openai_claims_client",
    "check_azure_connectivity",
    "check_openai_connectivity",
    "default_is_retryable_exception",
    "format_input_text",
    "load_azure_config",
    "load_openai_config",
    "load_system_template",
    "load_user_template",
    "openai_structured_completion",
    "parse_claims_only_output",
    "parse_claims_with_scores_output",
    "render_system",
    "render_user",
    "stable_task_id",
]
