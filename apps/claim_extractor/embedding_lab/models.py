"""Embedding model options for embedding profiles (BGE family)."""

from __future__ import annotations

from apps.claim_extractor.learned.constants import DEFAULT_ENCODER_MODEL_ID

DEFAULT_MODEL = DEFAULT_ENCODER_MODEL_ID  # "BAAI/bge-small-en-v1.5"

SEED_MODELS: tuple[str, ...] = (
    "BAAI/bge-small-en-v1.5",
    "BAAI/bge-base-en-v1.5",
    "BAAI/bge-large-en-v1.5",
)

# BGE v1.5 (English) retrieval convention: prepend this to the *query* only,
# never to the corpus/document side. Symmetric tasks (clustering, claim-vs-claim)
# use no instruction on either side.
DEFAULT_QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages:"
DEFAULT_DOC_INSTRUCTION = ""
