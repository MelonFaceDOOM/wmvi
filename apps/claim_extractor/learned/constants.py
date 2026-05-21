"""Shared paths and encoder id for learned claim-score models."""

from __future__ import annotations

from pathlib import Path

# apps/claim_extractor/learned/constants.py -> repo root is parents[3]
REPO_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_ENCODER_MODEL_ID = "BAAI/bge-small-en-v1.5"
