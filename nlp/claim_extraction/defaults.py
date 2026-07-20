"""Canonical claim-extraction model / runner defaults."""

from __future__ import annotations

MODEL_NAME = "gpt-5.4-mini"

# Placeholders the user prompt template must contain (checked at load time).
REQUIRED_USER_PLACEHOLDERS: tuple[str, ...] = ("{{max_claims}}", "{{text_input}}")

# Optional on system template; substituted when present.
OPTIONAL_SYSTEM_PLACEHOLDERS: tuple[str, ...] = ("{{max_claims}}",)

DEFAULT_MAX_CLAIMS = 8
DEFAULT_MAX_WORKERS = 6
DEFAULT_BATCH_COUNT = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_TARGET_RPM = 90
DEFAULT_429_COOLDOWN_S = 20.0
