"""Tests for get_claims claims-only mode."""

from __future__ import annotations

import json

from apps.claim_extractor.get_claims import (
    CLAIMS_ONLY_JSON_SCHEMA,
    _parse_and_validate_output_claims_only,
)


def test_parse_claims_only_output() -> None:
    raw = json.dumps({"claims": [{"claim": "Vaccines reduce disease."}]})
    out = _parse_and_validate_output_claims_only(raw)
    assert out["claims"][0]["claim"] == "Vaccines reduce disease."


def test_claims_only_schema_shape() -> None:
    props = CLAIMS_ONLY_JSON_SCHEMA["schema"]["properties"]["claims"]["items"]["properties"]
    assert set(props.keys()) == {"claim"}
    required = CLAIMS_ONLY_JSON_SCHEMA["schema"]["properties"]["claims"]["items"]["required"]
    assert required == ["claim"]
