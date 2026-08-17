"""Tests for claims-only extraction schema/parsing."""

from __future__ import annotations

import json

from nlp.claim_extraction.schema import CLAIMS_ONLY_JSON_SCHEMA, parse_claims_only_output


def test_parse_claims_only_output() -> None:
    raw = json.dumps({"claims": [{"claim": "Vaccines reduce disease."}]})
    out = parse_claims_only_output(raw)
    assert out["claims"][0]["claim"] == "Vaccines reduce disease."


def test_claims_only_schema_shape() -> None:
    props = CLAIMS_ONLY_JSON_SCHEMA["schema"]["properties"]["claims"]["items"]["properties"]
    assert set(props.keys()) == {"claim"}
    required = CLAIMS_ONLY_JSON_SCHEMA["schema"]["properties"]["claims"]["items"]["required"]
    assert required == ["claim"]
