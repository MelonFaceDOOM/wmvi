"""Tests for nlp.claim_extraction prompt load/render."""

from __future__ import annotations

import pytest

from nlp.claim_extraction.prompts import (
    PromptTemplateError,
    _assert_placeholders,
    load_system_template,
    load_user_template,
    render_system,
    render_user,
)


def test_load_user_template_has_required_placeholders():
    text = load_user_template()
    assert "{{max_claims}}" in text
    assert "{{text_input}}" in text
    assert "claim_vaccine_alignment_score" in text


def test_load_system_template_ok():
    text = load_system_template()
    assert "claim" in text.lower()
    assert len(text) > 100


def test_render_user_substitutes():
    out = render_user("Hello measles world", max_claims=5)
    assert "Hello measles world" in out
    assert "{{text_input}}" not in out
    assert "0-5" in out or "5" in out
    assert "{{max_claims}}" not in out


def test_render_system_ok():
    out = render_system(max_claims=8)
    assert isinstance(out, str)
    assert len(out) > 50


def test_assert_placeholders_raises():
    with pytest.raises(PromptTemplateError, match="missing required"):
        _assert_placeholders("no placeholders here", required=("{{text_input}}",), label="user prompt")
