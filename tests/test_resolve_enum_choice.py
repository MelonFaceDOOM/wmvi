"""Tests for apps.claim_extractor.model_common.resolve_enum_choice."""

from __future__ import annotations

import pytest

from apps.claim_extractor.model_common import resolve_enum_choice


@pytest.fixture
def toy_options() -> tuple[str, ...]:
    return ("foo", "foobar", "baz")


@pytest.fixture
def menu_options() -> tuple[str, ...]:
    return ("alpha", "beta", "neutral", "unclear")


def test_empty_returns_none() -> None:
    assert resolve_enum_choice("", menu_options) is None
    assert resolve_enum_choice("   ", menu_options) is None


def test_full_string_case_insensitive(menu_options: tuple[str, ...]) -> None:
    assert resolve_enum_choice("ALPHA", menu_options) == "alpha"
    assert resolve_enum_choice("Neutral", menu_options) == "neutral"


def test_unique_prefix(menu_options: tuple[str, ...]) -> None:
    assert resolve_enum_choice("u", menu_options) == "unclear"
    assert resolve_enum_choice("n", menu_options) == "neutral"


def test_ambiguous_prefix_returns_none(toy_options: tuple[str, ...]) -> None:
    assert resolve_enum_choice("f", toy_options) is None
    assert resolve_enum_choice("fo", toy_options) is None


def test_longer_prefix_disambiguates(toy_options: tuple[str, ...]) -> None:
    assert resolve_enum_choice("foo", toy_options) == "foo"
    assert resolve_enum_choice("foob", toy_options) == "foobar"


def test_no_match_returns_none(toy_options: tuple[str, ...]) -> None:
    assert resolve_enum_choice("zzz", toy_options) is None
    assert resolve_enum_choice("x", toy_options) is None
