"""Unit tests for scripts.migrate_vaccine_terms (no DB)."""

from __future__ import annotations

import pytest

from scripts.migrate_vaccine_terms import (
    NO_SUBSET_KEY,
    assert_safe_to_apply,
    compute_plan,
    load_desired_state,
    normalize_term,
    slice_hit_context,
)


def test_normalize_term():
    assert normalize_term("  CDC  ") == "cdc"
    assert normalize_term("UNICEF") == "unicef"


def test_load_desired_state_multi_subset_and_no_subset():
    desired = load_desired_state(
        {
            "core_search_terms": ["CDC", "Measles", "ACIP"],
            "public_health_organizations": ["CDC", "WHO"],
            NO_SUBSET_KEY: ["Orphan Term", "cdc"],  # cdc already in subsets
        }
    )
    assert "cdc" in desired.terms
    assert "measles" in desired.terms
    assert "orphan term" in desired.terms
    assert desired.memberships["cdc"] == frozenset(
        {"core_search_terms", "public_health_organizations"}
    )
    assert desired.memberships["orphan term"] == frozenset()
    assert "no subset" not in desired.subsets
    assert "core_search_terms" in desired.subsets


def test_load_desired_state_collision_within_bucket():
    desired = load_desired_state({"core_search_terms": ["CDC", "cdc", "Cdc"]})
    assert desired.terms == frozenset({"cdc"})
    assert "cdc" in desired.collisions


def test_slice_hit_context():
    text = "x" * 50 + "MEASLES" + "y" * 50
    start = 50
    end = 57
    ctx = slice_hit_context(text, start, end, pad=10)
    assert ctx.startswith("x" * 10)
    assert "MEASLES" in ctx
    assert ctx.endswith("y" * 10)
    # clamp at edges
    assert slice_hit_context("abc", 0, 1, pad=100) == "abc"


def test_compute_plan_adds_renames_deletes_memberships():
    desired = load_desired_state(
        {
            "core_search_terms": ["measles", "cdc"],
            "public_health_organizations": ["cdc"],
            NO_SUBSET_KEY: ["orphan"],
        }
    )
    db_terms = [
        (1, "Measles"),  # case rename
        (2, "old term"),  # delete
        (3, "cdc"),  # keep
    ]
    db_memberships = [
        ("Measles", "core_search_terms"),
        ("cdc", "core_search_terms"),
        ("cdc", "obsolete_subset"),  # remove for kept term
        ("old term", "core_search_terms"),  # ignored on delete path
    ]
    db_subsets = ["core_search_terms"]

    plan = compute_plan(
        desired,
        db_terms=db_terms,
        db_memberships=db_memberships,
        db_subsets=db_subsets,
    )
    assert plan.adds == ["orphan"]
    assert len(plan.case_renames) == 1
    assert plan.case_renames[0].old_name == "Measles"
    assert plan.case_renames[0].new_name == "measles"
    assert plan.deletes == [(2, "old term")]
    assert plan.subsets_to_create == ["public_health_organizations"]
    assert plan.blocking_errors == []
    assert plan.to_dict()["safe_to_apply"] is True

    mem_add = {(m.term_name, m.subset_name) for m in plan.membership_adds}
    assert ("cdc", "public_health_organizations") in mem_add
    assert ("orphan", "core_search_terms") not in mem_add  # orphan has no subsets

    mem_rm = {(m.term_name, m.subset_name) for m in plan.membership_removes}
    assert ("cdc", "obsolete_subset") in mem_rm
    # membership remove not listed for doomed "old term"
    assert ("old term", "core_search_terms") not in mem_rm


def test_compute_plan_db_case_collision_blocks_apply():
    desired = load_desired_state({"core_search_terms": ["cdc", "measles"]})
    plan = compute_plan(
        desired,
        db_terms=[(1, "CDC"), (2, "cdc"), (3, "measles")],
        db_memberships=[("CDC", "core_search_terms"), ("cdc", "other")],
        db_subsets=["core_search_terms", "other"],
    )
    assert len(plan.case_collisions) == 1
    assert plan.case_collisions[0].lower_name == "cdc"
    assert plan.case_collisions[0].variants == ((1, "CDC"), (2, "cdc"))
    assert plan.blocking_errors
    assert plan.to_dict()["safe_to_apply"] is False
    # Ambiguous key excluded from renames/adds/membership ops
    assert plan.case_renames == []
    assert "cdc" not in plan.adds
    assert all(m.term_name != "cdc" for m in plan.membership_adds)
    assert all(m.term_name != "cdc" for m in plan.membership_removes)
    # Unambiguous terms still planned
    assert plan.deletes == []  # measles kept
    with pytest.raises(RuntimeError, match="Refusing to apply"):
        assert_safe_to_apply(plan)


def test_compute_plan_to_dict_counts():
    desired = load_desired_state({"core_search_terms": ["a"]})
    plan = compute_plan(
        desired,
        db_terms=[],
        db_memberships=[],
        db_subsets=[],
    )
    d = plan.to_dict()
    assert d["counts"]["adds"] == 1
    assert d["adds"] == ["a"]
    assert d["safe_to_apply"] is True
