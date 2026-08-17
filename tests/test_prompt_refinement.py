"""Tests for Prompt Lab (`apps.prompt_refinement`)."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from apps.prompt_refinement import db, posts_data, prompt_vars, seed_import


def test_prompt_vars_render() -> None:
    post = {
        "platform": "reddit_submission",
        "text": "Vaccines work.",
        "reddit_submission_title": "Test title",
    }
    sys_out, user_out = prompt_vars.render_profile_prompts(
        system_prompt="Platform={platform} max={max_claims}",
        user_prompt="Body: {text_input}",
        post_row=post,
        max_claims=5,
    )
    assert "reddit_submission" in sys_out
    assert "5" in sys_out
    assert "Test title" in user_out
    assert "Vaccines work." in user_out


def test_seed_import_groups_by_task_id() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        labeler = Path(tmp) / "labeler.sqlite"
        refine = Path(tmp) / "refine.sqlite"
        lconn = sqlite3.connect(str(labeler))
        lconn.executescript(
            """
            CREATE TABLE problem_claims (
                task_id TEXT, claim_index INTEGER, post_json TEXT,
                claim_json TEXT, note TEXT, head_id INTEGER,
                flagged_from_head TEXT, created_at TEXT, updated_at TEXT,
                UNIQUE(task_id, claim_index)
            );
            """
        )
        post = {
            "platform": "reddit_submission",
            "text": "hello",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "A"}]},
        }
        pj = json.dumps(post)
        lconn.execute(
            "INSERT INTO problem_claims (task_id, claim_index, post_json, claim_json, note) VALUES (?,?,?,?,?)",
            ("t1", 0, pj, "{}", "note one"),
        )
        lconn.execute(
            "INSERT INTO problem_claims (task_id, claim_index, post_json, claim_json, note) VALUES (?,?,?,?,?)",
            ("t1", 1, pj, "{}", "note two"),
        )
        lconn.commit()
        lconn.close()

        rconn = db.connect(refine)
        db.init_schema(rconn)
        inserted, skipped = seed_import.import_from_labeler_lab(rconn, labeler_db_path=labeler)
        assert inserted == 1
        assert skipped == 0
        rows = db.fetch_problem_posts_sorted(rconn)
        assert len(rows) == 1
        assert rows[0]["task_id"] == "t1"
        assert "note one" in rows[0]["comment"]
        assert "note two" in rows[0]["comment"]
        assert rows[0]["source"] == "seed"

        inserted2, skipped2 = seed_import.import_from_labeler_lab(rconn, labeler_db_path=labeler)
        assert inserted2 == 0
        assert skipped2 == 1
        rconn.close()


def test_metrics_prf_and_aggregate() -> None:
    from apps.prompt_refinement import metrics

    prf = metrics.claim_prf(matched=2, missed=1, extra=1)
    assert abs(prf["precision"] - 2 / 3) < 1e-9
    assert prf["recall"] == 2 / 3
    agg = metrics.aggregate_per_post(
        [
            {
                "task_id": "a",
                "precision": 1.0,
                "recall": 0.5,
                "f1": 0.67,
                "alignment": {
                    "matched": [{"reference_index": 0, "candidate_index": 0, "note": ""}],
                    "missed": [{"reference_index": 1, "issue_category": "missed_implicit_claim", "note": ""}],
                    "extra": [],
                },
            }
        ]
    )
    assert agg["n_posts"] == 1
    assert agg["issue_categories"].get("missed_implicit_claim") == 1


def test_meta_prompt_validation() -> None:
    from apps.prompt_refinement.meta_defaults import validate_meta_prompt

    validate_meta_prompt("objective", "anything goes")
    validate_meta_prompt(
        "diagnose_post",
        "{objective} {post_text} {reference_claims} {candidate_claims}",
    )
    with pytest.raises(ValueError, match="missing placeholders"):
        validate_meta_prompt("diagnose_post", "{objective} only")


def test_baseline_profile_and_sync() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "refine.sqlite"
        conn = db.connect(path)
        db.init_lab(conn)
        baseline_id = db.ensure_baseline_profile(conn)
        prof = db.get_profile_by_name(conn, db.BASELINE_PROFILE_NAME)
        assert prof is not None
        assert prof.id == baseline_id
        assert "{text_input}" in prof.user_prompt or "{max_claims}" in prof.system_prompt
        db.upsert_problem_post(
            conn,
            task_id="t99",
            post_row={"platform": "reddit_submission", "text": "x"},
            baseline_claims=[{"claim": "Baseline claim"}],
            baseline_status="success",
            comment="c",
        )
        n = db.sync_baseline_extractions(conn)
        assert n >= 1
        hits = db.fetch_extractions_for_profile(conn, baseline_id)
        assert "t99" in hits
        assert hits["t99"]["output_json"]["claims"][0]["claim"] == "Baseline claim"
        conn.close()


def test_reference_claim_crud() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "refine.sqlite"
        conn = db.connect(path)
        db.init_schema(conn)
        db.add_reference_claim(conn, "t1", "Claim A")
        ref = db.get_reference_claims(conn, "t1")
        assert ref is not None
        assert len(ref.claims) == 1
        db.edit_reference_claim(conn, "t1", 0, "Claim A edited")
        db.add_reference_claim(conn, "t1", "Claim B")
        ref2 = db.get_reference_claims(conn, "t1")
        assert posts_data.claim_texts(ref2.claims) == ["Claim A edited", "Claim B"]
        db.delete_reference_claim(conn, "t1", 0)
        ref3 = db.get_reference_claims(conn, "t1")
        assert posts_data.claim_texts(ref3.claims) == ["Claim B"]
        conn.close()


def test_render_meta_template() -> None:
    from apps.prompt_refinement.meta_defaults import render_meta_template

    out = render_meta_template("Hello {name}", {"name": "world"})
    assert out == "Hello world"

