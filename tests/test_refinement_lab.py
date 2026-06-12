"""Tests for claim extraction refinement lab."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from apps.claim_extractor.refinement_lab import db, prompt_vars, seed_import


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
