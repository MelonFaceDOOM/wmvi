"""Tests for prompt-refinement eval sample + run_label (no API)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.claims.keys import claim_key
from apps.prompt_refinement import db
from apps.prompt_refinement import eval_sample as es


def _nested_fixture() -> dict:
    c_good = "MMR vaccine prevents measles."
    c_bad = "This post says vaccines are bad."
    return {
        "posts": [
            {
                "post_id": 1,
                "platform": "reddit_submission",
                "reddit_submission_title": "Measles",
                "text": "full post",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "task_id": "p1:0",
                        "text": "MMR works.",
                        "claim_extraction_disposition": "success",
                        "claims": [{"claim": c_good}],
                    },
                    {
                        "chunk_index": 1,
                        "task_id": "p1:1",
                        "text": "Meta claim chunk.",
                        "claim_extraction_disposition": "success",
                        "claims": [{"claim": c_bad}],
                    },
                ],
            },
            {
                "post_id": 2,
                "platform": "telegram_post",
                "text": "tg",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "task_id": "p2:0",
                        "text": "Outbreak facts.",
                        "claim_extraction_disposition": "success",
                        "claims": [{"claim": "Measles outbreaks are rising."}],
                    }
                ],
            },
        ]
    }


def test_canonical_to_lab_placeholders() -> None:
    system, user = es.canonical_to_lab_placeholders(
        "sys {{max_claims}}",
        "user {{text_input}} {{max_claims}}",
    )
    assert system == "sys {max_claims}"
    assert user == "user {text_input} {max_claims}"
    assert "{{" not in system + user


def test_flatten_and_write_sample_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from apps.claims import annotations as ann_mod
    from apps.claims import corpus as corpus_mod
    from apps.claims import io as claims_io

    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    corp = corpus_mod.create_corpus("toy", notes="test")
    payload = _nested_fixture()
    claims_io.write_json(corp.claims, payload)

    # standalone=0 on the meta claim only
    bad_key = claim_key("This post says vaccines are bad.")
    good_key = claim_key("MMR vaccine prevents measles.")
    other_key = claim_key("Measles outbreaks are rising.")
    ann_mod.write_annotation(
        corp.root,
        "standalone_pred_m1",
        {bad_key: 0.0, good_key: 1.0, other_key: 1.0},
        scope="group",
        producer="test",
    )

    rows = es.flatten_corpus_chunks(corpus="toy")
    assert len(rows) == 3
    by_id = {r["task_id"]: r for r in rows}
    assert by_id["p1:1"]["has_standalone_0"] is True
    assert by_id["p1:0"]["has_standalone_0"] is False
    assert by_id["p2:0"]["platform"] == "telegram_post"
    assert by_id["p1:0"]["post_row"]["claim_extraction_status"] == "success"

    pool_path = tmp_path / "pool.json"
    summary = es.write_pool_json(rows, pool_path, corpus="toy")
    assert summary["n_chunks"] == 3
    assert summary["n_has_standalone_0"] == 1

    ids_path = tmp_path / "ids.txt"
    ids_path.write_text("p1:0\np1:1\n", encoding="utf-8")
    sample_path = tmp_path / "eval30.json"
    result = es.write_sample_from_ids(
        pool_path=pool_path,
        ids=es.read_task_ids(ids_path),
        out=sample_path,
    )
    assert result["n"] == 2
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    assert len(sample["posts"]) == 2
    assert sample["posts"][0]["task_id"] == "p1:0"


def test_import_sample_and_run_label_isolation(tmp_path: Path) -> None:
    # Minimal sample
    posts = [
        {
            "task_id": "tA",
            "platform": "reddit_submission",
            "text": "hello",
            "claim_extraction_status": "success",
            "claim_extraction_output": {"claims": [{"claim": "A claim."}]},
        }
    ]
    sample_path = tmp_path / "eval.json"
    sample_path.write_text(
        json.dumps({"kind": "prompt_refinement_eval_sample", "posts": posts}),
        encoding="utf-8",
    )
    db_path = tmp_path / "lab.sqlite"
    out = es.import_sample_to_lab(sample_path=sample_path, db_path=db_path, clear_existing=True)
    assert out["inserted"] == 1
    assert out["n_problem"] == 1

    conn = db.connect(db_path)
    db.init_lab(conn)
    # create a non-baseline profile
    pid = db.create_profile(
        conn,
        name="current",
        system_prompt="s",
        user_prompt="{text_input}",
        model="dummy",
        max_claims=8,
    )
    db.upsert_profile_extraction(
        conn,
        profile_id=pid,
        task_id="tA",
        status="success",
        output_json={"claims": [{"claim": "run1"}]},
        run_label="1",
    )
    db.upsert_profile_extraction(
        conn,
        profile_id=pid,
        task_id="tA",
        status="success",
        output_json={"claims": [{"claim": "run2"}]},
        run_label="2",
    )
    r1 = db.get_extraction(conn, profile_id=pid, task_id="tA", run_label="1")
    r2 = db.get_extraction(conn, profile_id=pid, task_id="tA", run_label="2")
    assert r1 is not None and r1["output_json"]["claims"][0]["claim"] == "run1"
    assert r2 is not None and r2["output_json"]["claims"][0]["claim"] == "run2"
    snaps = db.list_extraction_snapshots(conn)
    keys = {s["key"] for s in snaps}
    assert "current / 1" in keys
    assert "current / 2" in keys
    assert db.next_run_label(conn, pid) == "3"
    conn.close()


def test_load_prompts_from_files(tmp_path: Path) -> None:
    sys_p = tmp_path / "s.txt"
    usr_p = tmp_path / "u.txt"
    sys_p.write_text("System {{max_claims}}", encoding="utf-8")
    usr_p.write_text("User {{text_input}} max={{max_claims}}", encoding="utf-8")
    system, user = db.load_prompts_from_files(system_path=sys_p, user_path=usr_p)
    assert system == "System {max_claims}"
    assert user == "User {text_input} max={max_claims}"
