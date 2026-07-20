"""Tests for validate, trim, encode import path, and runs export/import."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from apps.claims import io as claims_io
from apps.claims import runs_xfer
from apps.claims.embedding.encode import DEFAULT_ENCODER_MODEL_ID, encode_texts
from apps.claims.extraction import validate as validate_mod
from apps.claims.prepare import trim as trim_mod


def test_validate_summary_shape(tmp_path: Path):
    posts = {
        "posts": [
            {
                "task_id": "ok",
                "claim_extraction_status": "success",
                "claim_extraction_output": {
                    "claims": [{"claim": "a"}, {"claim": "b"}]
                },
            },
            {
                "task_id": "fail",
                "claim_extraction_status": "failed",
                "claim_extraction_error": "timeout",
            },
            {
                "task_id": "bad",
                "claim_extraction_status": "success",
                "claim_extraction_output": {},
            },
        ]
    }
    path = tmp_path / "claims.json"
    path.write_text(json.dumps(posts), encoding="utf-8")
    summary = validate_mod.run(path)
    assert summary["total_rows"] == 3
    assert summary["success_rows"] == 1
    assert summary["failed_rows"] == 1
    assert summary["malformed_rows"] == 1
    assert summary["total_claims"] == 2
    assert summary["claim_count_hist"]["2"] == 1
    assert isinstance(summary["top_errors"], list)
    assert summary["top_errors"]


def test_trim_synthetic_post(tmp_path: Path):
    text = (
        "First sentence about measles. "
        "Second sentence continues the thought. "
        "Third sentence mentions MMR vaccine explicitly. "
        "Fourth sentence wraps up."
    )
    # Hit span around "MMR vaccine"
    start = text.index("MMR")
    end = start + len("MMR vaccine")
    payload = {
        "posts": [
            {
                "post_id": 1,
                "text": text,
                "hits": [
                    {
                        "term_id": 1,
                        "term_name": "MMR vaccine",
                        "match_start": start,
                        "match_end": end,
                    }
                ],
            }
        ]
    }
    posts_path = tmp_path / "posts.json"
    out_path = tmp_path / "trimmed.json"
    posts_path.write_text(json.dumps(payload), encoding="utf-8")
    summary = trim_mod.run(posts_path=posts_path, out_path=out_path)
    assert summary["post_count"] == 1
    out = json.loads(out_path.read_text(encoding="utf-8"))
    chunks = out["posts"][0]["sentence_boundary_chunks"]
    assert isinstance(chunks, list)
    assert len(chunks) >= 1
    assert "MMR" in chunks[0]


def test_encode_module_importable():
    assert isinstance(DEFAULT_ENCODER_MODEL_ID, str)
    assert "bge" in DEFAULT_ENCODER_MODEL_ID.lower() or "BAAI" in DEFAULT_ENCODER_MODEL_ID
    assert callable(encode_texts)


def test_runs_export_import_roundtrip(tmp_path: Path, monkeypatch):
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    monkeypatch.setattr(claims_io, "runs_dir", lambda: runs_root)
    monkeypatch.setattr(runs_xfer.claims_io, "runs_dir", lambda: runs_root)

    src = runs_root / "measles__test"
    src.mkdir()
    vectors = np.zeros((3, 4), dtype=np.float32)
    np.save(src / claims_io.VECTORS_FILE, vectors)
    claims_io.write_json(
        src / claims_io.INDEX_FILE,
        {"claim_texts": ["a", "b", "c"], "groups": []},
    )
    claims_io.write_json(
        src / claims_io.METRICS_FILE,
        {
            "model_id": "dummy",
            "source_hash": "abc123",
            "claim_count": 3,
            "vector_dim": 4,
        },
    )

    zip_path = tmp_path / "bundle.zip"
    exported = runs_xfer.export_run(run_dir=src, out_zip=zip_path)
    assert exported["ok"] is True
    assert zip_path.is_file()
    assert exported["manifest"]["claim_count"] == 3

    imported = runs_xfer.import_run(
        from_zip=zip_path,
        run_name="measles__imported",
        force=False,
    )
    dest = Path(imported["run_dir"])
    assert dest.is_dir()
    assert (dest / claims_io.VECTORS_FILE).is_file()
    assert (dest / claims_io.INDEX_FILE).is_file()
    assert (dest / claims_io.METRICS_FILE).is_file()
    assert (dest / runs_xfer.MANIFEST_FILE).is_file()
    loaded = np.load(dest / claims_io.VECTORS_FILE)
    assert loaded.shape == (3, 4)
