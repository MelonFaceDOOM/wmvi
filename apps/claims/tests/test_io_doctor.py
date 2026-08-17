"""Tests for io run-dir reader and doctor CLI."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from apps.claims import io as claims_io
from apps.claims.__main__ import main


def test_load_run_arrays(tmp_path: Path):
    run_dir = tmp_path / "run_a"
    run_dir.mkdir()
    vectors = np.random.default_rng(0).normal(size=(5, 8)).astype(np.float32)
    np.save(run_dir / "vectors.npy", vectors)
    (run_dir / "index.json").write_text(
        json.dumps({"claim_texts": [f"c{i}" for i in range(5)], "model_id": "dummy"}),
        encoding="utf-8",
    )
    v2, index = claims_io.load_run_arrays(run_dir)
    assert v2.shape == (5, 8)
    assert len(claims_io.claim_texts_from_index(index)) == 5
    assert index["groups"]


def test_doctor_without_run():
    rc = main(["doctor", "--skip-model"])
    assert rc == 0


def test_ls_artifacts():
    rc = main(["ls-artifacts"])
    assert rc == 0
