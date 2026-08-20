"""Tests for embed_qwen_measles_bal (mocked embed/upload, no GPU)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from scripts.oneoffs import embed_qwen_measles_bal as mod


class _FakeCorpus:
    def __init__(self, root: Path) -> None:
        self._root = root

    def run_dir(self, model_tag: str) -> Path:
        return self._root / model_tag


def _fake_export(*, run_dir: Path, out_zip: Path) -> dict:
    Path(out_zip).parent.mkdir(parents=True, exist_ok=True)
    Path(out_zip).write_bytes(b"PK")
    return {"ok": True, "out": str(out_zip), "run_dir": str(run_dir)}


def test_parse_last_json() -> None:
    raw = 'not json\n{"ok":false}\n{"ok":true,"claim_count":1}\n'
    assert mod._parse_last_json(raw) == {"ok": True, "claim_count": 1}


def test_dry_run_uses_dry_tag_limit_force_and_uploads(tmp_path: Path) -> None:
    zip_path = tmp_path / "dry.embed.zip"
    embed_dir = tmp_path / "qwen3-emb-8b-dry"

    def fake_embed(**kwargs):
        return {"ok": True, "run_dir": str(embed_dir), "claim_count": 1}

    with (
        patch("apps.claims.corpus.get_corpus", return_value=_FakeCorpus(tmp_path)),
        patch.object(mod, "run_embed", side_effect=fake_embed) as embed,
        patch("apps.claims.runs_xfer.export_run", side_effect=_fake_export),
        patch("storage.nitwitch_upload.load_upload_config", return_value=("https://x/", "u", "p", True)),
        patch(
            "storage.nitwitch_upload.upload_file",
            return_value="https://nitwitch.com/u/dry.embed.zip",
        ) as upload,
    ):
        summary = mod.run(
            dry_run=True,
            out_zip=zip_path,
            log_path=tmp_path / "dry.log",
        )

    assert summary["model_tag"] == "qwen3-emb-8b-dry"
    assert summary["limit"] == 1
    assert summary["download_url"].endswith("dry.embed.zip")
    embed.assert_called_once()
    assert embed.call_args.kwargs["model_tag"] == "qwen3-emb-8b-dry"
    assert embed.call_args.kwargs["limit"] == 1
    assert embed.call_args.kwargs["force"] is True
    upload.assert_called_once()


def test_full_run_skips_upload_and_does_not_force(tmp_path: Path) -> None:
    zip_path = tmp_path / "full.embed.zip"
    embed_dir = tmp_path / "qwen3-emb-8b"

    with (
        patch("apps.claims.corpus.get_corpus", return_value=_FakeCorpus(tmp_path)),
        patch.object(
            mod,
            "run_embed",
            return_value={"ok": True, "run_dir": str(embed_dir), "claim_count": 99},
        ) as embed,
        patch("apps.claims.runs_xfer.export_run", side_effect=_fake_export),
    ):
        summary = mod.run(
            dry_run=False,
            skip_upload=True,
            out_zip=zip_path,
            log_path=tmp_path / "full.log",
        )

    assert summary["model_tag"] == "qwen3-emb-8b"
    assert summary["limit"] is None
    assert summary["uploaded_url"] is None
    assert zip_path.is_file()
    assert embed.call_args.kwargs["force"] is False
    assert embed.call_args.kwargs["limit"] is None


def test_main_dry_run_prints_ok() -> None:
    with patch.object(
        mod,
        "run",
        return_value={
            "model_tag": "qwen3-emb-8b-dry",
            "download_url": "https://nitwitch.com/dl/uploads/x.zip",
        },
    ):
        rc = mod.main(["--dry-run"])
    assert rc == 0
