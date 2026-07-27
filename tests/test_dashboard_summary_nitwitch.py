"""Unit tests for NitwitchUploadStorage and dashboard summary wiring (no network)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from services.dashboard_summary.summarizer import get_storage_backend
from storage.backends.nitwitch import NitwitchUploadStorage, _remote_name


def test_remote_name_bare_ok():
    assert _remote_name("dashboard_summary.json") == "dashboard_summary.json"


def test_remote_name_rejects_nested():
    with pytest.raises(ValueError, match="bare filename"):
        _remote_name("subdir/dashboard_summary.json")


def test_is_accessible_missing_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("NITWITCH_UPLOAD_URL", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_USER", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_PASSWORD", raising=False)
    ok, reason = NitwitchUploadStorage().is_accessible()
    assert ok is False
    assert reason is not None
    assert "NITWITCH_UPLOAD" in reason


def test_is_accessible_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/upload/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)
    with patch("storage.nitwitch_upload.REPO_ROOT", tmp_path):
        ok, reason = NitwitchUploadStorage().is_accessible()
    assert ok is True
    assert reason is None


def test_write_text_calls_upload_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/upload/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)

    captured: dict = {}

    def fake_upload(local_path: Path, *, remote_name: str | None = None, timeout_s: float = 300):
        captured["bytes"] = Path(local_path).read_bytes()
        captured["remote_name"] = remote_name
        return f"https://nitwitch.com/upload/{remote_name}"

    with patch("storage.backends.nitwitch.upload_file", side_effect=fake_upload):
        NitwitchUploadStorage().write_text("dashboard_summary.json", '{"ok": true}')

    assert captured["remote_name"] == "dashboard_summary.json"
    assert captured["bytes"] == b'{"ok": true}'


def test_get_storage_backend_nitwitch(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SUMMARY_STORAGE_KIND", "nitwitch")
    backend = get_storage_backend()
    assert isinstance(backend, NitwitchUploadStorage)
