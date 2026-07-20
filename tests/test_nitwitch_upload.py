"""Unit tests for storage.nitwitch_upload (no network)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from storage.nitwitch_upload import load_upload_config, upload_file


def test_load_upload_config_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("NITWITCH_UPLOAD_URL", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_USER", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="NITWITCH_UPLOAD_URL"):
        load_upload_config()


def test_load_upload_config_ok(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/u/secret")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    base, user, password = load_upload_config()
    assert base == "https://nitwitch.com/u/secret/"
    assert user == "melon"
    assert password == "pw"


def test_upload_file_put(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/u/secret/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    local = tmp_path / "measles_posts.json"
    local.write_text('{"posts":[]}\n', encoding="utf-8")

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.raise_for_status = MagicMock()

    with patch("storage.nitwitch_upload.requests.put", return_value=mock_resp) as put:
        url = upload_file(local, remote_name="measles_posts.json")

    assert url == "https://nitwitch.com/u/secret/measles_posts.json"
    put.assert_called_once()
    args, kwargs = put.call_args
    assert args[0] == url
    assert kwargs["auth"] == ("melon", "pw")
    assert kwargs["data"] == local.read_bytes()


def test_upload_file_rejects_bad_remote_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/u/secret/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    local = tmp_path / "x.json"
    local.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid remote"):
        upload_file(local, remote_name="../evil.json")
