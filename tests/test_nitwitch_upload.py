"""Unit tests for storage.nitwitch_upload (no network)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from storage.nitwitch_upload import load_upload_config, resolve_cacert_path, upload_file


def test_load_upload_config_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("NITWITCH_UPLOAD_URL", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_USER", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_PASSWORD", raising=False)
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)
    with pytest.raises(ValueError, match="NITWITCH_UPLOAD_URL"):
        load_upload_config()


def test_load_upload_config_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/u/secret")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)
    # Avoid picking up a real repo-root cert.pem during tests.
    with patch("storage.nitwitch_upload.REPO_ROOT", tmp_path):
        base, user, password, verify = load_upload_config()
    assert base == "https://nitwitch.com/u/secret/"
    assert user == "melon"
    assert password == "pw"
    assert verify is True


def test_resolve_cacert_relative_to_repo_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)
    cert = tmp_path / "cert.pem"
    cert.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    with patch("storage.nitwitch_upload.REPO_ROOT", tmp_path):
        assert resolve_cacert_path() == cert.resolve()
        assert resolve_cacert_path("cert.pem") == cert.resolve()


def test_resolve_cacert_explicit_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NITWITCH_UPLOAD_CACERT", "missing.pem")
    with patch("storage.nitwitch_upload.REPO_ROOT", tmp_path):
        with pytest.raises(FileNotFoundError, match="NITWITCH_UPLOAD_CACERT"):
            resolve_cacert_path()


def test_upload_file_put_with_cacert(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cert = tmp_path / "cert.pem"
    cert.write_text("-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n")
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/upload/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    monkeypatch.setenv("NITWITCH_UPLOAD_CACERT", "cert.pem")
    local = tmp_path / "new_terms.json"
    local.write_text("{}\n", encoding="utf-8")

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.raise_for_status = MagicMock()

    with (
        patch("storage.nitwitch_upload.REPO_ROOT", tmp_path),
        patch("storage.nitwitch_upload.requests.put", return_value=mock_resp) as put,
    ):
        url = upload_file(local, remote_name="new_terms.json")

    assert url == "https://nitwitch.com/upload/new_terms.json"
    put.assert_called_once()
    args, kwargs = put.call_args
    assert args[0] == url
    assert kwargs["auth"] == ("melon", "pw")
    assert kwargs["data"] == local.read_bytes()
    assert kwargs["verify"] == str(cert.resolve())


def test_upload_file_rejects_bad_remote_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NITWITCH_UPLOAD_URL", "https://nitwitch.com/u/secret/")
    monkeypatch.setenv("NITWITCH_UPLOAD_USER", "melon")
    monkeypatch.setenv("NITWITCH_UPLOAD_PASSWORD", "pw")
    monkeypatch.delenv("NITWITCH_UPLOAD_CACERT", raising=False)
    local = tmp_path / "x.json"
    local.write_text("{}", encoding="utf-8")
    with patch("storage.nitwitch_upload.REPO_ROOT", tmp_path):
        with pytest.raises(ValueError, match="Invalid remote"):
            upload_file(local, remote_name="../evil.json")
