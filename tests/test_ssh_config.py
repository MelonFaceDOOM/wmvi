from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from storage.ssh.config import SSHConfig


def test_open_tunnel_leaves_tunnel_none_when_ssh_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("dummy-key\n", encoding="utf-8")
    monkeypatch.setenv("SSH_HOST", "example.test")
    monkeypatch.setenv("SSH_USERNAME", "u")
    monkeypatch.setenv("SSH_PKEY", str(key))

    import storage.ssh.tunnel as tunnel_mod

    def fake_popen(cmd, **kwargs):
        proc = MagicMock()
        proc.poll.return_value = 255
        proc.returncode = 255
        proc.stderr.read.return_value = b"Permission denied (publickey)."
        return proc

    monkeypatch.setattr(tunnel_mod, "_ssh_binary", lambda: "ssh")
    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    tunnel_mod._TUNNEL = None

    with pytest.raises(RuntimeError, match="Permission denied"):
        tunnel_mod.open_tunnel("127.0.0.1", 5432)

    assert tunnel_mod._TUNNEL is None


def test_open_tunnel_builds_ssh_forward_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("dummy-key\n", encoding="utf-8")
    monkeypatch.setenv("SSH_HOST", "192.168.2.84")
    monkeypatch.setenv("SSH_USERNAME", "melon")
    monkeypatch.setenv("SSH_PKEY", str(key))
    monkeypatch.setenv("SSH_PORT", "22")

    import storage.ssh.tunnel as tunnel_mod

    captured_cmd: list[list[str]] = []

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        proc = MagicMock()
        proc.poll.return_value = None
        return proc

    monkeypatch.setattr(tunnel_mod, "_ssh_binary", lambda: "/usr/bin/ssh")
    monkeypatch.setattr(tunnel_mod, "_pick_local_port", lambda: 54321)
    monkeypatch.setattr(tunnel_mod, "_wait_until_listening", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    tunnel_mod._TUNNEL = None

    tunnel_mod.open_tunnel("127.0.0.1", 5432)
    try:
        assert len(captured_cmd) == 1
        cmd = captured_cmd[0]
        assert cmd[0] == "/usr/bin/ssh"
        assert "-N" in cmd
        assert "-i" in cmd and str(key) in cmd
        assert "-L" in cmd
        idx = cmd.index("-L")
        assert cmd[idx + 1] == "127.0.0.1:54321:127.0.0.1:5432"
        assert cmd[-1] == "melon@192.168.2.84"
    finally:
        tunnel_mod.close_tunnel()


def test_ssh_config_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("dummy-key\n", encoding="utf-8")

    monkeypatch.setenv("SSH_HOST", "nitwitch.example")
    monkeypatch.setenv("SSH_USERNAME", "melon")
    monkeypatch.setenv("SSH_PKEY", str(key))
    monkeypatch.setenv("SSH_PORT", "2222")

    cfg = SSHConfig.from_env()
    assert cfg.host == "nitwitch.example"
    assert cfg.username == "melon"
    assert cfg.port == 2222
    assert cfg.pkey_path == key


def test_ssh_config_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("SSH_HOST", "SSH_USERNAME", "SSH_PKEY"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="SSH_HOST"):
        SSHConfig.from_env()


def test_ssh_config_expands_tilde(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    key = home / ".ssh" / "wmvi_key"
    key.parent.mkdir(parents=True)
    key.write_text("k\n", encoding="utf-8")

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("SSH_HOST", "h")
    monkeypatch.setenv("SSH_USERNAME", "u")
    monkeypatch.setenv("SSH_PKEY", "~/.ssh/wmvi_key")

    cfg = SSHConfig.from_env()
    assert cfg.pkey_path.resolve() == key.resolve()
