from __future__ import annotations

import posixpath
from typing import Optional

import paramiko

from storage.ssh.config import SSHConfig

from .base import StorageBackend


class SftpStorage(StorageBackend):
    """SFTP storage rooted at a remote directory (shared SSH credentials with DB tunnel)."""

    def __init__(self, remote_root: str, *, config: SSHConfig | None = None) -> None:
        self.remote_root = remote_root.rstrip("/")
        self._config = config

    @classmethod
    def from_env(cls, remote_root: str) -> "SftpStorage":
        return cls(remote_root, config=SSHConfig.from_env())

    def _config_or_env(self) -> SSHConfig:
        return self._config or SSHConfig.from_env()

    def _connect(self) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        cfg = self._config_or_env()
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=cfg.host,
            port=cfg.port,
            username=cfg.username,
            key_filename=str(cfg.pkey_path),
            timeout=30,
        )
        return client, client.open_sftp()

    def _remote_path(self, rel_path: str) -> str:
        clean = rel_path.lstrip("/").replace("\\", "/")
        return posixpath.join(self.remote_root, clean)

    def _ensure_parent_dirs(self, sftp: paramiko.SFTPClient, remote_path: str) -> None:
        parent = posixpath.dirname(remote_path)
        if not parent or parent == "/":
            return
        parts: list[str] = []
        head = parent
        while head and head not in ("/", self.remote_root):
            parts.append(head)
            head = posixpath.dirname(head)
        for directory in reversed(parts):
            try:
                sftp.stat(directory)
            except OSError:
                sftp.mkdir(directory)

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        client = None
        try:
            client, sftp = self._connect()
            sftp.stat(self.remote_root)
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"
        finally:
            if client is not None:
                client.close()

    def write_text(self, rel_path: str, text: str) -> None:
        self.write_bytes(rel_path, text.encode("utf-8"), content_type="text/plain")

    def write_bytes(
        self,
        rel_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        remote = self._remote_path(rel_path)
        client = None
        try:
            client, sftp = self._connect()
            self._ensure_parent_dirs(sftp, remote)
            with sftp.file(remote, "wb") as f:
                f.write(data)
        finally:
            if client is not None:
                client.close()

    def read_bytes(self, rel_path: str) -> bytes:
        remote = self._remote_path(rel_path)
        client = None
        try:
            client, sftp = self._connect()
            with sftp.file(remote, "rb") as f:
                return f.read()
        finally:
            if client is not None:
                client.close()

    def list_names(self, prefix: str) -> list[str]:
        remote_dir = self._remote_path(prefix)
        client = None
        try:
            client, sftp = self._connect()
            return sorted(attr.filename for attr in sftp.listdir_attr(remote_dir))
        finally:
            if client is not None:
                client.close()
