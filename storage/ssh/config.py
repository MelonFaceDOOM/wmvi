from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SSHConfig:
    host: str
    username: str
    pkey_path: Path
    port: int = 22

    @classmethod
    def from_env(cls) -> "SSHConfig":
        host = os.environ.get("SSH_HOST", "").strip()
        username = os.environ.get("SSH_USERNAME", "").strip()
        pkey_raw = os.environ.get("SSH_PKEY", "").strip()
        port_raw = os.environ.get("SSH_PORT", "22").strip()

        missing = [
            name
            for name, val in (
                ("SSH_HOST", host),
                ("SSH_USERNAME", username),
                ("SSH_PKEY", pkey_raw),
            )
            if not val
        ]
        if missing:
            raise RuntimeError(
                f"SSH tunnel/SFTP requires env vars: {', '.join(missing)}"
            )

        try:
            port = int(port_raw)
        except ValueError as e:
            raise RuntimeError(f"Invalid SSH_PORT: {port_raw!r}") from e

        pkey_path = Path(pkey_raw).expanduser()
        if not pkey_path.is_file():
            raise RuntimeError(f"SSH private key not found: {pkey_path}")

        return cls(host=host, username=username, pkey_path=pkey_path, port=port)
