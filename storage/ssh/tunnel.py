from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

from .config import SSHConfig

log = logging.getLogger(__name__)

_TUNNEL: Optional["SshTunnel"] = None

_DEFAULT_CONNECT_TIMEOUT = 30
_DEFAULT_READY_TIMEOUT = 60


@dataclass(slots=True)
class SshTunnel:
    """OpenSSH ``ssh -L`` subprocess forwarding local TCP to remote host:port."""

    process: subprocess.Popen[bytes]
    local_port: int
    remote_host: str
    remote_port: int
    config: SSHConfig

    @property
    def is_active(self) -> bool:
        return self.process.poll() is None


def get_tunnel() -> Optional[SshTunnel]:
    return _TUNNEL


def _ssh_binary() -> str:
    override = os.environ.get("SSH_BIN", "").strip()
    if override:
        return override
    found = shutil.which("ssh")
    if not found:
        raise RuntimeError(
            "OpenSSH client not found on PATH. Install it or set SSH_BIN to the ssh executable."
        )
    return found


def _connect_timeout() -> int:
    raw = os.environ.get("SSH_CONNECT_TIMEOUT", str(_DEFAULT_CONNECT_TIMEOUT)).strip()
    try:
        return max(1, int(raw))
    except ValueError as e:
        raise RuntimeError(f"Invalid SSH_CONNECT_TIMEOUT: {raw!r}") from e


def _ready_timeout() -> float:
    raw = os.environ.get("SSH_TUNNEL_READY_TIMEOUT", str(_DEFAULT_READY_TIMEOUT)).strip()
    try:
        return max(1.0, float(raw))
    except ValueError as e:
        raise RuntimeError(f"Invalid SSH_TUNNEL_READY_TIMEOUT: {raw!r}") from e


def _pick_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _build_ssh_command(
    cfg: SSHConfig,
    *,
    local_port: int,
    remote_host: str,
    remote_port: int,
    connect_timeout: int,
) -> list[str]:
    forward = f"127.0.0.1:{local_port}:{remote_host}:{remote_port}"
    return [
        _ssh_binary(),
        "-N",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-p",
        str(cfg.port),
        "-i",
        str(cfg.pkey_path),
        "-L",
        forward,
        f"{cfg.username}@{cfg.host}",
    ]


def _read_process_error(proc: subprocess.Popen[bytes]) -> str:
    if proc.stderr is None:
        return f"ssh exited with code {proc.returncode}"
    try:
        err = proc.stderr.read().decode("utf-8", errors="replace").strip()
    except Exception:
        err = ""
    if err:
        return err
    return f"ssh exited with code {proc.returncode}"


def _kill_process(proc: subprocess.Popen[bytes], *, wait_s: float = 3.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=wait_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _wait_until_listening(
    proc: subprocess.Popen[bytes],
    local_port: int,
    *,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        code = proc.poll()
        if code is not None:
            raise RuntimeError(_read_process_error(proc))
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    _kill_process(proc)
    raise RuntimeError(
        f"SSH tunnel did not become ready on 127.0.0.1:{local_port} within {timeout_s:.0f}s"
    )


def open_tunnel(
    remote_host: str,
    remote_port: int,
    *,
    config: SSHConfig | None = None,
) -> SshTunnel:
    """
    Open (or reuse) an SSH tunnel: local 127.0.0.1:<ephemeral> -> remote_host:remote_port
    via OpenSSH ``ssh -L`` (subprocess; killable with terminate/kill).
    """
    global _TUNNEL

    if _TUNNEL is not None and _TUNNEL.is_active:
        return _TUNNEL

    cfg = config or SSHConfig.from_env()
    local_port = _pick_local_port()
    connect_timeout = _connect_timeout()
    cmd = _build_ssh_command(
        cfg,
        local_port=local_port,
        remote_host=remote_host,
        remote_port=remote_port,
        connect_timeout=connect_timeout,
    )
    log.info(
        "Starting SSH tunnel (ssh=%s@%s:%s -> %s:%s, local=127.0.0.1:%s).",
        cfg.username,
        cfg.host,
        cfg.port,
        remote_host,
        remote_port,
        local_port,
    )
    log.debug("SSH command: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_until_listening(proc, local_port, timeout_s=_ready_timeout())
    except BaseException:
        _kill_process(proc)
        raise

    tunnel = SshTunnel(
        process=proc,
        local_port=local_port,
        remote_host=remote_host,
        remote_port=remote_port,
        config=cfg,
    )
    _TUNNEL = tunnel
    log.info("SSH tunnel active (local=127.0.0.1:%s).", local_port)
    return tunnel


def close_tunnel() -> None:
    global _TUNNEL
    if _TUNNEL is None:
        return
    log.info("Stopping SSH tunnel.")
    proc = _TUNNEL.process
    _TUNNEL = None
    _kill_process(proc)


def local_bind_port() -> int:
    if _TUNNEL is None or not _TUNNEL.is_active:
        raise RuntimeError("SSH tunnel is not active")
    return _TUNNEL.local_port
