#!/usr/bin/env python3
"""
Diagnose DB env loading and SSH tunnel connectivity (GPU / remote Postgres).

Does NOT assume wmvi imports work correctly — prints env discovery, builds a
barebones ``ssh -L`` command, optional manual tunnel test, and compares with
``db.init_pool``.

Usage (from repo root):
  python -m scripts.troubleshoot_db_connection
  python -m scripts.troubleshoot_db_connection --prefix PROD
  python -m scripts.troubleshoot_db_connection --prefix PROD --probe-tunnel
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Keys we care about for DB + tunnel (passwords masked in output).
_DB_KEYS = (
    "DEFAULT_DB",
    "_DEFAULT_DB",
    "USE_SSH_TUNNEL",
    "PROD_USE_SSH_TUNNEL",
    "DEV_USE_SSH_TUNNEL",
    "SSH_HOST",
    "SSH_USERNAME",
    "SSH_PKEY",
    "SSH_PORT",
    "SSH_BIN",
    "PGCONNECT_TIMEOUT",
)
_PREFIX_KEYS = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE")


def _section(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def _mask(val: str | None) -> str:
    if val is None:
        return "(unset)"
    if not val:
        return "(empty)"
    if "PASSWORD" in val or "SECRET" in val or "KEY" in val:
        return "***"
    return val


def _print_kv(name: str, value: str | None) -> None:
    display = _mask(value) if "PASSWORD" in name else (value if value else "(unset)")
    print(f"  {name}={display}")


def discover_env(repo_env: Path) -> None:
    _section("1. Working directory and .env discovery")
    print(f"  cwd={Path.cwd().resolve()}")
    print(f"  repo_root={REPO_ROOT}")
    print(f"  repo .env exists={repo_env.is_file()} ({repo_env})")

    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        print("  python-dotenv not installed")
        return

    found = find_dotenv(usecwd=True)
    print(f"  find_dotenv(usecwd=True)={found or '(none)'}")

    # Snapshot before explicit load
    before_tunnel = os.environ.get("USE_SSH_TUNNEL")
    before_host = os.environ.get("PROD_PGHOST")

    if repo_env.is_file():
        load_dotenv(repo_env, override=True)
        print(f"  load_dotenv({repo_env}, override=True) -> done")
    else:
        print("  WARN: repo .env missing; only shell/process env applies")

    print(f"  USE_SSH_TUNNEL before explicit load={before_tunnel!r} after={os.environ.get('USE_SSH_TUNNEL')!r}")
    print(f"  PROD_PGHOST before explicit load={before_host!r} after={os.environ.get('PROD_PGHOST')!r}")

    if os.environ.get("_DEFAULT_DB") and not os.environ.get("DEFAULT_DB"):
        print(
            "\n  WARN: _DEFAULT_DB is set but DEFAULT_DB is not. "
            "db/db.py reads DEFAULT_DB (no leading underscore). "
            "Rename _DEFAULT_DB -> DEFAULT_DB in .env (use PROD on GPU if you always hit remote DB)."
        )


def print_relevant_env(prefix: str) -> None:
    _section("2. Relevant environment variables")
    p = prefix.upper()
    for key in _DB_KEYS:
        _print_kv(key, os.environ.get(key))
    for suffix in _PREFIX_KEYS:
        _print_kv(f"{p}_{suffix}", os.environ.get(f"{p}_{suffix}"))


def tunnel_decision(prefix: str) -> bool:
    p = prefix.upper()

    def flag(name: str) -> bool:
        raw = os.environ.get(name)
        if raw is None:
            return False
        return raw.strip().lower() in ("1", "true", "yes", "on")

    if flag("USE_SSH_TUNNEL"):
        return True
    return flag(f"{p}_USE_SSH_TUNNEL")


def build_barebones_ssh_command(prefix: str) -> list[str] | None:
    _section("3. Barebones SSH tunnel command (same shape as storage/ssh/tunnel.py)")
    p = prefix.upper()
    try:
        host = os.environ[f"{p}_PGHOST"]
        port = int(os.environ.get(f"{p}_PGPORT", "5432"))
    except KeyError as e:
        print(f"  Missing env: {e}")
        return None

    ssh_host = os.environ.get("SSH_HOST", "").strip()
    ssh_user = os.environ.get("SSH_USERNAME", "").strip()
    ssh_pkey = os.environ.get("SSH_PKEY", "").strip()
    ssh_port = os.environ.get("SSH_PORT", "22").strip()

    if not all((ssh_host, ssh_user, ssh_pkey)):
        print("  SSH_HOST / SSH_USERNAME / SSH_PKEY not all set — cannot build ssh -L")
        return None

    local_port = 15432  # fixed for manual testing; change if in use
    forward = f"127.0.0.1:{local_port}:{host}:{port}"
    cmd = [
        "ssh",
        "-N",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=30",
        "-p",
        ssh_port,
        "-i",
        ssh_pkey,
        "-L",
        forward,
        f"{ssh_user}@{ssh_host}",
    ]
    print("  Interpretation:")
    print(f"    DB {p}_PGHOST={host!r} {p}_PGPORT={port} is the address ON THE SSH HOST side")
    print(f"    (after tunnel: connect psycopg2 to 127.0.0.1:{local_port} on this machine)")
    print(f"  Manual command (run in another terminal, leave open):")
    print("   ", " ".join(cmd))
    print("\n  Then test Postgres:")
    db = os.environ.get(f"{p}_PGDATABASE", "postgres")
    user = os.environ.get(f"{p}_PGUSER", "postgres")
    ssl = os.environ.get(f"{p}_PGSSLMODE", "require")
    print(
        f"    PGPASSWORD='...' psql -h 127.0.0.1 -p {local_port} -U {user} -d {db} "
        f'"sslmode={ssl}" -c \"SELECT 1\"'
    )
    return cmd


def probe_local_postgres(host: str, port: int, timeout: float = 2.0) -> None:
    _section("4. TCP probe (no Postgres auth)")
    print(f"  Trying TCP connect to {host}:{port} ...")
    try:
        with socket.create_connection((host, port), timeout=timeout):
            print("  OK: something is listening (may be local GPU Postgres or an existing tunnel)")
    except OSError as e:
        print(f"  FAIL: {e}")


def probe_tunnel(prefix: str, ssh_cmd: list[str] | None) -> None:
    if ssh_cmd is None:
        return
    _section("5. Automated barebones tunnel + SELECT 1 (optional)")
    local_port = int(ssh_cmd[ssh_cmd.index("-L") + 1].split(":")[1])
    print(f"  Starting ssh -L on local port {local_port} ...")
    proc = subprocess.Popen(
        ssh_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
                print(f"  ssh exited early: {err or proc.returncode}")
                return
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            print("  FAIL: tunnel did not listen within 30s")
            return

        print("  Tunnel listening.")
        try:
            import psycopg2
        except ImportError:
            print("  psycopg2 not installed; skip SQL probe")
            return

        p = prefix.upper()
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=local_port,
            user=os.environ[f"{p}_PGUSER"],
            password=os.environ[f"{p}_PGPASSWORD"],
            dbname=os.environ.get(f"{p}_PGDATABASE", "postgres"),
            sslmode=os.environ.get(f"{p}_PGSSLMODE", "require"),
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print(f"  OK: SELECT 1 -> {cur.fetchone()}")
        finally:
            conn.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def probe_init_pool(prefix: str) -> None:
    _section("6. db.init_pool (wmvi code path)")
    use_tunnel = tunnel_decision(prefix)
    p = prefix.upper()
    try:
        remote_host = os.environ[f"{p}_PGHOST"]
        remote_port = os.environ.get(f"{p}_PGPORT", "5432")
    except KeyError as e:
        print(f"  Skip: missing {e}")
        return

    print(f"  tunnel_decision({p})={use_tunnel}")
    if not use_tunnel and remote_host.strip().lower() in ("localhost", "127.0.0.1", "::1"):
        print(
            "  WARN: PROD/DEV PGHOST is localhost WITHOUT tunnel — "
            "this connects to Postgres ON THIS MACHINE (gpu-pc), not over the network."
        )

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from db.db import close_pool, getcursor, init_pool

    try:
        init_pool(prefix=prefix, recreate=True)
        with getcursor() as cur:
            cur.execute("SELECT current_database(), inet_server_addr(), inet_server_port()")
            row = cur.fetchone()
        print(f"  OK: init_pool + SELECT -> db={row[0]!r} server={row[1]}:{row[2]}")
    except Exception as e:
        print(f"  FAIL: {type(e).__name__}: {e}")
    finally:
        close_pool()


def script_notes() -> None:
    _section("7. Script behaviour notes")
    print(
        "  • scripts/check_db_migration_version.py uses psycopg2.connect() directly.\n"
        "    It NEVER opens an SSH tunnel — PROD_PGHOST=localhost hits gpu-pc Postgres.\n"
        "    Use: python -m scripts.migrate_db --prod  OR  init_pool-based tools instead.\n"
        "  • db/db.py calls load_dotenv() with no path (searches from cwd upward).\n"
        "    systemd services use EnvironmentFile= absolute path to ~/wmvi/.env.\n"
        "  • transcription_checklist tests 7–8 need .env loaded (check #1 or latest checklist).\n"
        "  • With USE_SSH_TUNNEL=1, PGHOST=localhost means Postgres on SSH_HOST (192.168.2.32),\n"
        "    not on gpu-pc."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Troubleshoot DB env + SSH tunnel connectivity")
    ap.add_argument("--prefix", default="PROD", help="DB env prefix (default: PROD)")
    ap.add_argument(
        "--probe-tunnel",
        action="store_true",
        help="Start a test ssh -L and run SELECT 1 through it (needs SSH key auth)",
    )
    ap.add_argument(
        "--skip-init-pool",
        action="store_true",
        help="Skip db.init_pool test",
    )
    args = ap.parse_args()
    prefix = args.prefix.strip().upper()
    repo_env = REPO_ROOT / ".env"

    discover_env(repo_env)
    print_relevant_env(prefix)

    use_tunnel = tunnel_decision(prefix)
    print(f"\n  tunnel_decision({prefix})={use_tunnel}")

    p = prefix.upper()
    try:
        remote_host = os.environ[f"{p}_PGHOST"]
        remote_port = int(os.environ.get(f"{p}_PGPORT", "5432"))
    except KeyError:
        remote_host = "127.0.0.1"
        remote_port = 5432

    if use_tunnel:
        ssh_cmd = build_barebones_ssh_command(prefix)
        if args.probe_tunnel:
            probe_tunnel(prefix, ssh_cmd)
    else:
        print("\n  Tunnel disabled — probing PGHOST directly (likely local socket/TCP on gpu-pc):")
        probe_local_postgres(remote_host, remote_port)
        build_barebones_ssh_command(prefix)

    if not args.skip_init_pool:
        probe_init_pool(prefix)

    script_notes()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
