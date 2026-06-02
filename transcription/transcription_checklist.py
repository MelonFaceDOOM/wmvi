from __future__ import annotations

import argparse
import importlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

YT_DLP_TEST_URL = "https://www.youtube.com/watch?v=DTt_2sW90Lg"
PROXY_PROBE_URL = "https://www.youtube.com/generate_204"


class Status(Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class Result:
    status: Status
    message: str


@dataclass
class CheckContext:
    sample_audio: Path | None = None
    db_prefix: str = "dev"


@dataclass
class Check:
    id: int
    name: str
    group: str
    heavy: bool
    run: Callable[[CheckContext], Result]


def _ok(msg: str) -> Result:
    return Result(Status.OK, msg)


def _warn(msg: str) -> Result:
    return Result(Status.WARN, msg)


def _fail(msg: str) -> Result:
    return Result(Status.FAIL, msg)


def _env_flag(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _private_path(name: str) -> Path:
    return REPO_ROOT / "private" / name


def _resolve_yt_dlp_bin() -> str:
    env_bin = os.environ.get("YT_DLP_BIN")
    if env_bin:
        return env_bin
    venv_bin = Path(sys.prefix) / "bin" / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    exe_bin = Path(sys.executable).parent / "yt-dlp"
    if exe_bin.exists():
        return str(exe_bin)
    path_bin = shutil.which("yt-dlp")
    if path_bin:
        return path_bin
    raise RuntimeError("yt-dlp not found; set YT_DLP_BIN or install in venv")


def _has_db_prefix(prefix: str) -> bool:
    p = prefix.upper()
    return bool(os.environ.get(f"{p}_PGHOST"))


# ----------------------------
# Check implementations
# ----------------------------


def check_env(_ctx: CheckContext) -> Result:
    env_path = REPO_ROOT / ".env"
    if not env_path.is_file():
        return _fail(f".env not found at {env_path}")
    load_dotenv(env_path, override=True)
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT and REPO_ROOT not in cwd.parents:
        return _warn(
            f".env loaded from {env_path}; cwd is {cwd} (expected repo root {REPO_ROOT})"
        )
    return _ok(f".env loaded from {env_path}")


def check_python(_ctx: CheckContext) -> Result:
    ver = sys.version_info
    if ver < (3, 11):
        return _fail(f"Python {ver.major}.{ver.minor} (need >= 3.11)")
    msg = f"Python {ver.major}.{ver.minor}.{ver.micro} at {sys.executable}"
    venv_hint = REPO_ROOT / "venvs" / "transcription"
    if venv_hint.exists() and not str(sys.executable).startswith(str(venv_hint)):
        return _warn(f"{msg}; consider {venv_hint}/bin/python")
    return _ok(msg)


def check_ffmpeg(_ctx: CheckContext) -> Result:
    path = shutil.which("ffmpeg")
    if path:
        return _ok(f"ffmpeg at {path}")
    return _fail("ffmpeg not found in PATH")


def check_imports(_ctx: CheckContext) -> Result:
    modules = [
        "torch",
        "faster_whisper",
        "deepmultilingualpunctuation",
        "requests",
        "psycopg2",
    ]
    failed: list[str] = []
    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            failed.append(f"{mod}: {e}")
    if failed:
        return _fail("; ".join(failed))
    return _ok(f"imports OK: {', '.join(modules)}")


def check_cuda(_ctx: CheckContext) -> Result:
    try:
        import torch

        if torch.cuda.is_available():
            return _ok(f"CUDA: {torch.cuda.get_device_name(0)}")
        return _warn("CUDA not available (CPU-only transcription)")
    except Exception as e:
        return _fail(f"torch CUDA check failed: {e}")


def check_whisper_smoke(ctx: CheckContext) -> Result:
    sample = ctx.sample_audio
    if sample is None or not sample.is_file():
        return _warn(
            "no sample audio (--sample-audio PATH); skipping whisper smoke test"
        )
    try:
        import torch
        from faster_whisper import WhisperModel

        model_name = os.environ.get("WHISPER_MODEL", "tiny")
        use_cuda = torch.cuda.is_available()
        model = WhisperModel(
            model_name,
            device="cuda" if use_cuda else "cpu",
            compute_type="float16" if use_cuda else "int8",
        )
        segments, _info = model.transcribe(str(sample), vad_filter=True)
        text = " ".join(seg.text.strip() for seg in segments if seg.text)
        preview = text[:200] + ("..." if len(text) > 200 else "")
        return _ok(f"whisper ({model_name}) OK; preview: {preview!r}")
    except Exception as e:
        return _fail(f"whisper smoke failed: {e}")


def _db_ping(prefix: str) -> Result:
    if not _has_db_prefix(prefix):
        return _warn(f"{prefix.upper()}_PGHOST not set; skipping {prefix} DB check")
    from db.db import close_pool, getcursor, init_pool

    try:
        init_pool(prefix=prefix, recreate=True)
        with getcursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return _ok(f"{prefix.upper()} DB connected")
    except Exception as e:
        return _fail(f"{prefix.upper()} DB failed: {e}")
    finally:
        close_pool()


def check_db_dev(_ctx: CheckContext) -> Result:
    return _db_ping("dev")


def check_db_prod(_ctx: CheckContext) -> Result:
    return _db_ping("prod")


def check_ssh_tunnel(_ctx: CheckContext) -> Result:
    dev_tunnel = _env_flag("DEV_USE_SSH_TUNNEL")
    prod_tunnel = _env_flag("PROD_USE_SSH_TUNNEL")
    global_tunnel = _env_flag("USE_SSH_TUNNEL")
    if not (dev_tunnel or prod_tunnel or global_tunnel):
        return _ok("SSH tunnel not enabled (USE_SSH_TUNNEL / *_USE_SSH_TUNNEL off)")

    required = ["SSH_HOST", "SSH_USERNAME", "SSH_PKEY"]
    missing = [k for k in required if not os.environ.get(k, "").strip()]
    if missing:
        return _fail(f"SSH tunnel on but missing: {', '.join(missing)}")

    ssh_bin = shutil.which(os.environ.get("SSH_BIN", "ssh"))
    if not ssh_bin:
        return _warn("SSH tunnel configured but ssh binary not found in PATH")
    return _ok(f"SSH tunnel config OK (ssh at {ssh_bin})")


def check_yt_dlp_bin(_ctx: CheckContext) -> Result:
    try:
        bin_path = _resolve_yt_dlp_bin()
    except RuntimeError as e:
        return _fail(str(e))
    try:
        subprocess.run(
            [bin_path, "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as e:
        return _fail(f"yt-dlp at {bin_path} failed --version: {e}")
    return _ok(f"yt-dlp at {bin_path}")


def check_node(_ctx: CheckContext) -> Result:
    node = shutil.which("node")
    if not node:
        return _fail("node not found in PATH")
    try:
        out = subprocess.run(
            [node, "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        version = out.stdout.strip()
    except Exception as e:
        return _fail(f"node --version failed: {e}")
    m = re.match(r"v(\d+)", version)
    if m and int(m.group(1)) < 20:
        return _warn(f"{version} at {node} (recommend v20+)")
    return _ok(f"{version} at {node}")


def check_youtube_auth_files(_ctx: CheckContext) -> Result:
    cookies = _private_path("youtube-cookies.txt")
    agent = _private_path("youtube-agent.txt")
    issues: list[str] = []
    if not cookies.is_file() or cookies.stat().st_size == 0:
        issues.append(f"missing or empty {cookies}")
    if not agent.is_file() or not agent.read_text(encoding="utf-8").strip():
        issues.append(f"missing or empty {agent}")
    if issues:
        return _fail("; ".join(issues))
    return _ok(f"cookies + user-agent present under {REPO_ROOT / 'private'}")


def check_yt_proxy(_ctx: CheckContext) -> Result:
    url = os.environ.get("YT_PROXY_URL", "").strip()
    if not url:
        return _warn("YT_PROXY_URL not set (yt-dlp uses direct connection)")
    if not re.match(r"^https?://", url, re.I):
        return _fail(f"YT_PROXY_URL must be http(s) URL, got: {url!r}")
    try:
        import requests

        proxies = {"http": url, "https": url}
        resp = requests.get(
            PROXY_PROBE_URL,
            proxies=proxies,
            timeout=30,
            allow_redirects=True,
        )
        if resp.status_code >= 500:
            return _warn(f"proxy reachable but probe returned HTTP {resp.status_code}")
        return _ok(f"proxy probe OK (HTTP {resp.status_code})")
    except Exception as e:
        return _fail(f"proxy probe failed: {e}")


def check_yt_dlp_smoke(_ctx: CheckContext) -> Result:
    cookies = _private_path("youtube-cookies.txt")
    agent = _private_path("youtube-agent.txt")
    if not cookies.is_file() or not agent.is_file():
        return _fail("run check 12 (youtube_auth_files) first; missing private/ files")

    from storage.yt_proxy import yt_dlp_proxy_args

    try:
        yt_dlp = _resolve_yt_dlp_bin()
    except RuntimeError as e:
        return _fail(str(e))

    ua = agent.read_text(encoding="utf-8").strip()
    with tempfile.TemporaryDirectory() as tmp:
        out_base = Path(tmp) / "yt_checklist_test"
        cmd = [
            yt_dlp,
            "--no-playlist",
            "--js-runtimes",
            "node",
            "--cookies",
            str(cookies),
            "--add-headers",
            f"User-Agent:{ua}",
            *yt_dlp_proxy_args(),
            "-f",
            "bestaudio/best",
            "--extract-audio",
            "--audio-format",
            "mp3",
            "--audio-quality",
            "0",
            "--force-overwrites",
            "-o",
            str(out_base) + ".%(ext)s",
            YT_DLP_TEST_URL,
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            return _fail("yt-dlp smoke timed out (180s)")

        produced = any(out_base.with_suffix(ext).exists() for ext in ("mp3", "m4a", "opus", "webm"))
        if result.returncode == 0 and produced:
            return _ok("yt-dlp download smoke succeeded")

        stderr_tail = "\n".join((result.stderr or "").strip().splitlines()[-15:])
        msg = f"yt-dlp smoke failed (exit {result.returncode})"
        if "403" in (result.stderr or ""):
            msg += "; HTTP 403 (cookies/UA/proxy)"
        if stderr_tail:
            msg += f"\n--- stderr (last lines) ---\n{stderr_tail}"
        return _fail(msg)


def check_podcast_download_smoke(ctx: CheckContext) -> Result:
    if not _has_db_prefix(ctx.db_prefix):
        return _warn(
            f"{ctx.db_prefix.upper()}_PGHOST not set; skipping podcast download smoke"
        )

    from db.db import close_pool, getcursor, init_pool
    from services.podcast.transcriber.downloader import normalize_url, try_download

    try:
        init_pool(prefix=ctx.db_prefix, recreate=True)
        with getcursor() as cur:
            cur.execute(
                """
                SELECT download_url FROM podcasts.episodes
                WHERE download_url IS NOT NULL AND btrim(download_url) <> ''
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            return _warn("no podcast episodes with download_url in DB")

        raw_url = row[0]
        with tempfile.TemporaryDirectory() as tmp:
            out_path = str(Path(tmp) / "podcast_checklist_test")
            for candidate in normalize_url(raw_url):
                ok, size, reason = try_download(candidate, out_path)
                if ok:
                    return _ok(
                        f"podcast download OK ({size} bytes) via {candidate[:80]}..."
                    )
            return _fail(f"podcast download failed for {raw_url}: {reason}")
    except Exception as e:
        return _fail(f"podcast download smoke failed: {e}")
    finally:
        close_pool()


CHECKS: list[Check] = [
    Check(1, "env", "core", False, check_env),
    Check(2, "python", "core", False, check_python),
    Check(3, "ffmpeg", "core", False, check_ffmpeg),
    Check(4, "imports", "core", False, check_imports),
    Check(5, "cuda", "core", False, check_cuda),
    Check(6, "whisper_smoke", "core", True, check_whisper_smoke),
    Check(7, "db_dev", "db", False, check_db_dev),
    Check(8, "db_prod", "db", False, check_db_prod),
    Check(9, "ssh_tunnel", "db", False, check_ssh_tunnel),
    Check(10, "yt_dlp_bin", "youtube", False, check_yt_dlp_bin),
    Check(11, "node", "youtube", False, check_node),
    Check(12, "youtube_auth_files", "youtube", False, check_youtube_auth_files),
    Check(13, "yt_proxy", "youtube", False, check_yt_proxy),
    Check(14, "yt_dlp_smoke", "youtube", True, check_yt_dlp_smoke),
    Check(15, "podcast_download_smoke", "podcast", True, check_podcast_download_smoke),
]

CHECK_BY_ID: dict[int, Check] = {c.id: c for c in CHECKS}

GROUP_EXPANSIONS: dict[str, list[str]] = {
    "core": ["core"],
    "db": ["db"],
    "youtube": ["core", "db", "youtube"],
    "podcast": ["core", "db", "podcast"],
    "all": ["core", "db", "youtube", "podcast"],
}


def all_check_ids() -> set[int]:
    return {c.id for c in CHECKS}


def ids_for_groups(groups: list[str]) -> set[int]:
    expanded: set[str] = set()
    for g in groups:
        key = g.lower()
        if key not in GROUP_EXPANSIONS:
            raise ValueError(f"unknown group {g!r}; choose from {sorted(GROUP_EXPANSIONS)}")
        expanded.update(GROUP_EXPANSIONS[key])
    return {c.id for c in CHECKS if c.group in expanded}


def parse_selection(text: str) -> set[int]:
    raw = text.strip()
    if not raw:
        raise ValueError("empty selection")
    if raw == "0":
        return all_check_ids()
    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit():
            raise ValueError(f"invalid selection token: {part!r}")
        n = int(part)
        if n == 0:
            return all_check_ids()
        if n not in CHECK_BY_ID:
            raise ValueError(f"unknown test id: {n}")
        ids.add(n)
    if not ids:
        raise ValueError("no test ids selected")
    return ids


def print_menu() -> None:
    print("\nTranscription environment checklist\n")
    current_group = None
    for c in CHECKS:
        if c.group != current_group:
            current_group = c.group
            print(f"\n[{current_group}]")
        heavy = " (H)" if c.heavy else ""
        print(f"  {c.id:2d}. {c.name}{heavy}")
    print("\n  0. Run all tests (including heavy)")
    print("\nGroups for --group: core, db, youtube, podcast, all")
    print("  youtube = core + db + youtube")
    print("  podcast = core + db + podcast")


def _print_result(check: Check, result: Result) -> None:
    tag = result.status.value.upper()
    heavy = " (H)" if check.heavy else ""
    print(f"[{tag}] {check.id}. {check.name}{heavy}: {result.message}")


def run_checks(ids: set[int], ctx: CheckContext) -> int:
    """Run selected checks; return process exit code (1 if any fail)."""
    ordered = [CHECK_BY_ID[i] for i in sorted(ids)]
    fails = 0
    warns = 0
    oks = 0
    failed_ids: list[int] = []

    for check in ordered:
        print(f"\n--- {check.id}. {check.name} ---")
        try:
            result = check.run(ctx)
        except Exception as e:
            result = _fail(f"unexpected error: {e}")
        _print_result(check, result)
        if result.status == Status.FAIL:
            fails += 1
            failed_ids.append(check.id)
        elif result.status == Status.WARN:
            warns += 1
        else:
            oks += 1

    print("\n" + "=" * 60)
    print(f"Summary: {oks} ok, {warns} warn, {fails} fail (of {len(ordered)} run)")
    if failed_ids:
        print(f"Failed test ids: {', '.join(str(i) for i in failed_ids)}")
    print("=" * 60)
    return 1 if fails else 0


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Modular transcription environment checklist (GPU / prod).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python transcription/transcription_checklist.py
  python transcription/transcription_checklist.py 0
  python transcription/transcription_checklist.py 1,4,7
  python transcription/transcription_checklist.py --group podcast
  python transcription/transcription_checklist.py --group youtube --list
        """.strip(),
    )
    ap.add_argument(
        "selection",
        nargs="?",
        default=None,
        help="Test id(s): 0=all, or comma-separated (e.g. 1,4,7)",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Run all tests (same as selection 0)",
    )
    ap.add_argument(
        "--group",
        action="append",
        dest="groups",
        metavar="NAME",
        help="Run group: core, db, youtube, podcast, all (repeatable)",
    )
    ap.add_argument(
        "--list",
        action="store_true",
        help="Print menu and exit",
    )
    ap.add_argument(
        "--sample-audio",
        type=Path,
        default=None,
        help="Audio file for whisper smoke test (check 6)",
    )
    ap.add_argument(
        "--prefix",
        choices=("dev", "prod"),
        default="dev",
        help="DB prefix for podcast download smoke (default: dev)",
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = _build_parser()
    args = ap.parse_args(argv)

    if args.list:
        print_menu()
        return 0

    ctx = CheckContext(sample_audio=args.sample_audio, db_prefix=args.prefix)

    ids: set[int] | None = None
    if args.all:
        ids = all_check_ids()
    elif args.groups:
        ids = ids_for_groups(args.groups)
    elif args.selection is not None:
        ids = parse_selection(args.selection)

    if ids is None:
        print_menu()
        try:
            line = input("\nEnter test(s) [0=all]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 130
        ids = parse_selection(line or "0")

    return run_checks(ids, ctx)


if __name__ == "__main__":
    raise SystemExit(main())
