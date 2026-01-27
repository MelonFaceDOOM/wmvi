from __future__ import annotations

import shutil
import subprocess
import sys
import os
from pathlib import Path

"""
checks a list of requirements to run transcription
and prints status as steps are run
"""

# ----------------------------
# Helpers
# ----------------------------


def header(title: str):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def ok(msg: str):
    print(f"[OK]  {msg}")


def warn(msg: str):
    print(f"[WARN] {msg}")


def fail(msg: str):
    print(f"[FAIL] {msg}")


def run(cmd: list[str], timeout=30) -> bool:
    try:
        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=True,
        )
        return True
    except Exception:
        return False


# ----------------------------
# 1. System binaries
# ----------------------------

header("System binaries")

for bin_name in ["ffmpeg", "yt-dlp", "node"]:
    path = shutil.which(bin_name)
    if path:
        ok(f"{bin_name} found at {path}")
    else:
        fail(f"{bin_name} not found in PATH")

# ----------------------------
# 2. Python libraries
# ----------------------------

header("Python libraries")


def check_import(mod: str):
    try:
        __import__(mod)
        ok(f"import {mod}")
    except Exception as e:
        fail(f"import {mod} failed: {e}")


check_import("torch")
check_import("faster_whisper")
check_import("deepmultilingualpunctuation")

# ----------------------------
# 3. GPU / CPU
# ----------------------------

header("Compute backend")

try:
    import torch
    if torch.cuda.is_available():
        ok("CUDA available")
        ok(f"GPU: {torch.cuda.get_device_name(0)}")
    else:
        warn("CUDA not available (CPU-only)")
except Exception as e:
    fail(f"torch CUDA check failed: {e}")

# ----------------------------
# 4. Firefox cookies
# ----------------------------

header("Firefox cookies")

cookie_paths = [
    Path.home() / ".mozilla/firefox",
    Path.home() / ".var/app/org.mozilla.firefox/.mozilla/firefox",
]

cookies_found = False
for base in cookie_paths:
    if base.exists():
        for p in base.glob("**/cookies.sqlite"):
            ok(f"Found cookies.sqlite: {p}")
            cookies_found = True

if not cookies_found:
    warn("No Firefox cookies.sqlite found (yt-dlp may hit 403s)")

# ----------------------------
# 5. Whisper smoke test
# ----------------------------

header("Whisper transcription test")

sample = Path("sample_ep.mp3")
if not sample.exists():
    warn("sample_ep.mp3 not found — skipping transcription test")
else:
    try:
        from faster_whisper import WhisperModel

        model = WhisperModel(
            "tiny",
            device="cuda" if torch.cuda.is_available() else "cpu",
            compute_type="float16" if torch.cuda.is_available() else "int8",
        )

        segments, info = model.transcribe(str(sample), vad_filter=True)

        text = " ".join(seg.text.strip() for seg in segments if seg.text)
        ok("Transcription succeeded")
        print("\n--- transcript preview ---")
        print(text[:300])
        print("--- end preview ---")

    except Exception as e:
        fail(f"Whisper transcription failed: {e}")

# ----------------------------
# 6. YouTube download test
# ----------------------------

header("YouTube download test")

test_url = "https://www.youtube.com/watch?v=DTt_2sW90Lg"  # yt-dlp test video
out = Path("yt_test_audio.mp3")

cmd = [
    "yt-dlp",
    "--no-playlist",
    "--extract-audio",
    "--audio-format", "mp3",
    "--audio-quality", "0",
    "--js-runtimes", "node",
    "-o", str(out),
    test_url,
]

try:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
    )

    if result.returncode == 0 and out.exists() and out.stat().st_size > 0:
        ok("yt-dlp audio download succeeded")
        out.unlink(missing_ok=True)
    else:
        fail("yt-dlp audio download failed")
        print("\n--- yt-dlp stderr (last 20 lines) ---")
        stderr_lines = result.stderr.strip().splitlines()
        for line in stderr_lines[-20:]:
            print(line)
        print("--- end stderr ---")

        if "403" in result.stderr:
            warn("HTTP 403 detected (likely cookies / SABR / client issue)")
        if "No supported JavaScript runtime" in result.stderr:
            warn("JavaScript runtime issue (node/deno config)")
        if not out.exists():
            warn("No output file was created")

except subprocess.TimeoutExpired:
    fail("yt-dlp timed out (possible SABR stall or network issue)")
