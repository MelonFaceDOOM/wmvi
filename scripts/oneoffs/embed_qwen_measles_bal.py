"""Embed measles_bal with Qwen3-Embedding-8B, zip the run, PUT to nitwitch.

Dry-run encodes 1 claim into a separate ``qwen3-emb-8b-dry`` run dir so the
overnight job is never overwritten. It still zips and uploads, so CUDA load,
sklearn-before-torch, zip, and nitwitch auth are all checked.

On the GPU box (repo root, venv with CUDA torch)::

    python -u -m scripts.oneoffs.embed_qwen_measles_bal --dry-run
    python -u -m scripts.oneoffs.embed_qwen_measles_bal

Needs ``NITWITCH_UPLOAD_URL`` / ``USER`` / ``PASSWORD`` in ``.env``.
Public listing: https://nitwitch.com/dl/uploads/
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, TextIO

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]

CORPUS = "measles_bal"
MODEL_ID = "Qwen/Qwen3-Embedding-8B"
MODEL_TAG = "qwen3-emb-8b"
DRY_MODEL_TAG = "qwen3-emb-8b-dry"
SELECTION = "standalone_ok"
DOC_INSTRUCTION = (
    "Instruct: Retrieve claims that express the same underlying proposition "
    "or narrative.\nQuery:"
)
BATCH_SIZE = 16
MAX_SEQ_LENGTH = 512
UPLOAD_TIMEOUT_S = 7200

log = logging.getLogger("embed_qwen_measles_bal")


def _zip_name(model_tag: str) -> str:
    return f"{CORPUS}_{model_tag}.embed.zip"


def _parse_last_json(stdout: str) -> dict[str, Any]:
    last: dict[str, Any] | None = None
    for raw in stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            last = json.loads(line)
        except json.JSONDecodeError:
            continue
    if last is None:
        raise RuntimeError(f"embed produced no JSON on stdout: {stdout[-2000:]!r}")
    return last


def _tee_stream(src: TextIO, *writers: TextIO) -> None:
    for line in iter(src.readline, ""):
        for w in writers:
            w.write(line)
            w.flush()
    src.close()


def run_embed(
    *,
    model_tag: str,
    limit: int | None,
    force: bool,
    batch_size: int,
    log_path: Path,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-u",
        "-m",
        "apps.claims",
        "embed",
        "--corpus",
        CORPUS,
        "--model",
        MODEL_ID,
        "--model-tag",
        model_tag,
        "--selection",
        SELECTION,
        "--doc-instruction",
        DOC_INSTRUCTION,
        "--batch-size",
        str(batch_size),
        "--max-seq-length",
        str(MAX_SEQ_LENGTH),
        "--dtype",
        "auto",
        "--device",
        "auto",
    ]
    if limit is not None:
        cmd.extend(["--limit", str(int(limit))])
    if force:
        cmd.append("--force")

    log.info("embed cmd: %s", subprocess.list2cmdline(cmd))
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_chunks: list[str] = []

    with log_path.open("a", encoding="utf-8") as log_f:
        log_f.write("\n--- embed ---\n")
        log_f.write(subprocess.list2cmdline(cmd) + "\n")
        log_f.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert proc.stdout is not None and proc.stderr is not None

        def collect_stdout() -> None:
            assert proc.stdout is not None
            for line in iter(proc.stdout.readline, ""):
                stdout_chunks.append(line)
                sys.stdout.write(line)
                sys.stdout.flush()
                log_f.write(line)
                log_f.flush()
            proc.stdout.close()

        err_thread = threading.Thread(
            target=_tee_stream,
            args=(proc.stderr, sys.stderr, log_f),
            daemon=True,
        )
        out_thread = threading.Thread(target=collect_stdout, daemon=True)
        err_thread.start()
        out_thread.start()
        rc = proc.wait()
        out_thread.join()
        err_thread.join()

    stdout = "".join(stdout_chunks)
    if rc != 0:
        try:
            payload = _parse_last_json(stdout)
        except RuntimeError:
            payload = {}
        err = payload.get("error") if isinstance(payload, dict) else None
        raise RuntimeError(
            f"embed exited {rc}" + (f": {err}" if err else f"\n{stdout[-2000:]}")
        )
    result = _parse_last_json(stdout)
    if not result.get("ok"):
        raise RuntimeError(f"embed failed: {result}")
    return result


def run(
    *,
    dry_run: bool = False,
    skip_embed: bool = False,
    skip_upload: bool = False,
    force: bool = False,
    batch_size: int = BATCH_SIZE,
    limit: int | None = None,
    out_zip: Path | None = None,
    upload_as: str | None = None,
    log_path: Path | None = None,
) -> dict[str, Any]:
    from apps.claims.corpus import get_corpus
    from apps.claims.runs_xfer import export_run
    from storage.nitwitch_paths import NITWITCH_UPLOADS_DL_BASE_URL

    model_tag = DRY_MODEL_TAG if dry_run else MODEL_TAG
    embed_limit = 1 if dry_run and limit is None else limit
    embed_force = True if dry_run else force
    zip_path = Path(out_zip) if out_zip is not None else REPO_ROOT / _zip_name(model_tag)
    remote_name = (upload_as or zip_path.name).strip()
    log_file = log_path or (REPO_ROOT / "logs" / f"embed_{CORPUS}_{model_tag}.log")

    if not skip_upload:
        from storage.nitwitch_upload import load_upload_config

        load_upload_config()

    corpus = get_corpus(CORPUS)
    run_dir = corpus.run_dir(model_tag)
    summary: dict[str, Any] = {
        "dry_run": dry_run,
        "corpus": CORPUS,
        "model_id": MODEL_ID,
        "model_tag": model_tag,
        "selection": SELECTION,
        "limit": embed_limit,
        "run_dir": str(run_dir),
        "zip": str(zip_path.resolve()),
        "log": str(log_file.resolve()),
    }

    if skip_embed:
        log.info("skip embed; using existing %s", run_dir)
    else:
        embed_result = run_embed(
            model_tag=model_tag,
            limit=embed_limit,
            force=embed_force,
            batch_size=batch_size,
            log_path=log_file,
        )
        summary["embed"] = embed_result
        if embed_result.get("run_dir"):
            run_dir = Path(str(embed_result["run_dir"]))
            summary["run_dir"] = str(run_dir)

    exported = export_run(run_dir=run_dir, out_zip=zip_path)
    summary["export"] = exported
    summary["zip_bytes"] = zip_path.stat().st_size

    if skip_upload:
        summary["uploaded_url"] = None
        summary["download_url"] = None
        return summary

    from storage.nitwitch_upload import upload_file

    uploaded = upload_file(zip_path, remote_name=remote_name, timeout_s=UPLOAD_TIMEOUT_S)
    download = f"{NITWITCH_UPLOADS_DL_BASE_URL}{remote_name}"
    summary["uploaded_url"] = uploaded
    summary["download_url"] = download
    return summary


def main(argv: list[str] | None = None) -> int:
    load_dotenv(REPO_ROOT / ".env")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(
        description="Embed measles_bal with Qwen3-8B, zip the run, upload to nitwitch."
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Embed 1 claim into qwen3-emb-8b-dry, zip, and upload (does not touch the full run)",
    )
    ap.add_argument(
        "--skip-embed",
        action="store_true",
        help="Zip+upload an existing run dir (retry after a failed upload)",
    )
    ap.add_argument("--skip-upload", action="store_true", help="Stop after writing the zip")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the full run dir (dry-run always overwrites the dry tag)",
    )
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Embed first N groups (dry-run defaults to 1)",
    )
    ap.add_argument("--out", type=Path, default=None, help="Zip path (default: repo-root *.embed.zip)")
    ap.add_argument("--upload-as", type=str, default=None, help="Remote filename (default: zip basename)")
    args = ap.parse_args(argv)

    try:
        summary = run(
            dry_run=bool(args.dry_run),
            skip_embed=bool(args.skip_embed),
            skip_upload=bool(args.skip_upload),
            force=bool(args.force),
            batch_size=int(args.batch_size),
            limit=args.limit,
            out_zip=args.out,
            upload_as=args.upload_as,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)
        return 1

    print(json.dumps({"ok": True, **summary}, ensure_ascii=False, default=str), flush=True)
    if summary.get("download_url"):
        log.info("download: %s", summary["download_url"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
