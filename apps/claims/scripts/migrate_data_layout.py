"""One-shot migration: old claims data layout → corpora/fixtures/nested runs.

Moves::

    inputs/          → corpora/
    labels/          → fixtures/
    runs/<c>__<t>/   → runs/<c>/<t>/
    experiments/<c>__<t>/  → experiments/clustering/<c>/<t>/
    models/<flat-tag>/     → models/registered/<tag>/  (skips labelers/embedders/registered)

Leaves training/ and models/{labelers,embedders} untouched.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from apps.claims import io as claims_io

_RESERVED_MODEL_DIRS = frozenset({"registered", "labelers", "embedders"})


def _move(src: Path, dest: Path, *, dry_run: bool) -> str:
    if not src.exists():
        return f"skip (missing): {src}"
    if dest.exists():
        # Allow empty dest dir
        if dest.is_dir() and not any(dest.iterdir()):
            if not dry_run:
                dest.rmdir()
        else:
            raise FileExistsError(f"Destination already exists: {dest}")
    if dry_run:
        return f"would move: {src} → {dest}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))
    return f"moved: {src} → {dest}"


def _split_corpus_tag(name: str) -> tuple[str, str] | None:
    if "__" not in name:
        return None
    slug, _, tag = name.partition("__")
    if not slug or not tag:
        return None
    return slug, tag


def migrate(*, dry_run: bool = False) -> list[str]:
    root = claims_io.data_root()
    log: list[str] = []

    # inputs → corpora
    log.append(_move(root / "inputs", root / "corpora", dry_run=dry_run))
    # labels → fixtures
    log.append(_move(root / "labels", root / "fixtures", dry_run=dry_run))

    # Ensure registered/
    reg = root / "models" / "registered"
    if not dry_run:
        reg.mkdir(parents=True, exist_ok=True)
    else:
        log.append(f"would mkdir: {reg}")

    # Flat model tags → registered/
    models = root / "models"
    if models.is_dir():
        for p in sorted(models.iterdir()):
            if not p.is_dir() or p.name.startswith(".") or p.name in _RESERVED_MODEL_DIRS:
                continue
            dest = reg / p.name
            log.append(_move(p, dest, dry_run=dry_run))
            sidecar = models / f"{p.name}.json"
            if sidecar.is_file():
                log.append(_move(sidecar, reg / f"{p.name}.json", dry_run=dry_run))

    # runs/<corpus>__<tag> → runs/<corpus>/<tag>
    runs = root / "runs"
    if runs.is_dir():
        for p in sorted(runs.iterdir()):
            if not p.is_dir() or p.name.startswith("."):
                continue
            parts = _split_corpus_tag(p.name)
            if not parts:
                # Already nested corpus dir, or unknown — leave alone if it has no __
                continue
            slug, tag = parts
            dest = runs / slug / tag
            log.append(_move(p, dest, dry_run=dry_run))

    # experiments/<corpus>__<tag> → experiments/clustering/<corpus>/<tag>
    # Keep model_eval in place
    experiments = root / "experiments"
    clustering = experiments / "clustering"
    if experiments.is_dir():
        if not dry_run:
            clustering.mkdir(parents=True, exist_ok=True)
        for p in sorted(experiments.iterdir()):
            if not p.is_dir() or p.name.startswith("."):
                continue
            if p.name in ("model_eval", "clustering"):
                continue
            parts = _split_corpus_tag(p.name)
            if not parts:
                continue
            slug, tag = parts
            dest = clustering / slug / tag
            log.append(_move(p, dest, dry_run=dry_run))

    if not dry_run:
        claims_io.ensure_data_dirs()
    return log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    try:
        for line in migrate(dry_run=bool(args.dry_run)):
            print(line)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
