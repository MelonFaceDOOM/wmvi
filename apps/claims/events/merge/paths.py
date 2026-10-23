"""Disposable merge-attempt directories. Not coupled to extract run_N."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
MERGES_DIR = PACKAGE_ROOT / "merges"

CONFIG_FILE = "config.json"
CLUSTERS_FILE = "clusters.jsonl"
MEMBERS_FILE = "members.jsonl"
METRICS_FILE = "metrics.json"
QA_FILE = "qa.json"
DROPPED_FILE = "dropped.jsonl"
BROWSE_FILE = "browse.json"
BROWSE_HTML = "browse.html"


def next_merge_dir(merges_dir: Path | None = None) -> Path:
    root = Path(merges_dir) if merges_dir is not None else MERGES_DIR
    root.mkdir(parents=True, exist_ok=True)
    n = 1
    while True:
        candidate = root / f"merge_{n}"
        if not candidate.exists():
            return candidate
        n += 1
