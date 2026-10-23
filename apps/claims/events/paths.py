"""Run-directory layout for event-extraction experiments."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
RUNS_DIR = PACKAGE_ROOT / "runs"

CONFIG_FILE = "config.json"
DOCUMENTS_FILE = "documents.jsonl"
EXTRACT_LOG_FILE = "extract_log.jsonl"
MENTIONS_FILE = "mentions.jsonl"
BROWSE_FILE = "browse.json"
METRICS_FILE = "metrics.json"


def next_run_dir(runs_dir: Path | None = None) -> Path:
    root = Path(runs_dir) if runs_dir is not None else RUNS_DIR
    root.mkdir(parents=True, exist_ok=True)
    n = 1
    while True:
        candidate = root / f"run_{n}"
        if not candidate.exists():
            return candidate
        n += 1


def latest_run_dir(runs_dir: Path | None = None) -> Path | None:
    root = Path(runs_dir) if runs_dir is not None else RUNS_DIR
    if not root.is_dir():
        return None
    numbered = []
    for child in root.iterdir():
        if child.is_dir() and child.name.startswith("run_"):
            suffix = child.name[len("run_") :]
            if suffix.isdigit():
                numbered.append((int(suffix), child))
    if not numbered:
        return None
    numbered.sort()
    return numbered[-1][1]
