"""Corpus path conventions for file-mode development.

Layout::

    data/inputs/<slug>/
      NOTES.md
      posts.json
      claims.json
      groups.json
    data/runs/<slug>__<model_tag>/
    data/experiments/<slug>__<model_tag>/[<experiment>/]
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

POSTS_FILE = "posts.json"
CLAIMS_FILE = "claims.json"
GROUPS_FILE = "groups.json"
NOTES_FILE = "NOTES.md"


def validate_slug(name: str) -> str:
    slug = (name or "").strip().casefold()
    if not _SLUG_RE.match(slug):
        raise ValueError(
            f"Invalid corpus name {name!r}: use lowercase letters, digits, underscores "
            f"(must start with a letter), e.g. measles, all_vax"
        )
    return slug


def model_tag_from_path(model_id: str) -> str:
    """Derive a short run-name tag from a model id or local path."""
    raw = (model_id or "").strip().rstrip("/")
    if not raw:
        return "model"
    name = Path(raw).name or raw
    # HuggingFace ids like BAAI/bge-large-en-v1.5 -> bge-large-en-v1.5
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("._-")
    return (safe or "model")[:64]


@dataclass(frozen=True)
class CorpusPaths:
    slug: str

    @property
    def root(self) -> Path:
        return claims_io.inputs_dir() / self.slug

    @property
    def posts(self) -> Path:
        return self.root / POSTS_FILE

    @property
    def claims(self) -> Path:
        return self.root / CLAIMS_FILE

    @property
    def groups(self) -> Path:
        return self.root / GROUPS_FILE

    @property
    def notes(self) -> Path:
        return self.root / NOTES_FILE

    def run_name(self, model_tag: str) -> str:
        tag = validate_model_tag(model_tag)
        return f"{self.slug}__{tag}"

    def run_dir(self, model_tag: str) -> Path:
        return claims_io.runs_dir() / self.run_name(model_tag)

    def experiments_root(self, model_tag: str) -> Path:
        return claims_io.experiments_dir() / self.run_name(model_tag)

    def experiment_dir(self, model_tag: str, name: str) -> Path:
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip()).strip("._-") or "exp"
        return self.experiments_root(model_tag) / safe

    def status(self) -> dict[str, Any]:
        def _file_info(path: Path) -> dict[str, Any]:
            if not path.is_file():
                return {"exists": False}
            info: dict[str, Any] = {
                "exists": True,
                "bytes": path.stat().st_size,
                "path": str(path),
            }
            try:
                data = claims_io.read_json(path)
            except Exception:  # noqa: BLE001
                return info
            if path.name == POSTS_FILE:
                posts = data.get("posts") if isinstance(data, dict) else None
                if isinstance(posts, list):
                    info["post_count"] = len(posts)
                if isinstance(data, dict) and data.get("terms") is not None:
                    info["terms"] = data.get("terms")
            elif path.name == CLAIMS_FILE:
                posts = data.get("posts") if isinstance(data, dict) else None
                if isinstance(posts, list):
                    info["post_count"] = len(posts)
                    n_claims = 0
                    for row in posts:
                        if not isinstance(row, dict):
                            continue
                        out = row.get("claim_extraction_output")
                        if isinstance(out, dict) and isinstance(out.get("claims"), list):
                            n_claims += len(out["claims"])
                    info["claim_count"] = n_claims
            elif path.name == GROUPS_FILE:
                if isinstance(data, dict):
                    if data.get("claim_count") is not None:
                        info["claim_count"] = data.get("claim_count")
                    if data.get("source_claim_count") is not None:
                        info["source_claim_count"] = data.get("source_claim_count")
                    if data.get("source_hash"):
                        info["source_hash"] = str(data["source_hash"])[:16]
            return info

        runs: list[dict[str, Any]] = []
        if claims_io.runs_dir().is_dir():
            prefix = f"{self.slug}__"
            for p in sorted(claims_io.runs_dir().iterdir()):
                if p.is_dir() and p.name.startswith(prefix):
                    metrics_path = p / claims_io.METRICS_FILE
                    entry: dict[str, Any] = {"name": p.name, "path": str(p)}
                    if metrics_path.is_file():
                        try:
                            m = claims_io.read_json(metrics_path)
                            entry["claim_count"] = m.get("claim_count")
                            entry["vector_dim"] = m.get("vector_dim")
                            entry["model_id"] = m.get("model_id")
                        except Exception:  # noqa: BLE001
                            pass
                    runs.append(entry)

        experiments: list[dict[str, Any]] = []
        if claims_io.experiments_dir().is_dir():
            prefix = f"{self.slug}__"
            for run_root in sorted(claims_io.experiments_dir().iterdir()):
                if not (run_root.is_dir() and run_root.name.startswith(prefix)):
                    continue
                for exp in sorted(run_root.iterdir()):
                    if exp.is_dir():
                        experiments.append(
                            {
                                "run": run_root.name,
                                "name": exp.name,
                                "path": str(exp),
                                "mtime": exp.stat().st_mtime,
                            }
                        )
        experiments.sort(key=lambda e: float(e.get("mtime") or 0), reverse=True)
        latest = experiments[0] if experiments else None
        if latest:
            latest = {k: v for k, v in latest.items() if k != "mtime"}
        experiments_out = [{k: v for k, v in e.items() if k != "mtime"} for e in experiments[:20]]

        return {
            "slug": self.slug,
            "root": str(self.root),
            "posts": _file_info(self.posts),
            "claims": _file_info(self.claims),
            "groups": _file_info(self.groups),
            "notes": self.notes.is_file(),
            "runs": runs,
            "experiments": experiments_out,
            "latest_experiment": latest,
        }


def validate_model_tag(tag: str) -> str:
    t = (tag or "").strip()
    if not t:
        raise ValueError("model_tag must be non-empty")
    if "/" in t or "\\" in t or ".." in t:
        raise ValueError(f"Invalid model_tag {tag!r}: no path separators")
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", t).strip("._-")
    if not safe:
        raise ValueError(f"Invalid model_tag {tag!r}")
    return safe


def get_corpus(name: str) -> CorpusPaths:
    slug = validate_slug(name)
    return CorpusPaths(slug=slug)


def list_corpora() -> list[dict[str, Any]]:
    root = claims_io.inputs_dir()
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.iterdir()):
        if p.is_dir() and not p.name.startswith("."):
            try:
                out.append(get_corpus(p.name).status())
            except ValueError:
                continue
    return out


def create_corpus(name: str, *, notes: str | None = None) -> CorpusPaths:
    corpus = get_corpus(name)
    claims_io.ensure_data_dirs()
    claims_io.experiments_dir().mkdir(parents=True, exist_ok=True)
    if corpus.root.exists():
        raise FileExistsError(f"Corpus already exists: {corpus.root}")
    corpus.root.mkdir(parents=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    body = notes.strip() if notes and notes.strip() else f"# {corpus.slug}\n\nCreated {stamp}.\n"
    if not body.startswith("#"):
        body = f"# {corpus.slug}\n\n{body}\n"
    corpus.notes.write_text(body, encoding="utf-8")
    return corpus
