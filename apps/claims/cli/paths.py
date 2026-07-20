"""Resolve --corpus / --model-tag defaults for pipeline commands."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import corpus as corpus_mod


def require_corpus(args: Namespace) -> corpus_mod.CorpusPaths:
    name = getattr(args, "corpus", None)
    if not name:
        raise ValueError("Provide --corpus or explicit path flags")
    return corpus_mod.get_corpus(str(name))


def resolve_model_tag(args: Namespace, *, model_id: str | None = None) -> str:
    tag = getattr(args, "model_tag", None)
    if tag:
        return corpus_mod.validate_model_tag(str(tag))
    mid = model_id or getattr(args, "model", None)
    if mid:
        return corpus_mod.model_tag_from_path(str(mid))
    raise ValueError("Provide --model-tag (or --model so a tag can be derived)")


def path_or_corpus(explicit: Path | None, default: Path) -> Path:
    return Path(explicit) if explicit is not None else default
