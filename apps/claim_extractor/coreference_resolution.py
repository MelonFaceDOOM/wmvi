"""
Add ``text_coreference_resolved`` to each post using local neural coreference (F-Coref).

Uses the ``fastcoref`` spaCy component with ``resolve_text=True`` so pronouns and vague
NPs (e.g. "the vaccine") are rewritten toward cluster heads when the model finds a link.

Run **before** ``trim_transcripts.py``. Requires extra deps:

  pip install -r apps/claim_extractor/requirements-coref.txt

``transformers`` must stay on v4.x (v5 breaks ``fastcoref``).

Usage:
  python apps/claim_extractor/coreference_resolution.py
  python apps/claim_extractor/coreference_resolution.py --input sample.json --output sample_coref.json
"""

from __future__ import annotations

import argparse
import copy
import importlib.metadata
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "sample.json"
DEFAULT_OUTPUT = SCRIPT_DIR / "sample_coref.json"

SPACY_MODEL = "en_core_web_lg"
# "cpu", "cuda", "cuda:0", or None to let fastcoref pick (GPU when available).
DEVICE: str | None = None
PIPE_BATCH_SIZE = 16
# spaCy pipes not needed for POS-based head picking in fastcoref’s resolver.
SPACY_EXCLUDE = ("parser", "lemmatizer", "ner", "textcat")

_NLP = None


def _load_nlp():
    global _NLP
    if _NLP is not None:
        return _NLP
    try:
        import spacy
        from fastcoref import spacy_component  # noqa: F401 — registers "fastcoref"

        try:
            tv = importlib.metadata.version("transformers").split(".", 1)[0]
            if tv.isdigit() and int(tv) >= 5:
                print(
                    "Warning: transformers>=5 is incompatible with fastcoref; "
                    "pin transformers<5 (see apps/claim_extractor/requirements-coref.txt).",
                    file=sys.stderr,
                )
        except importlib.metadata.PackageNotFoundError:
            pass
    except ImportError as e:
        print(
            "Missing dependencies for coreference resolution. Install with:\n"
            "  pip install -r apps/claim_extractor/requirements-coref.txt",
            file=sys.stderr,
        )
        raise e
    _NLP = spacy.load(SPACY_MODEL, exclude=list(SPACY_EXCLUDE))
    cfg: dict[str, Any] = {"enable_progress_bar": False}
    if DEVICE is not None:
        cfg["device"] = DEVICE
    _NLP.add_pipe("fastcoref", config=cfg)
    return _NLP


def _resolve_batch(texts: list[str], *, progress: bool = False) -> list[str]:
    nlp = _load_nlp()
    cfg = {"fastcoref": {"resolve_text": True}}
    out: list[str] = []
    iterator = nlp.pipe(texts, batch_size=PIPE_BATCH_SIZE, component_cfg=cfg)
    if progress and texts:
        from tqdm import tqdm

        iterator = tqdm(
            iterator,
            total=len(texts),
            desc="coref",
            unit="post",
            leave=True,
        )
    for doc in iterator:
        out.append(doc._.resolved_text or doc.text)
    return out


def process_payload(data: dict[str, Any], *, progress: bool = False) -> dict[str, Any]:
    out = copy.deepcopy(data)
    posts = out.get("posts")
    if not isinstance(posts, list):
        return out

    indices: list[int] = []
    texts: list[str] = []
    for i, post in enumerate(posts):
        if not isinstance(post, dict):
            continue
        raw = post.get("text")
        if not isinstance(raw, str) or not raw.strip():
            post["text_coreference_resolved"] = raw if isinstance(raw, str) else ""
            continue
        indices.append(i)
        texts.append(raw)

    if texts:
        resolved_list = _resolve_batch(texts, progress=progress)
        for idx, resolved in zip(indices, resolved_list):
            posts[idx]["text_coreference_resolved"] = resolved

    return out


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Add text_coreference_resolved via F-Coref.")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(list(argv) if argv is not None else None)

    raw = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit("input JSON must be an object")
    processed = process_payload(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(processed, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    n = len(processed.get("posts", []))
    print(f"[ok] wrote {n} posts with coreference fields to {args.output.resolve()}")


if __name__ == "__main__":
    main()
