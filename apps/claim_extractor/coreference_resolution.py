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
import contextlib
import gc
import io
import importlib.metadata
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "sample.json"
DEFAULT_OUTPUT = SCRIPT_DIR / "sample_coref.json"

SPACY_MODEL = "en_core_web_lg"
# "cpu", "cuda", "cuda:0", or None to let fastcoref pick (GPU when available).
DEVICE: str | None = None
PIPE_BATCH_SIZE = max(1, int(os.getenv("COREF_PIPE_BATCH_SIZE", "8")))
COREF_RESET_EVERY_BATCHES = max(0, int(os.getenv("COREF_RESET_EVERY_BATCHES", "200")))
COREF_DEBUG_PERF = os.getenv("COREF_DEBUG_PERF", "").strip().lower() in ("1", "true", "yes", "on")
COREF_DEBUG_EVERY = int(os.getenv("COREF_DEBUG_EVERY", "25"))
# spaCy pipes not needed for POS-based head picking in fastcoref’s resolver.
SPACY_EXCLUDE = ("parser", "lemmatizer", "ner", "textcat")

_NLP = None
_RESOLVE_BATCH_CALLS = 0
_QUIET_SINK = io.StringIO()
QUIET_INFERENCE = True


def _maybe_cuda_gc() -> None:
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass


def _reset_runtime_state() -> None:
    global _NLP
    _NLP = None
    gc.collect()
    _maybe_cuda_gc()


def _maybe_periodic_reset() -> None:
    global _RESOLVE_BATCH_CALLS
    _RESOLVE_BATCH_CALLS += 1
    if COREF_RESET_EVERY_BATCHES > 0 and _RESOLVE_BATCH_CALLS % COREF_RESET_EVERY_BATCHES == 0:
        _reset_runtime_state()


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
    try:
        import datasets

        datasets.disable_progress_bars()
    except Exception:
        pass
    # Suppress noisy tokenizer/inference info logs from third-party libs.
    for name in ("fastcoref", "transformers", "tokenizers", "sentence_transformers"):
        logging.getLogger(name).setLevel(logging.ERROR)
    _NLP = spacy.load(SPACY_MODEL, exclude=list(SPACY_EXCLUDE))
    cfg: dict[str, Any] = {"enable_progress_bar": False}
    if DEVICE is not None:
        cfg["device"] = DEVICE
    _NLP.add_pipe("fastcoref", config=cfg)
    return _NLP


def _resolve_batch(texts: list[str]) -> list[str]:
    _maybe_periodic_reset()
    nlp = _load_nlp()
    cfg = {"fastcoref": {"resolve_text": True}}
    out: list[str] = []
    debug_every = max(1, int(COREF_DEBUG_EVERY))
    t0 = None
    if COREF_DEBUG_PERF:
        t0 = time.monotonic()
    try:
        import torch

        inference_ctx: Any = torch.inference_mode()
    except Exception:
        inference_ctx = contextlib.nullcontext()
    if QUIET_INFERENCE:
        _QUIET_SINK.seek(0)
        _QUIET_SINK.truncate(0)
        with contextlib.redirect_stdout(_QUIET_SINK), contextlib.redirect_stderr(_QUIET_SINK), inference_ctx:
            for doc in nlp.pipe(texts, batch_size=PIPE_BATCH_SIZE, component_cfg=cfg):
                out.append(doc._.resolved_text or doc.text)
    else:
        with inference_ctx:
            for doc in nlp.pipe(texts, batch_size=PIPE_BATCH_SIZE, component_cfg=cfg):
                out.append(doc._.resolved_text or doc.text)
    if COREF_DEBUG_PERF and t0 is not None and _RESOLVE_BATCH_CALLS % debug_every == 0:
        dt = max(0.001, time.monotonic() - t0)
        rate = len(texts) / dt
        print(
            f"[debug] coref batch_calls={_RESOLVE_BATCH_CALLS} size={len(texts)} elapsed={dt:.3f}s rate={rate:.2f} texts/s",
            file=sys.stderr,
            flush=True,
        )
    return out


def _resolve_with_fallback(texts: list[str]) -> tuple[list[str], int]:
    """
    Resolve a batch; if batch inference fails, retry per-item and keep original text on failure.
    Returns (resolved_texts, num_failed_items).
    """
    if not texts:
        return [], 0
    try:
        return _resolve_batch(texts), 0
    except Exception as e:
        print(f"[warn] coref batch failed ({len(texts)} texts): {e}", file=sys.stderr, flush=True)
    out: list[str] = []
    failed = 0
    for t in texts:
        try:
            one, _ = _resolve_with_fallback_single(t)
            out.append(one)
        except Exception as e:
            failed += 1
            print(f"[warn] coref item failed; using original text: {e}", file=sys.stderr, flush=True)
            out.append(t)
    return out, failed


def _resolve_with_fallback_single(text: str) -> tuple[str, int]:
    for _ in range(2):
        try:
            resolved = _resolve_batch([text])
            return (resolved[0] if resolved else text), 0
        except Exception:
            continue
    raise RuntimeError("single-item coref failed after retries")


def process_payload(data: dict[str, Any], *, progress: bool = False) -> dict[str, Any]:
    del progress  # single global progress bar is handled by caller
    out = data
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
        resolved_list, _ = _resolve_with_fallback(texts)
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
