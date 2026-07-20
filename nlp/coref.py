"""
Coreference helper for post dictionaries.

Accepts post objects with a ``text`` field, resolves coreference in batches,
and yields posts with ``text_coreference_resolved`` added.

Heavy deps are optional; install ``nlp/requirements-coref.txt``.
"""

from __future__ import annotations

import contextlib
import gc
import io
import importlib.metadata
import logging
import os
import sys
import time
from typing import Any, Iterable, Iterator

SPACY_MODEL = "en_core_web_lg"
DEVICE: str | None = None
PIPE_BATCH_SIZE = max(1, int(os.getenv("COREF_PIPE_BATCH_SIZE", "8")))
COREF_MAX_CHARS = max(1, int(os.getenv("COREF_MAX_CHARS", "12000")))
COREF_RESET_EVERY_BATCHES = max(0, int(os.getenv("COREF_RESET_EVERY_BATCHES", "200")))
COREF_DEBUG_PERF = os.getenv("COREF_DEBUG_PERF", "").strip().lower() in ("1", "true", "yes", "on")
COREF_DEBUG_EVERY = int(os.getenv("COREF_DEBUG_EVERY", "25"))
COREF_METRICS_EVERY = max(1, int(os.getenv("COREF_METRICS_EVERY", "1")))
SPACY_EXCLUDE = ("parser", "lemmatizer", "ner", "textcat")

_NLP = None
_RESOLVE_BATCH_CALLS = 0
_QUIET_SINK = io.StringIO()
QUIET_INFERENCE = True
_DEVICE_LOGGED = False
_POSTS_BATCH_CALLS = 0


def _process_rss_mb() -> float | None:
    try:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports ru_maxrss in KB.
        return float(rss) / 1024.0
    except Exception:
        return None


def _cuda_memory_mb() -> float | None:
    try:
        torch = importlib.import_module("torch")

        if not torch.cuda.is_available():
            return None
        return float(torch.cuda.memory_allocated()) / (1024.0 * 1024.0)
    except Exception:
        return None


def _device_label() -> str:
    if DEVICE is not None:
        return str(DEVICE)
    return "auto"


def _log_runtime_device_once() -> None:
    global _DEVICE_LOGGED
    if _DEVICE_LOGGED:
        return

    cuda_available = False
    cuda_device_name = "n/a"
    try:
        torch = importlib.import_module("torch")

        cuda_available = bool(torch.cuda.is_available())
        if cuda_available:
            cuda_device_name = str(torch.cuda.get_device_name(torch.cuda.current_device()))
    except Exception:
        pass

    print(
        "[coref] runtime: "
        f"configured_device={_device_label()} "
        f"pipe_batch_size={PIPE_BATCH_SIZE} "
        f"spacy_model={SPACY_MODEL} "
        f"cuda_available={cuda_available} "
        f"cuda_device={cuda_device_name}",
        file=sys.stderr,
        flush=True,
    )
    _DEVICE_LOGGED = True


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
                    "pin transformers<5 (see nlp/requirements-coref.txt).",
                    file=sys.stderr,
                )
        except importlib.metadata.PackageNotFoundError:
            pass
    except ImportError as e:
        print(
            "Missing dependencies for coreference resolution. Install with:\n"
            "  pip install -r nlp/requirements-coref.txt\n"
            "  (also requires spaCy model en_core_web_lg)",
            file=sys.stderr,
        )
        raise e
    try:
        import datasets

        datasets.disable_progress_bars()
    except Exception:
        pass
    for name in ("fastcoref", "transformers", "tokenizers", "sentence_transformers"):
        logging.getLogger(name).setLevel(logging.ERROR)
    _NLP = spacy.load(SPACY_MODEL, exclude=list(SPACY_EXCLUDE))
    cfg: dict[str, Any] = {"enable_progress_bar": False}
    if DEVICE is not None:
        cfg["device"] = DEVICE
    _NLP.add_pipe("fastcoref", config=cfg)
    _log_runtime_device_once()
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


def _process_batch(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    global _POSTS_BATCH_CALLS
    _POSTS_BATCH_CALLS += 1
    t0 = time.monotonic()
    indices: list[int] = []
    texts: list[str] = []
    for i, post in enumerate(posts):
        if not isinstance(post, dict):
            continue
        raw = post.get("text")
        if not isinstance(raw, str) or not raw.strip():
            post["text_coreference_resolved"] = raw if isinstance(raw, str) else ""
            continue
        if len(raw) > COREF_MAX_CHARS:
            print(raw)
            raise ValueError(
                f"coref input exceeds COREF_MAX_CHARS={COREF_MAX_CHARS}: "
                f"post_id={post.get('post_id')} chars={len(raw)}"
            )
        indices.append(i)
        texts.append(raw)

    if texts:
        resolved_list, _ = _resolve_with_fallback(texts)
        for idx, resolved in zip(indices, resolved_list):
            posts[idx]["text_coreference_resolved"] = resolved

    if _POSTS_BATCH_CALLS % COREF_METRICS_EVERY == 0:
        dt = max(0.001, time.monotonic() - t0)
        rss_mb = _process_rss_mb()
        cuda_mb = _cuda_memory_mb()
        post_rate = len(posts) / dt
        text_rate = len(texts) / dt if texts else 0.0
        rss_txt = f"{rss_mb:.1f}MB" if rss_mb is not None else "n/a"
        cuda_txt = f"{cuda_mb:.1f}MB" if cuda_mb is not None else "n/a"
        print(
            "[metrics] coref_batch "
            f"batch_calls={_POSTS_BATCH_CALLS} "
            f"posts={len(posts)} "
            f"texts={len(texts)} "
            f"elapsed={dt:.3f}s "
            f"posts_per_s={post_rate:.2f} "
            f"texts_per_s={text_rate:.2f} "
            f"rss={rss_txt} "
            f"cuda_alloc={cuda_txt}",
            file=sys.stderr,
            flush=True,
        )

    return posts


def iter_coref_resolved_posts(
    posts: Iterable[dict[str, Any]],
    *,
    batch_size: int = 8,
) -> Iterator[dict[str, Any]]:
    current_batch: list[dict[str, Any]] = []
    for post in posts:
        current_batch.append(post)
        if len(current_batch) >= max(1, int(batch_size)):
            for processed in _process_batch(current_batch):
                yield processed
            current_batch = []

    if current_batch:
        for processed in _process_batch(current_batch):
            yield processed


def process_payload(data: dict[str, Any], *, progress: bool = False) -> dict[str, Any]:
    del progress
    out = data
    posts = out.get("posts")
    if not isinstance(posts, list):
        return out
    out["posts"] = list(iter_coref_resolved_posts(posts, batch_size=PIPE_BATCH_SIZE))
    return out
