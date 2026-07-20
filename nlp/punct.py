"""Gated punctuation restoration for low-punctuation text (e.g. transcripts).

Uses ``deepmultilingualpunctuation`` (same stack as ``transcription/whisper_trial.py``).
Install: ``pip install -r nlp/requirements-punct.txt``

Gate (length-aware):
  restore when len ≥ min_chars and (
    ratio < threshold                          # default 0.004 — very low punct
    or (len ≥ long_min_chars and ratio < long_threshold)  # default 2000 / 0.008
  )
"""

from __future__ import annotations

import os
import re
from typing import Any

# Sentence / clause markers; commas included so lightly punctuated prose can pass the gate.
_PUNCT_RE = re.compile(r"[.?!;:,]")

# ≈ <4 marker chars per 1000 characters → VERY low (short/medium posts)
DEFAULT_PUNCT_RATIO_THRESHOLD = float(os.getenv("NLP_PUNCT_RATIO_THRESHOLD", "0.004"))
# Slightly more liberal for long bodies (podcast/ASR-ish) without flooding short posts
DEFAULT_LONG_PUNCT_RATIO_THRESHOLD = float(os.getenv("NLP_PUNCT_LONG_RATIO_THRESHOLD", "0.008"))
DEFAULT_LONG_MIN_CHARS = max(1, int(os.getenv("NLP_PUNCT_LONG_MIN_CHARS", "2000")))
MIN_CHARS_FOR_RESTORE = max(1, int(os.getenv("NLP_PUNCT_MIN_CHARS", "80")))

_MODEL: Any = None


def punctuation_ratio(text: str) -> float:
    if not text:
        return 0.0
    return len(_PUNCT_RE.findall(text)) / max(len(text), 1)


def needs_punctuation(
    text: str,
    *,
    threshold: float = DEFAULT_PUNCT_RATIO_THRESHOLD,
    long_threshold: float = DEFAULT_LONG_PUNCT_RATIO_THRESHOLD,
    long_min_chars: int = DEFAULT_LONG_MIN_CHARS,
    min_chars: int = MIN_CHARS_FOR_RESTORE,
) -> bool:
    """
    True when punctuation should be restored.

    Base gate: ``ratio < threshold``.
    Length-aware bump: also True when ``len(text) ≥ long_min_chars`` and
    ``ratio < long_threshold`` (captures large lightly-punctuated transcripts
    without raising the global threshold).
    """
    if not isinstance(text, str):
        return False
    n = len(text.strip())
    if n < min_chars:
        return False
    ratio = punctuation_ratio(text)
    if ratio < threshold:
        return True
    if n >= long_min_chars and ratio < long_threshold:
        return True
    return False


def _load_model():
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    try:
        from deepmultilingualpunctuation import PunctuationModel
    except ImportError as e:
        raise ImportError(
            "deepmultilingualpunctuation is required for nlp.punct. "
            "Install with: pip install -r nlp/requirements-punct.txt"
        ) from e
    _MODEL = PunctuationModel()
    return _MODEL


def _restore_punctuation_chunked(model: Any, text: str, *, max_words: int = 180) -> str:
    """
    Call ``model.restore_punctuation`` on word-sized slices.

    The upstream model joins ~230-word batches into one string for the HF pipeline;
    very long tokens / wide chars can still clip and assert. Smaller slices avoid that.
    """
    words = text.split()
    if len(words) <= max_words:
        return model.restore_punctuation(text)
    parts: list[str] = []
    for i in range(0, len(words), max_words):
        chunk = " ".join(words[i : i + max_words])
        parts.append(model.restore_punctuation(chunk))
    return " ".join(parts)


def restore_punctuation(
    text: str,
    *,
    force: bool = False,
    threshold: float = DEFAULT_PUNCT_RATIO_THRESHOLD,
    long_threshold: float = DEFAULT_LONG_PUNCT_RATIO_THRESHOLD,
    long_min_chars: int = DEFAULT_LONG_MIN_CHARS,
    min_chars: int = MIN_CHARS_FOR_RESTORE,
) -> tuple[str, bool]:
    """
    Return ``(text_out, did_restore)``.

    No-ops (returns original, False) when the gate says no unless ``force``.
    On model failure, returns the original text unchanged.
    """
    if not isinstance(text, str) or not text.strip():
        return text if isinstance(text, str) else "", False
    if not force and not needs_punctuation(
        text,
        threshold=threshold,
        long_threshold=long_threshold,
        long_min_chars=long_min_chars,
        min_chars=min_chars,
    ):
        return text, False
    model = _load_model()
    try:
        out = _restore_punctuation_chunked(model, text)
    except Exception:  # noqa: BLE001 — keep pipeline moving on pathological inputs
        return text, False
    if not isinstance(out, str) or not out.strip():
        return text, False
    return out, out != text


def remap_hits_to_text(
    original: str,
    new_text: str,
    hits: list[Any],
) -> list[dict[str, Any]]:
    """Best-effort remap hit offsets from ``original`` onto ``new_text`` after punct."""
    if not isinstance(hits, list):
        return []
    out: list[dict[str, Any]] = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        row = dict(hit)
        try:
            ms = int(hit.get("match_start", 0) or 0)
            me = int(hit.get("match_end", 0) or 0)
        except (TypeError, ValueError):
            out.append(row)
            continue
        ms = max(0, min(ms, len(original)))
        me = max(ms, min(me, len(original)))
        slice_ = original[ms:me]
        idx = new_text.find(slice_) if slice_ else -1
        if idx < 0:
            term = hit.get("term_name")
            if isinstance(term, str) and term.strip():
                idx = new_text.casefold().find(term.casefold())
                if idx >= 0:
                    me_new = idx + len(term)
                    row["match_start"] = idx
                    row["match_end"] = me_new
                    out.append(row)
                    continue
            # clamp old offsets
            row["match_start"] = max(0, min(ms, len(new_text)))
            row["match_end"] = max(row["match_start"], min(me, len(new_text)))
            out.append(row)
            continue
        row["match_start"] = idx
        row["match_end"] = idx + len(slice_)
        out.append(row)
    return out
