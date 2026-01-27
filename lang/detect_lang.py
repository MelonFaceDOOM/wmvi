from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Optional

from lingua import Language, LanguageDetectorBuilder


# ---------------------------
# Text cleaning / heuristics
# ---------------------------

# URLs (http/https, www.*, and bare domains like example.com/path)
_URL_RE = re.compile(
    r"""
    (?:
        https?://\S+              # http(s) URLs
        | www\.\S+                # www URLs
        | \b[a-z0-9-]+(?:\.[a-z0-9-]+)+/\S*  # bare domains with path
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# @handles
_HANDLE_RE = re.compile(r"@\w+")
# collapse whitespace
_WS_RE = re.compile(r"\s+")


def _strip_noise(text: str) -> str:
    """
    Remove non-language-heavy tokens that confuse detectors on short texts.
    Keeps hashtag words but drops the '#'.
    """
    t = (text or "").strip()
    if not t:
        return ""

    # strip URLs and @handles
    t = _URL_RE.sub(" ", t)
    t = _HANDLE_RE.sub(" ", t)

    # keep hashtag token but drop marker
    t = t.replace("#", " ")

    # normalize whitespace
    t = _WS_RE.sub(" ", t).strip()
    return t


def _alpha_char_count(text: str) -> int:
    return sum(1 for ch in text if ch.isalpha())


# ---------------------------
# Detector
# ---------------------------

@lru_cache(maxsize=1)
def _detector():
    """
    Build once per process.
    Optionally override the candidate language set via env:
      WMVI_LANGS="en,fr,es,de,it,pt"
    """
    langs_env = (os.getenv("WMVI_LANGS") or "").strip().lower()
    if langs_env:
        m = {
            "en": Language.ENGLISH,
            "fr": Language.FRENCH,
            "es": Language.SPANISH,
            "de": Language.GERMAN,
            "it": Language.ITALIAN,
            "pt": Language.PORTUGUESE,
        }
        langs = []
        for code in [x.strip() for x in langs_env.split(",") if x.strip()]:
            if code in m:
                langs.append(m[code])
        if langs:
            return LanguageDetectorBuilder.from_languages(*langs).build()

    # sensible default
    return LanguageDetectorBuilder.from_languages(
        Language.ENGLISH,
        Language.FRENCH,
        Language.SPANISH,
        Language.GERMAN,
        Language.ITALIAN,
        Language.PORTUGUESE,
    ).build()


def detect_is_en(
    text: str,
    *,
    min_len: int = 24,
    min_conf: float = 0.65,
    min_alpha: int = 8,
) -> Optional[bool]:
    """
    Returns:
      True  -> confidently English
      False -> confidently non-English
      None  -> too short/ambiguous to label

    Notes:
    - Runs language detection on a cleaned version of the text with URLs/@handles removed.
    - If the cleaned text is too short (or has too few alphabetic characters), returns None.
    """
    cleaned = _strip_noise(text)
    if len(cleaned) < min_len:
        return None

    if _alpha_char_count(cleaned) < min_alpha:
        return None

    det = _detector()
    confs = det.compute_language_confidence_values(cleaned)
    if not confs:
        return None

    confs = sorted(confs, key=lambda x: x.value, reverse=True)
    best = confs[0]
    best_conf = float(best.value)

    if best_conf < min_conf:
        return None

    return best.language == Language.ENGLISH


def detect_is_en_debug(
    text: str,
    *,
    min_len: int = 24,
    min_conf: float = 0.65,
    min_alpha: int = 8,
) -> Optional[bool]:
    """
    debug print version
    """
    print("input:", text)
    cleaned = _strip_noise(text)
    print("cleaned:", cleaned)
    if len(cleaned) < min_len:
        print("FAIL: too short")
        return None

    if _alpha_char_count(cleaned) < min_alpha:
        print("FAIL: not enough alphabet")
        return None

    det = _detector()
    confs = det.compute_language_confidence_values(cleaned)
    if not confs:
        return None

    confs = sorted(confs, key=lambda x: x.value, reverse=True)
    best = confs[0]
    best_conf = float(best.value)
    print("conf is:", best_conf)

    if best_conf < min_conf:
        print("FAIL: conf too low")
        return None

    if not best.language == Language.ENGLISH:
        print("FAIL: not english")
        return False
    return True


def is_en(text: str, *, min_len: int = 24, min_conf: float = 0.65) -> bool:
    """
    Convenience T/F API.

    NOTE: ambiguous/short text -> False (conservative).
    """
    r = detect_is_en(text, min_len=min_len, min_conf=min_conf)
    return bool(r is True)
