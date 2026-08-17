"""Sample claim rows from a run index, optionally excluding known claim_keys."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any, Iterable


def load_corpus_pool(
    corpus: str,
    *,
    allow_keys: set[str] | None = None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Return ``(index, claim_texts, claim_keys)`` for a corpus ``groups.json``.

    When ``allow_keys`` is set, the index is subset to those claim keys
    (order preserved from the parent groups file).
    """
    from apps.claims import corpus as corpus_mod
    from apps.claims import io as claims_io
    from apps.claims import selections as sel_mod

    corp = corpus_mod.get_corpus(corpus)
    if not corp.groups.is_file():
        raise FileNotFoundError(
            f"Missing groups.json at {corp.groups} "
            "(run `group --corpus ...` first)"
        )
    index = claims_io.normalize_index(claims_io.read_json(corp.groups))
    if allow_keys is not None:
        from apps.claims import filtering as filt

        index = filt.subset_index_by_keys(index, allow_keys)
    claim_texts = claims_io.claim_texts_from_index(index)
    claim_keys = sel_mod.claim_keys_from_index(index)
    return index, claim_texts, claim_keys


def load_exclude_claim_keys(path: Path | None) -> set[str]:
    """Load claim_keys to skip from a file.

    Accepts:
    - JSONL: each line a key string, or an object with ``claim_key`` / ``k``
    - JSON: list of key strings, or list of objects with ``claim_key`` / ``k``,
      or ``{"keys": [...]}`` / ``{"claim_keys": [...]}``
    - Plain text: one key per line (# comments and blanks ignored)
    """
    if path is None:
        return set()
    p = Path(path).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"Exclude file not found: {p}")
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        return set()

    # Try JSON first (whole file)
    if raw[0] in "[{":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if data is not None:
            return _keys_from_json_payload(data)

    out: set[str] = set()
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s[0] == "{":
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL in {p}: {exc}") from exc
            key = _key_from_obj(obj)
            if key:
                out.add(key)
        else:
            # bare key, or JSON string
            if s[0] == '"':
                try:
                    s = json.loads(s)
                except json.JSONDecodeError:
                    pass
            out.add(str(s).strip())
    return {k for k in out if k}


def _key_from_obj(obj: Any) -> str | None:
    if isinstance(obj, str):
        return obj.strip() or None
    if not isinstance(obj, dict):
        return None
    for field in ("claim_key", "k", "key"):
        v = obj.get(field)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _keys_from_json_payload(data: Any) -> set[str]:
    if isinstance(data, dict):
        for field in ("claim_keys", "keys", "exclude"):
            if field in data:
                return _keys_from_json_payload(data[field])
        key = _key_from_obj(data)
        return {key} if key else set()
    if isinstance(data, list):
        out: set[str] = set()
        for item in data:
            key = _key_from_obj(item)
            if key:
                out.add(key)
        return out
    if isinstance(data, str) and data.strip():
        return {data.strip()}
    return set()


def sample_claim_indices(
    claim_texts: list[str],
    *,
    n: int,
    seed: int,
    claim_keys: list[str] | None = None,
    exclude_keys: Iterable[str] | None = None,
) -> list[int]:
    """Sample up to ``n`` non-empty claim row indices, skipping exclude_keys."""
    excl = {str(k).strip() for k in (exclude_keys or []) if str(k).strip()}
    keys = claim_keys
    candidates: list[int] = []
    for i, text in enumerate(claim_texts):
        if not (text or "").strip():
            continue
        if excl:
            if keys is None or i >= len(keys):
                raise ValueError("exclude_keys requires claim_keys parallel to claim_texts")
            if keys[i] in excl:
                continue
        candidates.append(i)
    if not candidates:
        raise ValueError(
            "No candidate claims left"
            + (f" after excluding {len(excl)} keys" if excl else " (no non-empty texts)")
        )
    k = min(int(n), len(candidates))
    rng = random.Random(int(seed))
    return rng.sample(candidates, k=k)


def claim_rows_from_index(
    index: dict[str, Any],
    indices: list[int],
    *,
    claim_texts: list[str],
    claim_keys: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in indices:
        rows.append(
            {
                "idx": int(i),
                "claim_key": claim_keys[i] if i < len(claim_keys) else "",
                "text": claim_texts[i] if i < len(claim_texts) else "",
            }
        )
    return rows
