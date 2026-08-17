"""One-off: gather WMVI / measles narrative project stats for status updates.

Run from repo root:
  python -m scripts.oneoffs.status_update_stats
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CLAIMS_JSON = REPO_ROOT / "data" / "posts_with_claims_full.json"
TERM_JSON = REPO_ROOT / "data" / "posts_for_term.json"
TERM_RAW = REPO_ROOT / "data" / "posts_for_term_raw.json"
TERM_TRIMMED = REPO_ROOT / "data" / "posts_for_term_trimmed.json"
CLAIMS_LABELERS = REPO_ROOT / "apps" / "claims" / "data" / "training" / "labelers"


def _parse_ts(val: object) -> datetime | None:
    if not isinstance(val, str) or not val.strip():
        return None
    s = val.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _summarize_posts_file(path: Path, label: str) -> None:
    print(f"\n=== {label} ({path.name}) ===")
    if not path.is_file():
        print("  (file not found)")
        return
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"  size: {size_mb:.1f} MB")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"  parse error: {e}")
        return
    posts = payload.get("posts") if isinstance(payload, dict) else None
    if not isinstance(posts, list):
        print("  invalid: expected posts[]")
        return
    terms = payload.get("terms") if isinstance(payload, dict) else None
    if terms:
        print(f"  search terms ({len(terms)}): {', '.join(str(t) for t in terms[:12])}")
        if len(terms) > 12:
            print(f"    ... +{len(terms) - 12} more")
    platforms = Counter()
    created: list[datetime] = []
    entered: list[datetime] = []
    for row in posts:
        if not isinstance(row, dict):
            continue
        platforms[str(row.get("platform") or "unknown")] += 1
        for key in ("created_at_ts", "date_entered"):
            dt = _parse_ts(row.get(key))
            if dt:
                (created if key == "created_at_ts" else entered).append(dt)
    print(f"  posts: {len(posts)}")
    print("  platforms:")
    for p, n in platforms.most_common():
        print(f"    {p}: {n}")
    for name, dts in (("created_at_ts", created), ("date_entered", entered)):
        if dts:
            print(f"  {name} range: {min(dts).date()} to {max(dts).date()}")


def _summarize_claims(path: Path) -> None:
    print(f"\n=== Claims extraction ({path.name}) ===")
    if not path.is_file():
        print("  (file not found)")
        return
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"  size: {size_mb:.1f} MB")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"  parse error: {e}")
        return
    posts = payload.get("posts") if isinstance(payload, dict) else None
    if not isinstance(posts, list):
        print("  invalid: expected posts[]")
        return
    status_c = Counter()
    platforms = Counter()
    total_claims = 0
    created: list[datetime] = []
    for row in posts:
        if not isinstance(row, dict):
            continue
        status_c[str(row.get("claim_extraction_status") or "missing")] += 1
        platforms[str(row.get("platform") or "unknown")] += 1
        dt = _parse_ts(row.get("created_at_ts"))
        if dt:
            created.append(dt)
        if row.get("claim_extraction_status") == "success":
            out = row.get("claim_extraction_output")
            if isinstance(out, dict) and isinstance(out.get("claims"), list):
                total_claims += len(out["claims"])
    print(f"  post rows: {len(posts)}")
    print("  extraction status:")
    for s, n in status_c.most_common():
        print(f"    {s}: {n}")
    print(f"  total claims (success rows): {total_claims}")
    print("  platforms:")
    for p, n in platforms.most_common():
        print(f"    {p}: {n}")
    if created:
        print(f"  created_at_ts range: {min(created).date()} to {max(created).date()}")


def _summarize_claims_labelers(root: Path) -> None:
    print(f"\n=== Claims labeler training ({root.relative_to(REPO_ROOT)}) ===")
    if not root.is_dir():
        print("  (dir not found)")
        return
    intents = sorted(p for p in root.iterdir() if p.is_dir())
    if not intents:
        print("  (no intents)")
        return
    for intent_dir in intents:
        labels_path = intent_dir / "labels.jsonl"
        n = 0
        by_corpus: Counter[str] = Counter()
        if labels_path.is_file():
            for line in labels_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                n += 1
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                by_corpus[str(row.get("corpus") or "?")] += 1
        print(f"  {intent_dir.name}: {n} labels")
        for corpus, c in by_corpus.most_common():
            print(f"    {corpus}: {c}")


def main() -> None:
    print("WMVI status update stats")
    print(f"repo: {REPO_ROOT}")
    for path, label in (
        (TERM_RAW, "Term pipeline raw"),
        (TERM_TRIMMED, "Term pipeline trimmed"),
        (TERM_JSON, "Term pipeline coref"),
        (CLAIMS_JSON, "Claims full"),
    ):
        _summarize_posts_file(path, label)
    _summarize_claims(CLAIMS_JSON)
    _summarize_claims_labelers(CLAIMS_LABELERS)


if __name__ == "__main__":
    main()
