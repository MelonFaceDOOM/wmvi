"""
Sample podcast episodes (+ transcripts) into a single text file.

Usage:
  python -m scripts.sample_podcasts
  python -m scripts.sample_podcasts --prod
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from db.db import init_pool, close_pool, getcursor

# ----------------------------
# GLOBAL CONFIG
# ----------------------------

OUT_PATH = Path("podcast_samples.txt")

NUM_EPISODES = 10                       # how many episodes to sample total
LOOKBACK_DAYS: Optional[int] = 60       # None = all time
TRANSCRIPT_CHARS: Optional[int] = 3000  # None = full transcript

# If you want only episodes with transcripts, set this True.
ONLY_WITH_TRANSCRIPT = True


# ----------------------------
# Helpers
# ----------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_iso(dt: Optional[datetime]) -> str:
    dt2 = _ensure_utc(dt)
    return dt2.isoformat() if dt2 else "<none>"


def _trim_text(s: Optional[str], limit: Optional[int]) -> str:
    if not s:
        return ""
    if limit is None:
        return s
    if limit <= 0:
        return ""
    if len(s) <= limit:
        return s
    return s[:limit] + "\n\n[...TRUNCATED...]"


def _write_samples(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("podcast transcript sample")
    lines.append(f"generated_at_utc: {_utc_now().isoformat()}")
    lines.append(f"num_episodes: {NUM_EPISODES}")
    lines.append(f"lookback_days: {LOOKBACK_DAYS}")
    lines.append(f"transcript_chars: {TRANSCRIPT_CHARS}")
    lines.append(f"only_with_transcript: {ONLY_WITH_TRANSCRIPT}")
    lines.append("")
    lines.append("")

    for r in rows:
        lines.append(f"show_id: {r.get('show_id', '<none>')}")
        lines.append(f"show_title: {r.get('show_title', '<none>')}")
        lines.append(f"episode_id: {r.get('episode_id', '<none>')}")
        lines.append(f"episode_title: {r.get('episode_title', '<none>')}")
        lines.append(f"guid: {r.get('guid', '<none>')}")
        lines.append(f"created_at_ts_utc: {r.get('created_at_ts_utc', '<none>')}")
        lines.append(f"date_entered_utc: {r.get('date_entered_utc', '<none>')}")
        lines.append(f"download_url: {r.get('download_url', '<none>')}")
        lines.append(f"transcript_updated_at_utc: {r.get('transcript_updated_at_utc', '<none>')}")
        lines.append(f"is_en: {r.get('is_en', '<none>')}")
        lines.append(f"transcript_len: {r.get('transcript_len', 0)}")
        lines.append("")
        lines.append("transcript:")
        lines.append(_trim_text(r.get("transcript"), TRANSCRIPT_CHARS))
        lines.append("")
        lines.append("-" * 80)
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


# ----------------------------
# DB query
# ----------------------------

def _fetch_samples() -> list[dict]:
    where = []
    params: list[object] = []

    if LOOKBACK_DAYS is not None:
        cutoff = _utc_now() - timedelta(days=int(LOOKBACK_DAYS))
        # created_at_ts may be NULL; fall back to date_entered
        where.append("e.transcript_updated_at >= %s")
        params.append(cutoff)

    if ONLY_WITH_TRANSCRIPT:
        where.append("e.transcript IS NOT NULL AND e.transcript <> ''")

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    # ORDER BY random() can be slow on huge tables. If it is, switch to another sampler.
    sql = f"""
        SELECT
            s.id AS show_id,
            s.title AS show_title,
            e.id AS episode_id,
            e.title AS episode_title,
            e.guid AS guid,
            e.created_at_ts AS created_at_ts,
            e.date_entered AS date_entered,
            e.download_url AS download_url,
            e.transcript_updated_at AS transcript_updated_at,
            e.is_en AS is_en,
            e.transcript AS transcript
        FROM podcasts.episodes e
        JOIN podcasts.shows s
          ON s.id = e.podcast_id
        {where_sql}
        ORDER BY random()
        LIMIT %s
    """
    params.append(int(NUM_EPISODES))

    out: list[dict] = []
    with getcursor() as cur:
        cur.execute(sql, tuple(params))
        for (
            show_id,
            show_title,
            episode_id,
            episode_title,
            guid,
            created_at_ts,
            date_entered,
            download_url,
            transcript_updated_at,
            is_en,
            transcript,
        ) in cur.fetchall():
            created_dt = _ensure_utc(created_at_ts)
            entered_dt = _ensure_utc(date_entered)
            upd_dt = _ensure_utc(transcript_updated_at)

            tr = transcript or ""

            out.append(
                {
                    "show_id": show_id,
                    "show_title": show_title,
                    "episode_id": episode_id,
                    "episode_title": episode_title,
                    "guid": guid or "<none>",
                    "created_at_ts_utc": _safe_iso(created_dt),
                    "date_entered_utc": _safe_iso(entered_dt),
                    "download_url": download_url or "<none>",
                    "transcript_updated_at_utc": _safe_iso(upd_dt),
                    "is_en": is_en,
                    "transcript_len": len(tr),
                    "transcript": tr,
                }
            )

    return out


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m scripts.sample_podcasts")
    ap.add_argument("--prod", action="store_true")
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")
    try:
        rows = _fetch_samples()
        _write_samples(rows, OUT_PATH)
        print(f"[ok] wrote {len(rows)} episodes to {OUT_PATH.resolve()}")
    finally:
        close_pool()


if __name__ == "__main__":
    main()