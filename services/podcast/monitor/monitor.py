from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

import feedparser
import requests
from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool
from ingestion.podcast import PodcastEpisodeRow, PodcastShowRow, compute_episode_id, flush_podcast_episodes_batch

load_dotenv()

log = logging.getLogger(__name__)

HTTP_TIMEOUT_S = 25
USER_AGENT = "wmvi-podcast-monitor/1.0"
BATCH_SIZE = 500

def parse_entry_published(entry: Any) -> Optional[datetime]:
    for attr in ("published_parsed", "updated_parsed"):
        st = getattr(entry, attr, None) if hasattr(entry, attr) else entry.get(attr)
        if st:
            return datetime(*st[:6], tzinfo=timezone.utc)
    return None


def pick_download_url(entry: Any) -> Optional[str]:
    enclosures = getattr(entry, "enclosures", None) if hasattr(entry, "enclosures") else entry.get("enclosures")
    if enclosures:
        for enc in enclosures:
            href = enc.get("href") if isinstance(enc, dict) else getattr(enc, "href", None)
            if href:
                return str(href)
    link = getattr(entry, "link", None) if hasattr(entry, "link") else entry.get("link")
    return str(link) if link else None


def normalize_guid(entry: Any) -> Optional[str]:
    guid = None
    for key in ("guid", "id"):
        v = getattr(entry, key, None) if hasattr(entry, key) else entry.get(key)
        if v:
            guid = str(v)
            break
    if guid is None:
        return None
    guid = guid.strip()
    return guid or None


def normalize_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def fetch_shows() -> list[PodcastShowRow]:
    sql = """
        SELECT id, title, rss_url, etag, last_modified
        FROM podcasts.shows
        WHERE rss_url IS NOT NULL AND rss_url <> ''
        ORDER BY id
    """
    with getcursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [
        PodcastShowRow(
            id=int(r[0]),
            title=str(r[1]),
            rss_url=str(r[2]),
            etag=r[3],
            last_modified=r[4],
        )
        for r in rows
    ]

def update_show_fetch_state(
    *,
    cur,
    show_id: int,
    etag: str | None,
    last_modified: str | None,
    http_status: int,
    error: str | None,
) -> None:
    cur.execute(
        """
        UPDATE podcasts.shows
           SET etag = %s,
               last_modified = %s,
               last_fetch_ts = now(),
               last_http_status = %s,
               last_error = %s
         WHERE id = %s
        """,
        (etag, last_modified, http_status, error, show_id),
    )


def fetch_rss(show: PodcastShowRow) -> tuple[int, Optional[str], Optional[str], Optional[str]]:
    headers = {"User-Agent": USER_AGENT}
    if show.etag:
        headers["If-None-Match"] = show.etag
    if show.last_modified:
        headers["If-Modified-Since"] = show.last_modified

    resp = requests.get(show.rss_url or "", headers=headers, timeout=HTTP_TIMEOUT_S)
    etag = resp.headers.get("ETag") or show.etag
    last_modified = resp.headers.get("Last-Modified") or show.last_modified

    if resp.status_code == 304:
        return resp.status_code, None, etag, last_modified

    resp.raise_for_status()
    return resp.status_code, resp.text, etag, last_modified

def fetch_existing_episode_ids(ids: Sequence[str]) -> set[str]:
    if not ids:
        return set()
    with getcursor() as cur:
        cur.execute("SELECT id FROM podcasts.episodes WHERE id = ANY(%s)", (list(ids),))
        return {str(r[0]) for r in cur.fetchall()}


def process_show(show: PodcastShowRow) -> None:
    log.info("Checking show id=%s title=%r", show.id, show.title)

    try:
        status, rss_text, etag, last_modified = fetch_rss(show)

        # Always update fetch state, even for 304, so last_fetch_ts moves.
        with getcursor(commit=True) as cur:
            if status == 304:
                update_show_fetch_state(
                    cur=cur,
                    show_id=show.id,
                    etag=etag,
                    last_modified=last_modified,
                    http_status=status,
                    error=None,
                )
                log.info("No change (304) for show id=%s", show.id)
                return

        feed = feedparser.parse(rss_text or "")
        if getattr(feed, "bozo", False):
            err = getattr(feed, "bozo_exception", None)
            raise RuntimeError(f"feedparser bozo: {err!r}")

        entries = getattr(feed, "entries", []) or []

        candidate_rows: list[PodcastEpisodeRow] = []
        candidate_ids: list[str] = []

        for entry in entries:
            guid = normalize_guid(entry)
            title = normalize_text(entry.get("title") if isinstance(entry, dict) else getattr(entry, "title", None))
            desc = normalize_text(
                entry.get("description") if isinstance(entry, dict) else getattr(entry, "description", None)
            )
            created_at_ts = parse_entry_published(entry)
            download_url = pick_download_url(entry)

            ep_id = compute_episode_id(
                podcast_id=show.id,
                guid=guid,
                download_url=download_url,
                created_at_ts=created_at_ts,
                title=title,
            )

            candidate_ids.append(ep_id)
            candidate_rows.append(
                PodcastEpisodeRow(
                    id=ep_id,
                    guid=guid,
                    title=title,
                    description=desc,
                    created_at_ts=created_at_ts,
                    download_url=download_url,
                    podcast_id=show.id,
                )
            )

        existing: set[str] = set()
        for i in range(0, len(candidate_ids), BATCH_SIZE):
            existing |= fetch_existing_episode_ids(candidate_ids[i : i + BATCH_SIZE])

        new_rows = [r for r in candidate_rows if r.id not in existing]

        with getcursor(commit=True) as cur:
            if new_rows:
                inserted, skipped, _keys = flush_podcast_episodes_batch(new_rows, cur=cur)
                log.info(
                    "Show id=%s: attempted=%d inserted=%d skipped=%d",
                    show.id,
                    len(new_rows),
                    inserted,
                    skipped,
                )
            else:
                log.info("No new episodes for show id=%s", show.id)

            update_show_fetch_state(
                cur=cur,
                show_id=show.id,
                etag=etag,
                last_modified=last_modified,
                http_status=status,
                error=None,
            )

    except Exception as e:
        log.exception("Show id=%s failed: %s", show.id, e)
        try:
            with getcursor(commit=True) as cur:
                update_show_fetch_state(
                    cur=cur,
                    show_id=show.id,
                    etag=show.etag,
                    last_modified=show.last_modified,
                    http_status=0,
                    error=str(e)[:2000],
                )
        except Exception:
            log.exception("Failed to update fetch state for show id=%s", show.id)


def main(*, prod: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    init_pool(prefix="prod" if prod else "dev")
    try:
        shows = fetch_shows()
        log.info("Loaded %d shows", len(shows))
        for show in shows:
            process_show(show)
        log.info("Done. Checked %d shows; exiting.", len(shows))
    finally:
        close_pool()


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m scripts.podcast_monitor")
    ap.add_argument("--prod", action="store_true", help="Run against PROD (default: dev).")
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(prod=bool(args.prod))