"""
high level scraping tools used by both monitor/backfill
expects init_pool and load_dotenv to be instantiated by monitor/backfill
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from db.db import getcursor
from ingestion.youtube.videos import save_videos
from ingestion.youtube.comments import save_comments
from .quota_client import YTQuotaClient, iter_videos

@dataclass
class ScrapeWindowOutcome:
    pages: int = 0
    found_v: int = 0
    stops: dict[str, int] = field(default_factory=dict)

    ins_v: int = 0
    skip_v: int = 0
    ins_c: int = 0
    skip_c: int = 0

    new_vids: list[dict] = field(default_factory=list)
    new_comments: list[dict] = field(default_factory=list)


def scrape_window(
    *,
    qyt: YTQuotaClient,
    term_name: str,
    region: str | None = None,
    published_after: str | datetime,
    published_before: str | datetime | None = None,
    max_pages: int | None = None,
    min_comments_for_scrape: int = 50,
    new_ratio_threshold: float = 0.95,
) -> ScrapeWindowOutcome:

    out = ScrapeWindowOutcome()

    gen = iter_videos(
        qyt,
        term_name=term_name,
        region=region,
        published_after=published_after,
        published_before=published_before,
        max_pages=max_pages,
    )

    for page in gen:
        out.pages += 1

        vids = page.get("videos") or []
        out.found_v += len(vids)

        r = page.get("stopped_reason")
        if r:
            out.stops[r] = out.stops.get(r, 0) + 1

        if not vids:
            continue

        new_vids, ins_v, skip_v, new_comments, ins_c, skip_c = _ingest_page(
            qyt=qyt,
            term_name=term_name,
            vids=vids,
            min_comments_for_scrape=min_comments_for_scrape,
        )

        out.ins_v += ins_v
        out.skip_v += skip_v
        out.ins_c += ins_c
        out.skip_c += skip_c
        out.new_vids.extend(new_vids)
        out.new_comments.extend(new_comments)

        if bool(page.get("is_last_page")):
            break

        frac_new = (ins_v / len(vids)) if vids else 0.0
        if ins_v == 0 or frac_new < new_ratio_threshold:
            out.stops["early_stop_low_new_ratio"] = out.stops.get("early_stop_low_new_ratio", 0) + 1
            break

    return out

def load_search_terms(list_name: str) -> List[tuple[int, str]]:
    """
    Load (term_id, term_name) belonging to SEARCH_TERM_LIST_NAME.
    """
    with getcursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.name
            FROM taxonomy.vaccine_term_subset s
            JOIN taxonomy.vaccine_term_subset_member m
              ON m.subset_id = s.id
            JOIN taxonomy.vaccine_term t
              ON t.id = m.term_id
            WHERE s.name = %s
            ORDER BY t.name
            """,
            (list_name,),
        )
        return [(int(row[0]), row[1]) for row in cur.fetchall()]


def _ingest_page(
    *,
    qyt: YTQuotaClient,
    term_name: str,
    vids: list[dict],              # normalized videos
    min_comments_for_scrape: int,
) -> tuple[list[dict], int, int, list[dict], int, int]:
    ins_v, skip_v, inserted_vid_ids = save_videos(vids, term_name=term_name)
    new_vids = [v for v in vids if v.get("video_id") in inserted_vid_ids]

    ins_c = skip_c = 0
    new_comments: list[dict] = []
    if new_vids:
        for v in new_vids:
            if (v.get("comment_count") or 0) < min_comments_for_scrape:
                continue

            # IMPORTANT: fetch *normalized* comments from integration client
            comments, _ = qyt.fetch_comment_threads_normalized(
                video_id=v["video_id"],
                max_threads=100,
                order="relevance",
            )
            if not comments:
                continue

            ins, skip, inserted_comment_ids = save_comments(comments, term_name=term_name)
            ins_c += ins
            skip_c += skip

            new_comments.extend([c for c in comments if (c.get("video_id"), c.get("comment_id")) in inserted_comment_ids])

    return new_vids, ins_v, skip_v, new_comments, ins_c, skip_c
