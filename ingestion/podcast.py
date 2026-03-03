from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib


from db.db import getcursor
from ingestion.ingestion import flush_rows
from ingestion.row_model import InsertableRow

"""
Notes / current design:

- podcasts.shows and podcasts.episodes are populated by the monitor / scrapers.
- podcast episodes are NOT entered into sm.post_registry here.
  The transcription service should enter them into post_registry once transcription exists.
  Then later is_en will be set by the label_en service
"""


# -------------------------
# episode id stuff
# -------------------------
# id format is: "ep_<podcast_id>_<md5(token)>"
#   where token is built from the first of these that is present: guid > download_url > created_at_ts+title

def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def compute_episode_id(
    *,
    podcast_id: int,
    guid: str | None,
    download_url: str | None,
    created_at_ts: datetime | None,
    title: str | None,
) -> str:
    guid_s = (guid or "").strip()
    url_s = (download_url or "").strip()

    if guid_s:
        token = guid_s
    elif url_s:
        token = url_s
    else:
        created_s = created_at_ts.isoformat() if created_at_ts else ""
        title_s = title or ""
        token = f"{created_s}:{title_s}"

    material = f"{podcast_id}:{token}"
    return f"ep_{podcast_id}_{_md5_hex(material)}"
# -------------------------
# Row models
# -------------------------


@dataclass(frozen=True, slots=True)
class PodcastShowRow(InsertableRow):
    TABLE = "podcasts.shows"
    PK = ("id",)  # TODO: This will cause issues if we ever actually try to insert these objects;
                  #  since the db table generates the id values and needs special sql to specify value
                  #  furthermore, we can't just remove it from this class since InsertableRow needs PK
                  #  This is fine for now but we'll have to figure it out when we start inserting shows

    id: int
    title: str
    rss_url: str | None = None

    # monitor state (usually None on insert; populated on read)
    etag: str | None = None
    last_modified: str | None = None  # http header value; not datetime obj
    last_fetch_ts: datetime | None = None
    last_http_status: int | None = None
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class PodcastEpisodeRow(InsertableRow):
    TABLE = "podcasts.episodes"
    PK = ("id",)

    id: str
    podcast_id: int
    guid: str | None = None
    title: str | None = None
    description: str | None = None
    created_at_ts: datetime | None = None
    download_url: str | None = None


@dataclass(frozen=True, slots=True)
class PodcastTranscriptSegmentRow(InsertableRow):
    TABLE = "podcasts.transcript_segments"
    PK = ("episode_id", "seg_idx")

    episode_id: str
    seg_idx: int
    start_s: float
    end_s: float
    text: str | None = None


# -------------------------
# Flush functions
# -------------------------

# Note that none of these link to a scrape job
# There's no point because eps link to a podcast show and that's better and more specific
# than any scrape job

def flush_podcast_shows_batch(
    rows: list[PodcastShowRow],
    *,
    cur=None,
) -> tuple[int, int, set[tuple[str, ...]]]:
    if not rows:
        return 0, 0, set()
    if not isinstance(rows[0], PodcastShowRow):
        raise TypeError("rows must be list of PodcastShowRow")
    return flush_rows(rows=rows, cur=cur)


def flush_podcast_episodes_batch(
    rows: list[PodcastEpisodeRow],
    *,
    cur=None,
) -> tuple[int, int, set[tuple[str, ...]]]:
    if not rows:
        return 0, 0, set()
    if not isinstance(rows[0], PodcastEpisodeRow):
        raise TypeError("rows must be list of PodcastEpisodeRow")
    return flush_rows(rows=rows, cur=cur)


def flush_podcast_transcript_segments_batch(
    rows: list[PodcastTranscriptSegmentRow],
    *,
    cur=None,
    replace: bool = False,
) -> tuple[int, int, set[tuple[str, ...]]]:
    if not rows:
        return 0, 0, set()
    if not isinstance(rows[0], PodcastTranscriptSegmentRow):
        raise TypeError("rows must be list of PodcastTranscriptSegmentRow")

    if not replace:
        return flush_rows(rows=rows, cur=cur)

    def _run(cur2):
        episode_ids = sorted({r.episode_id for r in rows})
        cur2.execute(
            "DELETE FROM podcasts.transcript_segments WHERE episode_id = ANY(%s)",
            (episode_ids,),
        )
        return flush_rows(rows=rows, cur=cur2)

    if cur is None:
        with getcursor(commit=True) as cur2:
            return _run(cur2)
    return _run(cur)