from __future__ import annotations

import base64
import json
from decimal import Decimal
import datetime as dt
import hashlib
import hmac
import logging
import math
import os
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from db.db import init_pool, getcursor, close_pool


load_dotenv()


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ----------------------------------------------------------------------
# storage backends
# ----------------------------------------------------------------------

class StorageBackend(ABC):
    @abstractmethod
    def is_accessible(self) -> tuple[bool, Optional[str]]:
        raise NotImplementedError

    @abstractmethod
    def write_text(self, rel_path: str, text: str) -> None:
        raise NotImplementedError


class LocalFileStorage(StorageBackend):
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            test_path = self.base_dir / ".write_test"
            test_path.write_text("ok", encoding="utf-8")
            test_path.unlink()
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        dest = self.base_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")


class AzureBlobStorage(StorageBackend):
    # Pinned Azure Storage REST API version for Shared Key requests.
    # Deliberately fixed to a tested version; change only with re-testing.
    API_VERSION = "2023-11-03"

    def __init__(
        self,
        account: str,
        account_key: str,
        container: str,
    ) -> None:
        self.account = account.strip()
        self.account_key = account_key.strip()
        self.container = container.strip()

    @classmethod
    def from_env(cls) -> "AzureBlobStorage":
        return cls(
            account=os.environ["AZURE_STORAGE_ACCOUNT"],
            account_key=os.environ["AZURE_STORAGE_KEY"],
            container=os.environ["AZURE_STORAGE_CONTAINER"],
        )

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            url = self._container_url(restype="container")
            req = self._build_request(url=url, method="GET")
            with urllib.request.urlopen(req, timeout=30) as resp:
                if 200 <= resp.status < 300:
                    return True, None
                return False, f"Unexpected HTTP status: {resp.status}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        blob_name = self._full_blob_name(rel_path)
        url = self._blob_url(blob_name)

        body = text.encode("utf-8")
        content_length = str(len(body))
        content_type = "application/json; charset=utf-8"

        req = self._build_request(
            url=url,
            method="PUT",
            content_length=content_length,
            content_type=content_type,
            extra_headers={"x-ms-blob-type": "BlockBlob"},
            body=body,
        )

        with urllib.request.urlopen(req, timeout=60) as resp:
            if not (200 <= resp.status < 300):
                raise RuntimeError(f"Blob upload failed with HTTP {resp.status}")

    def _full_blob_name(self, rel_path: str) -> str:
        clean = rel_path.lstrip("/")
        return clean

    def _container_url(self, *, restype: str) -> str:
        return (
            f"https://{self.account}.blob.core.windows.net/"
            f"{self.container}?restype={restype}"
        )

    def _blob_url(self, blob_name: str) -> str:
        return (
            f"https://{self.account}.blob.core.windows.net/"
            f"{self.container}/{blob_name}"
        )

    def _build_request(
        self,
        *,
        url: str,
        method: str,
        content_length: str = "",
        content_type: str = "",
        extra_headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> urllib.request.Request:
        x_ms_date = dt.datetime.now(dt.timezone.utc).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )

        headers = {
            "x-ms-date": x_ms_date,
            "x-ms-version": self.API_VERSION,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if extra_headers:
            headers.update(extra_headers)

        auth = self._build_auth_header(
            method=method,
            url=url,
            content_length=content_length,
            content_type=content_type,
            headers=headers,
        )

        req = urllib.request.Request(url, data=body, method=method)
        for k, v in headers.items():
            req.add_header(k, v)
        req.add_header("Authorization", auth)
        if content_length:
            req.add_header("Content-Length", content_length)
        return req

    def _build_auth_header(
        self,
        *,
        method: str,
        url: str,
        content_length: str,
        content_type: str,
        headers: dict[str, str],
    ) -> str:
        canonicalized_headers = self._canonicalized_headers(headers)
        canonicalized_resource = self._canonicalized_resource(url)

        string_to_sign = (
            f"{method}\n"
            f"\n"
            f"\n"
            f"{content_length if content_length and content_length != '0' else ''}\n"
            f"\n"
            f"{content_type}\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"{canonicalized_headers}"
            f"{canonicalized_resource}"
        )

        key_bytes = base64.b64decode(self.account_key)
        sig = base64.b64encode(
            hmac.new(key_bytes, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")

        return f"SharedKey {self.account}:{sig}"

    def _canonicalized_headers(self, headers: dict[str, str]) -> str:
        x_ms_headers = {
            k.lower(): " ".join(v.strip().split())
            for k, v in headers.items()
            if k.lower().startswith("x-ms-")
        }
        return "".join(f"{k}:{x_ms_headers[k]}\n" for k in sorted(x_ms_headers))

    def _canonicalized_resource(self, url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        out = f"/{self.account}{parsed.path}"

        if parsed.query:
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            for key in sorted(k.lower() for k in params):
                vals = params[key]
                out += f"\n{key}:{','.join(sorted(vals))}"
        return out


def get_storage_backend() -> StorageBackend:
    kind = os.environ.get("SUMMARY_STORAGE_KIND", "local").strip().lower()

    if kind == "local":
        base_dir = os.environ.get("SUMMARY_LOCAL_DIR", "./tmp_summary_exports")
        return LocalFileStorage(base_dir)

    if kind == "azure":
        return AzureBlobStorage.from_env()

    raise ValueError(f"Unsupported SUMMARY_STORAGE_KIND: {kind!r}")


# ----------------------------------------------------------------------
# query helpers
# ----------------------------------------------------------------------

def run_query(sql: str) -> list[dict[str, Any]]:
    with getcursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def to_json(data: Any) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=_json_default,
    )


def timed_query(name: str, sql: str) -> list[dict[str, Any]]:
    q0 = dt.datetime.now(dt.timezone.utc)
    rows = run_query(sql)
    q1 = dt.datetime.now(dt.timezone.utc)
    logging.info(
        "Query done: %s (%d rows, %.3fs)",
        name,
        len(rows),
        (q1 - q0).total_seconds(),
    )
    return rows


# ----------------------------------------------------------------------
# SQL
# ----------------------------------------------------------------------

SQL_INGESTION_DAILY = """
WITH src AS (
    SELECT 'reddit_submission'::text AS platform, date_entered FROM sm.reddit_submission
    UNION ALL
    SELECT 'reddit_comment'::text AS platform, date_entered FROM sm.reddit_comment
    UNION ALL
    SELECT 'telegram_post'::text AS platform, date_entered FROM sm.telegram_post
    UNION ALL
    SELECT 'youtube_video'::text AS platform, date_entered FROM youtube.video
    UNION ALL
    SELECT 'youtube_comment'::text AS platform, date_entered FROM youtube.comment
    UNION ALL
    SELECT 'podcast_episode'::text AS platform, date_entered FROM podcasts.episodes
    UNION ALL
    SELECT 'news_article'::text AS platform, date_entered FROM news.article
)
SELECT
    date_trunc('day', date_entered)::date AS day,
    platform,
    COUNT(*)::bigint AS n
FROM src
WHERE date_entered >= now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;
"""

SQL_IS_EN_COUNTS_7D = """
SELECT
    date_trunc('day', date_entered)::date AS day,
    platform,
    COUNT(*) FILTER (WHERE is_en IS TRUE)::bigint  AS is_en_true,
    COUNT(*) FILTER (WHERE is_en IS FALSE)::bigint AS is_en_false,
    COUNT(*) FILTER (WHERE is_en IS NULL)::bigint  AS is_en_null
FROM sm.posts_all
WHERE date_entered >= now() - interval '7 days'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;
"""

SQL_TERM_MATCHES_PER_DAY = """
SELECT
    date_trunc('day', matched_at)::date AS day,
    COUNT(*)::bigint AS matches
FROM matches.post_term_hit
WHERE matched_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_PODCAST_TRANSCRIPTIONS_PER_DAY = """
SELECT
    date_trunc('day', transcript_updated_at)::date AS day,
    COUNT(*)::bigint AS transcriptions
FROM podcasts.episodes
WHERE transcript IS NOT NULL
  AND transcript_updated_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_PODCAST_TRANSCRIPTION_SUMMARY = """
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
    COUNT(*) FILTER (
        WHERE transcription_started_at IS NOT NULL
          AND transcript IS NULL
    )::bigint AS in_progress
FROM podcasts.episodes;
"""

SQL_YOUTUBE_TRANSCRIPTIONS_PER_DAY = """
SELECT
    date_trunc('day', transcript_updated_at)::date AS day,
    COUNT(*)::bigint AS transcriptions
FROM youtube.video
WHERE transcript IS NOT NULL
  AND transcript_updated_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_YOUTUBE_TRANSCRIPTION_SUMMARY = """
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
    COUNT(*) FILTER (
        WHERE transcription_started_at IS NOT NULL
          AND transcript IS NULL
    )::bigint AS in_progress
FROM youtube.video;
"""


# ----------------------------------------------------------------------
# postprocessing
# ----------------------------------------------------------------------

def add_transcription_forecast(
    summary_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not summary_rows:
        return summary_rows

    row = dict(summary_rows[0])

    total = int(row.get("total") or 0)
    completed = int(row.get("completed") or 0)
    in_progress = int(row.get("in_progress") or 0)
    remaining = max(total - completed, 0)

    pct = round((completed / total) * 100.0, 2) if total > 0 else None

    daily_total = 0
    for drow in daily_rows:
        n = int(drow.get("transcriptions") or 0)
        daily_total += n

    # Use 30-day average across all days, not only active days.
    avg_per_day_30 = daily_total / 30.0 if daily_total > 0 else 0.0

    est_finish_date = None
    est_days_remaining = None
    if remaining == 0:
        est_days_remaining = 0
        est_finish_date = dt.date.today().isoformat()
    elif avg_per_day_30 > 0:
        est_days_remaining = math.ceil(remaining / avg_per_day_30)
        est_finish_date = (
            dt.date.today() + dt.timedelta(days=est_days_remaining)
        ).isoformat()

    return [
        {
            "total": total,
            "completed": completed,
            "in_progress": in_progress,
            "percent_completion": pct,
            "avg_completed_per_day_30d": round(avg_per_day_30, 4) if avg_per_day_30 > 0 else 0.0,
            "estimated_days_remaining": est_days_remaining,
            "estimated_finish_date": est_finish_date,
        }
    ]


# ----------------------------------------------------------------------
# export helpers
# ----------------------------------------------------------------------

def build_dashboard_summary() -> dict[str, Any]:
    logging.info("Running summary queries...")
    t0 = dt.datetime.now(dt.timezone.utc)

    ingestion_daily = timed_query("ingestion_daily", SQL_INGESTION_DAILY)
    is_en_counts_7d = timed_query("is_en_counts_7d", SQL_IS_EN_COUNTS_7D)
    term_matches_per_day = timed_query("term_matches_per_day", SQL_TERM_MATCHES_PER_DAY)

    podcast_daily = timed_query(
        "podcast_transcriptions_per_day",
        SQL_PODCAST_TRANSCRIPTIONS_PER_DAY,
    )
    podcast_summary_raw = timed_query(
        "podcast_transcription_summary",
        SQL_PODCAST_TRANSCRIPTION_SUMMARY,
    )
    podcast_summary = add_transcription_forecast(podcast_summary_raw, podcast_daily)

    youtube_daily = timed_query(
        "youtube_transcriptions_per_day",
        SQL_YOUTUBE_TRANSCRIPTIONS_PER_DAY,
    )
    youtube_summary_raw = timed_query(
        "youtube_transcription_summary",
        SQL_YOUTUBE_TRANSCRIPTION_SUMMARY,
    )
    youtube_summary = add_transcription_forecast(youtube_summary_raw, youtube_daily)

    t1 = dt.datetime.now(dt.timezone.utc)
    logging.info("All queries complete (%.3fs total)", (t1 - t0).total_seconds())

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "ingestion_daily": ingestion_daily,
        "is_en_counts_7d": is_en_counts_7d,
        "term_matches_per_day": term_matches_per_day,
        "podcast_transcriptions_per_day": podcast_daily,
        "podcast_transcription_summary": podcast_summary,
        "youtube_transcriptions_per_day": youtube_daily,
        "youtube_transcription_summary": youtube_summary,
    }


def build_and_export_all(storage: StorageBackend) -> None:
    t0 = dt.datetime.now(dt.timezone.utc)

    dashboard_summary = build_dashboard_summary()

    logging.info("Starting export...")
    w0 = dt.datetime.now(dt.timezone.utc)
    storage.write_text("dashboard_summary.json", to_json(dashboard_summary))
    w1 = dt.datetime.now(dt.timezone.utc)

    logging.info("Wrote dashboard_summary.json (%.3fs)", (w1 - w0).total_seconds())
    logging.info("build_and_export_all complete (%.3fs total)", (w1 - t0).total_seconds())


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

def main(prod: bool = False) -> None:
    _setup_logging()

    if prod:
        init_pool(prefix="prod")
        logging.info("Initialized DB pool with PROD prefix.")
    else:
        init_pool(prefix="dev")
        logging.info("Initialized DB pool with DEV prefix.")

    storage = get_storage_backend()
    ok, reason = storage.is_accessible()
    if not ok:
        raise RuntimeError(f"Storage not accessible: {reason}")

    logging.info("Storage backend accessible.")

    try:
        build_and_export_all(storage)
        logging.info("Summaries exported successfully.")
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, shutting down summarizer...")
        raise
    finally:
        close_pool()
        logging.info("DB pool closed; exiting.")


if __name__ == "__main__":
    main(prod=False)