from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 4
EPISODES_PAYLOAD_FILENAME_TEMPLATE = "podcast_episodes_{bundle_id}.jsonl"
SHOWS_PAYLOAD_FILENAME_TEMPLATE = "podcast_shows_{bundle_id}.jsonl"

_BUNDLE_ID_FMT = "%Y-%m-%dT%H-%M-%SZ"


def make_bundle_id(dt: datetime) -> str:
    """Filesystem-safe UTC stamp for one export run (unique per run)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(_BUNDLE_ID_FMT)


def parse_bundle_id(bundle_id: str) -> datetime:
    """Parse bundle folder name (timestamped or legacy YYYY-MM-DD)."""
    bundle_id = bundle_id.strip().rstrip("/")
    if len(bundle_id) == 10 and bundle_id[4] == "-" and "T" not in bundle_id:
        return datetime.fromisoformat(bundle_id).replace(tzinfo=timezone.utc)
    parsed = datetime.strptime(bundle_id, _BUNDLE_ID_FMT)
    return parsed.replace(tzinfo=timezone.utc)


def _parse_ts(data: dict[str, Any], key: str) -> datetime:
    ts = data[key]
    if isinstance(ts, str):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if isinstance(ts, datetime):
        return ts
    raise TypeError(f"unexpected {key} type: {type(ts)!r}")


def _parse_optional_ts(data: dict[str, Any], key: str) -> datetime | None:
    raw = data.get(key)
    if raw is None:
        return None
    if isinstance(raw, str):
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if isinstance(raw, datetime):
        return raw
    raise TypeError(f"unexpected {key} type: {type(raw)!r}")


@dataclass(frozen=True)
class EpisodeExportRow:
    show_rss_url: str
    transcript: str
    transcript_updated_at: datetime
    guid: str | None = None
    download_url: str | None = None
    created_at_ts: datetime | None = None
    title: str | None = None
    description: str | None = None
    source_show_id: int | None = None
    source_episode_id: str | None = None

    def to_jsonl_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "show_rss_url": self.show_rss_url,
            "guid": self.guid,
            "download_url": self.download_url,
            "title": self.title,
            "description": self.description,
            "transcript": self.transcript,
            "transcript_updated_at": self.transcript_updated_at.isoformat(),
        }
        if self.created_at_ts is not None:
            out["created_at_ts"] = self.created_at_ts.isoformat()
        else:
            out["created_at_ts"] = None
        if self.source_show_id is not None:
            out["source_show_id"] = self.source_show_id
        if self.source_episode_id is not None:
            out["source_episode_id"] = self.source_episode_id
        return out

    @classmethod
    def from_jsonl_dict(cls, data: dict[str, Any]) -> "EpisodeExportRow":
        return cls(
            show_rss_url=str(data["show_rss_url"]),
            guid=data.get("guid"),
            download_url=data.get("download_url"),
            created_at_ts=_parse_optional_ts(data, "created_at_ts")
            if "created_at_ts" in data
            else None,
            title=data.get("title"),
            description=data.get("description"),
            transcript=str(data["transcript"]),
            transcript_updated_at=_parse_ts(data, "transcript_updated_at"),
            source_show_id=int(data["source_show_id"])
            if data.get("source_show_id") is not None
            else None,
            source_episode_id=str(data["source_episode_id"])
            if data.get("source_episode_id")
            else None,
        )


@dataclass(frozen=True)
class ShowRow:
    rss_url: str
    title: str
    source_show_id: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_fetch_ts: datetime | None = None
    last_http_status: int | None = None
    last_error: str | None = None

    def to_jsonl_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "rss_url": self.rss_url,
            "title": self.title,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "last_http_status": self.last_http_status,
            "last_error": self.last_error,
        }
        if self.source_show_id is not None:
            out["source_show_id"] = self.source_show_id
        if self.last_fetch_ts is not None:
            out["last_fetch_ts"] = self.last_fetch_ts.isoformat()
        else:
            out["last_fetch_ts"] = None
        return out

    @classmethod
    def from_jsonl_dict(cls, data: dict[str, Any]) -> "ShowRow":
        raw_ts = data.get("last_fetch_ts")
        last_fetch_ts = None
        if raw_ts is not None:
            if isinstance(raw_ts, str):
                last_fetch_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            elif isinstance(raw_ts, datetime):
                last_fetch_ts = raw_ts
            else:
                raise TypeError(f"unexpected last_fetch_ts type: {type(raw_ts)!r}")
        return cls(
            rss_url=str(data["rss_url"]),
            title=str(data["title"]),
            source_show_id=int(data["source_show_id"])
            if data.get("source_show_id") is not None
            else None,
            etag=data.get("etag"),
            last_modified=data.get("last_modified"),
            last_fetch_ts=last_fetch_ts,
            last_http_status=data.get("last_http_status"),
            last_error=data.get("last_error"),
        )


@dataclass(frozen=True)
class ExportManifest:
    schema_version: int
    bundle_id: str
    export_date: str
    since_ts: datetime | None
    until_ts: datetime
    row_count: int
    payload: str
    shows_payload: str | None = None
    shows_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "export_date": self.export_date,
            "since_ts": self.since_ts.isoformat() if self.since_ts else None,
            "until_ts": self.until_ts.isoformat(),
            "row_count": self.row_count,
            "payload": self.payload,
            "shows_payload": self.shows_payload,
            "shows_count": self.shows_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExportManifest":
        version = int(data["schema_version"])
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported export bundle schema_version={version} "
                f"(expected {SCHEMA_VERSION})"
            )
        since_raw = data.get("since_ts")
        since_ts = None
        if since_raw:
            since_ts = datetime.fromisoformat(str(since_raw).replace("Z", "+00:00"))
        until_raw = data["until_ts"]
        until_ts = datetime.fromisoformat(str(until_raw).replace("Z", "+00:00"))
        shows_payload = data.get("shows_payload")
        export_date = str(data.get("export_date") or until_ts.date().isoformat())
        bundle_id = data.get("bundle_id") or export_date
        return cls(
            schema_version=version,
            bundle_id=str(bundle_id),
            export_date=export_date,
            since_ts=since_ts,
            until_ts=until_ts,
            row_count=int(data["row_count"]),
            payload=str(data["payload"]),
            shows_payload=str(shows_payload) if shows_payload else None,
            shows_count=int(data.get("shows_count", 0)),
        )


def episodes_payload_filename(bundle_id: str) -> str:
    return EPISODES_PAYLOAD_FILENAME_TEMPLATE.format(bundle_id=bundle_id)


def shows_payload_filename(bundle_id: str) -> str:
    return SHOWS_PAYLOAD_FILENAME_TEMPLATE.format(bundle_id=bundle_id)


def write_episode_jsonl_row(fp, row: EpisodeExportRow) -> None:
    fp.write(json.dumps(row.to_jsonl_dict(), ensure_ascii=False) + "\n")


def write_show_jsonl_row(fp, row: ShowRow) -> None:
    fp.write(json.dumps(row.to_jsonl_dict(), ensure_ascii=False) + "\n")


def iter_episode_jsonl_rows(path: Path) -> Iterator[EpisodeExportRow]:
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{line_no}: invalid JSON") from e
            yield EpisodeExportRow.from_jsonl_dict(data)


def iter_show_jsonl_rows(path: Path) -> Iterator[ShowRow]:
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{line_no}: invalid JSON") from e
            yield ShowRow.from_jsonl_dict(data)


def write_manifest(path: Path, manifest: ExportManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def read_manifest(path: Path) -> ExportManifest:
    data = json.loads(path.read_text(encoding="utf-8"))
    return ExportManifest.from_dict(data)


def should_apply_import(
    *,
    prod_transcript: str | None,
    prod_updated_at: datetime | None,
    incoming_updated_at: datetime,
) -> bool:
    """Return True when incoming row should overwrite prod (newer-wins policy)."""
    if prod_transcript is None:
        return True
    if prod_updated_at is None:
        return True
    return incoming_updated_at > prod_updated_at
