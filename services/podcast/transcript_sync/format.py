from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 2
PAYLOAD_FILENAME_TEMPLATE = "podcast_transcripts_{export_date}.jsonl"
SHOWS_PAYLOAD_FILENAME_TEMPLATE = "podcast_shows_{export_date}.jsonl"


@dataclass(frozen=True)
class TranscriptRow:
    id: str
    transcript: str
    transcript_updated_at: datetime

    def to_jsonl_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "transcript": self.transcript,
            "transcript_updated_at": self.transcript_updated_at.isoformat(),
        }

    @classmethod
    def from_jsonl_dict(cls, data: dict[str, Any]) -> "TranscriptRow":
        ts = data["transcript_updated_at"]
        if isinstance(ts, str):
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        elif isinstance(ts, datetime):
            parsed = ts
        else:
            raise TypeError(f"unexpected transcript_updated_at type: {type(ts)!r}")
        return cls(
            id=str(data["id"]),
            transcript=str(data["transcript"]),
            transcript_updated_at=parsed,
        )


@dataclass(frozen=True)
class ShowRow:
    id: int
    title: str
    rss_url: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_fetch_ts: datetime | None = None
    last_http_status: int | None = None
    last_error: str | None = None

    def to_jsonl_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "title": self.title,
            "rss_url": self.rss_url,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "last_http_status": self.last_http_status,
            "last_error": self.last_error,
        }
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
            id=int(data["id"]),
            title=str(data["title"]),
            rss_url=data.get("rss_url"),
            etag=data.get("etag"),
            last_modified=data.get("last_modified"),
            last_fetch_ts=last_fetch_ts,
            last_http_status=data.get("last_http_status"),
            last_error=data.get("last_error"),
        )


@dataclass(frozen=True)
class ExportManifest:
    schema_version: int
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
        since_raw = data.get("since_ts")
        since_ts = None
        if since_raw:
            since_ts = datetime.fromisoformat(str(since_raw).replace("Z", "+00:00"))
        until_raw = data["until_ts"]
        until_ts = datetime.fromisoformat(str(until_raw).replace("Z", "+00:00"))
        shows_payload = data.get("shows_payload")
        return cls(
            schema_version=int(data["schema_version"]),
            export_date=str(data["export_date"]),
            since_ts=since_ts,
            until_ts=until_ts,
            row_count=int(data["row_count"]),
            payload=str(data["payload"]),
            shows_payload=str(shows_payload) if shows_payload else None,
            shows_count=int(data.get("shows_count", 0)),
        )


def payload_filename(export_date: str) -> str:
    return PAYLOAD_FILENAME_TEMPLATE.format(export_date=export_date)


def shows_payload_filename(export_date: str) -> str:
    return SHOWS_PAYLOAD_FILENAME_TEMPLATE.format(export_date=export_date)


def write_jsonl_row(fp, row: TranscriptRow) -> None:
    fp.write(json.dumps(row.to_jsonl_dict(), ensure_ascii=False) + "\n")


def write_show_jsonl_row(fp, row: ShowRow) -> None:
    fp.write(json.dumps(row.to_jsonl_dict(), ensure_ascii=False) + "\n")


def iter_jsonl_rows(path: Path) -> Iterator[TranscriptRow]:
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{line_no}: invalid JSON") from e
            yield TranscriptRow.from_jsonl_dict(data)


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
