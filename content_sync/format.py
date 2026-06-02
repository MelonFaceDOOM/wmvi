from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
MANIFEST_NAME = "manifest.json"

_BUNDLE_ID_FMT = "%Y-%m-%dT%H-%M-%SZ"

PLATFORM_YOUTUBE_VIDEO = "youtube_video"
PLATFORM_YOUTUBE_COMMENT = "youtube_comment"
PLATFORM_PODCAST_EPISODE = "podcast_episode"

SIDECAR_PODCAST_SHOWS = "podcast_shows"
SIDECAR_YOUTUBE_SEGMENTS = "youtube_segments"

DEFAULT_PLATFORMS = (
    PLATFORM_YOUTUBE_VIDEO,
    PLATFORM_YOUTUBE_COMMENT,
    PLATFORM_PODCAST_EPISODE,
)


def platform_filename(platform: str) -> str:
    return f"{platform}.jsonl"


def sidecar_filename(name: str, bundle_id: str) -> str:
    return f"{name}_{bundle_id}.jsonl"


def make_bundle_id(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(_BUNDLE_ID_FMT)


def parse_bundle_id(bundle_id: str) -> datetime:
    bundle_id = bundle_id.strip().rstrip("/")
    if len(bundle_id) == 10 and bundle_id[4] == "-" and "T" not in bundle_id:
        return datetime.fromisoformat(bundle_id).replace(tzinfo=timezone.utc)
    parsed = datetime.strptime(bundle_id, _BUNDLE_ID_FMT)
    return parsed.replace(tzinfo=timezone.utc)


def bundle_sort_key(bundle_id: str) -> tuple:
    return (parse_bundle_id(bundle_id), bundle_id)


@dataclass
class PlatformFileInfo:
    row_count: int
    file: str


@dataclass
class ContentSyncManifest:
    schema_version: int
    bundle_id: str
    since_ts: datetime | None
    until_ts: datetime
    platforms: dict[str, PlatformFileInfo] = field(default_factory=dict)
    sidecars: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "since_ts": self.since_ts.isoformat() if self.since_ts else None,
            "until_ts": self.until_ts.isoformat(),
            "platforms": {
                name: {"row_count": info.row_count, "file": info.file}
                for name, info in self.platforms.items()
            },
            "sidecars": dict(self.sidecars),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContentSyncManifest":
        version = int(data["schema_version"])
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported content sync schema_version={version} "
                f"(expected {SCHEMA_VERSION})"
            )
        since_raw = data.get("since_ts")
        since_ts = None
        if since_raw:
            since_ts = datetime.fromisoformat(str(since_raw).replace("Z", "+00:00"))
        until_ts = datetime.fromisoformat(
            str(data["until_ts"]).replace("Z", "+00:00")
        )
        platforms: dict[str, PlatformFileInfo] = {}
        for name, pinfo in (data.get("platforms") or {}).items():
            platforms[str(name)] = PlatformFileInfo(
                row_count=int(pinfo["row_count"]),
                file=str(pinfo["file"]),
            )
        sidecars = {str(k): str(v) for k, v in (data.get("sidecars") or {}).items()}
        return cls(
            schema_version=version,
            bundle_id=str(data["bundle_id"]),
            since_ts=since_ts,
            until_ts=until_ts,
            platforms=platforms,
            sidecars=sidecars,
        )


def write_manifest(path: Path, manifest: ContentSyncManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def read_manifest(path: Path) -> ContentSyncManifest:
    data = json.loads(path.read_text(encoding="utf-8"))
    return ContentSyncManifest.from_dict(data)


def write_jsonl_row(fp, row: dict[str, Any]) -> None:
    fp.write(json.dumps(row, ensure_ascii=False, default=_json_default) + "\n")


def iter_jsonl_rows(path: Path) -> Any:
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{line_no}: invalid JSON") from e


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
