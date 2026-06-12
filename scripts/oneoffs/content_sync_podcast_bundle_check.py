"""Diagnose content_sync podcast_episode bundles (no DB writes).

Downloads a bundle from nitwitch or reads a local export dir, parses
podcast_episode.jsonl + podcast_shows sidecar, and mirrors import resolve
logic. With --check-db, compares resolved episode ids against the target DB
to explain why import may report transcripts_updated=0.

Run from repo root:

  # Latest bundle from nitwitch (set CONTENT_SYNC_IMPORT_STORAGE_KIND=nitwitch or --source nitwitch)
  python -m scripts.oneoffs.content_sync_podcast_bundle_check --source nitwitch

  # Specific bundle, check Azure/prod DB
  python -m scripts.oneoffs.content_sync_podcast_bundle_check \\
    --bundle 2026-06-12T06-15-17Z --source nitwitch --check-db --prod

  # Local export on gpu-pc
  python -m scripts.oneoffs.content_sync_podcast_bundle_check \\
    --bundle 2026-06-12T06-15-17Z --source local
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from content_sync.format import (  # noqa: E402
    MANIFEST_NAME,
    PLATFORM_PODCAST_EPISODE,
    SIDECAR_PODCAST_SHOWS,
    bundle_sort_key,
    iter_jsonl_rows,
    read_manifest,
)
from services.podcast.transcript_sync.bundle_import import (  # noqa: E402
    ImportStats,
    _resolve_rows,
)
from services.podcast.transcript_sync.db_import import TranscriptApplyRow  # noqa: E402
from services.podcast.transcript_sync.format import (  # noqa: E402
    EpisodeExportRow,
    ShowRow,
    should_apply_import,
)
from services.podcast.transcript_sync.resolve import (  # noqa: E402
    has_transcript_match_key,
    target_episode_id,
)
from services.podcast.transcript_sync.rss_url import normalize_rss_url  # noqa: E402
from content_sync import db_sync_state  # noqa: E402
from storage.content_sync import (  # noqa: E402
    download_bundle,
    list_export_bundle_ids,
    list_pending_bundle_ids,
)
from storage.nitwitch_paths import CONTENT_SYNC_SUBDIR  # noqa: E402


@dataclass
class DbCheckStats:
    transcript_rows: int = 0
    episode_missing: int = 0
    would_update: int = 0
    would_skip_newer: int = 0
    would_skip_same_empty: int = 0
    sample_missing: list[str] = field(default_factory=list)
    sample_skip_newer: list[str] = field(default_factory=list)


def _episode_rows_from_jsonl(path: Path) -> list[EpisodeExportRow]:
    rows: list[EpisodeExportRow] = []
    for data in iter_jsonl_rows(path):
        payload = {
            k: v
            for k, v in data.items()
            if k not in ("platform", "key1", "key2")
        }
        rows.append(EpisodeExportRow.from_jsonl_dict(payload))
    return rows


def _show_rows_from_jsonl(path: Path) -> list[ShowRow]:
    return [ShowRow.from_jsonl_dict(data) for data in iter_jsonl_rows(path)]


def _fetch_rss_to_podcast_id(cur, rss_urls: list[str]) -> dict[str, int]:
    canonical = []
    for url in rss_urls:
        c = normalize_rss_url(url)
        if c:
            canonical.append(c)
    if not canonical:
        return {}
    cur.execute(
        """
        SELECT id, rss_url
        FROM podcasts.shows
        WHERE rss_url = ANY(%s)
        """,
        (canonical,),
    )
    return {str(rss): int(show_id) for show_id, rss in cur.fetchall()}


def _analyze_bundle_rows(episode_rows: list[EpisodeExportRow]) -> None:
    print("\n=== Bundle episode rows ===")
    print(f"  total rows: {len(episode_rows)}")
    empty_transcript = sum(
        1 for r in episode_rows if not (r.transcript or "").strip()
    )
    no_match_key = sum(1 for r in episode_rows if not has_transcript_match_key(r))
    print(f"  empty transcript: {empty_transcript}")
    print(f"  missing guid+download_url (no transcript key): {no_match_key}")
    if episode_rows:
        lens = sorted(len((r.transcript or "")) for r in episode_rows)
        print(
            f"  transcript length: min={lens[0]} median={lens[len(lens)//2]} "
            f"max={lens[-1]}"
        )
        latest = max(r.transcript_updated_at for r in episode_rows)
        print(f"  latest transcript_updated_at in bundle: {latest.isoformat()}")


def _simulate_resolve(
    episode_rows: list[EpisodeExportRow],
    show_rows: list[ShowRow],
    rss_to_podcast_id: dict[str, int],
    *,
    label: str,
) -> tuple[ImportStats, list[TranscriptApplyRow]]:
    to_insert, transcripts, stats = _resolve_rows(episode_rows, rss_to_podcast_id)
    print(f"\n=== Resolve ({label}) ===")
    print(f"  episodes_seen: {stats.episodes_seen}")
    print(f"  shows in map: {len(rss_to_podcast_id)}")
    print(f"  would insert episode shells: {len(to_insert)}")
    print(f"  transcript apply rows: {len(transcripts)}")
    print(f"  skipped_no_show_rss: {stats.skipped_no_show_rss}")
    print(f"  skipped_show_not_in_map: {stats.skipped_show_not_in_map}")
    print(f"  skipped_no_transcript_key: {stats.skipped_no_transcript_key}")
    print(f"  skipped_id_collision: {stats.skipped_id_collision}")
    return stats, transcripts


def _check_db_transcripts(
    cur,
    transcripts: list[TranscriptApplyRow],
    *,
    sample_limit: int,
) -> DbCheckStats:
    stats = DbCheckStats(transcript_rows=len(transcripts))
    if not transcripts:
        return stats

    ids = [t.episode_id for t in transcripts]
    cur.execute(
        """
        SELECT id, transcript, transcript_updated_at
        FROM podcasts.episodes
        WHERE id = ANY(%s)
        """,
        (ids,),
    )
    by_id = {
        str(row[0]): (row[1], row[2])
        for row in cur.fetchall()
    }

    for t in transcripts:
        ep_id = t.episode_id
        existing = by_id.get(ep_id)
        if existing is None:
            stats.episode_missing += 1
            if len(stats.sample_missing) < sample_limit:
                stats.sample_missing.append(ep_id)
            continue

        prod_transcript, prod_updated = existing
        if should_apply_import(
            prod_transcript=prod_transcript,
            prod_updated_at=prod_updated,
            incoming_updated_at=t.transcript_updated_at,
        ):
            stats.would_update += 1
        elif prod_transcript and str(prod_transcript).strip():
            stats.would_skip_newer += 1
            if len(stats.sample_skip_newer) < sample_limit:
                stats.sample_skip_newer.append(ep_id)
        else:
            stats.would_skip_same_empty += 1

    return stats


def _print_db_check(stats: DbCheckStats) -> None:
    print("\n=== DB transcript apply simulation ===")
    print(f"  transcript rows from bundle: {stats.transcript_rows}")
    print(f"  episode id NOT in podcasts.episodes: {stats.episode_missing}")
    print(f"  would UPDATE (import transcripts_updated): {stats.would_update}")
    print(f"  skip (prod already newer/same): {stats.would_skip_newer}")
    print(f"  skip (other): {stats.would_skip_same_empty}")
    if stats.sample_missing:
        print(f"  sample missing ids: {stats.sample_missing[:5]}")
    if stats.sample_skip_newer:
        print(f"  sample skipped (prod newer): {stats.sample_skip_newer[:5]}")
    if stats.transcript_rows and stats.would_update == 0:
        print(
            "\n  >> Import would log transcripts_updated=0 if episode ids are "
            "missing or prod already has same/newer transcripts."
        )


def _compare_source_vs_target_ids(
    episode_rows: list[EpisodeExportRow],
    rss_to_podcast_id: dict[str, int],
    *,
    sample_limit: int,
) -> None:
    """Show id drift: bundle source_episode_id vs id computed on target DB."""
    mismatches = 0
    samples: list[tuple[str, str, str]] = []
    for row in episode_rows[:500]:
        rss = normalize_rss_url(row.show_rss_url)
        if not rss or rss not in rss_to_podcast_id:
            continue
        podcast_id = rss_to_podcast_id[rss]
        computed = target_episode_id(podcast_id, row)
        source = row.source_episode_id
        if source and source != computed:
            mismatches += 1
            if len(samples) < sample_limit:
                samples.append((source, computed, rss[:60]))
    print("\n=== source_episode_id vs target compute_episode_id ===")
    print(f"  mismatches (first 500 rows checked): {mismatches}")
    for source, computed, rss in samples[:5]:
        print(f"    bundle {source} -> target would use {computed} ({rss}...)")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Inspect content_sync podcast bundle; optional DB dry-check.",
    )
    ap.add_argument(
        "--bundle",
        default=None,
        help="Bundle id (default: latest on source)",
    )
    ap.add_argument(
        "--source",
        choices=("nitwitch", "local"),
        default=None,
        help="nitwitch HTTP or local CONTENT_SYNC_LOCAL_DIR (default: nitwitch)",
    )
    ap.add_argument(
        "--check-db",
        action="store_true",
        help="Query DB for episode ids (read-only); no import",
    )
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Use PROD DB pool for --check-db",
    )
    ap.add_argument(
        "--sample-limit",
        type=int,
        default=8,
        help="Max sample ids to print per category",
    )
    args = ap.parse_args()

    source = args.source or "nitwitch"
    if source == "nitwitch":
        import os

        os.environ.setdefault("CONTENT_SYNC_IMPORT_STORAGE_KIND", "nitwitch")

    bundle_ids = list_export_bundle_ids()
    if not bundle_ids:
        print("No bundles found on source.", file=sys.stderr)
        raise SystemExit(1)

    bundle_id = args.bundle or sorted(bundle_ids, key=bundle_sort_key)[-1]
    print(f"Bundle: {bundle_id}")
    print(f"Source: {source} ({len(bundle_ids)} bundle(s) listed)")

    with tempfile.TemporaryDirectory(prefix="wmvi_bundle_check_") as tmp:
        bundle_dir = Path(tmp) / bundle_id
        download_bundle(bundle_id, bundle_dir)
        manifest = read_manifest(bundle_dir / MANIFEST_NAME)

        print("\n=== Manifest ===")
        print(json.dumps(manifest.to_dict(), indent=2))

        pinfo = manifest.platforms.get(PLATFORM_PODCAST_EPISODE)
        if pinfo is None:
            print("\nNo podcast_episode in manifest.", file=sys.stderr)
            raise SystemExit(1)

        episodes_path = bundle_dir / pinfo.file
        sidecar_name = manifest.sidecars.get(SIDECAR_PODCAST_SHOWS)
        shows_path = (
            bundle_dir / sidecar_name if sidecar_name else None
        )

        episode_rows = _episode_rows_from_jsonl(episodes_path)
        show_rows = (
            _show_rows_from_jsonl(shows_path)
            if shows_path and shows_path.is_file()
            else []
        )

        _analyze_bundle_rows(episode_rows)
        print(f"\n=== Sidecar {SIDECAR_PODCAST_SHOWS} ===")
        print(f"  show rows: {len(show_rows)}")

        # Resolve as import would after show upsert (sidecar rss -> fictional map).
        sidecar_rss_map: dict[str, int] = {}
        for i, row in enumerate(show_rows, start=1):
            c = normalize_rss_url(row.rss_url)
            if c:
                sidecar_rss_map[c] = row.source_show_id or i

        _simulate_resolve(
            episode_rows,
            show_rows,
            sidecar_rss_map,
            label="sidecar source_show_id (informational only)",
        )

        if not args.check_db:
            print(
                "\nTip: re-run with --check-db --prod on Azure to see why "
                "transcripts_updated would be 0."
            )
            return

        from db.db import close_pool, getcursor, init_pool

        prefix = "prod" if args.prod else "dev"
        init_pool(prefix=prefix)
        print(f"\nDB pool: {prefix.upper()}")

        try:
            with getcursor() as cur:
                rss_urls = list(
                    {normalize_rss_url(r.show_rss_url) for r in episode_rows}
                )
                rss_urls = [u for u in rss_urls if u]
                db_rss_map = _fetch_rss_to_podcast_id(cur, rss_urls)

                missing_shows = sorted(set(rss_urls) - set(db_rss_map.keys()))
                print(f"\n=== Target DB shows ({prefix}) ===")
                print(f"  distinct show rss in bundle: {len(rss_urls)}")
                print(f"  rss_url found in podcasts.shows: {len(db_rss_map)}")
                print(f"  rss_url MISSING (need upsert): {len(missing_shows)}")
                if missing_shows[:5]:
                    print(f"  sample missing rss: {missing_shows[:5]}")

                _, transcripts = _simulate_resolve(
                    episode_rows,
                    show_rows,
                    db_rss_map,
                    label=f"target DB rss map ({prefix})",
                )

                db_stats = _check_db_transcripts(
                    cur,
                    transcripts,
                    sample_limit=args.sample_limit,
                )
                _print_db_check(db_stats)

                _compare_source_vs_target_ids(
                    episode_rows,
                    db_rss_map,
                    sample_limit=args.sample_limit,
                )

                cur.execute(
                    "SELECT last_imported_bundle_at FROM sm.content_sync_state WHERE id = 'global'"
                )
                row = cur.fetchone()
                last_imported = row[0] if row else None
                if row:
                    print(f"\n=== sm.content_sync_state ===")
                    print(f"  last_imported_bundle_at: {last_imported}")

                pending = list_pending_bundle_ids(last_imported)
                print(f"\n=== Import queue (nitwitch/local listing) ===")
                print(f"  bundles on source: {len(bundle_ids)}")
                print(f"  pending import (bundle_id > watermark): {len(pending)}")
                if pending:
                    print(f"  pending ids: {pending}")
                    if bundle_id not in pending and last_imported is not None:
                        print(
                            f"\n  >> Bundle {bundle_id!r} is BEFORE the import watermark; "
                            "automatic import will not retry it. Use:\n"
                            "     python -m services.content_sync.import --prod "
                            f"--bundle {bundle_id}"
                        )
                elif last_imported is not None:
                    print("  (none — import timer will idle until next export)")
        finally:
            close_pool()


if __name__ == "__main__":
    main()
