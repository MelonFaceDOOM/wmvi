"""Run one mention-merge attempt.

Usage (repo root)::

    python -m apps.claims.events.merge.run \\
        --mentions apps/claims/events/runs/run_11/mentions.jsonl
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.events.extract import log_stderr
from apps.claims.events.merge.group import (
    STRATEGIES,
    STRATEGY_V1,
    STRATEGY_V2,
    STRATEGY_V3,
    attempt_config,
    build_metrics,
    build_qa,
    cluster_mentions,
    prepare_mentions,
)
from apps.claims.events.merge.browse import write_merge_browse, build_browse
from apps.claims.events.merge.paths import (
    CLUSTERS_FILE,
    CONFIG_FILE,
    DROPPED_FILE,
    MEMBERS_FILE,
    MERGES_DIR,
    METRICS_FILE,
    QA_FILE,
    next_merge_dir,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m apps.claims.events.merge.run",
        description="Cluster extract mentions into candidate events (no catalog writes).",
    )
    p.add_argument("--mentions", required=True, help="Path to mentions.jsonl from an extract run")
    p.add_argument("--merge-dir", default=None, help="Write here instead of next merges/merge_N")
    p.add_argument("--merges-dir", default=None, help="Override apps/claims/events/merges")
    p.add_argument(
        "--strategy",
        default=STRATEGY_V2,
        choices=STRATEGIES,
        help=f"Grouping strategy (default: {STRATEGY_V2})",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mentions_path = Path(args.mentions)
    if not mentions_path.is_file():
        raise SystemExit(f"Missing mentions file: {mentions_path}")
    if args.merge_dir:
        merge_dir = Path(args.merge_dir)
        merge_dir.mkdir(parents=True, exist_ok=True)
    else:
        root = Path(args.merges_dir) if args.merges_dir else MERGES_DIR
        merge_dir = next_merge_dir(root)

    strategy = str(args.strategy)
    config = attempt_config(mentions_path=str(mentions_path), strategy=strategy)
    log_stderr(f"merge: dir {merge_dir}")
    log_stderr(f"merge: strategy {strategy}")
    log_stderr(f"merge: key {config['key']}")
    log_stderr(f"merge: date {config['date']}")

    raw = claims_io.read_jsonl(mentions_path)
    log_stderr(f"merge: loaded {len(raw)} mentions")
    if strategy == STRATEGY_V1:
        mentions, dropped = raw, []
    else:
        mentions, dropped = prepare_mentions(raw, mode=strategy)
        log_stderr(f"merge: dropped {len(dropped)}  grouped {len(mentions)}")

    config["n_mentions_in"] = len(raw)
    clusters, members = cluster_mentions(mentions, strategy=strategy)
    metrics = build_metrics(
        mentions=mentions, clusters=clusters, config=config, dropped=dropped
    )
    qa = build_qa(clusters, dropped=dropped)

    claims_io.write_json(merge_dir / CONFIG_FILE, config)
    claims_io.write_jsonl(merge_dir / CLUSTERS_FILE, clusters)
    claims_io.write_jsonl(merge_dir / MEMBERS_FILE, members)
    claims_io.write_json(merge_dir / METRICS_FILE, metrics)
    claims_io.write_json(merge_dir / QA_FILE, qa)
    if dropped:
        claims_io.write_jsonl(
            merge_dir / DROPPED_FILE,
            [
                {
                    "mention_id": m.get("mention_id"),
                    "event_type": m.get("event_type"),
                    "title": m.get("title"),
                    "drop_reason": m.get("drop_reason"),
                    "post_id": m.get("post_id"),
                    "platform": m.get("platform"),
                }
                for m in dropped
            ],
        )

    mentions_by_id = {str(m.get("mention_id") or ""): m for m in raw if m.get("mention_id")}
    for row in list(mentions) + list(dropped):
        mid = str(row.get("mention_id") or "")
        if mid and mid in mentions_by_id:
            mentions_by_id[mid] = {**mentions_by_id[mid], **row}
    browse = build_browse(
        clusters=clusters,
        members=members,
        mentions_by_id=mentions_by_id,
        dropped=dropped,
        config=config,
    )
    json_path, html_path = write_merge_browse(merge_dir, browse)
    log_stderr(f"merge: browse {html_path}")

    log_stderr(
        f"merge: {metrics['n_clusters']} clusters  "
        f"dropped={metrics.get('n_dropped', 0)}  "
        f"singletons={metrics['n_singleton']}  "
        f"mixed_year={metrics['n_mixed_year']}  "
        f"overmerge_suspects={metrics['n_overmerge_suspects']}  "
        f"undermerge_groups={metrics['n_undermerge_suspect_groups']}  "
        f"imputed={metrics.get('n_imputed_proxy', 0)}"
    )
    log_stderr(f"merge: size_histogram {metrics['size_histogram']}")
    claims_io.emit_json(
        {
            "merge_dir": str(merge_dir),
            "strategy": strategy,
            "n_mentions_in": metrics.get("n_mentions_in"),
            "n_dropped": metrics.get("n_dropped"),
            "n_grouped": metrics.get("n_grouped"),
            "n_clusters": metrics["n_clusters"],
            "size_histogram": metrics["size_histogram"],
            "n_mixed_year": metrics["n_mixed_year"],
            "n_overmerge_suspects": metrics["n_overmerge_suspects"],
            "n_undermerge_suspect_groups": metrics["n_undermerge_suspect_groups"],
            "n_imputed_proxy": metrics.get("n_imputed_proxy"),
        }
    )
    log_stderr("merge: finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
