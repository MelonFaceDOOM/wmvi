"""Denormalized browse.json: posts → mentions."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.events.extract import _is_complete_row, log_stderr
from apps.claims.events.mention import Mention
from apps.claims.events.paths import BROWSE_FILE, EXTRACT_LOG_FILE, MENTIONS_FILE, METRICS_FILE


def _histogram(values: list[str]) -> dict[str, int]:
    return dict(Counter(values))


def build_browse(
    *,
    mentions: list[Mention],
    docs: list[dict[str, Any]],
    log_rows: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    mentions_by_doc: dict[str, list[Mention]] = {}
    for rec in mentions:
        key = f"{rec.post_id}:{rec.chunk_index}"
        mentions_by_doc.setdefault(key, []).append(rec)
    log_by_doc = {str(row.get("doc_id")): row for row in log_rows if row.get("doc_id")}

    posts_out: list[dict[str, Any]] = []
    for doc in docs:
        doc_id = str(doc.get("doc_id") or "")
        recs = mentions_by_doc.get(doc_id) or []
        log_row = log_by_doc.get(doc_id) or {}
        posts_out.append(
            {
                "doc_id": doc_id,
                "post_id": doc.get("post_id"),
                "chunk_index": doc.get("chunk_index"),
                "platform": doc.get("platform"),
                "posted_at": doc.get("posted_at"),
                "title": doc.get("title"),
                "channel": doc.get("channel"),
                "thread_title": doc.get("thread_title"),
                "url": doc.get("url"),
                "truncated": bool(doc.get("truncated")),
                "body": doc.get("body"),
                "n_raw": log_row.get("n_raw"),
                "n_quote_dropped": log_row.get("n_quote_dropped"),
                "n_mentions": len(recs),
                "mentions": [
                    {
                        "mention_id": m.mention_id,
                        "event_type": m.event_type,
                        "mention_kind": m.mention_kind,
                        "role_in_post": m.role_in_post,
                        "title": m.title,
                        "summary": m.summary,
                        "temporal_kind": m.temporal_kind,
                        "stated_start": m.stated_start,
                        "stated_end": m.stated_end,
                        "date_precision": m.date_precision,
                        "date_grain": m.date_grain,
                        "span_status": m.span_status,
                        "stated_date_text": m.stated_date_text,
                        "location": m.location,
                        "region": m.region,
                        "place_scope": m.place_scope,
                        "study_action": m.study_action,
                        "admin1_other": m.admin1_other,
                        "actor": m.actor,
                        "diseases": m.diseases,
                        "vaccines": m.vaccines,
                        "people": m.people,
                        "organizations": m.organizations,
                        "case_count": m.case_count,
                        "death_count": m.death_count,
                        "quotes": m.quotes,
                        "date_relation": m.date_relation,
                    }
                    for m in recs
                ],
            }
        )

    return {
        "corpus": config.get("corpus"),
        "n_docs": len(docs),
        "n_mentions": len(mentions),
        "posts": posts_out,
    }


def compute_metrics(
    *,
    mentions: list[Mention],
    docs: list[dict[str, Any]],
    log_rows: list[dict[str, Any]],
    browse_path: Path,
    mentions_path: Path,
) -> dict[str, Any]:
    complete_rows = [row for row in log_rows if _is_complete_row(row)]
    n_logged = len(complete_rows)
    n_empty = sum(1 for row in complete_rows if not (row.get("mention_ids") or []))
    n_quote_dropped = sum(int(row.get("n_quote_dropped") or 0) for row in complete_rows)
    n_quote_repaired = sum(int(row.get("n_quote_repaired") or 0) for row in complete_rows)
    n_capped = sum(int(row.get("n_capped") or 0) for row in complete_rows)
    n_raw = sum(int(row.get("n_raw") or 0) for row in complete_rows)
    n_fine_place = sum(
        1
        for m in mentions
        if (m.location or {}).get("admin2")
        or (m.location or {}).get("locality")
        or (m.location or {}).get("site")
    )
    n_dated = sum(1 for m in mentions if m.date_precision != "unknown")
    platforms = [str(d.get("platform") or "unknown") for d in docs]
    return {
        "n_docs": len(docs),
        "n_docs_logged": n_logged,
        "n_empty_docs": n_empty,
        "empty_event_rate": round(n_empty / n_logged, 4) if n_logged else None,
        "n_mentions": len(mentions),
        "mentions_per_logged_doc": round(len(mentions) / n_logged, 4) if n_logged else None,
        "n_raw_llm_events": n_raw,
        "n_quote_dropped": n_quote_dropped,
        "n_quote_repaired": n_quote_repaired,
        "quote_drop_rate": round(n_quote_dropped / n_raw, 4) if n_raw else None,
        "n_capped_events": n_capped,
        "share_fine_place": round(n_fine_place / len(mentions), 4) if mentions else None,
        "share_stated_dates": round(n_dated / len(mentions), 4) if mentions else None,
        "type_histogram": _histogram([m.event_type for m in mentions]),
        "mention_kind_histogram": _histogram([m.mention_kind for m in mentions]),
        "role_histogram": _histogram([m.role_in_post for m in mentions]),
        "precision_histogram": _histogram([m.date_precision for m in mentions]),
        "date_relation_histogram": _histogram([m.date_relation for m in mentions]),
        "platform_histogram": _histogram(platforms),
        "browse_path": str(browse_path),
        "mentions_path": str(mentions_path),
    }


def write_report(
    *,
    run_dir: Path,
    mentions: list[Mention],
    docs: list[dict[str, Any]],
    config: dict[str, Any],
    log=log_stderr,
) -> dict[str, Any]:
    log_path = Path(run_dir) / EXTRACT_LOG_FILE
    log_rows = claims_io.read_jsonl(log_path) if log_path.is_file() else []
    browse = build_browse(mentions=mentions, docs=docs, log_rows=log_rows, config=config)
    browse_path = Path(run_dir) / BROWSE_FILE
    claims_io.write_json(browse_path, browse)
    mentions_path = Path(run_dir) / MENTIONS_FILE
    metrics = compute_metrics(
        mentions=mentions,
        docs=docs,
        log_rows=log_rows,
        browse_path=browse_path,
        mentions_path=mentions_path,
    )
    claims_io.write_json(Path(run_dir) / METRICS_FILE, metrics)
    log(
        f"events: [report] {metrics['n_mentions']} mentions from "
        f"{metrics['n_docs_logged']} docs → {browse_path}"
    )
    return metrics
