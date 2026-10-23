"""Two-stage extract: cheap triage, then one detail call per event type."""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from apps.claims import io as claims_io
from apps.claims.events.mention import (
    EVENT_TYPES,
    Mention,
    coerce_event_type,
    evidence_text,
    item_needs_quote_repair,
    mention_from_extract,
    quote_in_source,
)
from apps.claims.events.paths import EXTRACT_LOG_FILE, MENTIONS_FILE
from apps.claims.events.prompts import (
    DEFAULT_MODEL,
    DEFAULT_PROVIDER,
    DEFAULT_TRIAGE_MODEL,
    QUOTE_REPAIR_SCHEMA,
    QUOTE_REPAIR_SYSTEM,
    TRIAGE_SCHEMA,
    TRIAGE_SYSTEM,
    format_quote_repair_prompt,
    format_user_prompt,
)
from apps.claims.events.type_prompts import DETAIL_SCHEMA, DETAIL_SYSTEM, format_detail_prompt
from nlp.claim_extraction.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    default_is_retryable_exception,
)
from nlp.claim_extraction.defaults import (
    DEFAULT_429_COOLDOWN_S,
    DEFAULT_MAX_RETRIES,
)

LogFn = Callable[[str], None]

MAX_EVENTS_PER_CHUNK = 4
DEFAULT_MAX_WORKERS = 24
DEFAULT_TARGET_RPM = 0

STAGE_TRIAGE = "triage"
STAGE_DETAIL = "detail"
STAGE_COMPLETE = "complete"


def _throttle_rpm() -> int:
    raw = os.getenv("CLAIMS_TARGET_RPM")
    if raw is None or not str(raw).strip():
        return DEFAULT_TARGET_RPM
    return int(raw)


def log_stderr(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_events_output(content: str) -> dict[str, Any]:
    parsed = json.loads(content.strip())
    if not isinstance(parsed, dict):
        raise ValueError("model output JSON top-level is not an object")
    events = parsed.get("events")
    if not isinstance(events, list):
        raise ValueError("model output missing list field 'events'")
    return parsed


class _PayloadStructuredClient:
    """Dispatch schema, system, and model from the request payload."""

    def __init__(
        self,
        *,
        provider: str,
        service_tier: str | None = None,
        complete_fn=None,
    ) -> None:
        self._provider = provider
        self._service_tier = service_tier
        self._complete_fn = complete_fn
        self._sdk = None

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        schema = payload.get("schema") or TRIAGE_SCHEMA
        system = str(payload.get("system") or TRIAGE_SYSTEM)
        user = str(payload["user"])
        model = str(payload.get("model") or DEFAULT_MODEL)
        if self._complete_fn is not None:
            out = self._complete_fn(
                provider=self._provider,
                model=model,
                system=system,
                user=user,
                schema=schema,
            )
            return out, {}
        return self._complete(model=model, system=system, user=user, schema=schema)

    def _complete(
        self,
        *,
        model: str,
        system: str,
        user: str,
        schema: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        from nlp.claim_extraction.api_requester import (
            _create_structured_chat_completion,
            _parse_chat_completion_response,
        )

        if self._sdk is None:
            if self._provider == "azure":
                from nlp.claim_extraction.clients import _build_azure_sdk_client

                self._sdk = _build_azure_sdk_client()
            else:
                from nlp.claim_extraction.clients import _build_openai_sdk_client

                self._sdk = _build_openai_sdk_client()
        resp = _create_structured_chat_completion(
            self._sdk,
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_schema=schema,
            service_tier=self._service_tier if self._provider != "azure" else None,
        )
        return _parse_chat_completion_response(resp, output_parser=parse_events_output)


def parse_raw_events(raw: dict[str, Any]) -> list[dict[str, Any]]:
    events = raw.get("events") if isinstance(raw, dict) else None
    if not isinstance(events, list):
        return []
    return [item for item in events if isinstance(item, dict)]


def _is_complete_row(row: dict[str, Any]) -> bool:
    if row.get("skip"):
        return False
    stage = row.get("stage")
    return stage in (None, "", STAGE_COMPLETE)


def logged_doc_ids(run_dir: Path) -> set[str]:
    done: set[str] = set()
    log_path = Path(run_dir) / EXTRACT_LOG_FILE
    for row in claims_io.read_jsonl(log_path):
        doc_id = row.get("doc_id")
        if doc_id and _is_complete_row(row):
            done.add(str(doc_id))
    return done


def remaining_docs(
    docs: list[dict[str, Any]],
    run_dir: Path,
    n: int | None = None,
) -> list[dict[str, Any]]:
    done = logged_doc_ids(run_dir)
    remaining = [doc for doc in docs if str(doc.get("doc_id") or "") not in done]
    if n is not None:
        remaining = remaining[: max(0, int(n))]
    return remaining


def load_mentions(path: Path) -> list[Mention]:
    if not path.is_file():
        return []
    return [Mention.from_dict(row) for row in claims_io.read_jsonl(path)]


def write_mentions(path: Path, records: list[Mention]) -> None:
    claims_io.write_jsonl(path, [rec.to_dict() for rec in records])


def cap_mentions(mentions: list[Mention], limit: int = MAX_EVENTS_PER_CHUNK) -> list[Mention]:
    """Keep at most `limit` mentions, in model emission order."""
    return list(mentions[: max(0, int(limit))])


def with_repaired_quotes(item: dict[str, Any], repair: dict[str, Any]) -> dict[str, Any]:
    """Keep every field from the original event. Replace only quotes."""
    quotes = repair.get("quotes") if isinstance(repair, dict) else None
    if not isinstance(quotes, list):
        quotes = []
    copied = dict(item)
    copied["quotes"] = [str(quote).strip() for quote in quotes if str(quote).strip()]
    return copied


def _repair_item(
    *,
    doc: dict[str, Any],
    item: dict[str, Any],
    provider: str,
    model: str,
    complete_fn,
) -> dict[str, Any] | None:
    user = format_quote_repair_prompt(doc=doc, item=item)
    if complete_fn is not None:
        out = complete_fn(
            provider=provider,
            model=model,
            system=QUOTE_REPAIR_SYSTEM,
            user=user,
            schema=QUOTE_REPAIR_SCHEMA,
        )
    elif provider == "azure":
        from nlp.claim_extraction.clients import azure_structured_completion

        out = azure_structured_completion(
            model=model,
            system=QUOTE_REPAIR_SYSTEM,
            user=user,
            schema=QUOTE_REPAIR_SCHEMA,
        )
    else:
        from nlp.claim_extraction.clients import openai_structured_completion

        out = openai_structured_completion(
            model=model,
            system=QUOTE_REPAIR_SYSTEM,
            user=user,
            schema=QUOTE_REPAIR_SCHEMA,
        )
    if not isinstance(out, dict):
        return None
    return with_repaired_quotes(item, out)


def _apply_mentions(
    *,
    doc: dict[str, Any],
    items: list[dict[str, Any]],
    existing_ids: set[str],
    log: LogFn,
    repair_fn: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
) -> tuple[list[Mention], int, int, int]:
    n_quote_dropped = 0
    n_quote_repaired = 0
    kept: list[Mention] = []
    source = evidence_text(title=doc.get("title"), body=doc.get("body"))
    for item in items:
        rec = mention_from_extract(item, doc=doc, existing_ids=existing_ids)
        if rec is None and repair_fn is not None and item_needs_quote_repair(item, source=source):
            log(f"events: [extract]   repair quotes for {item.get('title')}")
            try:
                repaired = repair_fn(item)
            except Exception as exc:
                log(f"events: [extract]   repair failed {item.get('title')}: {exc}")
                repaired = None
            if repaired is not None:
                rec = mention_from_extract(repaired, doc=doc, existing_ids=existing_ids)
                if rec is not None:
                    n_quote_repaired += 1
                    log(f"events: [extract]   repaired {rec.mention_id} {rec.title}")
                else:
                    log(f"events: [extract]   repair quotes still not in post: {item.get('title')}")
        if rec is None:
            n_quote_dropped += 1
            continue
        existing_ids.add(rec.mention_id)
        kept.append(rec)
    n_capped = 0
    if len(kept) > MAX_EVENTS_PER_CHUNK:
        n_capped = len(kept) - MAX_EVENTS_PER_CHUNK
        kept = cap_mentions(kept)
    for rec in kept:
        log(f"events: [extract]   {rec.event_type} {rec.mention_id} {rec.title}")
    if not kept:
        log("events: [extract]   (no events)" if not items else "events: [extract]   (quotes failed)")
    elif n_capped:
        log(f"events: [extract]   capped {n_capped} extra events (max {MAX_EVENTS_PER_CHUNK})")
    return kept, n_quote_dropped, n_capped, n_quote_repaired


def _stub_has_quote(stub: dict[str, Any], *, source: str) -> bool:
    quotes = [str(q).strip() for q in (stub.get("quotes") or []) if str(q).strip()]
    return any(quote_in_source(quote, source) for quote in quotes)


def _filter_triage_stubs(raw: list[dict[str, Any]], *, doc: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    source = evidence_text(title=doc.get("title"), body=doc.get("body"))
    kept: list[dict[str, Any]] = []
    n_dropped = 0
    for item in raw:
        event_type = coerce_event_type(str(item.get("event_type") or ""))
        stub = {
            "event_type": event_type,
            "mention_kind": item.get("mention_kind") or "event",
            "title": str(item.get("title") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "diseases": item.get("diseases") if isinstance(item.get("diseases"), list) else [],
            "quotes": [str(q).strip() for q in (item.get("quotes") or []) if str(q).strip()],
        }
        if not stub["title"] or not _stub_has_quote(stub, source=source):
            n_dropped += 1
            continue
        if len(kept) >= MAX_EVENTS_PER_CHUNK:
            n_dropped += 1
            continue
        kept.append(stub)
    return kept, n_dropped


def _group_stubs_by_type(stubs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for stub in stubs:
        etype = coerce_event_type(str(stub.get("event_type") or ""))
        if etype not in EVENT_TYPES:
            etype = "other"
        grouped.setdefault(etype, []).append(stub)
    return grouped


def _merge_stub_detail(stub: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(detail or {})
    merged["event_type"] = stub.get("event_type")
    merged["mention_kind"] = stub.get("mention_kind") or merged.get("mention_kind") or "event"
    if not str(merged.get("title") or "").strip():
        merged["title"] = stub.get("title") or ""
    if not merged.get("summary"):
        merged["summary"] = stub.get("summary") or ""
    if not merged.get("diseases"):
        merged["diseases"] = stub.get("diseases") or []
    if not merged.get("quotes"):
        merged["quotes"] = stub.get("quotes") or []
    return merged


def _merge_type_details(stubs: list[dict[str, Any]], details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unused = list(details)
    merged: list[dict[str, Any]] = []
    for i, stub in enumerate(stubs):
        detail = unused.pop(0) if i < len(details) else None
        if detail is None:
            title = str(stub.get("title") or "").strip()
            for j, candidate in enumerate(unused):
                if str(candidate.get("title") or "").strip() == title:
                    detail = unused.pop(j)
                    break
        merged.append(_merge_stub_detail(stub, detail))
    return merged


def load_extract_progress(
    run_dir: Path,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, set[str]], dict[str, list[str]]]:
    stubs_by_doc: dict[str, list[dict[str, Any]]] = {}
    done_types: dict[str, set[str]] = defaultdict(set)
    mention_ids_by_doc: dict[str, list[str]] = defaultdict(list)
    log_path = Path(run_dir) / EXTRACT_LOG_FILE
    for row in claims_io.read_jsonl(log_path):
        doc_id = str(row.get("doc_id") or "")
        if not doc_id or row.get("skip"):
            continue
        stage = row.get("stage")
        if stage == STAGE_TRIAGE:
            stubs = row.get("stubs")
            stubs_by_doc[doc_id] = [s for s in stubs if isinstance(s, dict)] if isinstance(stubs, list) else []
        elif stage == STAGE_DETAIL:
            etype = coerce_event_type(str(row.get("event_type") or ""))
            done_types[doc_id].add(etype)
            for mid in row.get("mention_ids") or []:
                if str(mid).strip():
                    mention_ids_by_doc[doc_id].append(str(mid))
    return stubs_by_doc, done_types, mention_ids_by_doc


def _new_requester(*, client: Any, workers: int, rpm: int, log: LogFn) -> ConcurrentApiRequester:
    throttle = ThrottlePolicy(
        target_requests_per_minute=rpm,
        global_429_cooldown_s=max(0.0, float(os.getenv("CLAIMS_429_COOLDOWN_S", str(DEFAULT_429_COOLDOWN_S)))),
    )
    return ConcurrentApiRequester(
        client=client,
        max_workers=workers,
        retry_policy=RetryPolicy(max_retries=DEFAULT_MAX_RETRIES),
        throttle_policy=throttle,
        is_retryable=default_is_retryable_exception,
        on_log=lambda msg: log(f"events: [extract] {msg}"),
    )


def _complete_doc(
    *,
    log_path: Path,
    doc: dict[str, Any],
    mention_ids: list[str],
    n_raw: int,
    n_quote_dropped: int,
    n_quote_repaired: int,
    n_capped: int,
    n_kept: int,
) -> None:
    claims_io.append_jsonl(
        log_path,
        {
            "doc_id": str(doc.get("doc_id") or ""),
            "stage": STAGE_COMPLETE,
            "post_id": doc.get("post_id"),
            "chunk_index": doc.get("chunk_index"),
            "platform": doc.get("platform"),
            "mention_ids": mention_ids,
            "n_raw": n_raw,
            "n_quote_dropped": n_quote_dropped,
            "n_quote_repaired": n_quote_repaired,
            "n_capped": n_capped,
            "n_kept": n_kept,
            "recorded_at": _utc_now(),
        },
    )


def run_extract(
    *,
    docs: list[dict[str, Any]],
    run_dir: Path,
    n: int | None = None,
    provider: str = DEFAULT_PROVIDER,
    model: str = DEFAULT_MODEL,
    triage_model: str = DEFAULT_TRIAGE_MODEL,
    max_workers: int = DEFAULT_MAX_WORKERS,
    service_tier: str | None = None,
    log: LogFn = log_stderr,
    complete_fn=None,
) -> list[Mention]:
    log_path = Path(run_dir) / EXTRACT_LOG_FILE
    mentions_path = Path(run_dir) / MENTIONS_FILE
    records = load_mentions(mentions_path)
    existing_ids = {r.mention_id for r in records}
    done = logged_doc_ids(run_dir)
    remaining = remaining_docs(docs, run_dir, n)
    rpm = _throttle_rpm()
    workers = max(1, int(max_workers))
    log(
        f"events: [extract] {len(docs)} docs on disk, {len(done)} already logged, "
        f"{len(remaining)} to prompt  provider={provider} model={model} "
        f"triage_model={triage_model} "
        f"service_tier={service_tier or 'standard'} workers={workers} rpm={rpm}"
    )
    if not remaining:
        return records

    client = _PayloadStructuredClient(
        provider=provider,
        service_tier=service_tier,
        complete_fn=complete_fn,
    )
    requester = _new_requester(client=client, workers=workers, rpm=rpm, log=log)
    by_id = {str(doc.get("doc_id") or ""): doc for doc in remaining}
    stubs_by_doc, done_types, prior_mention_ids = load_extract_progress(run_dir)
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {
        "n_raw": 0,
        "n_quote_dropped": 0,
        "n_quote_repaired": 0,
        "n_capped": 0,
        "n_kept": 0,
    })
    mentions_by_doc: dict[str, list[Mention]] = defaultdict(list)

    need_triage = [doc for doc in remaining if str(doc.get("doc_id") or "") not in stubs_by_doc]
    if need_triage:
        triage_tasks = [
            RequestTask(
                task_id=str(doc.get("doc_id") or ""),
                payload={
                    "user": format_user_prompt(doc=doc),
                    "system": TRIAGE_SYSTEM,
                    "schema": TRIAGE_SCHEMA,
                    "model": triage_model,
                },
            )
            for doc in need_triage
        ]
        n_total = len(triage_tasks)
        n_failed = 0
        finished = 0
        for result in requester.run(triage_tasks):
            finished += 1
            doc = by_id.get(result.task_id)
            if doc is None:
                continue
            log(
                f"events: [extract] triage {finished}/{n_total} id={result.task_id} "
                f"platform={doc.get('platform')}"
            )
            if result.status != RequestStatus.SUCCESS or result.output is None:
                n_failed += 1
                log(f"events: [extract]   triage FAILED {result.error or result.status}")
                continue
            raw = parse_raw_events(result.output)
            stubs, n_quote_dropped = _filter_triage_stubs(raw, doc=doc)
            stubs_by_doc[result.task_id] = stubs
            stats[result.task_id]["n_raw"] = len(raw)
            stats[result.task_id]["n_quote_dropped"] += n_quote_dropped
            claims_io.append_jsonl(
                log_path,
                {
                    "doc_id": result.task_id,
                    "stage": STAGE_TRIAGE,
                    "post_id": doc.get("post_id"),
                    "chunk_index": doc.get("chunk_index"),
                    "platform": doc.get("platform"),
                    "stubs": stubs,
                    "n_raw": len(raw),
                    "n_quote_dropped": n_quote_dropped,
                    "usage": (result.response_meta or {}).get("usage") or None,
                    "recorded_at": _utc_now(),
                },
            )
            log(f"events: [extract]   {len(stubs)} stubs ({n_quote_dropped} without quotes)")
        if n_failed:
            log(f"events: [extract]   {n_failed} triage failures; remaining types will wait")

    detail_tasks: list[RequestTask] = []
    pending_types: dict[str, set[str]] = {}
    for doc in remaining:
        doc_id = str(doc.get("doc_id") or "")
        if doc_id not in stubs_by_doc:
            continue
        grouped = _group_stubs_by_type(stubs_by_doc[doc_id])
        needed = set(grouped)
        already = done_types.get(doc_id) or set()
        pending_types[doc_id] = set(needed)
        if not needed:
            _complete_doc(
                log_path=log_path,
                doc=doc,
                mention_ids=list(prior_mention_ids.get(doc_id) or []),
                n_raw=stats[doc_id]["n_raw"],
                n_quote_dropped=stats[doc_id]["n_quote_dropped"],
                n_quote_repaired=0,
                n_capped=0,
                n_kept=len(prior_mention_ids.get(doc_id) or []),
            )
            continue
        for etype, type_stubs in grouped.items():
            if etype in already:
                pending_types[doc_id].discard(etype)
                continue
            detail_tasks.append(
                RequestTask(
                    task_id=f"{doc_id}::{etype}",
                    payload={
                        "user": format_detail_prompt(doc=doc, event_type=etype, stubs=type_stubs),
                        "system": DETAIL_SYSTEM,
                        "schema": DETAIL_SCHEMA,
                        "model": model,
                        "doc_id": doc_id,
                        "event_type": etype,
                    },
                )
            )
        if needed and not (needed - already):
            ids = list(prior_mention_ids.get(doc_id) or [])
            _complete_doc(
                log_path=log_path,
                doc=doc,
                mention_ids=ids,
                n_raw=stats[doc_id]["n_raw"],
                n_quote_dropped=stats[doc_id]["n_quote_dropped"],
                n_quote_repaired=stats[doc_id]["n_quote_repaired"],
                n_capped=0,
                n_kept=len(ids),
            )

    if not detail_tasks:
        log(f"events: [extract] done  {len(records)} mentions in {mentions_path}")
        return records

    n_total = len(detail_tasks)
    n_failed = 0
    finished = 0
    for result in requester.run(detail_tasks):
        finished += 1
        if "::" in result.task_id:
            doc_id, etype = result.task_id.split("::", 1)
        else:
            doc_id, etype = result.task_id, ""
        doc = by_id.get(doc_id)
        if doc is None:
            continue
        log(
            f"events: [extract] detail {finished}/{n_total} id={doc_id} type={etype} "
            f"platform={doc.get('platform')}"
        )
        if result.status != RequestStatus.SUCCESS or result.output is None:
            n_failed += 1
            log(f"events: [extract]   detail FAILED {result.error or result.status}")
            continue
        stubs = [s for s in (stubs_by_doc.get(doc_id) or []) if coerce_event_type(str(s.get("event_type") or "")) == etype]
        details = parse_raw_events(result.output)
        items = _merge_type_details(stubs, details)

        def repair_fn(item: dict[str, Any], _doc: dict[str, Any] = doc) -> dict[str, Any] | None:
            return _repair_item(
                doc=_doc,
                item=item,
                provider=provider,
                model=model,
                complete_fn=complete_fn,
            )

        kept, n_quote_dropped, n_capped, n_quote_repaired = _apply_mentions(
            doc=doc,
            items=items,
            existing_ids=existing_ids,
            log=log,
            repair_fn=repair_fn,
        )
        mentions_by_doc[doc_id].extend(kept)
        records.extend(kept)
        if kept:
            claims_io.write_jsonl(mentions_path, [rec.to_dict() for rec in kept], append=True)
        stats[doc_id]["n_quote_dropped"] += n_quote_dropped
        stats[doc_id]["n_quote_repaired"] += n_quote_repaired
        stats[doc_id]["n_capped"] += n_capped
        stats[doc_id]["n_kept"] += len(kept)
        claims_io.append_jsonl(
            log_path,
            {
                "doc_id": doc_id,
                "stage": STAGE_DETAIL,
                "event_type": etype,
                "post_id": doc.get("post_id"),
                "chunk_index": doc.get("chunk_index"),
                "platform": doc.get("platform"),
                "mention_ids": [m.mention_id for m in kept],
                "n_raw": len(items),
                "n_quote_dropped": n_quote_dropped,
                "n_quote_repaired": n_quote_repaired,
                "n_capped": n_capped,
                "n_kept": len(kept),
                "usage": (result.response_meta or {}).get("usage") or None,
                "recorded_at": _utc_now(),
            },
        )
        pending = pending_types.get(doc_id)
        if pending is not None:
            pending.discard(etype)
            if not pending:
                mention_ids = list(prior_mention_ids.get(doc_id) or [])
                mention_ids.extend(m.mention_id for m in mentions_by_doc[doc_id])
                _complete_doc(
                    log_path=log_path,
                    doc=doc,
                    mention_ids=mention_ids,
                    n_raw=stats[doc_id]["n_raw"],
                    n_quote_dropped=stats[doc_id]["n_quote_dropped"],
                    n_quote_repaired=stats[doc_id]["n_quote_repaired"],
                    n_capped=stats[doc_id]["n_capped"],
                    n_kept=stats[doc_id]["n_kept"] + len(prior_mention_ids.get(doc_id) or []),
                )
    log(
        f"events: [extract] done  {len(records)} mentions in {mentions_path}"
        + (f"  ({n_failed} failed, retry with --continue)" if n_failed else "")
    )
    return records
