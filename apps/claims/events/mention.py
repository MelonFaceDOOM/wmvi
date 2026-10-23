"""Extract-time event mentions (not catalog events)."""

from __future__ import annotations

import hashlib
import re
from calendar import monthrange
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from events.schema import (
    DATE_PRECISIONS,
    DATE_RELATIONS,
    PLACE_LEVELS,
    TEMPORAL_KINDS,
    normalize_location,
    slugify,
    utc_now,
)

# Extract-time types. Catalog `events.schema.EVENT_TYPES` can split later.
EVENT_TYPES = (
    "outbreak",
    "notable_case",
    "cases",
    "vaccine_injury",
    "institutional_action",
    "study_publication",
    "other",
)

EVENT_TYPE_ALIASES = {
    "death": "notable_case",
    "exposure": "notable_case",
    "case_reporting": "outbreak",
    "adverse_event": "vaccine_injury",
    "vaccine_reaction": "vaccine_injury",
    "policy_change": "institutional_action",
    "regulatory_action": "institutional_action",
    "personnel_change": "institutional_action",
    "legal_action": "institutional_action",
    "approval": "institutional_action",
    "recall": "institutional_action",
    "funding_change": "institutional_action",
    "public_statement": "other",
    "safety_signal": "other",
}

ROLES_IN_POST = ("focal", "mentioned")
PLACE_SCOPES = ("at_named_place", "countrywide", "unplaced", "global", "region")
MENTION_KINDS = ("event", "update", "inner")
DATE_GRAINS = ("day", "month", "year", "unknown")
SPAN_STATUSES = ("open", "closed")
STUDY_ACTIONS = ("publication", "retraction")
PERSON_ACTOR_TYPES = frozenset({"notable_case", "vaccine_injury"})

_ROLE_ACTOR_SLUGS = frozenset(
    {
        "child",
        "children",
        "baby",
        "person",
        "toddler",
        "infant",
        "boy",
        "girl",
        "man",
        "woman",
        "adult",
        "student",
        "refugee",
        "patient",
        "patients",
        "someone",
        "officials",
        "health-officials",
        "unvaccinated-adult",
        "the-child",
        "the-infant",
        "resident",
        "parent",
        "parents",
        "mother",
        "father",
        "nurse",
        "nurses",
        "doctor",
        "doctors",
        "daughter",
        "son",
        "brother",
        "sister",
        "grandmother",
        "grandfather",
        "uncle",
        "aunt",
        "cousin",
        "wife",
        "husband",
        "sibling",
    }
)
_DESCRIPTION_TAILS = frozenset(
    {
        "child",
        "children",
        "baby",
        "boy",
        "girl",
        "infant",
        "toddler",
        "man",
        "woman",
        "person",
        "adult",
        "student",
        "patient",
        "daughter",
        "son",
        "parent",
        "parents",
        "mother",
        "father",
    }
)
_AGENCY_TOKENS = frozenset(
    {
        "department",
        "ministry",
        "officials",
        "hospital",
        "agency",
        "cdc",
        "who",
        "acip",
        "hhs",
        "fda",
        "nih",
    }
)
_AGENCY_ACTOR_SLUGS = frozenset(
    {
        "cdc",
        "who",
        "acip",
        "hhs",
        "fda",
        "nih",
        "cdc-officials",
        "who-officials",
    }
)
_JOURNAL_ACTOR_SLUGS = frozenset(
    {
        "lancet",
        "the-lancet",
        "nejm",
        "bmj",
        "nature",
        "science",
        "pediatrics",
        "jama",
        "new-england-journal-of-medicine",
        "the-new-england-journal-of-medicine",
    }
)

_ELIMINATION_STATUS = re.compile(
    r"\beliminat\w*.{0,40}\bstatus\b"
    r"|\bstatus\b.{0,40}\beliminat"
    r"|\b(lost|loses|losing|loss of)\b.{0,40}\beliminat"
    r"|\b(declared|confirmed|certified)\b.{0,40}\beliminat"
    r"|\beliminated (from|in)\b",
    re.I,
)

_WS = re.compile(r"\s+")


def document_id(post_id: Any, chunk_index: Any) -> str:
    return f"{post_id}:{int(chunk_index or 0)}"


def normalize_ws(text: str) -> str:
    return _WS.sub(" ", str(text or "")).strip()


def _str_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def quote_in_source(quote: str, source: str) -> bool:
    q = normalize_ws(quote)
    if not q:
        return False
    return q.casefold() in normalize_ws(source).casefold()


def item_needs_quote_repair(item: dict[str, Any], *, source: str) -> bool:
    """True when the event has a title and none of its quotes occur in the post.

    An empty title is a different failure and is not sent for repair.
    """
    if not str(item.get("title") or "").strip():
        return False
    quotes = [str(q).strip() for q in (item.get("quotes") or []) if str(q).strip()]
    return not any(quote_in_source(quote, source) for quote in quotes)


def evidence_text(*, title: str | None, body: str | None) -> str:
    parts = [str(title or "").strip(), str(body or "").strip()]
    return "\n\n".join(p for p in parts if p)


def coerce_event_type(raw: str) -> str:
    val = str(raw or "").strip()
    val = EVENT_TYPE_ALIASES.get(val, val)
    return val if val in EVENT_TYPES else "other"


def coerce_event_type_from_text(raw: str, *, title: str, summary: str | None) -> str:
    """Map elimination-status loss onto institutional_action when the model used other."""
    typed = coerce_event_type(raw)
    if typed != "other":
        return typed
    blob = f"{title} {summary or ''}"
    if _ELIMINATION_STATUS.search(blob):
        return "institutional_action"
    return typed


def infer_date_relation(
    *,
    date_precision: str,
    temporal_kind: str,
    stated_start: str | None,
) -> str:
    """Derive how post time relates to the event; do not trust the LLM field."""
    if date_precision == "unknown" or not stated_start:
        return "mentioned_at"
    if temporal_kind == "instant":
        return "happened_by"
    if temporal_kind in ("open", "interval"):
        return "started_by"
    return "mentioned_at"


def coerce_choice(raw: str, allowed: tuple[str, ...], default: str) -> str:
    val = str(raw or "").strip()
    return val if val in allowed else default


def coerce_role(raw: str) -> str:
    val = str(raw or "").strip()
    if val == "background":
        val = "mentioned"
    return coerce_choice(val, ROLES_IN_POST, "focal")


def coerce_dates(
    *,
    stated_start: str | None,
    stated_end: str | None,
    date_precision: str | None,
    temporal_kind: str,
    stated_date_text: str | None,
) -> tuple[str | None, str | None, str, str, str]:
    precision = coerce_choice(str(date_precision or "unknown"), DATE_PRECISIONS, "unknown")
    kind = coerce_choice(str(temporal_kind or "unknown"), TEMPORAL_KINDS, "unknown")
    start = str(stated_start or "").strip() or None
    end = str(stated_end or "").strip() or None
    date_text = str(stated_date_text or "").strip()
    if precision != "unknown" and not date_text:
        precision = "unknown"
        start, end = None, None
    if precision == "unknown":
        start, end = None, None
    if precision == "range" and not (start and end):
        precision = "unknown"
        start, end = None, None
    if precision in ("day", "month", "year") and not start:
        precision = "unknown"
    if kind in ("instant", "open"):
        end = None
    return start, end, precision, kind, date_text


def _parse_iso_date(raw: str | None) -> date | None:
    text = str(raw or "").strip()[:10]
    if len(text) != 10:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def expand_stated_range(
    *,
    stated_start: str | None,
    stated_end: str | None,
    date_precision: str,
    temporal_kind: str,
    posted_at: str | None,
) -> tuple[str | None, str | None, str, str]:
    """Expand year and month into ranges capped by the post date. A day stays a day."""
    start_d = _parse_iso_date(stated_start)
    if start_d is None or date_precision not in ("day", "month", "year"):
        return stated_start, stated_end, date_precision, temporal_kind
    if date_precision == "day":
        return start_d.isoformat(), start_d.isoformat(), "day", temporal_kind
    if date_precision == "year":
        natural_start = date(start_d.year, 1, 1)
        natural_end = date(start_d.year, 12, 31)
    else:
        last = monthrange(start_d.year, start_d.month)[1]
        natural_start = date(start_d.year, start_d.month, 1)
        natural_end = date(start_d.year, start_d.month, last)
    cap = _parse_iso_date(posted_at)
    end_d = natural_end if cap is None or natural_end <= cap else cap
    if end_d < natural_start:
        end_d = natural_start
    return natural_start.isoformat(), end_d.isoformat(), "range", "interval"


def coerce_mention_kind(raw: str) -> str:
    return coerce_choice(str(raw or ""), MENTION_KINDS, "event")


def coerce_span_status(raw: str) -> str:
    return coerce_choice(str(raw or ""), SPAN_STATUSES, "open")


def coerce_date_grain(raw: str) -> str:
    return coerce_choice(str(raw or ""), DATE_GRAINS, "unknown")


def place_slots_from(location: dict[str, Any] | None, region: str = "") -> list[str]:
    slots: list[str] = []
    loc = location or {}
    for key in (*PLACE_LEVELS, "label", "region"):
        val = str(loc.get(key) or "").strip()
        if val:
            slots.append(val)
    extra = str(region or "").strip()
    if extra and extra not in slots:
        slots.append(extra)
    return slots


def person_actor_rejected(raw: str, *, slots: list[str] | None = None) -> bool:
    """A role, age, kinship word, agency, or place slot is not a patient's name."""
    actor = str(raw or "").strip()
    if not actor:
        return True
    slug = slugify(actor)
    if not slug or slug in _ROLE_ACTOR_SLUGS or slug in _AGENCY_ACTOR_SLUGS:
        return True
    if "year-old" in slug or "school-aged" in slug:
        return True
    parts = slug.split("-")
    if any(part in _AGENCY_TOKENS for part in parts):
        return True
    tail = parts[-1]
    if tail in _DESCRIPTION_TAILS and slug != tail:
        return True
    if slots:
        slot_slugs = {slugify(slot) for slot in slots if str(slot).strip()}
        if slug in slot_slugs:
            return True
    return False


def coerce_actor(raw: str, *, event_type: str, slots: list[str] | None = None) -> str:
    actor = str(raw or "").strip()
    if not actor:
        return ""
    slug = slugify(actor)
    if event_type in PERSON_ACTOR_TYPES and person_actor_rejected(actor, slots=slots):
        return ""
    if event_type == "study_publication" and slug in _JOURNAL_ACTOR_SLUGS:
        return ""
    return actor


def coerce_place_scope(raw: str, *, location: dict[str, Any], region: str) -> str:
    scope = coerce_choice(str(raw or ""), PLACE_SCOPES, "")
    has_country = bool(str(location.get("country") or "").strip())
    has_finer = any(str(location.get(level) or "").strip() for level in ("site", "locality", "admin2", "admin1"))
    has_region = bool(str(region or "").strip())
    if not has_country and not has_finer and not has_region:
        return "global" if scope == "global" else "unplaced"
    if has_region and not has_country and not has_finer:
        return "region"
    if scope in PLACE_SCOPES:
        return scope
    if has_country and not has_finer:
        return "countrywide"
    return "at_named_place"


def location_with_region(location: dict[str, Any], region: str) -> dict[str, Any]:
    loc = normalize_location(location)
    text = str(region or "").strip()
    if text:
        loc = dict(loc)
        loc["region"] = text
    return loc


def mint_mention_id(
    *,
    corpus: str,
    post_id: Any,
    chunk_index: Any,
    title: str,
    location: dict[str, Any],
    existing: set[str] | None = None,
) -> str:
    loc = normalize_location(location)
    place = slugify(" ".join(str(loc.get(k) or "") for k in (*PLACE_LEVELS, "region", "label")))
    raw = f"{corpus}|{post_id}|{int(chunk_index or 0)}|{slugify(title)}|{place}"
    base = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    if not existing or base not in existing:
        return base
    for n in range(2, 1000):
        candidate = f"{base}-{n}"
        if candidate not in existing:
            return candidate
    return f"{base}-x"


@dataclass
class Mention:
    mention_id: str
    event_type: str
    title: str
    summary: str | None = None
    mention_kind: str = "event"
    role_in_post: str = "focal"
    temporal_kind: str = "unknown"
    stated_start: str | None = None
    stated_end: str | None = None
    date_precision: str = "unknown"
    date_grain: str = "unknown"
    span_status: str = "open"
    stated_date_text: str = ""
    location: dict[str, Any] = field(default_factory=dict)
    region: str = ""
    admin1_other: list[str] = field(default_factory=list)
    place_scope: str = ""
    study_action: str = ""
    actor: str = ""
    diseases: list[str] = field(default_factory=list)
    vaccines: list[str] = field(default_factory=list)
    people: list[str] = field(default_factory=list)
    organizations: list[str] = field(default_factory=list)
    case_count: str = ""
    death_count: str = ""
    quotes: list[str] = field(default_factory=list)
    corpus: str = ""
    post_id: int | str | None = None
    platform: str = ""
    chunk_index: int = 0
    task_id: str = ""
    post_created_at: str | None = None
    url: str | None = None
    date_relation: str = "mentioned_at"
    recorded_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["location"] = location_with_region(self.location, self.region)
        if self.post_id is None:
            out.pop("post_id", None)
        if not self.url:
            out.pop("url", None)
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Mention:
        return cls(
            mention_id=str(data.get("mention_id") or ""),
            event_type=coerce_event_type(str(data.get("event_type") or "")),
            title=str(data.get("title") or ""),
            summary=(str(data["summary"]).strip() if data.get("summary") else None),
            mention_kind=coerce_mention_kind(str(data.get("mention_kind") or "")),
            role_in_post=coerce_role(str(data.get("role_in_post") or "")),
            temporal_kind=coerce_choice(str(data.get("temporal_kind") or ""), TEMPORAL_KINDS, "unknown"),
            stated_start=data.get("stated_start") or None,
            stated_end=data.get("stated_end") or None,
            date_precision=coerce_choice(str(data.get("date_precision") or ""), DATE_PRECISIONS, "unknown"),
            date_grain=coerce_date_grain(str(data.get("date_grain") or "")),
            span_status=coerce_span_status(str(data.get("span_status") or "")),
            stated_date_text=str(data.get("stated_date_text") or ""),
            location=location_with_region(
                data.get("location") or {},
                str(data.get("region") or (data.get("location") or {}).get("region") or ""),
            ),
            region=str(data.get("region") or (data.get("location") or {}).get("region") or "").strip(),
            admin1_other=[str(x).strip() for x in (data.get("admin1_other") or []) if str(x).strip()],
            place_scope=coerce_choice(str(data.get("place_scope") or ""), PLACE_SCOPES, ""),
            study_action=coerce_choice(str(data.get("study_action") or ""), STUDY_ACTIONS, ""),
            actor=str(data.get("actor") or ""),
            diseases=_str_list(data.get("diseases")),
            vaccines=_str_list(data.get("vaccines")),
            people=_str_list(data.get("people")),
            organizations=_str_list(data.get("organizations")),
            case_count=str(data.get("case_count") or ""),
            death_count=str(data.get("death_count") or ""),
            quotes=[str(q).strip() for q in (data.get("quotes") or []) if str(q).strip()],
            corpus=str(data.get("corpus") or ""),
            post_id=data.get("post_id"),
            platform=str(data.get("platform") or ""),
            chunk_index=int(data.get("chunk_index") or 0),
            task_id=str(data.get("task_id") or ""),
            post_created_at=data.get("post_created_at"),
            url=data.get("url"),
            date_relation=coerce_choice(str(data.get("date_relation") or ""), DATE_RELATIONS, "mentioned_at"),
            recorded_at=str(data.get("recorded_at") or ""),
        )


def mention_from_extract(
    item: dict[str, Any],
    *,
    doc: dict[str, Any],
    existing_ids: set[str],
) -> Mention | None:
    quotes = [str(q).strip() for q in (item.get("quotes") or []) if str(q).strip()]
    source = evidence_text(title=doc.get("title"), body=doc.get("body"))
    quotes = [q for q in quotes if quote_in_source(q, source)][:3]
    if not quotes:
        return None
    loc = {level: item.get(level) for level in PLACE_LEVELS}
    loc["label"] = item.get("location_label") or item.get("label")
    region = str(item.get("region") or "").strip()
    loc = location_with_region(loc, region)
    others = [str(x).strip() for x in (item.get("admin1_other") or []) if str(x).strip()]
    start, end, precision, kind, date_text = coerce_dates(
        stated_start=item.get("stated_start"),
        stated_end=item.get("stated_end"),
        date_precision=item.get("date_precision"),
        temporal_kind=item.get("temporal_kind") or "",
        stated_date_text=item.get("stated_date_text"),
    )
    date_grain = precision if precision in ("day", "month", "year") else "unknown"
    start, end, precision, kind = expand_stated_range(
        stated_start=start,
        stated_end=end,
        date_precision=precision,
        temporal_kind=kind,
        posted_at=doc.get("posted_at") or doc.get("post_created_at"),
    )
    span_status = coerce_span_status(str(item.get("span_status") or ""))
    title = str(item.get("title") or "").strip()
    if not title:
        return None
    summary = str(item.get("summary") or "").strip() or None
    event_type = coerce_event_type_from_text(
        str(item.get("event_type") or ""), title=title, summary=summary
    )
    slots = place_slots_from(loc, region)
    people = _str_list(item.get("people"))
    if event_type in PERSON_ACTOR_TYPES:
        people = [person for person in people if not person_actor_rejected(person, slots=slots)]
    mid = mint_mention_id(
        corpus=str(doc.get("corpus") or ""),
        post_id=doc.get("post_id"),
        chunk_index=doc.get("chunk_index"),
        title=title,
        location=loc,
        existing=existing_ids,
    )
    relation = infer_date_relation(
        date_precision=precision, temporal_kind=kind, stated_start=start
    )
    return Mention(
        mention_id=mid,
        event_type=event_type,
        title=title,
        summary=summary,
        mention_kind=coerce_mention_kind(str(item.get("mention_kind") or "")),
        role_in_post=coerce_role(str(item.get("role_in_post") or "")),
        temporal_kind=kind,
        stated_start=start,
        stated_end=end,
        date_precision=precision,
        date_grain=date_grain,
        span_status=span_status,
        stated_date_text=date_text,
        location=loc,
        region=region,
        admin1_other=others,
        place_scope=coerce_place_scope(str(item.get("place_scope") or ""), location=loc, region=region),
        study_action=coerce_choice(str(item.get("study_action") or ""), STUDY_ACTIONS, ""),
        actor=coerce_actor(str(item.get("actor") or ""), event_type=event_type, slots=slots),
        diseases=_str_list(item.get("diseases")),
        vaccines=_str_list(item.get("vaccines")),
        people=people,
        organizations=_str_list(item.get("organizations")),
        case_count=str(item.get("case_count") or "").strip(),
        death_count=str(item.get("death_count") or "").strip(),
        quotes=quotes,
        corpus=str(doc.get("corpus") or ""),
        post_id=doc.get("post_id"),
        platform=str(doc.get("platform") or ""),
        chunk_index=int(doc.get("chunk_index") or 0),
        task_id=str(doc.get("task_id") or ""),
        post_created_at=doc.get("posted_at") or doc.get("post_created_at"),
        url=doc.get("url"),
        date_relation=relation,
        recorded_at=utc_now(),
    )
