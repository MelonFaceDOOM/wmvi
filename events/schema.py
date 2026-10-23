"""Event record schema for the curated vaccine-event history.

Field names and vocabularies mirror ``db/potential_migrations/026_event_history.sql``
so a catalog file loads into ``events.event`` / ``events.event_source`` /
``events.event_subject`` without translation.

Stdlib only, and no knowledge of where events came from: the claims pipeline,
news scrapes, and a future DB loader all use these records.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

# Slug rule shared with taxonomy.subject.slug in migration 026.
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

MAX_KEY_LEN = 96

EVENT_TYPES = (
    "outbreak",
    "exposure",
    "death",
    "policy_change",
    "regulatory_action",
    "study_publication",
    "legal_action",
    "personnel_change",
    "other",
)

# Subset of the SQL vocabulary. 'exact' is omitted as redundant with 'day'.
DATE_PRECISIONS = ("day", "month", "year", "range", "unknown")

# How time works for this happening. Independent of date_precision (grain).
TEMPORAL_KINDS = ("instant", "interval", "open", "unknown")

# Finest-first place slots on location.
PLACE_LEVELS = ("site", "locality", "admin2", "admin1", "country")

# How a source's timestamp relates to the event.
DATE_RELATIONS = ("started_by", "happened_by", "mentioned_at", "ended_by")

EVENT_STATUSES = ("active", "resolved", "superseded", "merged", "disputed")

SUBJECT_RELATIONSHIPS = ("primary", "related", "implicated")

SUBJECT_KINDS = (
    "disease",
    "vaccine",
    "pathogen",
    "policy_topic",
    "organization",
    "person",
    "other",
)

TERM_RELATIONSHIPS = ("canonical", "alias", "discovery")

SOURCE_TYPES = ("claim", "post", "web_document", "api_record")

# events.event_source.evidence_role
EVIDENCE_ROLES = ("primary", "corroboration", "contradiction", "mention")

REVIEW_STATES = ("unreviewed", "confirmed", "corrected", "rejected")

# Locator fields required per source_type, mirroring source_item_locator_chk.
_SOURCE_REQUIRED: dict[str, tuple[str, ...]] = {
    "claim": ("corpus", "claim_key"),
    "post": ("platform", "post_id"),
    "web_document": ("url",),
    "api_record": ("provider", "external_id"),
}

_SOURCE_OPTIONAL = (
    "corpus",
    "claim_key",
    "claim_text",
    "row_id",
    "post_id",
    "platform",
    "post_created_at",
    "url",
    "publication",
    "source_title",
    "provider",
    "external_id",
    "published_at",
    "stated_start",
    "stated_end",
    "stated_precision",
    "date_relation",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slugify(text: str, *, max_len: int = MAX_KEY_LEN) -> str:
    """Collapse arbitrary text to a SLUG_RE-valid slug (empty if nothing survives)."""
    norm = unicodedata.normalize("NFKD", str(text or ""))
    ascii_only = norm.encode("ascii", "ignore").decode("ascii").lower()
    parts = [p for p in re.split(r"[^a-z0-9]+", ascii_only) if p]
    slug = "-".join(parts)[:max_len]
    return slug.strip("-")


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return False
    return True


def normalize_location(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Keep known place slots; drop blanks."""
    src = dict(raw or {})
    out: dict[str, Any] = {}
    for key in (*PLACE_LEVELS, "label"):
        val = str(src.get(key) or "").strip()
        if val:
            out[key] = val
    return out


def finest_place_level(location: dict[str, Any] | None) -> str | None:
    loc = location or {}
    for level in PLACE_LEVELS:
        if str(loc.get(level) or "").strip():
            return level
    return None


def location_label(location: dict[str, Any] | None) -> str:
    loc = location or {}
    if loc.get("label"):
        return str(loc["label"])
    parts = [str(loc[k]) for k in PLACE_LEVELS if loc.get(k)]
    return ", ".join(reversed(parts))


@dataclass
class EventSubject:
    """One taxonomy.subject classification for an event."""

    slug: str
    relationship: str = "related"
    confidence: float | None = None
    assigned_by: str = "llm"

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"slug": self.slug, "relationship": self.relationship}
        if self.confidence is not None:
            out["confidence"] = round(float(self.confidence), 4)
        out["assigned_by"] = self.assigned_by
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventSubject:
        return cls(
            slug=str(data.get("slug") or ""),
            relationship=str(data.get("relationship") or "related"),
            confidence=(
                float(data["confidence"]) if data.get("confidence") is not None else None
            ),
            assigned_by=str(data.get("assigned_by") or "llm"),
        )

    def errors(self) -> list[str]:
        out: list[str] = []
        if not SLUG_RE.match(self.slug or ""):
            out.append(f"subject slug {self.slug!r} is not a valid slug")
        if self.relationship not in SUBJECT_RELATIONSHIPS:
            out.append(f"subject relationship {self.relationship!r} not in {SUBJECT_RELATIONSHIPS}")
        if self.confidence is not None and not 0.0 <= float(self.confidence) <= 1.0:
            out.append(f"subject confidence {self.confidence!r} outside [0, 1]")
        return out


@dataclass
class EventSource:
    """One piece of evidence for an event.

    ``source_type`` discriminates which locator fields are meaningful; the rest
    stay unset so a claim-derived event and a news-derived event share one list.
    """

    source_type: str
    role: str = "corroboration"
    is_discovery: bool = False
    confidence: float | None = None
    support_excerpt: str | None = None

    corpus: str | None = None
    claim_key: str | None = None
    claim_text: str | None = None
    row_id: str | None = None
    post_id: int | None = None
    platform: str | None = None
    post_created_at: str | None = None
    url: str | None = None
    publication: str | None = None
    source_title: str | None = None
    provider: str | None = None
    external_id: str | None = None
    published_at: str | None = None

    stated_start: str | None = None
    stated_end: str | None = None
    stated_precision: str | None = None
    date_relation: str | None = None

    recorded_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def identity(self) -> tuple[str, ...]:
        """Dedupe key: the locator fields required for this source_type."""
        required = _SOURCE_REQUIRED.get(self.source_type, ())
        vals = [str(getattr(self, name, "") or "") for name in required]
        # A claim occurrence is distinct per row_id when one is recorded.
        if self.source_type == "claim" and self.row_id:
            vals.append(str(self.row_id))
        return (self.source_type, *vals)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"source_type": self.source_type, "role": self.role}
        if self.is_discovery:
            out["is_discovery"] = True
        for name in _SOURCE_OPTIONAL:
            val = getattr(self, name, None)
            if val is not None and val != "":
                out[name] = val
        if self.confidence is not None:
            out["confidence"] = round(float(self.confidence), 4)
        if self.support_excerpt:
            out["support_excerpt"] = self.support_excerpt
        out["recorded_at"] = self.recorded_at or utc_now()
        if self.metadata:
            out["metadata"] = self.metadata
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventSource:
        kwargs: dict[str, Any] = {
            name: data.get(name) for name in _SOURCE_OPTIONAL if data.get(name) is not None
        }
        if kwargs.get("post_id") is not None:
            kwargs["post_id"] = int(kwargs["post_id"])
        return cls(
            source_type=str(data.get("source_type") or ""),
            role=str(data.get("role") or "corroboration"),
            is_discovery=bool(data.get("is_discovery") or False),
            confidence=(
                float(data["confidence"]) if data.get("confidence") is not None else None
            ),
            support_excerpt=data.get("support_excerpt"),
            recorded_at=str(data.get("recorded_at") or ""),
            metadata=dict(data.get("metadata") or {}),
            **kwargs,
        )

    def errors(self) -> list[str]:
        out: list[str] = []
        if self.source_type not in SOURCE_TYPES:
            out.append(f"source_type {self.source_type!r} not in {SOURCE_TYPES}")
            return out
        if self.role not in EVIDENCE_ROLES:
            out.append(f"source role {self.role!r} not in {EVIDENCE_ROLES}")
        for name in _SOURCE_REQUIRED[self.source_type]:
            if getattr(self, name, None) in (None, ""):
                out.append(f"source_type {self.source_type!r} requires {name}")
        if self.confidence is not None and not 0.0 <= float(self.confidence) <= 1.0:
            out.append(f"source confidence {self.confidence!r} outside [0, 1]")
        if self.date_relation and self.date_relation not in DATE_RELATIONS:
            out.append(f"date_relation {self.date_relation!r} not in {DATE_RELATIONS}")
        if self.stated_precision and self.stated_precision not in DATE_PRECISIONS:
            out.append(f"stated_precision {self.stated_precision!r} not in {DATE_PRECISIONS}")
        for name in ("stated_start", "stated_end"):
            val = getattr(self, name)
            if val is not None and val != "" and not _is_iso_date(val):
                out.append(f"{name} {val!r} is not an ISO date (YYYY-MM-DD)")
        return out


@dataclass
class EventReview:
    """Human curation state. File-mode only; not part of the DB schema."""

    state: str = "unreviewed"
    reviewed_by: str | None = None
    reviewed_at: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"state": self.state}
        for name in ("reviewed_by", "reviewed_at", "notes"):
            val = getattr(self, name)
            if val:
                out[name] = val
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventReview:
        return cls(
            state=str(data.get("state") or "unreviewed"),
            reviewed_by=data.get("reviewed_by"),
            reviewed_at=data.get("reviewed_at"),
            notes=data.get("notes"),
        )

    def errors(self) -> list[str]:
        if self.state not in REVIEW_STATES:
            return [f"review state {self.state!r} not in {REVIEW_STATES}"]
        return []


@dataclass
class EventRecord:
    """One real-world event.

    ``event_key`` is the permanent identity: immutable once assigned, since
    claim→event links reference it. Every other field is freely correctable.
    """

    event_key: str
    event_type: str
    title: str
    summary: str | None = None

    start_date: str | None = None
    end_date: str | None = None
    date_precision: str = "unknown"
    temporal_kind: str = "unknown"
    location: dict[str, Any] = field(default_factory=dict)
    place_level: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    subjects: list[EventSubject] = field(default_factory=list)

    status: str = "active"
    merged_into: str | None = None

    review: EventReview = field(default_factory=EventReview)
    provenance: list[EventSource] = field(default_factory=list)

    created_at: str = ""
    updated_at: str = ""

    @property
    def first_mention_at(self) -> str | None:
        """Earliest timestamp across provenance.

        For social-media evidence this is an upper bound on the event start,
        not the start itself.
        """
        stamps = [
            str(s.post_created_at or s.published_at)
            for s in self.provenance
            if s.post_created_at or s.published_at
        ]
        return min(stamps) if stamps else None

    @property
    def discovery_source(self) -> EventSource | None:
        for src in self.provenance:
            if src.is_discovery:
                return src
        return self.provenance[0] if self.provenance else None

    def claim_keys(self, *, corpus: str | None = None) -> list[str]:
        out: list[str] = []
        for src in self.provenance:
            if src.source_type != "claim" or not src.claim_key:
                continue
            if corpus is not None and src.corpus != corpus:
                continue
            if src.claim_key not in out:
                out.append(src.claim_key)
        return out

    def to_dict(self) -> dict[str, Any]:
        """Fixed key order so catalog rewrites produce stable diffs."""
        return {
            "event_key": self.event_key,
            "event_type": self.event_type,
            "title": self.title,
            "summary": self.summary,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "date_precision": self.date_precision,
            "temporal_kind": self.temporal_kind,
            "location": normalize_location(self.location),
            "place_level": self.place_level,
            "attributes": self.attributes,
            "subjects": [s.to_dict() for s in self.subjects],
            "status": self.status,
            "merged_into": self.merged_into,
            "review": self.review.to_dict(),
            "provenance": [s.to_dict() for s in self.provenance],
            "created_at": self.created_at or utc_now(),
            "updated_at": self.updated_at or utc_now(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventRecord:
        return cls(
            event_key=str(data.get("event_key") or ""),
            event_type=str(data.get("event_type") or ""),
            title=str(data.get("title") or ""),
            summary=data.get("summary"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            date_precision=str(data.get("date_precision") or "unknown"),
            temporal_kind=str(data.get("temporal_kind") or "unknown"),
            location=normalize_location(data.get("location") or {}),
            place_level=(str(data["place_level"]) if data.get("place_level") else None),
            attributes=dict(data.get("attributes") or {}),
            subjects=[
                EventSubject.from_dict(s) for s in (data.get("subjects") or []) if isinstance(s, dict)
            ],
            status=str(data.get("status") or "active"),
            merged_into=data.get("merged_into"),
            review=EventReview.from_dict(data.get("review") or {}),
            provenance=[
                EventSource.from_dict(s)
                for s in (data.get("provenance") or [])
                if isinstance(s, dict)
            ],
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
        )


def validate_event(record: EventRecord) -> list[str]:
    """Return human-readable violations; empty list means the record is loadable.

    Checks mirror the constraints on ``events.event`` plus the file-mode
    date_precision semantics documented in ``events/README.md``.
    """
    out: list[str] = []
    key = record.event_key

    if not SLUG_RE.match(key or "") or len(key) > MAX_KEY_LEN:
        out.append(f"event_key {key!r} must match {SLUG_RE.pattern} and be <= {MAX_KEY_LEN} chars")
    if record.event_type not in EVENT_TYPES:
        out.append(f"event_type {record.event_type!r} not in {EVENT_TYPES}")
    if not (record.title or "").strip():
        out.append("title must not be blank")
    if record.date_precision not in DATE_PRECISIONS:
        out.append(f"date_precision {record.date_precision!r} not in {DATE_PRECISIONS}")
    if record.temporal_kind not in TEMPORAL_KINDS:
        out.append(f"temporal_kind {record.temporal_kind!r} not in {TEMPORAL_KINDS}")
    if record.status not in EVENT_STATUSES:
        out.append(f"status {record.status!r} not in {EVENT_STATUSES}")

    # event_merge_state_chk / event_not_merged_into_self_chk
    if (record.status == "merged") != (record.merged_into is not None):
        out.append("status 'merged' and merged_into must be set together")
    if record.merged_into is not None:
        if record.merged_into == key:
            out.append("merged_into must not equal event_key")
        elif not SLUG_RE.match(str(record.merged_into)):
            out.append(f"merged_into {record.merged_into!r} is not a valid event_key")

    for name in ("start_date", "end_date"):
        val = getattr(record, name)
        if val is not None and not _is_iso_date(val):
            out.append(f"{name} {val!r} is not an ISO date (YYYY-MM-DD)")

    # event_date_order_chk
    if (
        record.start_date
        and record.end_date
        and _is_iso_date(record.start_date)
        and _is_iso_date(record.end_date)
        and date.fromisoformat(record.end_date) < date.fromisoformat(record.start_date)
    ):
        out.append("end_date must not precede start_date")

    # File-mode date_precision semantics.
    if record.date_precision == "unknown" and (record.start_date or record.end_date):
        out.append("date_precision 'unknown' requires no start_date/end_date")
    if record.date_precision == "range" and not (record.start_date and record.end_date):
        out.append("date_precision 'range' requires both start_date and end_date")
    if record.date_precision in ("day", "month", "year") and not record.start_date:
        out.append(f"date_precision {record.date_precision!r} requires start_date")
    if record.temporal_kind == "instant" and record.end_date and record.end_date != record.start_date:
        out.append("temporal_kind 'instant' requires no distinct end_date")
    if record.temporal_kind == "open" and record.end_date:
        out.append("temporal_kind 'open' requires no end_date")

    loc = record.location if isinstance(record.location, dict) else None
    if loc is None:
        out.append("location must be an object")
    else:
        unknown_place = set(loc) - set(PLACE_LEVELS) - {"label"}
        if unknown_place:
            out.append(f"location has unknown keys {sorted(unknown_place)}")
    if record.place_level is not None and record.place_level not in PLACE_LEVELS:
        out.append(f"place_level {record.place_level!r} not in {PLACE_LEVELS}")
    if not isinstance(record.attributes, dict):
        out.append("attributes must be an object")

    seen_subjects: set[str] = set()
    n_primary = 0
    for subj in record.subjects:
        out.extend(subj.errors())
        if subj.slug in seen_subjects:
            out.append(f"duplicate subject {subj.slug!r}")
        seen_subjects.add(subj.slug)
        if subj.relationship == "primary":
            n_primary += 1
    if n_primary > 1:
        out.append("at most one subject may have relationship 'primary'")

    seen_sources: set[tuple[str, ...]] = set()
    n_discovery = 0
    for src in record.provenance:
        out.extend(src.errors())
        ident = src.identity()
        if ident in seen_sources:
            out.append(f"duplicate provenance entry {ident}")
        seen_sources.add(ident)
        if src.is_discovery:
            n_discovery += 1
    if n_discovery > 1:
        out.append("at most one provenance entry may be is_discovery")

    out.extend(record.review.errors())
    return [f"{key or '<no event_key>'}: {msg}" for msg in out]


@dataclass
class Subject:
    """A taxonomy.subject row plus its taxonomy.subject_term links."""

    slug: str
    name: str
    kind: str
    description: str | None = None
    is_active: bool = True
    # term name -> relationship, resolved to taxonomy.vaccine_term ids at load time
    terms: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "is_active": self.is_active,
            "terms": self.terms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Subject:
        return cls(
            slug=str(data.get("slug") or ""),
            name=str(data.get("name") or ""),
            kind=str(data.get("kind") or "other"),
            description=data.get("description"),
            is_active=bool(data.get("is_active", True)),
            terms={str(k): str(v) for k, v in dict(data.get("terms") or {}).items()},
        )


def validate_subject(subject: Subject) -> list[str]:
    out: list[str] = []
    if not SLUG_RE.match(subject.slug or ""):
        out.append(f"subject slug {subject.slug!r} is not a valid slug")
    if not (subject.name or "").strip():
        out.append("subject name must not be blank")
    if subject.kind not in SUBJECT_KINDS:
        out.append(f"subject kind {subject.kind!r} not in {SUBJECT_KINDS}")
    for term, rel in subject.terms.items():
        if rel not in TERM_RELATIONSHIPS:
            out.append(f"term {term!r} relationship {rel!r} not in {TERM_RELATIONSHIPS}")
    return [f"{subject.slug or '<no slug>'}: {msg}" for msg in out]
