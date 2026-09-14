"""Curated vaccine-event history: record schema and catalog store.

Provenance-agnostic by design. The claims pipeline
(``apps/claims/events/``), future news ingest, and a future DB loader all
build on these records rather than each other.

Public API::

    from events import EventRecord, EventSource, EventSubject, load_events

See ``events/README.md`` for the file format and the path to migration 026.
"""

from __future__ import annotations

from events.schema import (
    DATE_PRECISIONS,
    EVENT_STATUSES,
    EVENT_TYPES,
    EVIDENCE_ROLES,
    SOURCE_TYPES,
    SUBJECT_KINDS,
    SUBJECT_RELATIONSHIPS,
    EventRecord,
    EventReview,
    EventSource,
    EventSubject,
    Subject,
    slugify,
    validate_event,
    validate_subject,
)
from events.store import (
    CatalogError,
    active_events,
    add_source,
    claim_key_links,
    events_path,
    index_by_key,
    load_events,
    load_subjects,
    merge_events,
    mint_event_key,
    resolve_key,
    subjects_path,
    unknown_subject_slugs,
    upsert_event,
    write_events,
    write_subjects,
)

__all__ = [
    "DATE_PRECISIONS",
    "EVENT_STATUSES",
    "EVENT_TYPES",
    "EVIDENCE_ROLES",
    "SOURCE_TYPES",
    "SUBJECT_KINDS",
    "SUBJECT_RELATIONSHIPS",
    "CatalogError",
    "EventRecord",
    "EventReview",
    "EventSource",
    "EventSubject",
    "Subject",
    "active_events",
    "add_source",
    "claim_key_links",
    "events_path",
    "index_by_key",
    "load_events",
    "load_subjects",
    "merge_events",
    "mint_event_key",
    "resolve_key",
    "slugify",
    "subjects_path",
    "unknown_subject_slugs",
    "upsert_event",
    "validate_event",
    "validate_subject",
    "write_events",
    "write_subjects",
]
