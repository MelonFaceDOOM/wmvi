"""Event schema round-trip, validation, and catalog store behavior."""

from __future__ import annotations

import json

import pytest

from events import schema, store


def _event(**overrides) -> schema.EventRecord:
    base = {
        "event_key": "west-texas-measles-outbreak-2025",
        "event_type": "outbreak",
        "title": "West Texas measles outbreak",
        "summary": "Sustained measles transmission centered on Gaines County, Texas.",
        "start_date": "2025-01-01",
        "date_precision": "month",
        "location": {"country": "US", "admin1": "TX"},
        "subjects": [schema.EventSubject(slug="measles", relationship="primary")],
    }
    base.update(overrides)
    return schema.EventRecord(**base)


def _claim_source(**overrides) -> schema.EventSource:
    base = {
        "source_type": "claim",
        "corpus": "measles2",
        "claim_key": "a1b2c3d4e5f60718",
        "claim_text": "There is a measles outbreak in West Texas.",
        "row_id": "4919:0",
        "post_id": 4919,
        "platform": "reddit_submission",
        "post_created_at": "2025-02-14T23:24:23+00:00",
        "role": "primary",
        "is_discovery": True,
        "confidence": 0.86,
    }
    base.update(overrides)
    return schema.EventSource(**base)


# --- schema ---------------------------------------------------------------


def test_event_round_trips_through_dict():
    rec = _event()
    store.add_source(rec, _claim_source())

    restored = schema.EventRecord.from_dict(json.loads(json.dumps(rec.to_dict())))

    assert restored.to_dict() == rec.to_dict()
    assert restored.provenance[0].claim_key == "a1b2c3d4e5f60718"
    assert restored.provenance[0].post_id == 4919
    assert restored.subjects[0].relationship == "primary"


def test_valid_event_has_no_errors():
    rec = _event()
    store.add_source(rec, _claim_source())
    assert schema.validate_event(rec) == []


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"event_key": "West Texas 2025"}, "must match"),
        ({"event_type": "epidemic"}, "event_type"),
        ({"title": "   "}, "blank"),
        ({"date_precision": "exact"}, "date_precision"),
        ({"status": "merged"}, "merged_into must be set together"),
        (
            {"merged_into": "other-event", "status": "active"},
            "merged_into must be set together",
        ),
        (
            {"status": "merged", "merged_into": "west-texas-measles-outbreak-2025"},
            "must not equal event_key",
        ),
        ({"start_date": "2025-13-99"}, "ISO date"),
        (
            {"start_date": "2025-06-01", "end_date": "2025-01-01", "date_precision": "range"},
            "must not precede",
        ),
        ({"date_precision": "unknown"}, "requires no start_date"),
        ({"date_precision": "range"}, "requires both"),
        ({"date_precision": "day", "start_date": None}, "requires start_date"),
        ({"temporal_kind": "bogus"}, "temporal_kind"),
        (
            {"temporal_kind": "open", "end_date": "2025-06-01", "date_precision": "range"},
            "no end_date",
        ),
        ({"place_level": "continent"}, "place_level"),
    ],
)
def test_validation_catches_constraint_violations(overrides, expected):
    errors = schema.validate_event(_event(**overrides))
    assert any(expected in e for e in errors), errors


def test_one_primary_subject_and_one_discovery_source():
    rec = _event(
        subjects=[
            schema.EventSubject(slug="measles", relationship="primary"),
            schema.EventSubject(slug="mmr-vaccine", relationship="primary"),
        ]
    )
    rec.provenance = [
        _claim_source(),
        _claim_source(claim_key="ffff000011112222", row_id="5000:1"),
    ]
    errors = schema.validate_event(rec)
    assert any("one subject" in e for e in errors), errors
    assert any("is_discovery" in e for e in errors), errors


def test_source_requires_locator_for_its_type():
    assert any(
        "requires url" in e for e in schema.EventSource(source_type="web_document").errors()
    )
    assert any(
        "requires claim_key" in e
        for e in schema.EventSource(source_type="claim", corpus="measles2").errors()
    )
    assert schema.EventSource(source_type="web_document", url="https://example.org/a").errors() == []


def test_slugify_collapses_punctuation_and_accents():
    assert schema.slugify("  Québec: MMR/Measles — 2025!! ") == "quebec-mmr-measles-2025"
    assert schema.slugify("!!!") == ""


def test_first_mention_is_earliest_provenance_timestamp():
    rec = _event()
    store.add_source(rec, _claim_source(post_created_at="2025-03-01T00:00:00+00:00"))
    store.add_source(
        rec,
        _claim_source(
            claim_key="bbbb0000cccc1111",
            row_id="777:2",
            post_created_at="2025-01-09T00:00:00+00:00",
            is_discovery=False,
        ),
    )
    assert rec.first_mention_at == "2025-01-09T00:00:00+00:00"


# --- store ----------------------------------------------------------------


def test_add_source_dedupes_on_locator_identity():
    rec = _event()
    assert store.add_source(rec, _claim_source()) is True
    assert store.add_source(rec, _claim_source(confidence=0.2)) is False
    assert store.add_source(rec, _claim_source(row_id="4919:1")) is True
    assert len(rec.provenance) == 2

    # Only the first discovery source keeps the flag.
    assert [s.is_discovery for s in rec.provenance] == [True, False]


def test_mint_event_key_appends_year_and_disambiguates():
    assert (
        store.mint_event_key("West Texas measles outbreak", existing=[], start_date="2025-01-01")
        == "west-texas-measles-outbreak-2025"
    )
    # Year already present is not duplicated.
    assert (
        store.mint_event_key("2025 measles outbreak", existing=[], start_date="2025-01-01")
        == "2025-measles-outbreak"
    )
    assert (
        store.mint_event_key("Measles outbreak", existing=["measles-outbreak"])
        == "measles-outbreak-2"
    )
    assert store.mint_event_key("!!!", existing=[]) == "event"


def test_write_then_load_is_stable_and_ordered(tmp_path):
    path = tmp_path / "events.jsonl"
    undated = _event(
        event_key="hhs-vaccine-policy-shift",
        event_type="policy_change",
        title="HHS vaccine policy shift",
        start_date=None,
        date_precision="unknown",
    )
    early = _event(event_key="measles-outbreak-ohio-2022", start_date="2022-05-01")

    store.write_events([undated, _event(), early], path)
    loaded = store.load_events(path)

    # Chronological, undated last.
    assert [r.event_key for r in loaded] == [
        "measles-outbreak-ohio-2022",
        "west-texas-measles-outbreak-2025",
        "hhs-vaccine-policy-shift",
    ]

    first_bytes = path.read_bytes()
    store.write_events(loaded, path)
    assert path.read_bytes() == first_bytes


def test_load_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(_event().to_dict()) + "\n" + json.dumps(_event().to_dict()) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(store.CatalogError, match="duplicate event_key"):
        store.load_events(path)


def test_load_rejects_dangling_merge_target(tmp_path):
    path = tmp_path / "events.jsonl"
    orphan = _event(status="merged", merged_into="does-not-exist")
    path.write_text(json.dumps(orphan.to_dict()) + "\n", encoding="utf-8")
    with pytest.raises(store.CatalogError, match="not in the catalog"):
        store.load_events(path)


def test_write_refuses_invalid_record(tmp_path):
    path = tmp_path / "events.jsonl"
    with pytest.raises(store.CatalogError, match="event_type"):
        store.write_events([_event(event_type="nope")], path)
    assert not path.exists()


def test_upsert_preserves_created_at_and_identity():
    records = [_event(created_at="2026-01-01T00:00:00Z")]
    replacement = _event(title="West Texas measles outbreak (2025)")

    store.upsert_event(records, replacement)

    assert len(records) == 1
    assert records[0].created_at == "2026-01-01T00:00:00Z"
    assert records[0].title.endswith("(2025)")
    assert records[0].updated_at != "2026-01-01T00:00:00Z"


def test_merge_moves_provenance_and_resolves_through_chain():
    winner = _event()
    loser = _event(event_key="texas-measles-cases-2025", start_date="2025-02-01")
    store.add_source(winner, _claim_source())
    store.add_source(loser, _claim_source(claim_key="9999888877776666", row_id="6001:0"))
    records = [winner, loser]

    store.merge_events(records, source_key=loser.event_key, into_key=winner.event_key)

    assert loser.status == "merged"
    assert loser.merged_into == winner.event_key
    assert loser.provenance == []
    assert winner.claim_keys(corpus="measles2") == ["a1b2c3d4e5f60718", "9999888877776666"]
    assert [s.is_discovery for s in winner.provenance] == [True, False]

    # Merging into an already-merged event follows the chain to the survivor.
    third = _event(event_key="gaines-county-measles-2025", start_date="2025-03-01")
    records.append(third)
    store.merge_events(records, source_key=third.event_key, into_key=loser.event_key)
    assert third.merged_into == winner.event_key

    index = store.index_by_key(records)
    assert store.resolve_key(index, third.event_key) == winner.event_key
    assert store.active_events(records) == [winner]


def test_merge_rejects_cycles_and_unknown_keys():
    winner = _event()
    loser = _event(event_key="texas-measles-cases-2025", start_date="2025-02-01")
    records = [winner, loser]
    store.merge_events(records, source_key=loser.event_key, into_key=winner.event_key)

    with pytest.raises(store.CatalogError, match="cycle"):
        store.merge_events(records, source_key=winner.event_key, into_key=loser.event_key)
    with pytest.raises(store.CatalogError, match="unknown event_key"):
        store.merge_events(records, source_key="nope", into_key=winner.event_key)


def test_claim_key_links_are_annotation_shaped():
    winner = _event()
    loser = _event(event_key="texas-measles-cases-2025", start_date="2025-02-01")
    store.add_source(winner, _claim_source())
    store.add_source(loser, _claim_source(claim_key="9999888877776666", row_id="6001:0"))
    store.add_source(
        loser,
        _claim_source(corpus="resp", claim_key="1111222233334444", row_id="70:0", is_discovery=False),
    )
    records = [winner, loser]
    store.merge_events(records, source_key=loser.event_key, into_key=winner.event_key)

    links = store.claim_key_links(records, corpus="measles2")

    assert links == {
        "a1b2c3d4e5f60718": "west-texas-measles-outbreak-2025",
        "9999888877776666": "west-texas-measles-outbreak-2025",
    }
    assert all(isinstance(v, str) for v in links.values())


# --- shipped catalog ------------------------------------------------------


def test_shipped_catalog_is_valid():
    events = store.load_events()
    subjects = store.load_subjects()

    assert {s.slug for s in subjects} >= {"measles"}
    assert store.unknown_subject_slugs(events, subjects) == []
