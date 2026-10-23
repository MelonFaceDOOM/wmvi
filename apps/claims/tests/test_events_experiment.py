"""Post-chunk event extraction: prompts, quotes, sampling, extract loop."""

from __future__ import annotations

from pathlib import Path

from apps.claims.events.documents import allocate_stratified, iter_documents, stratified_sample
from apps.claims.events.extract import remaining_docs, run_extract
from apps.claims.events.mention import (
    coerce_dates,
    expand_stated_range,
    mention_from_extract,
    quote_in_source,
)
from apps.claims.events.paths import EXTRACT_LOG_FILE
from apps.claims.events.prompts import TRIAGE_SCHEMA, format_user_prompt
from apps.claims.events.type_prompts import DETAIL_SCHEMA, PLACE_AND_DATES, TYPE_INSTRUCTIONS, format_detail_prompt


def _doc(**overrides) -> dict:
    row = {
        "doc_id": "42:0",
        "corpus": "measles2",
        "post_id": 42,
        "chunk_index": 0,
        "task_id": "42:0",
        "platform": "reddit_submission",
        "posted_at": "2025-11-13T21:11:49+00:00",
        "title": "Measles in Texas",
        "body": "Yeah we saw measles warnings at the clinic yesterday here in Tx.",
        "channel": None,
        "thread_title": None,
        "url": "/r/x/42",
        "truncated": False,
    }
    row.update(overrides)
    return row


def _extract_item(**overrides) -> dict:
    item = {
        "event_type": "outbreak",
        "role_in_post": "focal",
        "title": "Texas clinic issued measles warnings",
        "summary": "A clinic in Texas issued measles warnings.",
        "temporal_kind": "instant",
        "stated_start": "2025-11-12",
        "stated_end": "",
        "date_precision": "day",
        "stated_date_text": "yesterday",
        "country": "United States",
        "admin1": "Texas",
        "admin2": "",
        "locality": "",
        "site": "clinic",
        "region": "",
        "location_label": "Texas",
        "place_scope": "at_named_place",
        "admin1_other": [],
        "actor": "",
        "mention_kind": "event",
        "case_count": "",
        "death_count": "",
        "quotes": ["measles warnings at the clinic yesterday here in Tx"],
    }
    item.update(overrides)
    return item


def _triage_stub(**overrides) -> dict:
    item = {
        "event_type": "outbreak",
        "mention_kind": "event",
        "title": "Texas clinic issued measles warnings",
        "summary": "A clinic in Texas issued measles warnings.",
        "diseases": ["measles"],
        "quotes": ["measles warnings at the clinic yesterday here in Tx"],
    }
    item.update(overrides)
    return item


def test_comment_prompt_omits_thread_title():
    doc = _doc(
        platform="reddit_comment",
        title=None,
        thread_title="Measles outbreak linked to a Florida university",
        body="This is terrifying for my newborn.",
    )
    text = format_user_prompt(doc=doc)
    assert "Florida university" not in text
    assert "This is terrifying for my newborn." in text
    assert "reddit_comment" in text


def test_submission_prompt_includes_title():
    text = format_user_prompt(doc=_doc())
    assert "Measles in Texas" in text
    assert "clinic yesterday" in text


def test_iter_documents_skips_empty_and_keeps_failed_claim_chunks():
    posts = [
        {
            "post_id": 1,
            "platform": "reddit_comment",
            "created_at_ts": "2025-01-01T00:00:00+00:00",
            "reddit_comment_submission_title": "Parent thread about Utah",
            "chunks": [
                {
                    "chunk_index": 0,
                    "text": "Utah has measles cases.",
                    "task_id": "1:0",
                    "claim_extraction_disposition": "terminal_failure",
                    "claims": [],
                }
            ],
        },
        {
            "post_id": 2,
            "platform": "reddit_submission",
            "reddit_submission_title": "Empty",
            "chunks": [{"chunk_index": 0, "text": "   ", "task_id": "2:0"}],
        },
    ]
    docs = iter_documents(posts, corpus="measles2")
    assert len(docs) == 1
    assert docs[0]["doc_id"] == "1:0"
    assert docs[0]["title"] is None
    assert docs[0]["thread_title"] == "Parent thread about Utah"
    assert docs[0]["body"] == "Utah has measles cases."


def test_quote_fail_drops_mention():
    rec = mention_from_extract(
        _extract_item(quotes=["this quote is not in the post at all"]),
        doc=_doc(),
        existing_ids=set(),
    )
    assert rec is None


def test_valid_quote_keeps_mention_and_dates():
    rec = mention_from_extract(_extract_item(), doc=_doc(), existing_ids=set())
    assert rec is not None
    assert rec.stated_start == "2025-11-12"
    assert rec.stated_end == "2025-11-12"
    assert rec.stated_date_text == "yesterday"
    assert rec.location.get("admin1") == "Texas"
    assert rec.location.get("site") == "clinic"
    assert rec.date_relation == "happened_by"
    assert rec.role_in_post == "focal"


def test_date_relation_inferred_not_copied_from_llm():
    rec = mention_from_extract(
        _extract_item(role_in_post="mentioned"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert rec is not None
    assert rec.date_relation == "happened_by"
    assert rec.role_in_post == "mentioned"
    undated = mention_from_extract(
        _extract_item(
            stated_start="",
            date_precision="unknown",
            stated_date_text="",
            temporal_kind="open",
            date_relation="started_by",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert undated is not None
    assert undated.date_relation == "mentioned_at"


def test_elimination_status_coerced_to_institutional_action():
    rec = mention_from_extract(
        _extract_item(
            event_type="other",
            title="Canada loses measles elimination status",
            summary="PHAC announced the loss of elimination status in 2025.",
            quotes=["Canada loses measles elimination status"],
        ),
        doc=_doc(title="Canada loses measles elimination status", body="Canada loses measles elimination status in 2025."),
        existing_ids=set(),
    )
    assert rec is not None
    assert rec.event_type == "institutional_action"
    declared = mention_from_extract(
        _extract_item(
            event_type="other",
            title="Measles eliminated in the United States in 2000",
            summary="CDC declared measles eliminated in the United States.",
            quotes=["Measles eliminated in the United States in 2000"],
        ),
        doc=_doc(
            title="Measles eliminated in the United States in 2000",
            body="Measles eliminated in the United States in 2000",
        ),
        existing_ids=set(),
    )
    assert declared is not None
    assert declared.event_type == "institutional_action"


def test_legacy_types_alias():
    death = mention_from_extract(
        _extract_item(event_type="death", title="Child died of measles in Texas"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert death is not None
    assert death.event_type == "notable_case"
    policy = mention_from_extract(
        _extract_item(event_type="policy_change", title="Florida ends school vaccine mandates"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert policy is not None
    assert policy.event_type == "institutional_action"


def test_missing_stated_date_text_downcasts_dates():
    start, end, precision, kind, text = coerce_dates(
        stated_start="2025-11-12",
        stated_end=None,
        date_precision="day",
        temporal_kind="instant",
        stated_date_text="",
    )
    assert start is None
    assert end is None
    assert precision == "unknown"
    assert kind == "instant"
    assert text == ""


def test_quote_in_source_normalizes_whitespace():
    assert quote_in_source("clinic   yesterday", "warnings at the clinic yesterday here")


def test_prompt_asks_for_public_happenings_and_counties():
    text = format_user_prompt(doc=_doc())
    assert "up to 4" in text
    assert "news agency" in text
    assert "Anchored" in text
    assert "One anchor is enough" in text
    for event_type in (
        "- outbreak:",
        "- cases:",
        "- notable_case:",
        "- vaccine_injury:",
        "- institutional_action:",
        "- study_publication:",
        "- other:",
    ):
        assert event_type in text
    assert "- event:" in text
    assert "- update:" in text
    assert "- inner:" in text
    assert "mention_focus" not in text
    assert "West Texas" not in text
    assert "Gaines County" not in text
    props = TRIAGE_SCHEMA["schema"]["properties"]["events"]["items"]["properties"]
    assert list(props) == ["event_type", "mention_kind", "title", "summary", "diseases", "quotes"]
    assert "place_scope" not in props
    assert "role_in_post" not in props
    assert "date_relation" not in props


def test_detail_prompt_place_dates_and_type_rules():
    for scope in ("at_named_place", "countrywide", "unplaced", "global", "region"):
        assert f"- {scope}:" in PLACE_AND_DATES
    assert "One value per slot" in PLACE_AND_DATES
    assert "YYYY-MM-DD only" in PLACE_AND_DATES
    assert "day, month, year, or unknown" in PLACE_AND_DATES
    assert "role_in_post" in PLACE_AND_DATES
    assert TYPE_INSTRUCTIONS.keys() == {
        "outbreak",
        "cases",
        "notable_case",
        "vaccine_injury",
        "institutional_action",
        "study_publication",
        "other",
    }
    assert "Do not fill dates from a case-count sentence" in TYPE_INSTRUCTIONS["outbreak"]
    assert "span_status" in TYPE_INSTRUCTIONS["outbreak"]
    assert "span_status" in PLACE_AND_DATES
    assert "actor is the patient's name" in TYPE_INSTRUCTIONS["notable_case"]
    assert "actor is the paper's author, not the journal" in TYPE_INSTRUCTIONS["study_publication"]
    user = format_detail_prompt(
        doc=_doc(),
        event_type="outbreak",
        stubs=[_triage_stub()],
    )
    assert "Texas clinic issued measles warnings" in user
    assert "Do not add or drop events" in user
    props = DETAIL_SCHEMA["schema"]["properties"]["events"]["items"]["properties"]
    assert "region" in props
    assert "span_status" in props
    assert "place_scope" in props
    assert "study_action" in props
    assert "event_type" not in props
    assert "mention_kind" not in props


def test_stratified_sampler_floor_and_total():
    counts = {"reddit_comment": 200, "telegram_post": 80, "podcast_episode": 10}
    alloc = allocate_stratified(counts, 150, floor=50)
    assert sum(alloc.values()) == 150
    assert alloc["reddit_comment"] >= 50
    assert alloc["telegram_post"] >= 50
    assert alloc["podcast_episode"] > 0

    docs = []
    for plat, n in counts.items():
        for i in range(n):
            docs.append({"doc_id": f"{plat}-{i}", "platform": plat, "body": "x"})
    sample = stratified_sample(docs, 150, seed=17, floor=50)
    assert len(sample) == 150
    by = {}
    for d in sample:
        by[d["platform"]] = by.get(d["platform"], 0) + 1
    assert by == alloc


def test_extract_skips_logged_docs(tmp_path: Path):
    from apps.claims import io as claims_io

    already = _doc(doc_id="1:0", post_id=1, body="already done")
    next_doc = _doc(doc_id="2:0", post_id=2)
    extra = _doc(doc_id="3:0", post_id=3, body="third chunk")
    claims_io.append_jsonl(
        tmp_path / EXTRACT_LOG_FILE,
        {"doc_id": "1:0", "mention_ids": ["already"], "n_raw": 0, "n_quote_dropped": 0, "n_kept": 0},
    )

    def complete_fn(**kwargs):
        return {"events": [_extract_item()]}

    remaining = remaining_docs([already, next_doc, extra], tmp_path, n=1)
    assert [d["doc_id"] for d in remaining] == ["2:0"]

    records = run_extract(
        docs=[already, next_doc, extra],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert len(records) == 1
    assert records[0].quotes == ["measles warnings at the clinic yesterday here in Tx"]
    leftover = remaining_docs([already, next_doc, extra], tmp_path)
    assert [d["doc_id"] for d in leftover] == ["3:0"]


def test_extract_runs_two_docs_in_one_batch(tmp_path: Path):
    a = _doc(doc_id="10:0", post_id=10, body="Yeah we saw measles warnings at the clinic yesterday here in Tx.")
    b = _doc(doc_id="11:0", post_id=11, body="Yeah we saw measles warnings at the clinic yesterday here in Tx.")
    seen: list[str] = []

    def complete_fn(**kwargs):
        seen.append(kwargs["schema"]["name"])
        return {"events": [_extract_item()]}

    records = run_extract(
        docs=[a, b],
        run_dir=tmp_path,
        n=2,
        max_workers=2,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert len(records) == 2
    assert {r.post_id for r in records} == {10, 11}
    assert seen.count("chunk_event_triage") == 2
    assert seen.count("chunk_event_detail") == 2
    leftover = remaining_docs([a, b], tmp_path)
    assert leftover == []


def test_extract_caps_to_four_in_order(tmp_path: Path):
    items = [
        _extract_item(title="First event"),
        _extract_item(title="Second event"),
        _extract_item(title="Third event"),
        _extract_item(title="Fourth event"),
        _extract_item(title="Fifth event"),
    ]

    def complete_fn(**kwargs):
        return {"events": items}

    records = run_extract(
        docs=[_doc()],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert [r.title for r in records] == [
        "First event",
        "Second event",
        "Third event",
        "Fourth event",
    ]


def test_extract_repairs_a_quote_and_keeps_the_event(tmp_path: Path):
    from apps.claims import io as claims_io
    from apps.claims.events.prompts import QUOTE_REPAIR_SCHEMA

    calls: list[str] = []

    def complete_fn(**kwargs):
        name = kwargs["schema"]["name"]
        if name == QUOTE_REPAIR_SCHEMA["name"]:
            calls.append("repair")
            assert "not exact substrings" in kwargs["user"]
            assert "Texas clinic issued measles warnings" in kwargs["user"]
            return {"quotes": ["measles warnings at the clinic yesterday here in Tx"], "title": "changed"}
        if name == "chunk_event_triage":
            calls.append("triage")
            return {"events": [_triage_stub()]}
        calls.append("detail")
        return {"events": [_extract_item(quotes=["a paraphrase that is not in the post"])]}

    records = run_extract(
        docs=[_doc()],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert calls == ["triage", "detail", "repair"]
    assert len(records) == 1
    assert records[0].title == "Texas clinic issued measles warnings"
    assert records[0].quotes == ["measles warnings at the clinic yesterday here in Tx"]
    assert records[0].location["admin1"] == "Texas"
    log = claims_io.read_jsonl(tmp_path / EXTRACT_LOG_FILE)
    detail_rows = [row for row in log if row.get("stage") == "detail"]
    assert detail_rows[0]["n_quote_repaired"] == 1
    assert detail_rows[0]["n_quote_dropped"] == 0


def test_extract_drops_a_quote_when_repair_still_misses(tmp_path: Path):
    from apps.claims.events.prompts import QUOTE_REPAIR_SCHEMA

    def complete_fn(**kwargs):
        name = kwargs["schema"]["name"]
        if name == QUOTE_REPAIR_SCHEMA["name"]:
            return {"quotes": ["still not in the post"]}
        if name == "chunk_event_triage":
            return {"events": [_triage_stub()]}
        return {"events": [_extract_item(quotes=["a paraphrase that is not in the post"])]}

    records = run_extract(
        docs=[_doc()],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert records == []


def test_year_and_month_expand_into_ranges_capped_by_post_date():
    year = mention_from_extract(
        _extract_item(
            date_precision="year",
            stated_start="2025-01-01",
            stated_end="",
            stated_date_text="2025",
            temporal_kind="open",
        ),
        doc=_doc(posted_at="2025-06-15T12:00:00+00:00"),
        existing_ids=set(),
    )
    assert year is not None
    assert year.stated_start == "2025-01-01"
    assert year.stated_end == "2025-06-15"
    assert year.date_precision == "range"
    assert year.date_grain == "year"
    assert year.temporal_kind == "interval"

    month = mention_from_extract(
        _extract_item(
            date_precision="month",
            stated_start="2025-12-01",
            stated_end="",
            stated_date_text="December 2025",
            temporal_kind="open",
        ),
        doc=_doc(posted_at="2025-12-10T00:00:00+00:00"),
        existing_ids=set(),
    )
    assert month is not None
    assert month.stated_start == "2025-12-01"
    assert month.stated_end == "2025-12-10"
    assert month.date_precision == "range"
    assert month.date_grain == "month"

    full_month = mention_from_extract(
        _extract_item(
            date_precision="month",
            stated_start="2025-11-01",
            stated_end="",
            stated_date_text="November",
            temporal_kind="interval",
        ),
        doc=_doc(posted_at="2025-12-10T00:00:00+00:00"),
        existing_ids=set(),
    )
    assert full_month is not None
    assert full_month.stated_end == "2025-11-30"

    start, end, precision, kind = expand_stated_range(
        stated_start="2025-01-01",
        stated_end=None,
        date_precision="year",
        temporal_kind="open",
        posted_at="2026-02-01T00:00:00+00:00",
    )
    assert start == "2025-01-01"
    assert end == "2025-12-31"
    assert precision == "range"
    assert kind == "interval"

    day = mention_from_extract(_extract_item(), doc=_doc(), existing_ids=set())
    assert day is not None
    assert day.stated_start == "2025-11-12"
    assert day.stated_end == "2025-11-12"
    assert day.date_precision == "day"
    assert day.date_grain == "day"


def test_triage_without_quotes_skips_detail(tmp_path: Path):
    calls: list[str] = []

    def complete_fn(**kwargs):
        calls.append(kwargs["schema"]["name"])
        if kwargs["schema"]["name"] == "chunk_event_triage":
            return {"events": [_triage_stub(quotes=["this quote is not in the post at all"])]}
        raise AssertionError("detail should not run when triage quotes miss")

    records = run_extract(
        docs=[_doc()],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert records == []
    assert calls == ["chunk_event_triage"]
    leftover = remaining_docs([_doc()], tmp_path)
    assert leftover == []


def test_outbreak_and_cases_make_two_detail_calls_not_seven(tmp_path: Path):
    calls: list[str] = []

    def complete_fn(**kwargs):
        name = kwargs["schema"]["name"]
        calls.append(name)
        if name == "chunk_event_triage":
            assert kwargs["model"] == "gpt-5.4-mini"
            return {
                "events": [
                    _triage_stub(event_type="outbreak", mention_kind="event", title="Texas measles wave"),
                    _triage_stub(event_type="cases", mention_kind="update", title="Texas case count"),
                ]
            }
        assert kwargs["model"] == "gpt-6-luna"
        return {"events": [_extract_item()]}

    records = run_extract(
        docs=[_doc()],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert calls.count("chunk_event_triage") == 1
    assert calls.count("chunk_event_detail") == 2
    assert len(records) == 2
    assert {r.event_type for r in records} == {"outbreak", "cases"}
    assert {r.mention_kind for r in records} == {"event", "update"}


def test_continue_skips_finished_triage(tmp_path: Path):
    from apps.claims import io as claims_io

    doc = _doc()
    claims_io.append_jsonl(
        tmp_path / EXTRACT_LOG_FILE,
        {
            "doc_id": "42:0",
            "stage": "triage",
            "stubs": [_triage_stub()],
            "n_raw": 1,
            "n_quote_dropped": 0,
        },
    )
    calls: list[str] = []

    def complete_fn(**kwargs):
        calls.append(kwargs["schema"]["name"])
        return {"events": [_extract_item()]}

    records = run_extract(
        docs=[doc],
        run_dir=tmp_path,
        n=1,
        complete_fn=complete_fn,
        log=lambda _msg: None,
    )
    assert "chunk_event_triage" not in calls
    assert calls == ["chunk_event_detail"]
    assert len(records) == 1


def test_role_and_agency_actors_cleared_on_notable_case():
    child = mention_from_extract(
        _extract_item(event_type="notable_case", actor="child"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert child is not None
    assert child.actor == ""
    cdc = mention_from_extract(
        _extract_item(event_type="notable_case", actor="CDC"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert cdc is not None
    assert cdc.actor == ""
    named = mention_from_extract(
        _extract_item(event_type="notable_case", actor="Daisy Hildebrand"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert named is not None
    assert named.actor == "Daisy Hildebrand"
    nurses = mention_from_extract(
        _extract_item(event_type="vaccine_injury", actor="Nurses"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert nurses is not None
    assert nurses.actor == ""
    aged = mention_from_extract(
        _extract_item(event_type="notable_case", actor="6-year-old girl"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert aged is not None
    assert aged.actor == ""
    place_actor = mention_from_extract(
        _extract_item(event_type="notable_case", actor="Texas"),
        doc=_doc(),
        existing_ids=set(),
    )
    assert place_actor is not None
    assert place_actor.actor == ""


def test_journal_is_not_a_study_author():
    journal = mention_from_extract(
        _extract_item(
            event_type="study_publication",
            actor="The Lancet",
            study_action="publication",
            title="Wakefield paper published",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert journal is not None
    assert journal.actor == ""
    author = mention_from_extract(
        _extract_item(
            event_type="study_publication",
            actor="Wakefield",
            study_action="publication",
            title="Wakefield paper published",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert author is not None
    assert author.actor == "Wakefield"


def test_empty_place_slots_including_region_become_unplaced():
    empty = mention_from_extract(
        _extract_item(
            country="",
            admin1="",
            admin2="",
            locality="",
            site="",
            region="",
            location_label="",
            place_scope="at_named_place",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert empty is not None
    assert empty.place_scope == "unplaced"
    assert empty.region == ""
    worldwide = mention_from_extract(
        _extract_item(
            country="",
            admin1="",
            admin2="",
            locality="",
            site="",
            region="",
            location_label="",
            place_scope="global",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert worldwide is not None
    assert worldwide.place_scope == "global"
    europe = mention_from_extract(
        _extract_item(
            country="",
            admin1="",
            admin2="",
            locality="",
            site="",
            region="Europe",
            location_label="Europe",
            place_scope="at_named_place",
        ),
        doc=_doc(),
        existing_ids=set(),
    )
    assert europe is not None
    assert europe.place_scope == "region"
    assert europe.region == "Europe"
    assert europe.location.get("region") == "Europe"


def test_prompt_sample_labels_match_posts():
    import json

    from apps.claims.events.mention import EVENT_TYPES, PLACE_SCOPES, STUDY_ACTIONS
    from events.schema import DATE_PRECISIONS

    root = Path(__file__).resolve().parents[1] / "events" / "prompt_sample"
    posts = [json.loads(line)["doc_id"] for line in (root / "posts.jsonl").read_text().splitlines() if line.strip()]
    labels = [json.loads(line) for line in (root / "labels.jsonl").read_text().splitlines() if line.strip()]
    # 9771068:0 stays in the posts. Its label was removed on purpose.
    assert [doc_id for doc_id in posts if doc_id != "9771068:0"] == [row["doc_id"] for row in labels]
    for row in labels:
        assert len(row["events"]) <= 4
        for event in row["events"]:
            assert event["event_type"] in EVENT_TYPES
            assert event["date_precision"] in DATE_PRECISIONS
            assert event["role_in_post"] in {"focal", "mentioned"}
            assert event["place_scope"] in PLACE_SCOPES
            if event["event_type"] == "study_publication":
                assert event["study_action"] in STUDY_ACTIONS
            else:
                assert event["study_action"] == ""
            if "optional" in event:
                assert event["optional"] is True
            also = event.get("also") or {}
            assert isinstance(also, dict)
            for key, values in also.items():
                assert key in {"country", "admin1", "admin2", "locality", "site"}
                assert values and all(isinstance(value, str) and value.strip() for value in values)


def test_model_compare_matches_shared_quotes_and_ignores_short_spans():
    from apps.claims.events.model_compare import compare, quote_overlap

    shared = "the outbreak in gaines county killed two children last year"
    ref = [
        {
            "task_id": "1:0",
            "event_type": "outbreak",
            "title": "Texas measles outbreak",
            "quotes": [shared],
            "date_precision": "year",
            "stated_start": "2025-01-01",
            "location": {"country": "United States", "admin1": "Texas"},
        }
    ]
    cand = [
        {
            "task_id": "1:0",
            "event_type": "outbreak",
            "title": "A completely different title",
            "quotes": ["reports say " + shared],
            "date_precision": "year",
            "stated_start": "2025-06-01",
            "location": {"country": "United States", "admin1": "Texas"},
        },
        {
            "task_id": "2:0",
            "event_type": "notable_case",
            "title": "extra",
            "quotes": ["daisy hildebrand died of measles in lubbock"],
            "date_precision": "unknown",
            "location": {},
        },
    ]
    assert quote_overlap({"quotes": ["measles"]}, {"quotes": ["measles outbreak in texas today"]}) == 0.0
    assert quote_overlap({"quotes": ["ACIP Disbanded"]}, {"quotes": ["acip   disbanded"]}) == 1.0
    scored = compare(reference=ref, candidate=cand, doc_ids={"1:0", "2:0", "3:0"})
    assert scored["quote_matched"] == 1
    assert scored["quote_recall"] == 1.0
    assert scored["quote_precision"] == 0.5
    assert scored["both_empty"] == 1
    assert scored["cand_only_docs"] == 1
    assert scored["type_agree"] == 1.0
    assert scored["year_agree_when_ref_dated"] == 1.0
    assert scored["place_conflict_rate"] == 0.0


def test_prompt_score_matches_type_and_counts_emptiness(tmp_path: Path):
    import json

    from apps.claims.events.prompt_score import score_try

    labels = tmp_path / "labels.jsonl"
    mentions = tmp_path / "mentions.jsonl"
    labels.write_text(
        json.dumps(
            {
                "doc_id": "1:0",
                "events": [
                    {
                        "identity": "wave",
                        "event_type": "outbreak",
                        "country": "United States",
                        "admin1": "Texas",
                        "admin2": "",
                        "locality": "",
                        "site": "",
                        "date_precision": "unknown",
                        "stated_start": "",
                    }
                ],
            }
        )
        + "\n"
        + json.dumps({"doc_id": "2:0", "events": []})
        + "\n"
    )
    mentions.write_text(
        json.dumps(
            {
                "task_id": "1:0",
                "event_type": "outbreak",
                "title": "Texas outbreak",
                "location": {"country": "United States", "admin1": "Texas"},
                "date_precision": "unknown",
                "stated_start": None,
            }
        )
        + "\n"
        + json.dumps(
            {
                "task_id": "2:0",
                "event_type": "outbreak",
                "title": "should be empty",
                "location": {},
                "date_precision": "unknown",
            }
        )
        + "\n"
    )
    scored = score_try(labels_path=labels, mentions_path=mentions)
    assert scored["fp_docs"] == 1
    assert scored["fn_docs"] == 0
    assert scored["matched"] == 1
    assert scored["place_acc"] == 1.0
    assert scored["scope_acc"] == 1.0
    assert scored["study_acc"] == 1.0


def test_prompt_score_pairs_publication_with_retraction(tmp_path: Path):
    import json

    from apps.claims.events.prompt_score import score_try

    labels = tmp_path / "labels.jsonl"
    mentions = tmp_path / "mentions.jsonl"
    labels.write_text(
        json.dumps(
            {
                "doc_id": "1:0",
                "events": [
                    {
                        "identity": "paper",
                        "event_type": "study_publication",
                        "date_precision": "year",
                        "stated_start": "1998-01-01",
                        "place_scope": "unplaced",
                        "study_action": "publication",
                    },
                    {
                        "identity": "retract",
                        "event_type": "study_publication",
                        "date_precision": "unknown",
                        "stated_start": "",
                        "place_scope": "unplaced",
                        "study_action": "retraction",
                    },
                ],
            }
        )
        + "\n"
    )
    mentions.write_text(
        json.dumps(
            {
                "task_id": "1:0",
                "event_type": "study_publication",
                "title": "Paper retracted",
                "date_precision": "unknown",
                "stated_start": None,
                "place_scope": "countrywide",
                "study_action": "retraction",
                "location": {},
            }
        )
        + "\n"
        + json.dumps(
            {
                "task_id": "1:0",
                "event_type": "study_publication",
                "title": "Paper published",
                "date_precision": "year",
                "stated_start": "1998-01-01",
                "place_scope": "unplaced",
                "study_action": "publication",
                "location": {},
            }
        )
        + "\n"
    )
    scored = score_try(labels_path=labels, mentions_path=mentions)
    assert scored["matched"] == 2
    by_identity = {row["identity"]: row for row in scored["docs"][0]["matched"]}
    assert by_identity["paper"]["pred_title"] == "Paper published"
    assert by_identity["retract"]["pred_title"] == "Paper retracted"
    assert by_identity["paper"]["diffs"] == []
    assert any(diff.startswith("place_scope:") for diff in by_identity["retract"]["diffs"])
    assert scored["scope_acc"] == 0.5
    assert scored["study_acc"] == 1.0


def test_prompt_score_tie_breaks_on_disease_and_identity(tmp_path: Path):
    import json

    from apps.claims.events.prompt_score import score_try

    labels = tmp_path / "labels.jsonl"
    mentions = tmp_path / "mentions.jsonl"
    labels.write_text(
        json.dumps(
            {
                "doc_id": "1:0",
                "events": [
                    {
                        "identity": "england_outbreaks",
                        "event_type": "outbreak",
                        "diseases": ["measles"],
                        "date_precision": "unknown",
                        "place_scope": "at_named_place",
                        "country": "United Kingdom",
                        "admin1": "England",
                    },
                    {
                        "identity": "fda_new_studies",
                        "event_type": "institutional_action",
                        "date_precision": "unknown",
                        "place_scope": "unplaced",
                    },
                ],
            }
        )
        + "\n"
    )
    shared = "We've seen more cases of measles and pooping cough, and outbreaks have been popping up across the country."
    mentions.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                {
                    "task_id": "1:0",
                    "event_type": "outbreak",
                    "title": "Whooping cough outbreaks reported across England",
                    "diseases": ["pooping cough"],
                    "quotes": [shared],
                    "date_precision": "unknown",
                    "place_scope": "at_named_place",
                    "location": {"country": "United Kingdom", "admin1": "England"},
                },
                {
                    "task_id": "1:0",
                    "event_type": "outbreak",
                    "title": "Measles outbreaks reported across England",
                    "diseases": ["measles"],
                    "quotes": [shared],
                    "date_precision": "unknown",
                    "place_scope": "at_named_place",
                    "location": {"country": "United Kingdom", "admin1": "England"},
                },
                {
                    "task_id": "1:0",
                    "event_type": "institutional_action",
                    "title": "FDA and CDC strengthen recommendation for separate MMR and varicella shots",
                    "quotes": ["long-standing position of the FDA and CDC, but they got a bit firmer"],
                    "date_precision": "unknown",
                    "place_scope": "unplaced",
                    "location": {},
                },
                {
                    "task_id": "1:0",
                    "event_type": "institutional_action",
                    "title": "FDA requires new studies that could delay approval",
                    "quotes": ["The new FDA is requiring new studies that could delay approval"],
                    "date_precision": "unknown",
                    "place_scope": "unplaced",
                    "location": {},
                },
            )
        )
        + "\n"
    )
    scored = score_try(labels_path=labels, mentions_path=mentions)
    by_identity = {row["identity"]: row["pred_title"] for row in scored["docs"][0]["matched"]}
    assert by_identity["england_outbreaks"] == "Measles outbreaks reported across England"
    assert by_identity["fda_new_studies"] == "FDA requires new studies that could delay approval"
    assert scored["matched"] == 2
    extras = {row["title"] for row in scored["docs"][0]["extra"]}
    assert "Whooping cough outbreaks reported across England" in extras
    assert "FDA and CDC strengthen recommendation for separate MMR and varicella shots" in extras


def test_prompt_score_optional_event_may_be_absent_or_present(tmp_path: Path):
    import json

    from apps.claims.events.prompt_score import score_try

    labels = tmp_path / "labels.jsonl"
    labels.write_text(
        json.dumps(
            {
                "doc_id": "1:0",
                "events": [
                    {
                        "identity": "required_dose",
                        "event_type": "institutional_action",
                        "date_precision": "year",
                        "stated_start": "1989-01-01",
                        "place_scope": "unplaced",
                        "study_action": "",
                    },
                    {
                        "identity": "optional_intro",
                        "event_type": "institutional_action",
                        "date_precision": "year",
                        "stated_start": "1971-01-01",
                        "place_scope": "unplaced",
                        "study_action": "",
                        "optional": True,
                    },
                ],
            }
        )
        + "\n"
    )

    def mentions(*rows: dict) -> None:
        (tmp_path / "mentions.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))

    only_required = {
        "task_id": "1:0",
        "event_type": "institutional_action",
        "title": "Second dose",
        "date_precision": "year",
        "stated_start": "1989-01-01",
        "place_scope": "unplaced",
        "study_action": "",
        "location": {},
    }
    also_optional = {
        "task_id": "1:0",
        "event_type": "institutional_action",
        "title": "MMR introduced",
        "date_precision": "year",
        "stated_start": "1971-01-01",
        "place_scope": "unplaced",
        "study_action": "",
        "location": {},
    }
    mentions(only_required)
    omitted = score_try(labels_path=labels, mentions_path=tmp_path / "mentions.jsonl")
    assert omitted["matched"] == 1
    assert omitted["optional_omitted"] == 1
    assert omitted["optional_matched"] == 0
    assert omitted["gold_events"] == 1
    assert omitted["recall"] == 1.0
    assert omitted["precision"] == 1.0
    assert omitted["docs"] == []

    mentions(also_optional, only_required)
    both = score_try(labels_path=labels, mentions_path=tmp_path / "mentions.jsonl")
    assert both["matched"] == 1
    assert both["optional_matched"] == 1
    assert both["optional_omitted"] == 0
    assert both["precision"] == 1.0
    assert both["recall"] == 1.0
    assert both["docs"] == []


def test_prompt_score_accepts_an_alternate_site_string(tmp_path: Path):
    import json

    from apps.claims.events.prompt_score import score_try

    labels = tmp_path / "labels.jsonl"
    mentions = tmp_path / "mentions.jsonl"
    labels.write_text(
        json.dumps(
            {
                "doc_id": "1:0",
                "events": [
                    {
                        "identity": "kroger",
                        "event_type": "notable_case",
                        "site": "Englewood Kroger",
                        "also": {"site": ["Englewood Kroger store"]},
                        "date_precision": "unknown",
                        "place_scope": "at_named_place",
                    }
                ],
            }
        )
        + "\n"
    )
    mentions.write_text(
        json.dumps(
            {
                "task_id": "1:0",
                "event_type": "notable_case",
                "title": "Exposure at the store",
                "date_precision": "unknown",
                "place_scope": "at_named_place",
                "location": {"site": "Englewood Kroger store"},
            }
        )
        + "\n"
    )
    scored = score_try(labels_path=labels, mentions_path=mentions)
    assert scored["place_acc"] == 1.0
    assert scored["docs"] == []
