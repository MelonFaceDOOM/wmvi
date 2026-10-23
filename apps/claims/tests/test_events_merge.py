"""Merge grouping and the type-specific strict join."""

from __future__ import annotations

from apps.claims.events.merge.group import (
    cluster_id_for,
    cluster_mentions,
    grouping_key,
    is_mixed_year,
    is_overmerge_suspect,
    title_slug,
)


def _m(**overrides):
    row = {
        "mention_id": "a",
        "event_type": "outbreak",
        "title": "Measles outbreak in Texas",
        "location": {"country": "United States", "admin1": "Texas"},
        "date_precision": "unknown",
        "stated_start": None,
        "post_id": 1,
    }
    row.update(overrides)
    return row


def test_title_slug_collapses_punctuation():
    assert title_slug("Measles outbreak in Texas") == title_slug("measles outbreak, in Texas!")


def test_same_key_includes_unknown_and_dated():
    a = _m(mention_id="u", date_precision="unknown")
    b = _m(mention_id="d", date_precision="year", stated_start="2025-01-01", post_id=2)
    assert grouping_key(a) == grouping_key(b)
    clusters, members = cluster_mentions([a, b])
    assert len(clusters) == 1
    assert clusters[0]["n_mentions"] == 2
    assert clusters[0]["year_histogram"]["unknown"] == 1
    assert clusters[0]["year_histogram"]["2025"] == 1
    assert {m["mention_id"] for m in members} == {"u", "d"}
    assert is_mixed_year(clusters[0]) is False


def test_unplaced_does_not_join_placed_same_title():
    placed = _m(mention_id="p")
    unplaced = _m(mention_id="u", location={}, post_id=2)
    assert grouping_key(placed) != grouping_key(unplaced)
    clusters, _ = cluster_mentions([placed, unplaced])
    assert len(clusters) == 2
    by_flag = {c["unplaced"]: c for c in clusters}
    assert by_flag[False]["n_mentions"] == 1
    assert by_flag[True]["n_mentions"] == 1


def test_different_titles_split():
    a = _m(mention_id="a", title="West Texas measles outbreak")
    b = _m(mention_id="b", title="Dilley detention measles outbreak", post_id=2)
    clusters, _ = cluster_mentions([a, b])
    assert len(clusters) == 2


def test_mixed_year_and_overmerge_flags():
    rows = [
        _m(mention_id="a", date_precision="year", stated_start="2025-01-01", post_id=1),
        _m(mention_id="b", date_precision="year", stated_start="2026-01-01", post_id=2),
    ]
    clusters, _ = cluster_mentions(rows)
    assert is_mixed_year(clusters[0]) is True
    assert is_overmerge_suspect(clusters[0]) is False  # size 2, years 1 apart
    rows.append(_m(mention_id="c", date_precision="year", stated_start="2019-01-01", post_id=3))
    clusters, _ = cluster_mentions(rows)
    assert is_overmerge_suspect(clusters[0]) is True


def test_v2_waves_ignore_title_and_join_dates():
    from apps.claims.events.merge.group import STRATEGY_V2, cluster_mentions, prepare_mentions

    a = _m(mention_id="a", title="West Texas measles outbreak")
    b = _m(
        mention_id="b",
        title="Dilley detention measles outbreak",
        post_id=2,
        date_precision="year",
        stated_start="2026-01-01",
    )
    kept, dropped = prepare_mentions([a, b])
    assert dropped == []
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V2)
    assert len(clusters) == 1
    assert clusters[0]["n_mentions"] == 2
    assert clusters[0]["year_histogram"]["unknown"] == 1
    assert clusters[0]["year_histogram"]["2026"] == 1


def test_v2_drops_unplaced_outbreak():
    from apps.claims.events.merge.group import prepare_mentions

    kept, dropped = prepare_mentions([_m(mention_id="u", location={}, title="Huge measles outbreak")])
    assert kept == []
    assert dropped[0]["drop_reason"] == "vague_unplaced_wave"


def test_v2_usa_aliases_join():
    from apps.claims.events.merge.group import STRATEGY_V2, cluster_mentions, prepare_mentions

    a = _m(mention_id="a", location={"country": "USA", "admin1": "Texas"})
    b = _m(
        mention_id="b",
        location={"country": "United States", "admin1": "Texas"},
        post_id=2,
        title="Texas measles outbreak",
    )
    kept, dropped = prepare_mentions([a, b])
    assert dropped == []
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V2)
    assert len(clusters) == 1
    assert clusters[0]["country"] == "United States"


def test_v2_named_types_keep_discriminator():
    from apps.claims.events.merge.group import STRATEGY_V2, cluster_mentions, prepare_mentions

    a = _m(
        mention_id="a",
        event_type="vaccine_injury",
        title="Hannah Poling autism case",
        actor="Hannah Poling",
        location={},
    )
    b = _m(
        mention_id="b",
        event_type="vaccine_injury",
        title="Violet Skye Rodela died after MMR",
        actor="Violet Skye Rodela",
        location={},
        post_id=2,
    )
    kept, dropped = prepare_mentions([a, b])
    assert dropped == []
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V2)
    assert len(clusters) == 2


def test_cluster_id_stable():
    key = grouping_key(_m())
    assert cluster_id_for(key) == cluster_id_for(key)


def test_v3_drops_us_national_outbreak_keeps_samoa_and_texas():
    from apps.claims.events.merge.group import STRATEGY_V3, cluster_mentions, prepare_mentions

    us = _m(
        mention_id="us",
        title="Measles outbreak in the United States",
        location={"country": "United States"},
    )
    samoa = _m(
        mention_id="ws",
        title="Measles outbreak in Samoa",
        location={"country": "Samoa"},
        post_id=2,
    )
    tx = _m(
        mention_id="tx",
        title="West Texas measles outbreak",
        location={"country": "United States", "admin1": "Texas"},
        post_id=3,
    )
    kept, dropped = prepare_mentions([us, samoa, tx], mode=STRATEGY_V3)
    reasons = {d["mention_id"]: d["drop_reason"] for d in dropped}
    assert reasons["us"] == "national_status"
    ids = {m["mention_id"] for m in kept}
    assert ids == {"ws", "tx"}
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V3)
    keys = {(c["country"], c["admin1"], c["event_type"]) for c in clusters}
    assert ("Samoa", "", "outbreak") in keys
    assert ("United States", "Texas", "outbreak") in keys


def test_v3_keeps_elimination_status_drops_generic_cdc():
    from apps.claims.events.merge.group import STRATEGY_V3, prepare_mentions

    elim = _m(
        mention_id="e",
        event_type="institutional_action",
        title="Canada loses measles elimination status",
        actor="Canada",
        location={"country": "Canada"},
    )
    cdc = _m(
        mention_id="c",
        event_type="institutional_action",
        title="CDC confirms additional US measles cases",
        actor="CDC",
        location={"country": "United States"},
        post_id=2,
    )
    kept, dropped = prepare_mentions([elim, cdc], mode=STRATEGY_V3)
    assert [m["mention_id"] for m in kept] == ["e"]
    assert dropped[0]["drop_reason"] == "national_institution"


def test_browse_splits_year_then_place():
    from apps.claims.events.merge.browse import attach_children, member_card

    cluster = {
        "cluster_id": "tx",
        "event_type": "outbreak",
        "country": "United States",
        "admin1": "Texas",
        "title_slug": "",
        "unplaced": False,
        "n_mentions": 3,
        "n_posts": 3,
        "year_histogram": {"2025": 1, "2026": 1, "unknown": 1},
        "example_titles": ["Texas measles outbreak"],
    }
    cards = [
        member_card(
            {
                "mention_id": "a",
                "title": "West Texas 2025",
                "location": {"country": "United States", "admin1": "Texas", "locality": "Lubbock"},
                "date_precision": "year",
                "stated_start": "2025-01-01",
            }
        ),
        member_card(
            {
                "mention_id": "b",
                "title": "Dilley 2026",
                "location": {"country": "United States", "admin1": "Texas", "locality": "Dilley"},
                "date_precision": "year",
                "stated_start": "2026-01-01",
            }
        ),
        member_card(
            {
                "mention_id": "c",
                "title": "Texas measles",
                "location": {"country": "United States", "admin1": "Texas"},
                "date_precision": "unknown",
            }
        ),
    ]
    node = attach_children(cluster, cards)
    years = [c["key"] for c in node["children"]]
    assert years == ["2026", "2025", "unknown"]
    assert node["members"] == []
    dilley = node["children"][0]
    assert dilley["label"] == "2026"
    assert dilley["children"] == []
    assert dilley["members"][0]["mention_id"] == "b"


def test_browse_splits_place_when_years_do_not():
    from apps.claims.events.merge.browse import attach_children, member_card

    cluster = {
        "cluster_id": "tx",
        "event_type": "outbreak",
        "country": "United States",
        "admin1": "Texas",
        "title_slug": "",
        "n_mentions": 2,
    }
    cards = [
        member_card(
            {
                "mention_id": "d",
                "title": "Dilley",
                "location": {"country": "United States", "admin1": "Texas", "locality": "Dilley"},
            }
        ),
        member_card(
            {
                "mention_id": "t",
                "title": "Texas",
                "location": {"country": "United States", "admin1": "Texas"},
            }
        ),
    ]
    node = attach_children(cluster, cards)
    assert [c["label"] for c in node["children"]] == ["Dilley", "place not specified"]
    assert node["members"] == []


def test_include_matchers_keep_suggested_only():
    from apps.claims.events.merge.browse import (
        include_leftover_other,
        include_national_institution,
        include_national_status,
        include_unanchored_case,
    )

    assert include_national_status({"location": {"country": "New Zealand"}}) == "country_wave:New Zealand"
    assert include_national_status({"location": {"country": "United States"}}) is None
    assert (
        include_national_institution({"title": "UK loses WHO measles-free status"})
        == "measles_free_or_elimination"
    )
    assert include_national_institution({"title": "CDC confirms additional US measles cases"}) is None
    assert include_unanchored_case({"title": "Daisy Hildebrand died in the 2025 measles outbreak"})
    assert include_unanchored_case({"title": "Child dies of measles"}) is None
    assert include_leftover_other(
        {
            "title": "Children hospitalized with Vitamin A toxicity in Texas",
            "location": {"country": "United States", "admin1": "Texas"},
        }
    )
    assert include_leftover_other({"title": "Glyphosate detected in five tested childhood vaccines"}) is None


def test_v4_splits_stated_year_and_keeps_unknown_apart():
    from apps.claims.events.merge.group import STRATEGY_V4, cluster_mentions, prepare_mentions

    y2025 = _m(mention_id="a", date_precision="year", stated_start="2025-01-01", diseases=["measles"])
    y2026 = _m(
        mention_id="b",
        post_id=2,
        date_precision="year",
        stated_start="2026-01-01",
        diseases=["measles"],
    )
    undated = _m(mention_id="c", post_id=3, diseases=["measles"])
    kept, dropped = prepare_mentions([y2025, y2026, undated], mode=STRATEGY_V4)
    assert dropped == []
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V4)
    assert len(clusters) == 3
    assert {c["join_year"] for c in clusters} == {"", "2025", "2026"}


def test_v4_city_without_state_does_not_join_country_blob():
    from apps.claims.events.merge.group import STRATEGY_V4, cluster_mentions, prepare_mentions

    dilley = _m(
        mention_id="d",
        title="Measles outbreak at Dilley",
        location={"country": "United States", "locality": "Dilley"},
        diseases=["measles"],
    )
    national = _m(
        mention_id="n",
        title="Measles cases across the United States",
        location={"country": "United States"},
        post_id=2,
        diseases=["measles"],
    )
    kept, _ = prepare_mentions([dilley, national], mode=STRATEGY_V4)
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V4)
    assert len(clusters) == 2
    places = {c["admin1"] for c in clusters}
    assert "Dilley" in places
    assert "" in places


def test_v4_disease_splits_state_wave():
    from apps.claims.events.merge.group import STRATEGY_V4, cluster_mentions, prepare_mentions

    measles = _m(
        mention_id="m",
        location={"country": "United States", "admin1": "Pennsylvania"},
        diseases=["measles"],
        date_precision="year",
        stated_start="2025-01-01",
    )
    legion = _m(
        mention_id="l",
        title="Legionnaires cluster in Pennsylvania",
        location={"country": "United States", "admin1": "Pennsylvania"},
        diseases=["legionnaires"],
        date_precision="year",
        stated_start="2025-01-01",
        post_id=2,
    )
    kept, _ = prepare_mentions([measles, legion], mode=STRATEGY_V4)
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V4)
    assert len(clusters) == 2


def test_v4_generic_actor_does_not_join_institutional_acts():
    from apps.claims.events.merge.group import STRATEGY_V4, cluster_mentions, prepare_mentions

    elim = _m(
        mention_id="e",
        event_type="institutional_action",
        title="United States loses measles elimination status",
        actor="CDC",
        location={"country": "United States"},
    )
    schedule = _m(
        mention_id="s",
        event_type="institutional_action",
        title="CDC changes the childhood vaccine schedule",
        actor="CDC",
        location={"country": "United States"},
        post_id=2,
    )
    kept, dropped = prepare_mentions([elim, schedule], mode=STRATEGY_V4)
    assert dropped == []
    clusters, _ = cluster_mentions(kept, strategy=STRATEGY_V4)
    assert len(clusters) == 2


def test_strict_outbreak_requires_year_and_person_does_not():
    from apps.claims.events.merge.strict import build_payload

    dated = _m(
        mention_id="d",
        diseases=["measles"],
        date_precision="year",
        stated_start="2025-01-01",
    )
    undated = _m(mention_id="u", diseases=["measles"], post_id=2)
    daisy_a = _m(
        mention_id="da",
        event_type="notable_case",
        title="Child dies of measles",
        actor="Daisy Hildebrand",
        diseases=["measles"],
        location={"country": "United States", "admin1": "Texas"},
        post_id=3,
    )
    daisy_b = _m(
        mention_id="db",
        event_type="notable_case",
        title="Daisy Hildebrand's death",
        actor="Dr. Daisy Hildebrand",
        diseases=["measles"],
        location={"country": "United States"},
        post_id=4,
    )
    payload = build_payload([dated, undated, daisy_a, daisy_b])
    assert payload["n_merged_groups"] == 1
    assert payload["merged"][0]["event_type"] == "notable_case"
    assert payload["n_ambiguous"] == 1
    assert payload["ambiguous"][0]["reason"] == "missing_year"
    assert payload["n_alone"] == 1


def test_strict_county_does_not_join_its_state():
    from apps.claims.events.merge.strict import build_payload

    state = _m(
        mention_id="state",
        diseases=["measles"],
        date_precision="year",
        stated_start="2025-01-01",
        place_scope="at_named_place",
    )
    county = _m(
        mention_id="county",
        title="Measles outbreak in Gaines County",
        diseases=["measles"],
        date_precision="year",
        stated_start="2025-01-01",
        location={"country": "United States", "admin1": "Texas", "admin2": "Gaines County"},
        place_scope="at_named_place",
        post_id=2,
    )
    payload = build_payload([state, county])
    assert payload["n_merged_groups"] == 0
    assert payload["n_alone"] == 2
    assert payload["n_ambiguous"] == 0


def _org(**overrides):
    row = {
        "mention_id": "m",
        "event_type": "cases",
        "mention_kind": "update",
        "title": "Measles cases",
        "diseases": ["measles"],
        "location": {"country": "United States", "admin1": "Texas"},
        "place_scope": "at_named_place",
        "date_grain": "day",
        "date_precision": "day",
        "stated_start": "2025-03-02",
        "span_status": "open",
        "actor": "",
        "post_id": 1,
    }
    row.update(overrides)
    return row


def _by(result, **match):
    return [event for event in result["events"] if all(event.get(key) == value for key, value in match.items())]


def _member_ids(result, event_id):
    return {row["mention_id"] for row in result["members"] if row["event_id"] == event_id}


def test_five_updates_open_one_event_and_a_year_does_not_start_on_january_first():
    from apps.claims.events.merge.organize import organize_mentions

    days = ["2025-03-02", "2025-04-01", "2025-05-10", "2025-06-01", "2025-08-15"]
    updates = [
        _org(mention_id=f"u{i}", stated_start=day, post_id=i, title=f"Texas count {day}")
        for i, day in enumerate(days, start=1)
    ]
    year_only = _org(
        mention_id="year",
        event_type="outbreak",
        mention_kind="event",
        title="Texas measles wave",
        date_grain="year",
        date_precision="range",
        stated_start="2025-01-01",
        stated_end="2025-12-31",
        post_id=9,
    )
    result = organize_mentions(updates + [year_only])
    waves = _by(result, kind="wave", grain_path="United States / Texas", year="2025")
    assert len(waves) == 1
    assert waves[0]["span_start"] == "2025-03-02"
    assert waves[0]["span_end"] == ""
    assert waves[0]["span_open"] is True
    assert _member_ids(result, waves[0]["event_id"]) == {f"u{i}" for i in range(1, 6)} | {"year"}


def test_gaines_belongs_to_texas_and_keeps_its_mentions():
    from apps.claims.events.merge.organize import organize_mentions

    texas = _org(mention_id="tx", event_type="outbreak", mention_kind="event", title="Texas wave")
    gaines = _org(
        mention_id="gaines",
        title="Gaines County cases",
        location={"country": "United States", "admin1": "Texas", "admin2": "Gaines County"},
        stated_start="2025-04-04",
        post_id=2,
    )
    result = organize_mentions([texas, gaines])
    state = _by(result, grain_path="United States / Texas")
    county = _by(result, grain_path="United States / Texas / Gaines County")
    assert len(state) == 1 and len(county) == 1
    assert county[0]["parent_event_id"] == state[0]["event_id"]
    assert _member_ids(result, state[0]["event_id"]) == {"tx"}
    assert _member_ids(result, county[0]["event_id"]) == {"gaines"}


def test_us_tally_does_not_absorb_texas():
    from apps.claims.events.merge.organize import organize_mentions

    texas = _org(mention_id="tx", event_type="outbreak", mention_kind="event", title="Texas wave")
    national = _org(
        mention_id="us",
        title="US measles cases",
        location={"country": "United States"},
        place_scope="countrywide",
        post_id=2,
    )
    result = organize_mentions([texas, national])
    series = _by(result, kind="series", grain_path="United States")
    state = _by(result, kind="wave", grain_path="United States / Texas")
    assert len(series) == 1 and len(state) == 1
    assert state[0]["parent_event_id"] == series[0]["event_id"]
    assert _member_ids(result, series[0]["event_id"]) == {"us"}
    assert _member_ids(result, state[0]["event_id"]) == {"tx"}


def test_new_year_opens_a_second_event_and_closed_ends_the_span():
    from apps.claims.events.merge.organize import organize_mentions

    first = _org(mention_id="a", span_status="closed", stated_start="2025-08-01", title="Wave declared over")
    second = _org(mention_id="b", stated_start="2026-02-02", title="Cases in 2026", post_id=2)
    result = organize_mentions([first, second])
    y2025 = _by(result, year="2025")
    y2026 = _by(result, year="2026")
    assert len(y2025) == 1 and len(y2026) == 1
    assert y2025[0]["span_open"] is False
    assert y2025[0]["span_end"] == "2025-08-01"
    assert y2026[0]["span_open"] is True
    assert y2026[0]["parent_event_id"] == ""


def test_death_and_exposure_sites_belong_to_the_wave_and_stay_separate():
    from apps.claims.events.merge.organize import organize_mentions

    wave = _org(mention_id="tx", event_type="outbreak", mention_kind="event", title="Texas wave")
    death = _org(
        mention_id="death",
        event_type="notable_case",
        mention_kind="inner",
        title="Child died in Lubbock",
        actor="",
        stated_start="2025-02-26",
        post_id=2,
    )
    site_a = _org(
        mention_id="site-a",
        event_type="notable_case",
        mention_kind="inner",
        title="Exposure at the clinic",
        location={"country": "United States", "admin1": "Texas", "site": "Clinic A"},
        post_id=3,
    )
    site_b = _org(
        mention_id="site-b",
        event_type="notable_case",
        mention_kind="inner",
        title="Exposure at the church",
        location={"country": "United States", "admin1": "Texas", "site": "Church B"},
        post_id=4,
    )
    result = organize_mentions([wave, death, site_a, site_b])
    texas = _by(result, kind="wave")[0]
    inners = _by(result, kind="inner")
    assert {event["title"] for event in inners} == {
        "Child died in Lubbock",
        "Exposure at the clinic",
        "Exposure at the church",
    }
    assert {event["parent_event_id"] for event in inners} == {texas["event_id"]}
    inner_ids = {event["event_id"] for event in inners}
    assert not any(
        link["child_event_id"] in inner_ids and link["parent_event_id"] in inner_ids
        for link in result["links"]
    )


def test_unplaced_region_and_global_stay_on_their_own_grains():
    from apps.claims.events.merge.organize import organize_mentions

    unplaced = _org(
        mention_id="none",
        title="Cases somewhere",
        location={},
        place_scope="unplaced",
        region="",
    )
    europe = _org(
        mention_id="eu",
        event_type="outbreak",
        mention_kind="event",
        title="European wave",
        location={},
        region="Europe",
        place_scope="region",
        post_id=2,
    )
    france = _org(
        mention_id="fr",
        event_type="outbreak",
        mention_kind="event",
        title="French wave",
        location={"country": "France"},
        place_scope="at_named_place",
        post_id=3,
    )
    worldwide = _org(
        mention_id="world",
        title="Worldwide measles cases",
        location={},
        place_scope="global",
        post_id=4,
    )
    result = organize_mentions([unplaced, europe, france, worldwide])
    assert result["held"][0]["mention_id"] == "none"
    assert result["held"][0]["reason"] == "unplaced"
    europe_event = _by(result, grain_path="Europe")
    france_event = _by(result, grain_path="France")
    world_event = _by(result, kind="series", grain_path="global")
    assert len(europe_event) == 1 and len(france_event) == 1 and len(world_event) == 1
    assert france_event[0]["parent_event_id"] == ""
    assert europe_event[0]["parent_event_id"] == ""
    assert not any(link["parent_event_id"] == world_event[0]["event_id"] for link in result["links"])


def test_description_and_agency_are_not_a_person_key_and_a_name_still_joins():
    from apps.claims.events.merge.organize import organize_mentions

    wave = _org(mention_id="tx", event_type="outbreak", mention_kind="event", title="Texas wave")
    child = _org(
        mention_id="child",
        event_type="notable_case",
        mention_kind="inner",
        title="A 6-year-old girl died",
        actor="6-year-old girl",
        post_id=2,
    )
    agency = _org(
        mention_id="cdc",
        event_type="notable_case",
        mention_kind="inner",
        title="CDC describes a death",
        actor="CDC",
        post_id=3,
    )
    daisy_a = _org(
        mention_id="d1",
        event_type="notable_case",
        mention_kind="event",
        title="Daisy Hildebrand died",
        actor="Daisy Hildebrand",
        post_id=4,
    )
    daisy_b = _org(
        mention_id="d2",
        event_type="notable_case",
        mention_kind="event",
        title="Death of Daisy Hildebrand",
        actor="Dr. Daisy Hildebrand",
        post_id=5,
    )
    result = organize_mentions([wave, child, agency, daisy_a, daisy_b])
    assert _by(result, kind="identity")
    identity = _by(result, kind="identity")
    assert len(identity) == 1
    assert _member_ids(result, identity[0]["event_id"]) == {"d1", "d2"}
    descriptions = _by(result, kind="inner")
    assert {event["title"] for event in descriptions} == {"A 6-year-old girl died", "CDC describes a death"}
    assert len({event["event_id"] for event in descriptions}) == 2


def test_census_of_outbreaks_is_an_update_on_the_standing_series():
    from apps.claims.events.merge.organize import organize_mentions

    census = _org(
        mention_id="census",
        event_type="outbreak",
        mention_kind="event",
        title="35 measles outbreaks across the United States",
        location={"country": "United States"},
        place_scope="countrywide",
        date_grain="year",
        date_precision="year",
        stated_start="2025-01-01",
    )
    result = organize_mentions([census])
    assert _by(result, kind="wave") == []
    series = _by(result, kind="series", grain_path="United States")
    assert len(series) == 1
    roles = [row["role"] for row in result["members"] if row["mention_id"] == "census"]
    assert roles == ["update"]
    assert series[0]["span_start"] == ""



