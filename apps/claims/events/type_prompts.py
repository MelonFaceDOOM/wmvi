"""One short follow-up prompt per event type. Fills fields; does not find events."""

from __future__ import annotations

import json
from typing import Any

from apps.claims.events.mention import EVENT_TYPES
from apps.claims.events.prompts import format_chunk_header

DETAIL_SYSTEM = (
    "You fill place, date, and type-specific fields for events already extracted "
    "from one post chunk. Return only JSON matching the schema. "
    "Do not add events. Do not drop events. Keep the same titles and order."
)

PLACE_AND_DATES = """\
PLACE

Fill the finest place the text supports and every parent place the text or the named place itself supports. A named US state gives country United States. One value per slot: never two places joined by "and" in one slot. An organization or agency is not a place: do not fill a country from ICE, FDA, CDC, or a ministry name alone. The place is where this happening is, not a second place a sentence says it spread to. A display label is not a place slot.
- country: English short name. "United States" and "United Kingdom" (not USA, US, Britain, or England used for the UK).
- region: a named continent or multi-country region (Europe, North America, Latin America). Empty when the text names none. A list of countries is not a region and is not one event per country.
- admin1: state, province, or constituent country.
- admin2: the division directly under admin1 (county, département, district). Empty when the text names none.
- locality: city. A city that is also its own first-level division (Singapore, Berlin, the District of Columbia) fills both locality and admin1.
- site: a named facility, venue, school, or hospital, in the text's wording.
- location_label: a short display label.
- admin1_other: other states or provinces only when they are places of the same single happening.

place_scope (exactly one):
- at_named_place: the happening is at the finest filled place, including a wave in a country.
- countrywide: an act or status that applies to a whole country and has no finer place. The country slot must be filled.
- region: a named region with no finer place. The region slot must be filled.
- unplaced: the happening has an anchor and the text names no place. Use this when every place slot, including region, is empty.
- global: the text presents the happening as worldwide and it has no place. A region is not global.

DATES

The model returns the words and a precision of day, month, year, or unknown. Do not use range. Code will expand a year or a month into a range later.
- stated_start and stated_end are YYYY-MM-DD only, or empty.
- date_precision is exactly one of day, month, year, unknown.
- A single day only when the happening itself was that day (a death, a vote, a publication). A year or a month is not a day. A year alone is YYYY-01-01 with precision year. A month is YYYY-MM-01 with precision month.
- Relative days resolve to day ("yesterday"). "This year" and "last year" resolve to year.
- Never copy posted_at onto an older event. No date in the text for this event means unknown.
- stated_date_text is the words in the text that gave the date. It is required when precision is not unknown.
- temporal_kind: instant, interval, open, or unknown.
- span_status: closed only when the text says this happening ended. Otherwise open. A report day does not close a wave.

OTHER FIELDS

role_in_post: focal if this is what the post is about; mentioned for a distinct secondary happening.
vaccines, people, organizations: only strings the text uses. Empty lists if none.
case_count, death_count: as stated in the text, or empty string.
quotes: 1-3 exact substrings of the title or body that support the event's type and, where present, its place and date. Copy character for character.
"""

TYPE_INSTRUCTIONS = {
    "outbreak": (
        "These stubs are outbreaks: a wave in a place. A census of separate outbreaks "
        "does not belong here. Do not fill dates from a case-count sentence "
        '("40 more cases today," "1,912 cases as of December 10"). That dates the update, not the wave. '
        "Leave the outbreak unknown unless the text dates the wave itself. "
        "span_status is closed only when the text says the wave ended."
    ),
    "cases": (
        "These stubs are case counts or confirmations. A local count keeps the finest place "
        "the text gives; it does not take the country as its place only because the country is named. "
        "A worldwide tally is place_scope global. For a count update, the report date is this event's date. "
        "span_status stays open unless the text says that happening ended."
    ),
    "notable_case": (
        "These stubs are notable cases. actor is the patient's name, or empty. "
        "A role, age, kinship word, agency, official, or place is not a name "
        '("child," "6-year-old girl," "CDC," "parents"). '
        "Each named exposure site is its own event. Fill diseases when the text names one."
    ),
    "vaccine_injury": (
        "These stubs are vaccine injuries. actor is the injured person's name, or empty. "
        '"Nurses" is not a name. A role, age, kinship word, agency, or place is not a name.'
    ),
    "institutional_action": (
        "These stubs are institutional actions. The event is the act or the change, "
        "not a rule already in force. actor is the party that acted."
    ),
    "study_publication": (
        "These stubs are study publications or retractions. study_action is publication or retraction. "
        "actor is the paper's author, not the journal. The Lancet is not an author. "
        "The title names the paper."
    ),
    "other": (
        "These stubs are other public happenings. Fill place and date the same way as an institutional act."
    ),
}

_DETAIL_PROPERTIES = {
    "title": {"type": "string"},
    "summary": {"type": "string"},
    "role_in_post": {"type": "string"},
    "temporal_kind": {"type": "string"},
    "stated_start": {"type": "string"},
    "stated_end": {"type": "string"},
    "date_precision": {"type": "string"},
    "stated_date_text": {"type": "string"},
    "span_status": {"type": "string"},
    "country": {"type": "string"},
    "region": {"type": "string"},
    "admin1": {"type": "string"},
    "admin2": {"type": "string"},
    "locality": {"type": "string"},
    "site": {"type": "string"},
    "location_label": {"type": "string"},
    "place_scope": {"type": "string"},
    "study_action": {"type": "string"},
    "admin1_other": {"type": "array", "items": {"type": "string"}},
    "actor": {"type": "string"},
    "diseases": {"type": "array", "items": {"type": "string"}},
    "vaccines": {"type": "array", "items": {"type": "string"}},
    "people": {"type": "array", "items": {"type": "string"}},
    "organizations": {"type": "array", "items": {"type": "string"}},
    "case_count": {"type": "string"},
    "death_count": {"type": "string"},
    "quotes": {"type": "array", "items": {"type": "string"}},
}

DETAIL_SCHEMA: dict[str, Any] = {
    "name": "chunk_event_detail",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": _DETAIL_PROPERTIES,
                    "required": list(_DETAIL_PROPERTIES),
                },
            }
        },
        "required": ["events"],
    },
}

DETAIL_USER = """\
Fill place, date, and the other fields for these {event_type} events already extracted from the post. Return one object per stub, in the same order, with the same titles. Do not add or drop events.

{chunk}

{type_instructions}

{place_and_dates}

stubs:
{stubs}
"""


def format_detail_prompt(*, doc: dict[str, Any], event_type: str, stubs: list[dict[str, Any]]) -> str:
    etype = str(event_type or "").strip() or "other"
    if etype not in EVENT_TYPES:
        etype = "other"
    lines = []
    for stub in stubs:
        lines.append(
            json.dumps(
                {
                    "title": stub.get("title") or "",
                    "summary": stub.get("summary") or "",
                    "mention_kind": stub.get("mention_kind") or "",
                    "diseases": stub.get("diseases") or [],
                    "quotes": stub.get("quotes") or [],
                },
                ensure_ascii=False,
            )
        )
    return DETAIL_USER.format(
        event_type=etype,
        chunk=format_chunk_header(doc=doc),
        type_instructions=TYPE_INSTRUCTIONS[etype],
        place_and_dates=PLACE_AND_DATES,
        stubs="\n".join(lines) or "(none)",
    )


def format_detail_system() -> str:
    return DETAIL_SYSTEM
