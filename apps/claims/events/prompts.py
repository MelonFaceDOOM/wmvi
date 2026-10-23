"""Triage prompt + schema for post-chunk → event stubs."""

from __future__ import annotations

from typing import Any

DEFAULT_MODEL = "gpt-6-luna"
DEFAULT_TRIAGE_MODEL = "gpt-5.4-mini"
DEFAULT_PROVIDER = "openai"

TRIAGE_SYSTEM = (
    "You extract public happenings from one post chunk: disease spread and cases, "
    "notable cases, vaccine injuries, institutional actions on disease or vaccination, "
    "and research whose publication or retraction became news. "
    "Any disease or vaccine. "
    "Return only JSON matching the schema. Empty events is often correct. "
    "The goal is a general-interest news timeline, not every fact in the text. "
    "Do not fill place or dates. A later pass does that."
)

SYSTEM_PROMPT = TRIAGE_SYSTEM

TRIAGE_USER = """\
Extract public happenings from this post that a news agency could report and that people not directly involved would care about. The subject may be any disease or vaccine. Extract every distinct happening the text supports, including secondary ones, up to 4. If there is none, return an empty events array.

posted_at: {posted_at}
platform: {platform}
channel: {channel}
title: {title}

body:
{body}

WHAT QUALIFIES

A qualifying event meets all three:
1. Relevant: a real-world development in disease, vaccination, vaccine policy, an institution's response to disease or vaccination, or research that itself became news.
2. Public: a general-interest news organization could report it as a standalone fact.
3. Anchored: the text gives at least one detail that tells this happening apart from others of its kind. An anchor is a named place, a named person, a named organization together with what it did, a named paper, or a specific date together with what happened. One anchor is enough. A missing date, a missing finer place, an unnamed patient, or a missing case count is never a reason to drop an event that has an anchor. Drop only when nothing in the chunk distinguishes it: "cases are rising in my area," "they mandated it that year," "a study found," "an outbreak somewhere."

Judge whether the text states a specific happening, not why the author mentions it. A named or dated happening counts when it is context, an example, or a point in an argument.

A public happening still qualifies when:
- the people involved are unnamed;
- it is historical, or is mentioned as context or as a point in an argument;
- the post is mostly commentary, argument, or ridicule;
- it appears only in a headline or one clause;
- it is one of several happenings in the same sentence;
- the text gives only a case count or a simple surveillance update.

Do not emit:
- the speaker's own or their family's illness, vaccination, titers, side effects, or other private experiences, or an unnamed acquaintance's illness or death;
- routine operations: normal clinic work, ordinary reminders to get vaccinated, programs already running, recommendations that did not change;
- generic medical facts, known side-effect classes, biology, or hypotheticals;
- opinions, accusations, speculation, predictions, or rhetoric, unless the text also reports the happening underneath them. An alleged hidden act (concealed, destroyed, covered up, lied about) is an accusation even when it is stated as fact, unless the text reports an official finding, ruling, or public record of it;
- general history or statistics with no specific happening ("vaccines eliminated measles," "deaths were common before the vaccine"), a bibliography, or a study cited as evidence when its publication or retraction is not itself reported;
- journalism, testimony, investigations, publicity, campaigns, or records requests, unless the act is itself an institutional action (a court orders disclosure, an agency formally releases data);
- a proposal, a planned vote, or an intended action when the text does not say what happened.

EVENT TYPES (exactly one per event)

The three infection types are decided by what the text says, not by how large the event seems.
- outbreak: the text presents spread or a wave in a named place. A count of many separate outbreaks ("35 outbreaks," "outbreaks in 57 countries") is not one outbreak.
- cases: a reported case count or case confirmation with no wave wording and nothing notable about the case. National, state, and city tallies are cases. A worldwide tally is cases.
- notable_case: something about the case itself is the news: who it was, a death or rare complication, a named exposure site, an import with a story, a first in years. A bare official confirmation is cases. Each named exposure site is its own notable_case.
- vaccine_injury: a specific public case of injury or death attributed to a vaccine. Generic claims that a vaccine causes a condition are not events. A lawsuit or ruling is institutional_action.
- institutional_action: a government, regulator, court, public institution, or named organization takes or changes an action about disease or vaccination. The event is the act or the change, not a rule already in force.
- study_publication: the text reports that a named paper or review was published or retracted, and that is the happening.
- other: a qualifying happening that fits none of the above.

mention_kind (exactly one per event):
- event: the outbreak, the standing series, the act, the paper, or the named patient itself.
- update: a case-count or status update about an ongoing happening ("40 more cases," "1,912 cases as of December 10"). A report of more cases is an update, not a second outbreak.
- inner: a death, a named exposure site, or a superspreader inside a wave. Do not name a parent event.

A worldwide tally is cases and update. An agency confirmation or alert about cases or an outbreak is part of that infection happening, not institutional_action. A death, exposure site, or other notable case in a wave is its own inner event.

TITLE, SUMMARY, TAGS

Title: the happening in headline form, not the post's claim about it. Summary: one sentence.
diseases: only strings the text uses. Empty list if none.

EVIDENCE

quotes: 1-3 exact substrings of the title or body that support the event's type. Copy character for character, including markdown, asterisks, and punctuation. A short exact span is better than a cleaned sentence. No quote means no event.
"""

USER_PROMPT = TRIAGE_USER

_STUB_PROPERTIES = {
    "event_type": {"type": "string"},
    "mention_kind": {"type": "string"},
    "title": {"type": "string"},
    "summary": {"type": "string"},
    "diseases": {"type": "array", "items": {"type": "string"}},
    "quotes": {"type": "array", "items": {"type": "string"}},
}

TRIAGE_SCHEMA: dict[str, Any] = {
    "name": "chunk_event_triage",
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
                    "properties": _STUB_PROPERTIES,
                    "required": list(_STUB_PROPERTIES),
                },
            }
        },
        "required": ["events"],
    },
}

RESPONSE_SCHEMA = TRIAGE_SCHEMA


def _field(val: Any, empty: str = "(none)") -> str:
    text = str(val or "").strip()
    return text if text else empty


def format_chunk_header(*, doc: dict[str, Any]) -> str:
    return (
        f"posted_at: {_field(doc.get('posted_at'), 'unknown')}\n"
        f"platform: {_field(doc.get('platform'), 'unknown')}\n"
        f"channel: {_field(doc.get('channel'))}\n"
        f"title: {_field(doc.get('title'))}\n"
        f"\nbody:\n{_field(doc.get('body'), '(empty)')}"
    )


def format_user_prompt(*, doc: dict[str, Any]) -> str:
    return TRIAGE_USER.format(
        posted_at=_field(doc.get("posted_at"), "unknown"),
        platform=_field(doc.get("platform"), "unknown"),
        channel=_field(doc.get("channel")),
        title=_field(doc.get("title")),
        body=_field(doc.get("body"), "(empty)"),
    )


QUOTE_REPAIR_SYSTEM = (
    "You replace event quotes with exact substrings of one post. "
    "Return only JSON matching the schema. Copy characters from the post. Do not paraphrase."
)

QUOTE_REPAIR_USER = """\
The quotes on this event are not exact substrings of the title or body. Return 1-3 quotes copied character for character from the title or body, including markdown, asterisks, and unusual spaces. A short exact span is enough. Do not change the event. If no span in the post supports it, return an empty quotes list.

title: {title}

body:
{body}

event title: {event_title}
event_type: {event_type}
place: {place}
rejected quotes:
{rejected}
"""

QUOTE_REPAIR_SCHEMA: dict[str, Any] = {
    "name": "quote_repair",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "quotes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["quotes"],
    },
}


def format_quote_repair_prompt(*, doc: dict[str, Any], item: dict[str, Any]) -> str:
    place = ", ".join(
        str(item.get(field) or "").strip()
        for field in ("site", "locality", "admin2", "admin1", "region", "country")
        if str(item.get(field) or "").strip()
    )
    rejected = item.get("quotes") or []
    if not isinstance(rejected, list):
        rejected = []
    rejected_text = "\n".join(f"- {quote}" for quote in rejected if str(quote).strip()) or "(none)"
    return QUOTE_REPAIR_USER.format(
        title=_field(doc.get("title")),
        body=_field(doc.get("body"), "(empty)"),
        event_title=_field(item.get("title")),
        event_type=_field(item.get("event_type")),
        place=place or "(none)",
        rejected=rejected_text,
    )
