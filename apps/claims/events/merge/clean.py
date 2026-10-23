"""Canonicalize extract place strings. Not a gazetteer."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

GEO_DIR = Path(__file__).resolve().parent.parent / "geo"


@cache
def _load_json(name: str) -> Any:
    path = GEO_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


def country_aliases() -> dict[str, str]:
    return dict(_load_json("country_aliases.json"))


def admin1_aliases() -> dict[str, str]:
    return dict(_load_json("admin1_aliases.json"))


def admin2_aliases() -> dict[str, str]:
    return dict(_load_json("admin2_aliases.json"))


def not_a_country() -> set[str]:
    return set(_load_json("not_a_country.json"))


def country_not_atomic() -> set[str]:
    return set(_load_json("country_not_atomic.json"))


def small_wave_countries() -> set[str]:
    return set(_load_json("small_wave_countries.json"))


def generic_national_actors() -> set[str]:
    return set(_load_json("generic_national_actors.json"))


def _canon(raw: str, table: dict[str, str]) -> str:
    val = str(raw or "").strip()
    if not val:
        return ""
    return table.get(val, val)


def clean_country(raw: str) -> str:
    val = str(raw or "").strip()
    if not val or val in not_a_country() or val in country_not_atomic():
        return ""
    return _canon(val, country_aliases())


def clean_admin1(raw: str) -> str:
    return _canon(raw, admin1_aliases())


def clean_admin2(raw: str) -> str:
    return _canon(raw, admin2_aliases())


def grain(place: dict[str, str]) -> tuple[str, str]:
    """Finest filled level, and the path from country down to that level.

    A county and its state are different grains. The path keeps two places
    that share a local name from joining. A region with no finer place is its
    own grain and is not prefixed onto a country path.
    """
    region = str(place.get("region") or "").strip()
    finest = ""
    for level in ("site", "locality", "admin2", "admin1", "country"):
        if str(place.get(level) or "").strip():
            finest = level
            break
    if not finest:
        if region:
            return "region", region
        return "", ""
    chain: list[str] = []
    for level in ("country", "admin1", "admin2", "locality", "site"):
        val = str(place.get(level) or "").strip()
        if val:
            chain.append(val)
        if level == finest:
            break
    return finest, " / ".join(chain)


def clean_place(mention: dict[str, Any]) -> dict[str, str]:
    loc = mention.get("location") or {}
    if not isinstance(loc, dict):
        loc = {}
    country_raw = str(loc.get("country") or "").strip()
    admin1_raw = str(loc.get("admin1") or "").strip()
    country = clean_country(country_raw)
    admin1 = clean_admin1(admin1_raw)
    if country_raw == "England" and not admin1:
        admin1 = "England"
    admin2 = clean_admin2(str(loc.get("admin2") or ""))
    locality = str(loc.get("locality") or "").strip()
    site = str(loc.get("site") or "").strip()
    region = str(mention.get("region") or loc.get("region") or "").strip()
    return {
        "country": country,
        "region": region,
        "admin1": admin1,
        "admin2": admin2,
        "locality": locality,
        "site": site,
    }


def iso_date_only(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text
