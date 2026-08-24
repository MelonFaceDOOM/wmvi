"""Human labels and Reddit grouping for demo platform filters."""

from __future__ import annotations

from typing import Any

# Display groups. Reddit comment+submission are one chip so comments do not dominate.
GROUPS: dict[str, tuple[str, ...]] = {
    "reddit": ("reddit_comment", "reddit_submission"),
    "tweet": ("tweet",),
    "youtube_comment": ("youtube_comment",),
    "youtube_video": ("youtube_video",),
    "telegram_post": ("telegram_post",),
    "podcast_episode": ("podcast_episode",),
    "news_article": ("news_article",),
}

GROUP_ORDER: tuple[str, ...] = (
    "reddit",
    "tweet",
    "youtube_comment",
    "youtube_video",
    "telegram_post",
    "podcast_episode",
    "news_article",
)

LABELS: dict[str, str] = {
    "reddit": "Reddit",
    "tweet": "Twitter / X",
    "youtube_comment": "YouTube comment",
    "youtube_video": "YouTube video",
    "telegram_post": "Telegram",
    "podcast_episode": "Podcast",
    "news_article": "News",
}

_KEY_TO_GROUP: dict[str, str] = {key: gid for gid, keys in GROUPS.items() for key in keys}


def group_id(raw: str | None) -> str:
    key = str(raw or "unknown")
    return _KEY_TO_GROUP.get(key, key)


def group_to_keys(gid: str) -> tuple[str, ...]:
    if gid in GROUPS:
        return GROUPS[gid]
    return (gid,)


def label(raw_or_group: str | None) -> str:
    token = str(raw_or_group or "unknown")
    if token in LABELS:
        return LABELS[token]
    gid = group_id(token)
    if gid in LABELS:
        return LABELS[gid]
    return token.replace("_", " ").title()


def keys_for_groups(group_ids: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for gid in group_ids:
        for key in group_to_keys(gid):
            if key not in seen:
                seen.add(key)
                out.append(key)
    return tuple(out)


def sort_groups(group_ids: list[str]) -> list[str]:
    rank = {gid: i for i, gid in enumerate(GROUP_ORDER)}
    return sorted(group_ids, key=lambda g: (rank.get(g, len(rank)), label(g).lower(), g))


def groups_from_keys(raw_keys: list[str]) -> list[str]:
    seen: list[str] = []
    have: set[str] = set()
    for key in raw_keys:
        gid = group_id(key)
        if gid not in have:
            have.add(gid)
            seen.append(gid)
    return sort_groups(seen)


def collapse_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    acc: dict[str, int] = {}
    for row in rows:
        gid = group_id(str(row.get("platform") or "unknown"))
        acc[gid] = acc.get(gid, 0) + int(row.get("n") or 0)
    return [
        {"platform": gid, "label": label(gid), "n": n}
        for gid, n in sorted(acc.items(), key=lambda item: (-item[1], item[0]))
    ]


def collapse_weekly(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    acc: dict[tuple[str, str], int] = {}
    for row in rows:
        week = str(row.get("week") or "")
        gid = group_id(str(row.get("platform") or "unknown"))
        acc[(week, gid)] = acc.get((week, gid), 0) + int(row.get("n") or 0)
    return [
        {"week": week, "platform": label(gid), "n": n}
        for (week, gid), n in sorted(acc.items(), key=lambda item: (item[0][0], item[0][1]))
    ]
