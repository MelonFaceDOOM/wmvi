from typing import AsyncIterator, List, Dict, Any, Optional
from datetime import timezone, datetime
import asyncio
from filtering.anonymization import redact_pii
from telethon import TelegramClient, errors
from telethon.tl.types import Message


def ensure_ascii(s: Optional[str], limit: int = 4000) -> Optional[str]:
    if s is None:
        return None
    s = s.replace("\r", " ").strip()
    return (s[:limit] + "…") if len(s) > limit else s


def norm_channel(s: str) -> str:
    s = s.strip()
    s = s.replace("https://", "").replace("http://", "")
    s = s.replace("t.me/s/", "").replace("t.me/", "")
    if s.startswith("@"):
        s = s[1:]
    return s


def clean_created_at_ts_from_telegram(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Telethon message.date is usually a datetime (often tz-aware UTC, but be defensive).
    Returns a tz-aware UTC datetime or None.
    """
    if not dt:
        return None

    try:
        # If naive, assume UTC (Telethon typically uses UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def normalize_message(msg: Message, chan_username: Optional[str], chan_id: int) -> Dict[str, Any]:
    # Telethon gives naive UTC; make it explicit
    dt = msg.date.replace(tzinfo=timezone.utc) if msg.date else None

    # metrics (may be None)
    views = getattr(msg, "views", None)
    forwards = getattr(msg, "forwards", None)

    replies = None
    try:
        if msg.replies and hasattr(msg.replies, "replies"):
            replies = msg.replies.replies
    except Exception:
        pass

    reactions_total = None
    try:
        if msg.reactions and getattr(msg.reactions, "results", None):
            reactions_total = sum((r.count or 0)
                                  for r in msg.reactions.results)
    except Exception:
        pass

    link = f"https://t.me/{chan_username}/{msg.id}" if chan_username else None

    created_at_ts = clean_created_at_ts_from_telegram(
        getattr(msg, "date", None))

    text = ensure_ascii(msg.message)
    filtered = redact_pii(text)

    return {
        "platform": "telegram",
        "channel_username": chan_username,
        "channel_id": chan_id,
        "message_id": msg.id,
        "created_at_ts": created_at_ts,
        "text": text,
        "filtered_text": filtered,
        "views": views,
        "forwards": forwards,
        "replies": replies,
        "reactions_total": reactions_total,
        "link": link,
        "is_pinned": bool(getattr(msg, "pinned", False)),
        "has_media": bool(msg.media),
        "raw_type": type(msg).__name__,
    }


async def probe_channel(client: TelegramClient, channel_name: str):
    chan = norm_channel(channel_name)
    try:
        entity = await client.get_entity(chan)
    except errors.UsernameInvalidError:
        raise ValueError(f"Invalid channel username: {channel_name}")
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        entity = await client.get_entity(chan)

    return entity


async def scrape_channel_batches(
    client: TelegramClient,
    channel: str,
    since_dt,
    *,
    entity=None,
    batch_size: int = 100,
    sleep_every: int = 500,
    sleep_s: float = 0.3,
) -> AsyncIterator[List[Dict[str, Any]]]:
    """
    Yield batches of normalized messages for a single channel.

    - Newest → oldest
    - Stops once msg.date < since_dt
    - No dedupe, no persistence
    - Caller controls client lifecycle
    """
    if not entity:
        entity = await probe_channel(client, channel)
    username = getattr(entity, "username", None)
    chan_id = getattr(entity, "id", None)

    batch: List[Dict[str, Any]] = []
    processed = 0

    try:
        async for msg in client.iter_messages(entity, limit=None):
            if msg.date is None:
                continue

            msg_dt = msg.date.replace(tzinfo=timezone.utc)
            if msg_dt < since_dt:
                break
            normalized = normalize_message(msg, username, chan_id)
            text = normalized.get("text", "")
            if text and text.strip():
                batch.append(normalize_message(msg, username, chan_id))
                processed += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

            if sleep_every and processed % sleep_every == 0:
                await asyncio.sleep(sleep_s)

    except errors.FloodWaitError as e:
        # Yield what we have before sleeping
        if batch:
            yield batch
        await asyncio.sleep(e.seconds + 1)
        return

    except errors.RPCError as e:
        if batch:
            yield batch
        return

    except Exception:
        if batch:
            yield batch
        raise

    if batch:
        yield batch
