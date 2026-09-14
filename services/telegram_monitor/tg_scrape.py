from typing import AsyncIterator, List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import timezone, datetime
import asyncio
import logging
from filtering.anonymization import redact_pii
from telethon import TelegramClient, errors
from telethon.tl.types import InputPeerChannel, Message

log = logging.getLogger(__name__)

# Stay under Telegram resolve/history hammer. Longer waits skip the channel.
MAX_FLOOD_SLEEP_S = 90


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


def flood_sleep_s(e: errors.FloodWaitError) -> int:
    return int(getattr(e, "seconds", 0) or 0) + 1


def flood_exceeds_cap(e: errors.FloodWaitError, cap_s: int = MAX_FLOOD_SLEEP_S) -> bool:
    return flood_sleep_s(e) > cap_s


async def sleep_flood_or_skip(
    e: errors.FloodWaitError,
    where: str,
    *,
    cap_s: int = MAX_FLOOD_SLEEP_S,
) -> bool:
    """Sleep out a short FloodWait. Return False if the wait is too long (caller should skip)."""
    wait_s = flood_sleep_s(e)
    if wait_s > cap_s:
        log.warning(
            "%s: FloodWait %ds exceeds cap %ds; skipping instead of sleeping",
            where, e.seconds, cap_s,
        )
        return False
    log.warning("%s: FloodWait %ds; sleeping %ds", where, e.seconds, wait_s)
    await asyncio.sleep(wait_s)
    return True


def peer_channel_id(entity) -> Optional[int]:
    if entity is None:
        return None
    cid = getattr(entity, "channel_id", None)
    if cid is None:
        cid = getattr(entity, "id", None)
    if cid is None:
        return None
    return int(cid)


def peer_access_hash(entity) -> Optional[int]:
    ah = getattr(entity, "access_hash", None)
    if ah is None:
        return None
    return int(ah)


def peer_username(entity, fallback: Optional[str] = None) -> Optional[str]:
    uname = getattr(entity, "username", None)
    if uname:
        return norm_channel(str(uname))
    if fallback:
        return norm_channel(fallback)
    return None


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


@dataclass
class ScrapePass:
    """Filled in by scrape_channel_batches as it walks newest → oldest."""

    reason: str = "running"  # caught_up | history_end | flood | rpc | error
    flood_seconds: Optional[int] = None
    newest_id: Optional[int] = None
    newest_ts: Optional[datetime] = None
    oldest_id: Optional[int] = None
    oldest_ts: Optional[datetime] = None
    messages_seen: int = 0
    messages_with_text: int = 0

    def note(self, msg_id: int, msg_ts: Optional[datetime]) -> None:
        self.messages_seen += 1
        if self.newest_id is None or msg_id > self.newest_id:
            self.newest_id = msg_id
            self.newest_ts = msg_ts
        if self.oldest_id is None or msg_id < self.oldest_id:
            self.oldest_id = msg_id
            self.oldest_ts = msg_ts


@dataclass
class ChannelState:
    channel_id: int
    username: Optional[str] = None
    access_hash: Optional[int] = None
    high_watermark_id: Optional[int] = None
    high_watermark_ts: Optional[datetime] = None
    low_watermark_id: Optional[int] = None
    low_watermark_ts: Optional[datetime] = None
    backfill_complete: bool = False
    last_error: Optional[str] = None
    last_scraped_at: Optional[datetime] = None

    def input_peer(self) -> Optional[InputPeerChannel]:
        if self.channel_id is None or self.access_hash is None:
            return None
        return InputPeerChannel(int(self.channel_id), int(self.access_hash))


def apply_catch_up(
    state: ChannelState,
    progress: ScrapePass,
) -> ChannelState:
    """Advance high watermark only after a contiguous walk down to the previous high."""
    if progress.reason != "caught_up":
        return state
    if progress.newest_id is None:
        return state
    if state.high_watermark_id is None or progress.newest_id > state.high_watermark_id:
        state.high_watermark_id = progress.newest_id
        state.high_watermark_ts = progress.newest_ts
    return state


def apply_backfill(
    state: ChannelState,
    progress: ScrapePass,
) -> ChannelState:
    """Record the contiguous newest-first chunk; mark complete only on history_end."""
    if state.high_watermark_id is None and progress.newest_id is not None:
        state.high_watermark_id = progress.newest_id
        state.high_watermark_ts = progress.newest_ts
    if progress.oldest_id is not None:
        if state.low_watermark_id is None or progress.oldest_id < state.low_watermark_id:
            state.low_watermark_id = progress.oldest_id
            state.low_watermark_ts = progress.oldest_ts
    if progress.reason == "history_end":
        state.backfill_complete = True
    return state


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


async def probe_channel(
    client: TelegramClient,
    channel_name: str,
    *,
    reraise_flood: bool = False,
):
    """Resolve a public @username to a full entity (title, username, hash)."""
    chan = norm_channel(channel_name)
    try:
        return await client.get_entity(chan)
    except (errors.UsernameInvalidError, errors.UsernameNotOccupiedError):
        return None
    except errors.FloodWaitError as e:
        log.warning("FloodWait %ds resolving %s", e.seconds, chan)
        if reraise_flood:
            raise
        if not await sleep_flood_or_skip(e, f"resolve {chan}"):
            return None
        try:
            return await client.get_entity(chan)
        except errors.FloodWaitError as e2:
            log.warning(
                "FloodWait %ds still resolving %s; skipping", e2.seconds, chan,
            )
            return None
    except errors.RPCError:
        log.warning("RPC error resolving %s", chan, exc_info=True)
        return None


async def resolve_channel_peer(
    client: TelegramClient,
    channel_name: str,
    *,
    cached: Optional[ChannelState] = None,
    reraise_flood: bool = False,
) -> Optional[tuple[Any, int, Optional[int], Optional[str]]]:
    """
    Return (peer, channel_id, access_hash, username) without resolving the
    username when we already have id+hash (DB or Telethon session cache).
    """
    handle = norm_channel(channel_name)
    if cached is not None:
        peer = cached.input_peer()
        if peer is not None:
            return (
                peer,
                int(cached.channel_id),
                cached.access_hash,
                cached.username or handle,
            )

    try:
        entity = await client.get_input_entity(handle)
    except (ValueError, TypeError, errors.UsernameInvalidError,
            errors.UsernameNotOccupiedError):
        entity = None
    except errors.FloodWaitError as e:
        log.warning("FloodWait %ds resolving %s (get_input_entity)", e.seconds, handle)
        if reraise_flood:
            raise
        if not await sleep_flood_or_skip(e, f"resolve {handle}"):
            return None
        entity = None
    except errors.RPCError:
        log.warning("RPC error resolving %s via cache", handle, exc_info=True)
        entity = None

    if entity is not None:
        cid = peer_channel_id(entity)
        if cid is not None:
            return (
                entity,
                cid,
                peer_access_hash(entity),
                peer_username(entity, handle),
            )

    try:
        entity = await client.get_entity(handle)
    except (errors.UsernameInvalidError, errors.UsernameNotOccupiedError):
        return None
    except errors.FloodWaitError as e:
        log.warning("FloodWait %ds resolving %s", e.seconds, handle)
        if reraise_flood:
            raise
        if not await sleep_flood_or_skip(e, f"resolve {handle}"):
            return None
        try:
            entity = await client.get_entity(handle)
        except errors.FloodWaitError as e2:
            log.warning(
                "FloodWait %ds still resolving %s; skipping channel",
                e2.seconds, handle,
            )
            return None
    except errors.RPCError:
        log.warning("RPC error resolving %s", handle, exc_info=True)
        return None

    cid = peer_channel_id(entity)
    if cid is None:
        return None
    return (
        entity,
        cid,
        peer_access_hash(entity),
        peer_username(entity, handle),
    )


async def scrape_channel_batches(
    client: TelegramClient,
    channel: str,
    *,
    entity=None,
    min_id: int = 0,
    max_id: int = 0,
    batch_size: int = 100,
    sleep_every: int = 500,
    sleep_s: float = 0.3,
    progress: Optional[ScrapePass] = None,
) -> AsyncIterator[List[Dict[str, Any]]]:
    """
    Yield batches of normalized messages for a single channel.

    Newest → oldest. Telethon min_id/max_id are exclusive.
    Catch-up: min_id = high watermark (only newer ids).
    Backfill: max_id = low watermark (only older ids).
    First scrape: both 0, walk until history ends.

    `progress` records extrema over *all* seen messages (including empty text)
    and the stop reason. FloodWait longer than MAX_FLOOD_SLEEP_S does not sleep.
    """
    if progress is None:
        progress = ScrapePass()
    if not entity:
        resolved = await resolve_channel_peer(client, channel)
        if resolved is None:
            progress.reason = "error"
            return
        entity = resolved[0]
    username = peer_username(entity, channel)
    chan_id = peer_channel_id(entity)
    if chan_id is None:
        progress.reason = "error"
        log.warning("channel has no id? channel=%s entity=%r", channel, entity)
        return

    batch: List[Dict[str, Any]] = []
    processed = 0
    natural_end = "caught_up" if min_id else "history_end"

    try:
        async for msg in client.iter_messages(
            entity, limit=None, min_id=min_id, max_id=max_id,
        ):
            if msg.date is None:
                continue

            msg_dt = clean_created_at_ts_from_telegram(msg.date)
            if msg_dt is None:
                continue

            if min_id and msg.id <= min_id:
                progress.reason = "caught_up"
                break

            progress.note(msg.id, msg_dt)

            normalized = normalize_message(msg, username, chan_id)
            text = normalized.get("text", "")

            if text and text.strip():
                batch.append(normalized)
                processed += 1
                progress.messages_with_text += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

            if sleep_every and processed > 0 and processed % sleep_every == 0:
                await asyncio.sleep(sleep_s)

    except errors.FloodWaitError as e:
        if batch:
            yield batch
        progress.reason = "flood"
        progress.flood_seconds = int(getattr(e, "seconds", 0) or 0)
        log.warning(
            "channel scrape FloodWait %ds (cap %ds); stopping this pass after %d msgs channel=%s",
            e.seconds, MAX_FLOOD_SLEEP_S, progress.messages_seen, channel,
        )
        await sleep_flood_or_skip(e, f"scrape {channel}")
        return

    except errors.RPCError:
        if batch:
            yield batch
        progress.reason = "rpc"
        log.warning("channel scrape RPC error; stopping this pass channel=%s",
                    channel, exc_info=True)
        return

    except Exception:
        if batch:
            yield batch
        progress.reason = "error"
        raise

    if batch:
        yield batch
    if progress.reason == "running":
        progress.reason = natural_end
