from datetime import datetime, timezone
import asyncio
import logging
import time

from db.db import getcursor, init_pool, close_pool
from ingestion.telegram import TelegramPostRow, flush_telegram_batch
from ingestion.ingestion import ensure_scrape_job
from .tg_scrape import (
    ChannelState,
    ScrapePass,
    apply_backfill,
    apply_catch_up,
    norm_channel,
    resolve_channel_peer,
    scrape_channel_batches,
)
from telethon import TelegramClient
from pathlib import Path

from dotenv import load_dotenv
import os
load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION = os.getenv("TG_SESSION", "services/telegram_monitor/tg_scrape")


CHANNEL_LIST = [
    "@brownstoneinst",
    "@mrn_death",
    "@STEALTHWARRIOR7",
    "@ThePatriot17",
    "@NicHulscher",
    "@thomassheridanarts",
    "@CovidVaccineTruth",
    "@covid_vaccine_injuries",
    "@CNN_English_News",
    "@PeterMcCulloughMD",
    "@Australians_Against_Vax_Mandates",
    "@australiaoneparty_official",
    "@youllfindout",
    "@SGTnewsNetwork",
    "@IVERMECTIN444",
    "@NEWSVIDEOS56",
    "@SlayNews",
    "@NFSCHimalayaNews",
    "@pastcipher",
    "@chancechronicles8",
    "@CeTvlxeew6NkZDZh",
    "@communityhealthproject",
    # First expansion from probe (see CHANNEL_DISCOVERY.md)
    "@TruthAboutCOVID",
    "@childrenshd",
    "@DiedSuddenlyConnecticut",
    "@P_McCulloughMD",
    "@IMA_Health",
    "@arkmedic",
    "@DrDavidMartin",
    "@covidvaccineadversereactions",
    "@covid19vaccinevictims",
    "@childcovidvaccineinjuriesuk",
    "@NVIC_VaccineFreedom",
    "@DrPaulMarik",
    "@HarveyRisch",
    "@TheHigh_Wire",
    "@vaccinechoicecanada",
    "@CaliVaxChoice",
    "@CHDCanada",
    "@RemediesCV19VaxxVictims",
    "@antivaxxfacts",
    "@VigilantFox",
]

log = logging.getLogger(__name__)

CHANNEL_PAUSE_S = 2.0

_STATE_SELECT = """
    channel_id, username, access_hash,
    high_watermark_id, high_watermark_ts,
    low_watermark_id, low_watermark_ts,
    backfill_complete, last_error, last_scraped_at
"""


def _setup_logging() -> None:
    """
    Minimal, sane defaults:
    - INFO level by default
    - timestamps in UTC
    - quiet Telethon noise unless you bump log level
    """
    if logging.getLogger().handlers:
        # Don't double-configure if something else already called basicConfig.
        return

    class _UTCFormatter(logging.Formatter):
        converter = time.gmtime

    handler = logging.StreamHandler()
    handler.setFormatter(_UTCFormatter(
        fmt="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

    logging.getLogger("telethon").setLevel(logging.WARNING)


def _session_paths(session: str) -> tuple[Path, Path]:
    """
    Telethon accepts either a base name or a filename. In practice:
    - if you pass "tg_scrape", it creates "tg_scrape.session"
    - if you pass "tg_scrape.session", it creates that exact file
    Also create a "-journal" sidecar sometimes.
    """
    p = Path(session)
    if p.suffix == ".session":
        main = p
    else:
        main = Path(f"{session}.session")
    journal = Path(str(main) + "-journal")
    return main, journal


def require_session_file(session: str) -> None:
    main, _journal = _session_paths(session)
    if not main.exists():
        raise RuntimeError(
            f"Telethon session file not found: {main}. "
            f"Run an interactive login once to create it:\n"
            f"  python -m services.telegram_monitor --login\n"
            f"(Run from project root so the session lands in the expected location.)"
        )


def _row_to_state(row) -> ChannelState:
    return ChannelState(
        channel_id=int(row[0]),
        username=row[1],
        access_hash=int(row[2]) if row[2] is not None else None,
        high_watermark_id=int(row[3]) if row[3] is not None else None,
        high_watermark_ts=row[4],
        low_watermark_id=int(row[5]) if row[5] is not None else None,
        low_watermark_ts=row[6],
        backfill_complete=bool(row[7]),
        last_error=row[8],
        last_scraped_at=row[9],
    )


def get_channel_state_by_id(channel_id: int) -> ChannelState | None:
    with getcursor() as cur:
        cur.execute(
            f"SELECT {_STATE_SELECT} FROM scrape.telegram_channel_state "
            "WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    return _row_to_state(row) if row else None


def get_channel_state_by_username(username: str) -> ChannelState | None:
    handle = norm_channel(username)
    with getcursor() as cur:
        cur.execute(
            f"SELECT {_STATE_SELECT} FROM scrape.telegram_channel_state "
            "WHERE lower(username) = lower(%s) "
            "ORDER BY last_updated DESC NULLS LAST LIMIT 1",
            (handle,),
        )
        row = cur.fetchone()
    return _row_to_state(row) if row else None


def get_channel_state_from_posts(channel_id: int) -> ChannelState | None:
    with getcursor() as cur:
        cur.execute(
            """
            SELECT
                MAX(message_id), MAX(created_at_ts),
                MIN(message_id), MIN(created_at_ts)
            FROM sm.telegram_post
            WHERE channel_id = %s
            """,
            (channel_id,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return ChannelState(
        channel_id=int(channel_id),
        high_watermark_id=int(row[0]),
        high_watermark_ts=row[1],
        low_watermark_id=int(row[2]),
        low_watermark_ts=row[3],
        backfill_complete=False,
    )


def upsert_channel_state(state: ChannelState) -> None:
    with getcursor() as cur:
        cur.execute(
            """
            INSERT INTO scrape.telegram_channel_state (
                channel_id, username, access_hash,
                high_watermark_id, high_watermark_ts,
                low_watermark_id, low_watermark_ts,
                backfill_complete, last_error, last_scraped_at, last_updated
            ) VALUES (
                %(channel_id)s, %(username)s, %(access_hash)s,
                %(high_watermark_id)s, %(high_watermark_ts)s,
                %(low_watermark_id)s, %(low_watermark_ts)s,
                %(backfill_complete)s, %(last_error)s, %(last_scraped_at)s,
                now()
            )
            ON CONFLICT (channel_id) DO UPDATE SET
                username = COALESCE(
                    EXCLUDED.username, scrape.telegram_channel_state.username),
                access_hash = COALESCE(
                    EXCLUDED.access_hash, scrape.telegram_channel_state.access_hash),
                high_watermark_id = EXCLUDED.high_watermark_id,
                high_watermark_ts = EXCLUDED.high_watermark_ts,
                low_watermark_id = EXCLUDED.low_watermark_id,
                low_watermark_ts = EXCLUDED.low_watermark_ts,
                backfill_complete = EXCLUDED.backfill_complete,
                last_error = EXCLUDED.last_error,
                last_scraped_at = EXCLUDED.last_scraped_at,
                last_updated = now()
            """,
            {
                "channel_id": state.channel_id,
                "username": state.username,
                "access_hash": state.access_hash,
                "high_watermark_id": state.high_watermark_id,
                "high_watermark_ts": state.high_watermark_ts,
                "low_watermark_id": state.low_watermark_id,
                "low_watermark_ts": state.low_watermark_ts,
                "backfill_complete": state.backfill_complete,
                "last_error": state.last_error,
                "last_scraped_at": state.last_scraped_at,
            },
        )


def load_channel_state(channel_id: int, username: str | None) -> ChannelState:
    state = get_channel_state_by_id(channel_id)
    if state is None and username:
        by_name = get_channel_state_by_username(username)
        if by_name is not None and by_name.channel_id == channel_id:
            state = by_name
    if state is None:
        state = get_channel_state_from_posts(channel_id)
    if state is None:
        state = ChannelState(channel_id=channel_id)
    if username:
        state.username = username
    return state


def insert_batch(batch, job_id):
    """convert to expected class obj then insert"""
    rows = [
        TelegramPostRow(
            channel_id=str(d["channel_id"]),
            message_id=str(d["message_id"]),
            link=d.get("link"),
            text=d.get("text"),
            filtered_text=d.get("filtered_text"),
            created_at_ts=d.get("created_at_ts"),
            views=d.get("views"),
            forwards=d.get("forwards"),
            replies=d.get("replies"),
            reactions_total=d.get("reactions_total"),
            is_pinned=d.get("is_pinned"),
            has_media=d.get("has_media"),
            raw_type=d.get("raw_type"),
        )
        for d in batch
    ]

    flush_telegram_batch(rows, job_id)


def _error_for_progress(progress: ScrapePass) -> str | None:
    if progress.reason == "flood":
        secs = progress.flood_seconds
        return f"flood:{secs}" if secs is not None else "flood"
    if progress.reason in {"rpc", "error"}:
        return progress.reason
    return None


async def _scrape_pass(
    client,
    channel: str,
    *,
    entity,
    job_id: int,
    min_id: int = 0,
    max_id: int = 0,
    on_batch=None,
) -> tuple[ScrapePass, int, int]:
    progress = ScrapePass()
    rows = 0
    batches = 0
    async for batch in scrape_channel_batches(
        client,
        channel,
        entity=entity,
        min_id=min_id,
        max_id=max_id,
        batch_size=200,
        progress=progress,
    ):
        if not batch:
            continue
        batches += 1
        rows += len(batch)
        insert_batch(batch, job_id)
        if on_batch is not None:
            on_batch(progress)
        if batches == 1 or batches % 10 == 0:
            log.info(
                "channel progress: %s pass=%s batches=%d rows=%d",
                channel, progress.reason, batches, rows,
            )
    return progress, rows, batches


async def scrape_one_channel(client, channel: str, job_id: int) -> tuple[int, int]:
    handle = norm_channel(channel)
    cached = get_channel_state_by_username(handle)
    resolved = await resolve_channel_peer(client, channel, cached=cached)
    if resolved is None:
        log.warning("channel invalid/unreachable: %s", channel)
        return 0, 0

    entity, chan_id, access_hash, username = resolved
    state = load_channel_state(chan_id, username or handle)
    if access_hash is not None:
        state.access_hash = access_hash
    state.last_error = None
    upsert_channel_state(state)

    total_rows = 0
    total_batches = 0

    if state.high_watermark_id is not None:
        catch_up, rows, batches = await _scrape_pass(
            client, channel, entity=entity, job_id=job_id,
            min_id=state.high_watermark_id,
        )
        total_rows += rows
        total_batches += batches
        apply_catch_up(state, catch_up)
        state.last_error = _error_for_progress(catch_up)
        state.last_scraped_at = datetime.now(timezone.utc)
        upsert_channel_state(state)
        log.info(
            "channel catch-up: %s reason=%s seen=%d text=%d high=%s",
            channel, catch_up.reason, catch_up.messages_seen,
            catch_up.messages_with_text, state.high_watermark_id,
        )
        if catch_up.reason in {"flood", "rpc", "error"}:
            return total_rows, total_batches

    if not state.backfill_complete:
        def _persist_backfill(progress: ScrapePass) -> None:
            apply_backfill(state, progress)
            state.last_scraped_at = datetime.now(timezone.utc)
            upsert_channel_state(state)

        backfill, rows, batches = await _scrape_pass(
            client, channel, entity=entity, job_id=job_id,
            max_id=state.low_watermark_id or 0,
            on_batch=_persist_backfill,
        )
        total_rows += rows
        total_batches += batches
        apply_backfill(state, backfill)
        state.last_error = _error_for_progress(backfill)
        state.last_scraped_at = datetime.now(timezone.utc)
        upsert_channel_state(state)
        log.info(
            "channel backfill: %s reason=%s seen=%d text=%d low=%s complete=%s",
            channel, backfill.reason, backfill.messages_seen,
            backfill.messages_with_text, state.low_watermark_id,
            state.backfill_complete,
        )

    return total_rows, total_batches


async def monitor_loop(client):
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telegram client is not authorized (session invalid/expired).\n"
            "Run:\n"
            "  python -m services.telegram_monitor --login\n"
        )

    job_id = ensure_scrape_job(
        name="core_tg_monitoring",
        description="scrape a list of tg channels known for vaxx misinfo",
        platforms=["telegram_post"]
    )
    log.info("monitor start: channels=%d job_id=%s session=%s",
             len(CHANNEL_LIST), job_id, SESSION)

    for i, channel in enumerate(CHANNEL_LIST):
        t0 = time.monotonic()
        total_rows = 0
        total_batches = 0

        log.info("channel start: %s", channel)
        try:
            total_rows, total_batches = await scrape_one_channel(
                client, channel, job_id,
            )
        except Exception:
            log.exception("channel error: %s", channel)
            continue
        finally:
            dt = time.monotonic() - t0
            log.info("channel done: %s batches=%d rows=%d took=%.2fs",
                     channel, total_batches, total_rows, dt)
            if i < len(CHANNEL_LIST) - 1:
                await asyncio.sleep(CHANNEL_PAUSE_S)

    log.info("monitor done")
    await client.disconnect()


####################################
# TWO VALID ENTRY POINTS
####################################


async def login_once(prod: bool = False) -> None:
    # Login only writes the Telethon session file; no DB needed.
    _ = prod
    client = TelegramClient(SESSION, int(API_ID), API_HASH)
    try:
        # This may prompt once in a real terminal and will write SESSION.session
        await client.start()
        print("Login OK; session file ready.")
    finally:
        if client.is_connected():
            await client.disconnect()


async def main(prod=False):
    _setup_logging()
    log.info("main start: prod=%s session=%s", prod, SESSION)
    require_session_file(SESSION)
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")

    client = TelegramClient(SESSION, int(API_ID), API_HASH)

    try:
        log.info("main start: prod=%s", prod)
        await monitor_loop(client)
    finally:
        close_pool()
        if client.is_connected():
            await client.disconnect()
        log.info("main done")
