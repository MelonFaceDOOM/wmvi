from datetime import datetime, timezone
import asyncio
import logging
import time

from db.db import getcursor, init_pool, close_pool
from ingestion.telegram_post import flush_telegram_batch
from ingestion.ingestion import ensure_scrape_job
from .tg_scrape import scrape_channel_batches, probe_channel
from telethon import TelegramClient
from pathlib import Path

from dotenv import load_dotenv
import os
load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION = os.getenv("TG_SESSION", "tg_scrape")


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
    "@communityhealthproject"
]

log = logging.getLogger(__name__)


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


def get_most_recent_ts_for_tg_channel_in_db(channel_id):
    """
    Returns the most recent created_at_ts for a Telegram channel,
    or epoch if none exist.
    """
    with getcursor() as cur:
        cur.execute(
            """
            SELECT max(created_at_ts)
            FROM sm.telegram_post
            WHERE channel_id = %s
            """,
            (channel_id,),
        )
        row = cur.fetchone()

    if row is None or row[0] is None:
        # epoch fallback: scrape everything once
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    return row[0]


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

    for channel in CHANNEL_LIST:
        t0 = time.monotonic()
        total_rows = 0
        total_batches = 0

        log.info("channel start: %s", channel)
        try:
            entity = await probe_channel(client, channel)
            if not entity:
                log.warning("channel invalid/unreachable: %s", channel)
                continue

            chan_id = getattr(entity, "id", None)
            if chan_id is None:
                log.warning(
                    "channel has no id? channel=%s entity=%r", channel, entity)
                continue

            # give channel name, it converts to id
            most_recent_ts = get_most_recent_ts_for_tg_channel_in_db(chan_id)

            async for batch in scrape_channel_batches(
                client,
                channel,
                most_recent_ts,
                entity=entity,
                batch_size=200,
            ):

                if not batch:
                    continue

                total_batches += 1
                total_rows += len(batch)

                flush_telegram_batch(batch, job_id)
                if total_batches == 1 or total_batches % 10 == 0:
                    log.info("channel progress: %s batches=%d rows=%d",
                             channel, total_batches, total_rows)
        except Exception:
            log.exception("channel error: %s", channel)
            continue
        finally:
            dt = time.monotonic() - t0
            log.info("channel done: %s batches=%d rows=%d took=%.2fs",
                     channel, total_batches, total_rows, dt)

    log.info("monitor done")
    await client.disconnect()

####################################
# TWO VALID ENTRY POINTS
####################################


async def login_once(prod: bool = False) -> None:
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")

    client = TelegramClient(SESSION, int(API_ID), API_HASH)
    try:
        # This may prompt once in a real terminal and will write SESSION.session
        await client.start()
        print("Login OK; session file ready.")
    finally:
        close_pool()
        if client.is_connected():
            await client.disconnect()


async def main(prod=False):
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
