-- Per-channel Telegram scrape watermarks.
--
-- telegram_monitor walks newest → oldest. A flood/abort in the middle used to
-- leave max(sm.telegram_post.created_at_ts) at the newest inserted posts, so
-- the next hourly run treated the channel as caught up and never fetched the
-- older history.
--
-- high_watermark_* = newest message we have a contiguous prefix to (catch-up).
-- low_watermark_*  = oldest message reached while backfilling.
-- backfill_complete = walked past the oldest available history.
--
-- Existing channels are seeded incomplete so the next run probes below
-- min(message_id). That is one extra GetHistory page when history is already
-- complete, and a real resume when a first scrape was cut off.

BEGIN;

CREATE TABLE scrape.telegram_channel_state (
    channel_id          BIGINT PRIMARY KEY,
    username            TEXT,
    access_hash         BIGINT,
    high_watermark_id   BIGINT,
    high_watermark_ts   TIMESTAMPTZ,
    low_watermark_id    BIGINT,
    low_watermark_ts    TIMESTAMPTZ,
    backfill_complete   BOOLEAN NOT NULL DEFAULT FALSE,
    last_error          TEXT,
    last_scraped_at     TIMESTAMPTZ,
    date_entered        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX telegram_channel_state_username_lower_idx
    ON scrape.telegram_channel_state (lower(username))
    WHERE username IS NOT NULL;

INSERT INTO scrape.telegram_channel_state (
    channel_id,
    high_watermark_id,
    high_watermark_ts,
    low_watermark_id,
    low_watermark_ts,
    backfill_complete
)
SELECT
    channel_id,
    MAX(message_id),
    MAX(created_at_ts),
    MIN(message_id),
    MIN(created_at_ts),
    FALSE
FROM sm.telegram_post
GROUP BY channel_id;

COMMIT;
