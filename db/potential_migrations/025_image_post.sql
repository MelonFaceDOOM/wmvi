-- 025_image_post.sql
--
-- Image attachments become first-class posts after an LLM interprets them.
-- This is the transcript pattern (async media -> text -> post_registry) but as a
-- *child* post, so the parent caption stays its own post_id. Term matcher /
-- label_en / claim extract then pick up new post_ids with no rematch reset.
--
-- Also v1.5 capture: persist fetchable image refs at scrape time (galleries,
-- comment images, telegram photos), not only reddit shared_url.
--
-- Out of scope: tweets (no media columns / no ingest), news figures, youtube
-- thumbnails, video/audio (already transcribed).
--
-- ---------------------------------------------------------------------------
-- Design
-- ---------------------------------------------------------------------------
-- sm.post_media
--   One row per image attached to an existing post_registry parent.
--   Filled at scrape (or backfill). Interpreter queue lives here (status /
--   claimed_at), same idea as youtube.video.transcription_started_at.
--   Do NOT register post_media in post_registry.
--
-- sm.image_post
--   One row per successful interpretation. Insert only when text exists.
--   Insert trigger registers platform='image_post'. posts_all exposes
--   filtered_text as text. Re-interpret in place (unique parent+idx).
--
-- Parent convenience cols (discovery only; post_media is canonical):
--   sm.reddit_submission.is_gallery
--   sm.telegram_post.media_kind   -- 'photo'|'video'|'document'|... ; null if none
--
-- Interpreter (not in this file): claim pending post_media rows, fetch
-- source_url or re-download telegram via (channel_id, message_id), write
-- sm.image_post, set post_media.status='done'. Failures stay on post_media;
-- no image_post row => not a post.
--
-- Python plumbing when this is applied (not SQL):
--   - ingestion: ImagePostRow (TABLE sm.image_post, PK ("id",)) +
--     PLATFORM_ROW["image_post"] so label_en can write is_en.
--   - scripts/recheck_is_en.py PLATFORM_UPDATE_SPEC
--   - optional: dashboard_summary UNION; content_sync handler if interpret
--     runs off-prod (same shape as youtube_video transcripts).
--   - scrape-job linking is unnecessary; parent already has the job.
--
-- ---------------------------------------------------------------------------
-- Going forward (v1.5 scrape) — do this in the monitors, not a later schema
-- ---------------------------------------------------------------------------
-- REDDIT  services/reddit_monitor/scrape_runner.py
--   After inserting the parent (submission or comment), upsert sm.post_media.
--   Need post_registry.id: INSERT trigger already fired; SELECT id FROM
--   sm.post_registry WHERE platform=... AND key1=... AND key2=''.
--   Also update skipped/existing parents (ON CONFLICT DO NOTHING on
--   post_media) so reruns fill gaps.
--
--   Submissions (_submission_to_row / post-flush):
--     image_idx order:
--       1) gallery: submission.is_gallery + gallery_data.items[].media_id
--          -> media_metadata[id].s.u or largest p[].u (unescape &amp;)
--       2) else native/preview: preview.images[0].source.url (unescape)
--       3) else direct link: shared_url when domain in
--          i.redd.it, i.imgur.com, preview.redd.it, or url path looks like
--          an image. kind='link_image'. imgur.com HTML pages are not direct
--          bytes; fetcher must resolve or skip.
--     Set is_gallery from submission.is_gallery (new col).
--     Do not put image URLs into filtered_text; parent text stays title/selftext.
--     PRAW submission.media is oembed/reddit_video and is often NULL for
--     i.redd.it stills — do not rely on it for stills.
--
--   Comments (_comment_to_row / post-flush):
--     comment.media_metadata (same s.u / p[].u). No schema change on
--     reddit_comment; post_media is enough.
--
-- TELEGRAM  services/telegram_monitor/tg_scrape.py + monitor.py
--   Today scrape_channel_batches drops messages with empty text, so image-only
--   posts never exist. Change keep rule to:
--       (text and text.strip()) OR is_still_image(msg)
--   is_still_image: MessageMediaPhoto, or document with image mime
--   (image/jpeg, image/png, image/webp). Exclude video/sticker/animation
--   from post_media (has_media stays true for those).
--
--   normalize_message: set media_kind; coerce text/filtered_text to '' not
--   NULL (columns are NOT NULL). link is NOT NULL — use
--   https://t.me/{username}/{id} or https://t.me/c/{channel_id}/{id}.
--
--   After flush_telegram_batch, for each photo (and album pages: grouped
--   messages share grouped_id; one parent telegram_post per message, one
--   post_media row each, image_idx=0 per message is fine).
--   Telegram photos have no stable public URL. Leave source_url NULL and
--   put enough in extra to re-download at interpret time without storing
--   bytes in postgres, e.g.
--     extra = {grouped_id, photo_id, access_hash, dc_id}  -- from msg.photo
--   Interpreter: Telethon get_messages(channel_id, ids=message_id) then
--   download_media, or reconstruct InputPhoto from extra.
--
--   Do not download at scrape time unless you want a local/object-store
--   cache; storage_key on post_media is for that later.
--
-- ---------------------------------------------------------------------------
-- Backfill (re-hit APIs) — existing rows
-- ---------------------------------------------------------------------------
-- REDDIT (no API, cheap seed — run after this migration):
--   Direct-image submissions already have shared_url. See commented INSERT
--   at bottom. Covers i.redd.it / i.imgur.com / preview.redd.it. Does NOT
--   cover galleries, comments, or imgur.com album/HTML links.
--
-- REDDIT (API):
--   Submissions: SELECT id FROM sm.reddit_submission WHERE NOT is_self
--     AND (is_gallery OR domain in image hosts OR shared_url IS NOT NULL)
--     AND id NOT IN (select parents that already have post_media).
--   PRAW reddit.submission(id=id.removeprefix('t3_')) then same extractors
--   as scrape. Rate-limit; many posts will be deleted/removed — mark skip.
--   Comments: no local signal. Options: (a) sample high-score comments and
--   fetch, (b) skip historical comment images, (c) re-fetch comments for
--   submissions that are galleries / image domains.
--   Do not UPDATE reddit_*.filtered_text.
--
-- TELEGRAM:
--   Image-only messages are absent. Must re-scrape each channel in
--   CHANNEL_LIST with the new keep rule, from the same since_dt the monitor
--   uses (or 1970 for a full pass). Existing telegram_post rows with
--   has_media=true: Telethon get_messages(entity, ids=[message_id]) and
--   attach post_media if the media is a still. If the channel/message is
--   gone, skip. has_media is also true for video — filter with media_kind.
--
-- After post_media exists, interpreter backfill is just claiming pending
-- rows; no second API for reddit if source_url is still live.
--
-- ---------------------------------------------------------------------------
-- Apply notes
-- ---------------------------------------------------------------------------
-- DROP post_search_en before replacing posts_all (depends on it).
-- Recreate post_search_en after. key2 is '' not NULL (012).
-- Register image_post only via its insert trigger (insert completed rows).
--

BEGIN;

-- ==========================
-- 1) Parent convenience columns
-- ==========================
ALTER TABLE sm.reddit_submission
    ADD COLUMN IF NOT EXISTS is_gallery BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE sm.telegram_post
    ADD COLUMN IF NOT EXISTS media_kind TEXT;

COMMENT ON COLUMN sm.telegram_post.media_kind IS
    'photo|video|document|sticker|animation|web_page|other; null if no media. has_media is the coarse flag.';

-- ==========================
-- 2) sm.post_media  (capture + interpret queue)
-- ==========================
CREATE TABLE IF NOT EXISTS sm.post_media (
    parent_post_id  BIGINT NOT NULL REFERENCES sm.post_registry(id) ON DELETE CASCADE,
    image_idx       INT    NOT NULL DEFAULT 0,
    date_entered    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- photo | gallery_item | link_image | preview | document_image
    kind            TEXT NOT NULL,

    -- http(s) fetchable (reddit i.redd.it, preview, imgur direct). NULL for
    -- telegram native photos — use extra / re-fetch message.
    source_url      TEXT,
    mime_type       TEXT,
    storage_key     TEXT,   -- optional object-store key if bytes are cached

    extra           JSONB,  -- gallery media_id, tg photo_id/access_hash/dc_id, grouped_id

    -- interpret queue (youtube transcription_started_at analogue)
    -- pending | claimed | done | failed | skip
    status          TEXT NOT NULL DEFAULT 'pending',
    claimed_at      TIMESTAMPTZ,
    interpreter_version TEXT,
    interpretation_failed_at TIMESTAMPTZ,
    interpretation_failure_reason TEXT,

    PRIMARY KEY (parent_post_id, image_idx),
    CONSTRAINT post_media_idx_chk CHECK (image_idx >= 0),
    CONSTRAINT post_media_status_chk CHECK (
        status = ANY (ARRAY['pending'::text, 'claimed'::text, 'done'::text, 'failed'::text, 'skip'::text])
    )
);

CREATE INDEX IF NOT EXISTS post_media_pending_idx
    ON sm.post_media (claimed_at, parent_post_id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS post_media_status_idx
    ON sm.post_media (status);

COMMENT ON TABLE sm.post_media IS
    'Still-image attachments on a parent post. Interpreter queue. Not a post_registry platform.';

-- ==========================
-- 3) sm.image_post  (derived post; insert only when interpreted)
-- ==========================
CREATE TABLE IF NOT EXISTS sm.image_post (
    id              BIGSERIAL PRIMARY KEY,
    date_entered    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at_ts   TIMESTAMPTZ NOT NULL,  -- copy parent created_at_ts

    parent_post_id  BIGINT NOT NULL,
    image_idx       INT    NOT NULL DEFAULT 0,

    source_url      TEXT,   -- copy from post_media at interpret time (may be null)
    text            TEXT NOT NULL,          -- raw LLM output
    filtered_text   TEXT NOT NULL,          -- PII-redacted; posts_all.text
    tsv_en          tsvector GENERATED ALWAYS AS (to_tsvector('english', filtered_text)) STORED,
    is_en           BOOLEAN,

    interpreter_version TEXT NOT NULL DEFAULT '',
    interpreted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT image_post_parent_idx_uniq UNIQUE (parent_post_id, image_idx),
    CONSTRAINT image_post_media_fk
        FOREIGN KEY (parent_post_id, image_idx)
        REFERENCES sm.post_media (parent_post_id, image_idx)
        ON DELETE CASCADE,
    CONSTRAINT image_post_parent_fk
        FOREIGN KEY (parent_post_id)
        REFERENCES sm.post_registry(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS image_post_date_entered_brin
    ON sm.image_post USING BRIN (date_entered);

CREATE INDEX IF NOT EXISTS image_post_created_at_brin
    ON sm.image_post USING BRIN (created_at_ts);

CREATE INDEX IF NOT EXISTS image_post_tsv_en_gin
    ON sm.image_post USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS image_post_text_trgm
    ON sm.image_post USING GIN (filtered_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS image_post_is_en_null_date_idx
    ON sm.image_post (date_entered)
    WHERE is_en IS NULL;

CREATE INDEX IF NOT EXISTS image_post_parent_idx
    ON sm.image_post (parent_post_id);

COMMENT ON TABLE sm.image_post IS
    'LLM interpretation of one post_media image. Registered as platform=image_post.';

-- ==========================
-- 4) post_registry platform + triggers
-- ==========================
ALTER TABLE sm.post_registry
    DROP CONSTRAINT IF EXISTS post_registry_platform_chk;

ALTER TABLE sm.post_registry
    ADD CONSTRAINT post_registry_platform_chk CHECK (
        platform = ANY (ARRAY[
            'tweet'::text,
            'reddit_submission'::text,
            'reddit_comment'::text,
            'telegram_post'::text,
            'youtube_video'::text,
            'youtube_comment'::text,
            'podcast_episode'::text,
            'news_article'::text,
            'image_post'::text
        ])
    );

CREATE OR REPLACE FUNCTION sm.trg_image_post_reg_ins() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('image_post', NEW.id::text, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION sm.trg_image_post_reg_del() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'image_post'
     AND key1 = OLD.id::text
     AND key2 = '';
  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS image_post_reg_ins ON sm.image_post;
CREATE TRIGGER image_post_reg_ins
AFTER INSERT ON sm.image_post
FOR EACH ROW EXECUTE FUNCTION sm.trg_image_post_reg_ins();

DROP TRIGGER IF EXISTS image_post_reg_del ON sm.image_post;
CREATE TRIGGER image_post_reg_del
AFTER DELETE ON sm.image_post
FOR EACH ROW EXECUTE FUNCTION sm.trg_image_post_reg_del();

-- ==========================
-- 5) Rebuild posts_all + post_search_en
-- ==========================
DROP VIEW IF EXISTS sm.post_search_en;

CREATE OR REPLACE VIEW sm.posts_all AS
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        t.date_entered       AS date_entered,
        t.created_at_ts      AS created_at_ts,
        t.filtered_text      AS text,
        t.tsv_en             AS tsv_en,
        t.is_en              AS is_en,
        t.like_count::BIGINT AS primary_metric,
        NULL::TEXT           AS url
    FROM sm.post_registry pr
    JOIN sm.tweet t
      ON pr.platform = 'tweet'
     AND pr.key1 = t.id::text
     AND pr.key2 = ''

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        rs.date_entered,
        rs.created_at_ts,
        rs.filtered_text,
        rs.tsv_en,
        rs.is_en,
        rs.score::BIGINT,
        rs.permalink
    FROM sm.post_registry pr
    JOIN sm.reddit_submission rs
      ON pr.platform = 'reddit_submission'
     AND pr.key1 = rs.id
     AND pr.key2 = ''

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        rc.date_entered,
        rc.created_at_ts,
        rc.filtered_text,
        rc.tsv_en,
        rc.is_en,
        rc.score::BIGINT,
        rc.permalink
    FROM sm.post_registry pr
    JOIN sm.reddit_comment rc
      ON pr.platform = 'reddit_comment'
     AND pr.key1 = rc.id
     AND pr.key2 = ''

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        tp.date_entered,
        tp.created_at_ts,
        tp.filtered_text,
        tp.tsv_en,
        tp.is_en,
        tp.views::BIGINT,
        tp.link
    FROM sm.post_registry pr
    JOIN sm.telegram_post tp
      ON pr.platform = 'telegram_post'
     AND pr.key1 = tp.channel_id::text
     AND pr.key2 = tp.message_id::text

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        yv.date_entered,
        yv.created_at_ts,
        yv.transcript,
        yv.tsv_en,
        yv.is_en,
        yv.view_count,
        yv.url
    FROM sm.post_registry pr
    JOIN youtube.video yv
      ON pr.platform = 'youtube_video'
     AND pr.key1 = yv.video_id
     AND pr.key2 = ''

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        yc.date_entered,
        yc.created_at_ts,
        yc.filtered_text,
        yc.tsv_en,
        yc.is_en,
        yc.like_count,
        yc.comment_url
    FROM sm.post_registry pr
    JOIN youtube.comment yc
      ON pr.platform = 'youtube_comment'
     AND pr.key1 = yc.video_id
     AND pr.key2 = yc.comment_id

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        e.date_entered,
        e.created_at_ts,
        e.transcript,
        e.tsv_en,
        e.is_en,
        NULL::BIGINT,
        e.download_url
    FROM sm.post_registry pr
    JOIN podcasts.episodes e
      ON pr.platform = 'podcast_episode'
     AND pr.key1 = e.id
     AND pr.key2 = ''

    UNION ALL

    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        a.date_entered,
        a.created_at_ts,
        a.text,
        a.tsv_en,
        a.is_en,
        NULL::BIGINT,
        a.url
    FROM sm.post_registry pr
    JOIN news.article a
      ON pr.platform = 'news_article'
     AND pr.key1 = a.id::text
     AND pr.key2 = ''

    UNION ALL

    -- Image interpretations (child posts)
    SELECT
        pr.id,
        pr.platform,
        pr.key1,
        pr.key2,
        ip.date_entered,
        ip.created_at_ts,
        ip.filtered_text,
        ip.tsv_en,
        ip.is_en,
        NULL::BIGINT,
        ip.source_url
    FROM sm.post_registry pr
    JOIN sm.image_post ip
      ON pr.platform = 'image_post'
     AND pr.key1 = ip.id::text
     AND pr.key2 = '';

CREATE OR REPLACE VIEW sm.post_search_en AS
    SELECT post_id, tsv_en
    FROM sm.posts_all
    WHERE is_en IS TRUE;

COMMIT;

-- ==========================
-- Optional: seed post_media from existing reddit shared_url (no API)
-- Run separately after apply. imgur.com (non-direct) omitted on purpose.
-- ==========================
-- INSERT INTO sm.post_media (parent_post_id, image_idx, kind, source_url, status)
-- SELECT
--     pr.id,
--     0,
--     'link_image',
--     rs.shared_url,
--     'pending'
-- FROM sm.reddit_submission rs
-- JOIN sm.post_registry pr
--   ON pr.platform = 'reddit_submission'
--  AND pr.key1 = rs.id
--  AND pr.key2 = ''
-- WHERE rs.shared_url IS NOT NULL
--   AND btrim(rs.shared_url) <> ''
--   AND rs.is_self IS FALSE
--   AND rs.is_video IS FALSE
--   AND rs.domain IN ('i.redd.it', 'i.imgur.com', 'preview.redd.it')
-- ON CONFLICT (parent_post_id, image_idx) DO NOTHING;
