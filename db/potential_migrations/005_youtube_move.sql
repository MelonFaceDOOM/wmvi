-- Purpose:
--   - Move YouTube tables out of sm into youtube schema
--   - Rename to youtube.video / youtube.comment
--   - Keep youtube.comment as a "social media post" (still in sm.post_registry via triggers)
--   - Remove youtube.video from the registry flow (drop its registry triggers)
--   - Add transcript columns to youtube.video and podcasts.episodes
--   - Rebuild sm.posts_all and sm.post_search_en to:
--       - EXCLUDE youtube.video
--       - INCLUDE youtube.comment

-- ==========================
-- 0) Safety: drop dependent views that will break when youtube tables move
-- ==========================
DROP VIEW IF EXISTS sm.post_search_en;
DROP VIEW IF EXISTS sm.post_summary;
DROP VIEW IF EXISTS sm.posts_all;

-- ==========================
-- 1) Create youtube schema + move tables
-- ==========================
CREATE SCHEMA IF NOT EXISTS youtube;

ALTER TABLE IF EXISTS sm.youtube_video   SET SCHEMA youtube;
ALTER TABLE IF EXISTS sm.youtube_comment SET SCHEMA youtube;

-- Rename for cleanliness
ALTER TABLE IF EXISTS youtube.youtube_video   RENAME TO video;
ALTER TABLE IF EXISTS youtube.youtube_comment RENAME TO comment;

-- ==========================
-- 2) Registry triggers
--    - youtube.video: STOP inserting into post_registry (drop triggers)
--    - youtube.comment: ensure triggers exist on new table location
-- ==========================

-- Stop registry updates for youtube.video
DROP TRIGGER IF EXISTS yv_reg_ins ON youtube.video;
DROP TRIGGER IF EXISTS yv_reg_del ON youtube.video;

-- Ensure youtube.comment continues to update post_registry
DROP TRIGGER IF EXISTS yc_reg_ins ON youtube.comment;
CREATE TRIGGER yc_reg_ins
AFTER INSERT ON youtube.comment
FOR EACH ROW EXECUTE FUNCTION sm.trg_yc_reg_ins();

DROP TRIGGER IF EXISTS yc_reg_del ON youtube.comment;
CREATE TRIGGER yc_reg_del
AFTER DELETE ON youtube.comment
FOR EACH ROW EXECUTE FUNCTION sm.trg_yc_reg_del();

-- ==========================
-- 3) Add transcript columns (long-form substrates)
-- ==========================
ALTER TABLE IF EXISTS youtube.video
  ADD COLUMN IF NOT EXISTS transcript TEXT;

ALTER TABLE IF EXISTS podcasts.episodes
  ADD COLUMN IF NOT EXISTS transcript TEXT;

ALTER TABLE IF EXISTS youtube.video
  ADD COLUMN IF NOT EXISTS transcript_updated_at TIMESTAMPTZ;

ALTER TABLE IF EXISTS podcasts.episodes
  ADD COLUMN IF NOT EXISTS transcript_updated_at TIMESTAMPTZ;

-- ==========================
-- 4) Rebuild sm.posts_all (posts-only) and sm.post_search_en
--    - YouTube videos removed
--    - YouTube comments remain (now in youtube schema)
-- ==========================

CREATE OR REPLACE VIEW sm.posts_all AS
    -- Tweets
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        t.date_entered       AS date_entered,
        t.created_at_ts      AS created_at_ts,
        t.filtered_text      AS filtered_text,
        t.tsv_en             AS tsv_en,
        t.is_en              AS is_en,
        t.like_count::BIGINT AS primary_metric,
        NULL::TEXT           AS url
    FROM sm.post_registry pr
    JOIN sm.tweet t
      ON pr.platform = 'tweet'
     AND pr.key1 = t.id::text
     AND pr.key2 IS NULL

    UNION ALL

    -- Reddit submissions
    SELECT
        pr.id              AS post_id,
        pr.platform        AS platform,
        pr.key1            AS key1,
        pr.key2            AS key2,
        rs.date_entered    AS date_entered,
        rs.created_at_ts   AS created_at_ts,
        rs.filtered_text   AS filtered_text,
        rs.tsv_en          AS tsv_en,
        rs.is_en           AS is_en,
        rs.score::BIGINT   AS primary_metric,
        rs.permalink       AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_submission rs
      ON pr.platform = 'reddit_submission'
     AND pr.key1 = rs.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Reddit comments
    SELECT
        pr.id              AS post_id,
        pr.platform        AS platform,
        pr.key1            AS key1,
        pr.key2            AS key2,
        rc.date_entered    AS date_entered,
        rc.created_at_ts   AS created_at_ts,
        rc.filtered_text   AS filtered_text,
        rc.tsv_en          AS tsv_en,
        rc.is_en           AS is_en,
        rc.score::BIGINT   AS primary_metric,
        rc.permalink       AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_comment rc
      ON pr.platform = 'reddit_comment'
     AND pr.key1 = rc.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Telegram posts
    SELECT
        pr.id              AS post_id,
        pr.platform        AS platform,
        pr.key1            AS key1,
        pr.key2            AS key2,
        tp.date_entered    AS date_entered,
        tp.created_at_ts   AS created_at_ts,
        tp.filtered_text   AS filtered_text,
        tp.tsv_en          AS tsv_en,
        tp.is_en           AS is_en,
        tp.views::BIGINT   AS primary_metric,
        tp.link            AS url
    FROM sm.post_registry pr
    JOIN sm.telegram_post tp
      ON pr.platform = 'telegram_post'
     AND pr.key1 = tp.channel_id::text
     AND pr.key2 = tp.message_id::text

    UNION ALL

    -- YouTube comments (now in youtube schema)
    SELECT
        pr.id                 AS post_id,
        pr.platform           AS platform,
        pr.key1               AS key1,
        pr.key2               AS key2,
        yc.date_entered       AS date_entered,
        yc.created_at_ts      AS created_at_ts,
        yc.filtered_text      AS filtered_text,
        yc.tsv_en             AS tsv_en,
        yc.is_en              AS is_en,
        yc.like_count::BIGINT AS primary_metric,
        yc.comment_url        AS url
    FROM sm.post_registry pr
    JOIN youtube.comment yc
      ON pr.platform = 'youtube_comment'
     AND pr.key1 = yc.video_id
     AND pr.key2 = yc.comment_id;

CREATE OR REPLACE VIEW sm.post_search_en AS
    SELECT post_id, tsv_en
    FROM sm.posts_all
    WHERE is_en IS TRUE;
