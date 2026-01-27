-- Canonical unified post view (to replace the 3 previously existing once)
-- Dashboard-support indices.

BEGIN;

-- ==========================
-- Drop deprecated view
-- ==========================
DROP VIEW IF EXISTS sm.posts_unified;


-- ==========================
-- Canonical unified view
-- ==========================
CREATE OR REPLACE VIEW sm.posts_all AS
    -- Tweets
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        t.date_entered      AS date_entered,
        t.created_at_ts     AS created_at_ts,
        t.filtered_text     AS filtered_text,
        t.tsv_en            AS tsv_en,
        t.is_en             AS is_en,
        t.like_count::BIGINT AS primary_metric,
        NULL::TEXT          AS url
    FROM sm.post_registry pr
    JOIN sm.tweet t
      ON pr.platform = 'tweet'
     AND pr.key1 = t.id::text
     AND pr.key2 IS NULL

    UNION ALL

    -- Reddit submissions
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        rs.date_entered     AS date_entered,
        rs.created_at_ts    AS created_at_ts,
        rs.filtered_text    AS filtered_text,
        rs.tsv_en           AS tsv_en,
        rs.is_en            AS is_en,
        rs.score::BIGINT    AS primary_metric,
        rs.permalink        AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_submission rs
      ON pr.platform = 'reddit_submission'
     AND pr.key1 = rs.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Reddit comments
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        rc.date_entered     AS date_entered,
        rc.created_at_ts    AS created_at_ts,
        rc.filtered_text    AS filtered_text,
        rc.tsv_en           AS tsv_en,
        rc.is_en            AS is_en,
        rc.score::BIGINT    AS primary_metric,
        rc.permalink        AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_comment rc
      ON pr.platform = 'reddit_comment'
     AND pr.key1 = rc.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Telegram posts
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        tp.date_entered     AS date_entered,
        tp.created_at_ts    AS created_at_ts,
        tp.filtered_text    AS filtered_text,
        tp.tsv_en           AS tsv_en,
        tp.is_en            AS is_en,
        tp.views::BIGINT    AS primary_metric,
        tp.link             AS url
    FROM sm.post_registry pr
    JOIN sm.telegram_post tp
      ON pr.platform = 'telegram_post'
     AND pr.key1 = tp.channel_id::text
     AND pr.key2 = tp.message_id::text

    UNION ALL

    -- YouTube videos
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        yv.date_entered     AS date_entered,
        yv.created_at_ts    AS created_at_ts,
        yv.filtered_text    AS filtered_text,
        yv.tsv_en           AS tsv_en,
        yv.is_en            AS is_en,
        yv.view_count::BIGINT AS primary_metric,
        yv.url              AS url
    FROM sm.post_registry pr
    JOIN sm.youtube_video yv
      ON pr.platform = 'youtube_video'
     AND pr.key1 = yv.video_id
     AND pr.key2 IS NULL

    UNION ALL

    -- YouTube comments
    SELECT
        pr.id               AS post_id,
        pr.platform         AS platform,
        pr.key1             AS key1,
        pr.key2             AS key2,
        yc.date_entered     AS date_entered,
        yc.created_at_ts    AS created_at_ts,
        yc.filtered_text    AS filtered_text,
        yc.tsv_en           AS tsv_en,
        yc.is_en            AS is_en,
        yc.like_count::BIGINT AS primary_metric,
        yc.comment_url      AS url
    FROM sm.post_registry pr
    JOIN sm.youtube_comment yc
      ON pr.platform = 'youtube_comment'
     AND pr.key1 = yc.video_id
     AND pr.key2 = yc.comment_id
;


-- ==========================
-- Wrapper views (replace old redundant union views)
-- ==========================
-- Claim-extractor 
CREATE OR REPLACE VIEW sm.post_summary AS
    SELECT
        post_id,
        platform,
        key1,
        key2,
        created_at_ts,
        filtered_text,
        is_en,
        primary_metric,
        url
    FROM sm.posts_all;

-- Term-matching convenience
CREATE OR REPLACE VIEW sm.post_search_en AS
    SELECT
        post_id,
        tsv_en
    FROM sm.posts_all
    WHERE is_en IS TRUE;


-- ==========================
-- Podcasts: add date_entered to transcript_segments
-- ==========================
ALTER TABLE podcasts.transcript_segments
    ADD COLUMN IF NOT EXISTS date_entered TIMESTAMPTZ;

UPDATE podcasts.transcript_segments
   SET date_entered = COALESCE(date_entered, now())
 WHERE date_entered IS NULL;

ALTER TABLE podcasts.transcript_segments
    ALTER COLUMN date_entered SET DEFAULT now();

ALTER TABLE podcasts.transcript_segments
    ALTER COLUMN date_entered SET NOT NULL;


-- ==========================
-- Dashboard indices
-- ==========================
-- Ingestion volume + "last seen" + rows in last 24h
-- Use date_entered (append-ish) -> BRIN works well.
CREATE INDEX IF NOT EXISTS tweet_date_entered_brin ON sm.tweet USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS rs_date_entered_brin    ON sm.reddit_submission USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS rc_date_entered_brin    ON sm.reddit_comment USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS tg_date_entered_brin    ON sm.telegram_post USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS yv_date_entered_brin    ON sm.youtube_video USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS yc_date_entered_brin    ON sm.youtube_comment USING BRIN (date_entered);

-- English labeling coverage + backlog
-- Backlog queries typically filter is_en IS NULL and often by date_entered window.
CREATE INDEX IF NOT EXISTS tweet_is_en_null_date_idx ON sm.tweet(date_entered) WHERE is_en IS NULL;
CREATE INDEX IF NOT EXISTS rs_is_en_null_date_idx    ON sm.reddit_submission(date_entered) WHERE is_en IS NULL;
CREATE INDEX IF NOT EXISTS rc_is_en_null_date_idx    ON sm.reddit_comment(date_entered) WHERE is_en IS NULL;
CREATE INDEX IF NOT EXISTS tg_is_en_null_date_idx    ON sm.telegram_post(date_entered) WHERE is_en IS NULL;
CREATE INDEX IF NOT EXISTS yv_is_en_null_date_idx    ON sm.youtube_video(date_entered) WHERE is_en IS NULL;
CREATE INDEX IF NOT EXISTS yc_is_en_null_date_idx    ON sm.youtube_comment(date_entered) WHERE is_en IS NULL;

-- Scrape jobs (created per day) + posts linked per day/job
CREATE INDEX IF NOT EXISTS scrape_job_created_at_idx ON scrape.job(created_at);
CREATE INDEX IF NOT EXISTS post_scrape_linked_at_idx ON scrape.post_scrape(linked_at);

-- Term matching throughput + coverage
CREATE INDEX IF NOT EXISTS post_term_match_matched_at_idx ON matches.post_term_match(matched_at);
CREATE INDEX IF NOT EXISTS post_term_match_matcher_time_idx ON matches.post_term_match(matcher_version, matched_at);

-- Podcasts ingestion (episodes per day + transcript segments per day)
CREATE INDEX IF NOT EXISTS episodes_date_entered_brin ON podcasts.episodes USING BRIN (date_entered);
CREATE INDEX IF NOT EXISTS seg_date_entered_brin      ON podcasts.transcript_segments USING BRIN (date_entered);

-- Support sorting by creation date (i.e. pub date)
CREATE INDEX IF NOT EXISTS episodes_created_at_idx
ON podcasts.episodes(created_at_ts);


COMMIT;
