-- sm.post_registry currently has a constraints post_registry_uniq
-- with: UNIQUE (platform, key1, key2)
-- This doesn't work because key2 is null by default everywhere
-- psql doesn't evaluate null == null as True, so
-- it can't assess uniqueness properly

-- CHANGES IN THIS FILE:
-- NULL for key2 is replaced with ''
-- places that reference key2 are updated too


--------------------------------------------------------------------
-- 1. CLEAN UP EXISTING DATA
--------------------------------------------------------------------

-- Replace NULL key2 values with empty string
UPDATE sm.post_registry
SET key2 = ''
WHERE key2 IS NULL;

--------------------------------------------------------------------
-- 2. ENFORCE INVARIANT AT SCHEMA LEVEL
--------------------------------------------------------------------

ALTER TABLE sm.post_registry
ALTER COLUMN key2 SET NOT NULL;

--------------------------------------------------------------------
-- 3. REPLACE TRIGGER FUNCTIONS (INSERT SIDE)
--------------------------------------------------------------------

-- News articles
CREATE OR REPLACE FUNCTION sm.trg_article_reg_ins()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('news_article', NEW.id::text, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

-- Reddit comments
CREATE OR REPLACE FUNCTION sm.trg_rc_reg_ins()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('reddit_comment', NEW.id, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

-- Reddit submissions
CREATE OR REPLACE FUNCTION sm.trg_rs_reg_ins()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('reddit_submission', NEW.id, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

-- Telegram posts (already correct, but keep consistent)
CREATE OR REPLACE FUNCTION sm.trg_tg_reg_ins()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('telegram_post', NEW.channel_id::text, NEW.message_id::text)
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

-- Tweets
CREATE OR REPLACE FUNCTION sm.trg_tweet_reg_ins()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('tweet', NEW.id::text, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

--------------------------------------------------------------------
-- 4. UPDATE POSTS_AlL (used to have where key2 is null)
--------------------------------------------------------------------

CREATE OR REPLACE VIEW sm.posts_all AS

-- Tweets
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    t.date_entered,
    t.created_at_ts,
    t.filtered_text AS text,
    t.tsv_en,
    t.is_en,
    t.like_count::bigint AS primary_metric,
    NULL::text AS url
FROM sm.post_registry pr
JOIN sm.tweet t
  ON pr.platform = 'tweet'
 AND pr.key1 = t.id::text
 AND pr.key2 = ''

UNION ALL

-- Reddit submissions
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    rs.date_entered,
    rs.created_at_ts,
    rs.filtered_text AS text,
    rs.tsv_en,
    rs.is_en,
    rs.score::bigint AS primary_metric,
    rs.permalink AS url
FROM sm.post_registry pr
JOIN sm.reddit_submission rs
  ON pr.platform = 'reddit_submission'
 AND pr.key1 = rs.id
 AND pr.key2 = ''

UNION ALL

-- Reddit comments
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    rc.date_entered,
    rc.created_at_ts,
    rc.filtered_text AS text,
    rc.tsv_en,
    rc.is_en,
    rc.score::bigint AS primary_metric,
    rc.permalink AS url
FROM sm.post_registry pr
JOIN sm.reddit_comment rc
  ON pr.platform = 'reddit_comment'
 AND pr.key1 = rc.id
 AND pr.key2 = ''

UNION ALL

-- Telegram posts
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    tp.date_entered,
    tp.created_at_ts,
    tp.filtered_text AS text,
    tp.tsv_en,
    tp.is_en,
    tp.views::bigint AS primary_metric,
    tp.link AS url
FROM sm.post_registry pr
JOIN sm.telegram_post tp
  ON pr.platform = 'telegram_post'
 AND pr.key1 = tp.channel_id::text
 AND pr.key2 = tp.message_id::text

UNION ALL

-- YouTube videos
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    yv.date_entered,
    yv.created_at_ts,
    yv.transcript AS text,
    yv.tsv_en,
    yv.is_en,
    yv.view_count AS primary_metric,
    yv.url
FROM sm.post_registry pr
JOIN youtube.video yv
  ON pr.platform = 'youtube_video'
 AND pr.key1 = yv.video_id
 AND pr.key2 = ''

UNION ALL

-- YouTube comments
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    yc.date_entered,
    yc.created_at_ts,
    yc.filtered_text AS text,
    yc.tsv_en,
    yc.is_en,
    yc.like_count AS primary_metric,
    yc.comment_url AS url
FROM sm.post_registry pr
JOIN youtube.comment yc
  ON pr.platform = 'youtube_comment'
 AND pr.key1 = yc.video_id
 AND pr.key2 = yc.comment_id

UNION ALL

-- Podcast episodes
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    e.date_entered,
    e.created_at_ts,
    e.transcript AS text,
    e.tsv_en,
    e.is_en,
    NULL::bigint AS primary_metric,
    e.download_url AS url
FROM sm.post_registry pr
JOIN podcasts.episodes e
  ON pr.platform = 'podcast_episode'
 AND pr.key1 = e.id
 AND pr.key2 = ''

UNION ALL

-- News articles
SELECT
    pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    a.date_entered,
    a.created_at_ts,
    a.text,
    a.tsv_en,
    a.is_en,
    NULL::bigint AS primary_metric,
    a.url
FROM sm.post_registry pr
JOIN news.article a
  ON pr.platform = 'news_article'
 AND pr.key1 = a.id::text
 AND pr.key2 = '';
