BEGIN;

-- ==========================
-- 0) Drop dependent views (order matters)
-- ==========================
DROP VIEW IF EXISTS sm.post_search_en;
DROP VIEW IF EXISTS sm.post_summary;
DROP VIEW IF EXISTS sm.posts_all;

-- ==========================
-- 1) Extensions
-- ==========================
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

-- ==========================
-- 2) NEWS: schema + table + triggers
-- ==========================
CREATE SCHEMA IF NOT EXISTS news;

CREATE TABLE IF NOT EXISTS news.article (
    id            BIGSERIAL PRIMARY KEY,
    date_entered  TIMESTAMPTZ NOT NULL DEFAULT now(),

    url           TEXT NOT NULL,
    url_hash      VARCHAR(32) GENERATED ALWAYS AS (MD5(url)) STORED,

    publication   TEXT,
    title         TEXT,

    text          TEXT NOT NULL,
    tsv_en        tsvector GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,

    created_at_ts TIMESTAMPTZ,
    is_en         BOOLEAN,

    CONSTRAINT news_article_url_hash_uniq UNIQUE (url_hash)
);

CREATE OR REPLACE FUNCTION sm.trg_article_reg_ins() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('news_article', NEW.id::text, NULL)
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION sm.trg_article_reg_del() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'news_article'
     AND key1 = OLD.id::text
     AND key2 IS NULL;
  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS article_reg_ins ON news.article;
CREATE TRIGGER article_reg_ins
AFTER INSERT ON news.article
FOR EACH ROW EXECUTE FUNCTION sm.trg_article_reg_ins();

DROP TRIGGER IF EXISTS article_reg_del ON news.article;
CREATE TRIGGER article_reg_del
AFTER DELETE ON news.article
FOR EACH ROW EXECUTE FUNCTION sm.trg_article_reg_del();

-- ==========================
-- 3) PODCASTS: transcript + search + registry
-- ==========================
ALTER TABLE podcasts.episodes
  ADD COLUMN IF NOT EXISTS transcript TEXT,
  ADD COLUMN IF NOT EXISTS transcript_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS is_en BOOLEAN,
  ADD COLUMN IF NOT EXISTS tsv_en tsvector
    GENERATED ALWAYS AS (to_tsvector('english', COALESCE(transcript, ''))) STORED;

-- Drop old episode search indexes (title/description)
DROP INDEX IF EXISTS podcasts.ep_title_tsv_gin;
DROP INDEX IF EXISTS podcasts.ep_desc_tsv_gin;

-- Drop pod ep insert trigger. We don't want eps with 
-- empty transcripts in post_registry
-- they will be added to registry at the same time transcripts are added
DROP TRIGGER IF EXISTS ep_reg_ins ON podcasts.episodes;
DROP FUNCTION IF EXISTS sm.trg_ep_reg_ins();

-- still want deletion trigger
CREATE OR REPLACE FUNCTION sm.trg_ep_reg_del() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  DROP TRIGGER IF EXISTS yv_reg_ins ON sm.youtube_video;
  DELETE FROM sm.post_registry
   WHERE platform = 'podcast_episode'
     AND key1 = OLD.id::text
     AND key2 IS NULL;
  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS ep_reg_del ON podcasts.episodes;
CREATE TRIGGER ep_reg_del
AFTER DELETE ON podcasts.episodes
FOR EACH ROW EXECUTE FUNCTION sm.trg_ep_reg_del();

-- ==========================
-- 4) YOUTUBE VIDEO: transcript-based search
-- ==========================

-- Drop old post_registry insert (same logic as podcast episode)
DROP TRIGGER IF EXISTS yv_reg_ins ON sm.youtube_video;
DROP FUNCTION IF EXISTS sm.trg_yv_reg_ins();

ALTER TABLE sm.youtube_video
  ADD COLUMN IF NOT EXISTS transcript TEXT,
  ADD COLUMN IF NOT EXISTS transcript_updated_at TIMESTAMPTZ;

-- Drop old indexes tied to filtered_text / title / description
DROP INDEX IF EXISTS sm.yv_tsv_en_gin;
DROP INDEX IF EXISTS sm.yv_desc_trgm;
DROP INDEX IF EXISTS sm.yv_title_trgm;

ALTER TABLE sm.youtube_video
  DROP COLUMN IF EXISTS filtered_text CASCADE,
  DROP COLUMN IF EXISTS tsv_en CASCADE;

ALTER TABLE sm.youtube_video
  ADD COLUMN tsv_en tsvector
    GENERATED ALWAYS AS (to_tsvector('english', COALESCE(transcript, ''))) STORED;

-- ==========================
-- 5) TERM MATCHING: replace with post_term_hit
-- ==========================
DROP TABLE IF EXISTS matches.post_term_match CASCADE;

CREATE TABLE matches.post_term_hit (
    id              BIGSERIAL PRIMARY KEY,
    post_id         BIGINT NOT NULL REFERENCES sm.post_registry(id) ON DELETE CASCADE,
    term_id         INT    NOT NULL REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE,

    match_start     INT NOT NULL,
    match_end       INT NOT NULL,

    matched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    matcher_version TEXT NOT NULL DEFAULT '',

    CONSTRAINT post_term_hit_span_chk
      CHECK (match_start >= 0 AND match_end >= match_start),

    CONSTRAINT post_term_hit_uniq
      UNIQUE (post_id, term_id, match_start, match_end, matcher_version)
);

CREATE INDEX post_term_hit_term_post_idx
  ON matches.post_term_hit (term_id, post_id);

CREATE INDEX post_term_hit_post_idx
  ON matches.post_term_hit (post_id);

CREATE INDEX post_term_hit_matched_at_brin
  ON matches.post_term_hit USING BRIN (matched_at);

CREATE INDEX post_term_hit_matcher_time_idx
  ON matches.post_term_hit (matcher_version, matched_at);

-- ==========================
-- 6) post_registry constraints + indexes
-- ==========================
ALTER TABLE sm.post_registry
ADD CONSTRAINT post_registry_platform_chk
CHECK (
  platform IN (
    'tweet',
    'reddit_submission',
    'reddit_comment',
    'telegram_post',
    'youtube_video',
    'youtube_comment',
    'podcast_episode',
    'news_article'
  )
);

CREATE INDEX IF NOT EXISTS post_registry_id_brin
  ON sm.post_registry USING BRIN (id);

-- ==========================
-- 7) Rebuild posts_all
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
        t.filtered_text      AS text,
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
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        rs.date_entered      AS date_entered,
        rs.created_at_ts     AS created_at_ts,
        rs.filtered_text     AS text,
        rs.tsv_en            AS tsv_en,
        rs.is_en             AS is_en,
        rs.score::BIGINT     AS primary_metric,
        rs.permalink         AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_submission rs
      ON pr.platform = 'reddit_submission'
     AND pr.key1 = rs.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Reddit comments
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        rc.date_entered      AS date_entered,
        rc.created_at_ts     AS created_at_ts,
        rc.filtered_text     AS text,
        rc.tsv_en            AS tsv_en,
        rc.is_en             AS is_en,
        rc.score::BIGINT     AS primary_metric,
        rc.permalink         AS url
    FROM sm.post_registry pr
    JOIN sm.reddit_comment rc
      ON pr.platform = 'reddit_comment'
     AND pr.key1 = rc.id
     AND pr.key2 IS NULL

    UNION ALL

    -- Telegram posts
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        tp.date_entered      AS date_entered,
        tp.created_at_ts     AS created_at_ts,
        tp.filtered_text     AS text,
        tp.tsv_en            AS tsv_en,
        tp.is_en             AS is_en,
        tp.views::BIGINT     AS primary_metric,
        tp.link              AS url
    FROM sm.post_registry pr
    JOIN sm.telegram_post tp
      ON pr.platform = 'telegram_post'
     AND pr.key1 = tp.channel_id::text
     AND pr.key2 = tp.message_id::text

    UNION ALL

    -- YouTube videos
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        yv.date_entered      AS date_entered,
        yv.created_at_ts     AS created_at_ts,
        yv.transcript        AS text,
        yv.tsv_en            AS tsv_en,
        yv.is_en             AS is_en,
        yv.view_count::BIGINT AS primary_metric,
        yv.url               AS url
    FROM sm.post_registry pr
    JOIN sm.youtube_video yv
      ON pr.platform = 'youtube_video'
     AND pr.key1 = yv.video_id
     AND pr.key2 IS NULL

    UNION ALL

    -- YouTube comments
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        yc.date_entered      AS date_entered,
        yc.created_at_ts     AS created_at_ts,
        yc.filtered_text     AS text,
        yc.tsv_en            AS tsv_en,
        yc.is_en             AS is_en,
        yc.like_count::BIGINT AS primary_metric,
        yc.comment_url       AS url
    FROM sm.post_registry pr
    JOIN sm.youtube_comment yc
      ON pr.platform = 'youtube_comment'
     AND pr.key1 = yc.video_id
     AND pr.key2 = yc.comment_id

    UNION ALL

    -- Podcast episodes
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        e.date_entered       AS date_entered,
        e.created_at_ts      AS created_at_ts,
        e.transcript         AS text,
        e.tsv_en             AS tsv_en,
        e.is_en              AS is_en,
        NULL::BIGINT         AS primary_metric,
        e.download_url       AS url
    FROM sm.post_registry pr
    JOIN podcasts.episodes e
      ON pr.platform = 'podcast_episode'
     AND pr.key1 = e.id
     AND pr.key2 IS NULL

    UNION ALL

    -- News articles
    SELECT
        pr.id                AS post_id,
        pr.platform          AS platform,
        pr.key1              AS key1,
        pr.key2              AS key2,
        a.date_entered       AS date_entered,
        a.created_at_ts      AS created_at_ts,
        a.text               AS text,
        a.tsv_en             AS tsv_en,
        a.is_en              AS is_en,
        NULL::BIGINT         AS primary_metric,
        a.url                AS url
    FROM sm.post_registry pr
    JOIN news.article a
      ON pr.platform = 'news_article'
     AND pr.key1 = a.id::text
     AND pr.key2 IS NULL;

-- ==========================
-- 8) post_search_en (now includes podcasts)
-- ==========================
CREATE OR REPLACE VIEW sm.post_search_en AS
SELECT post_id, tsv_en
FROM sm.posts_all
WHERE is_en IS TRUE;

-- ==========================
-- 9) Indexes
-- ==========================
CREATE INDEX IF NOT EXISTS news_article_tsv_en_gin
  ON news.article USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS news_article_text_trgm
  ON news.article USING GIN (text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS episodes_transcript_tsv_en_gin
  ON podcasts.episodes USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS yv_tsv_en_gin
  ON sm.youtube_video USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS youtube_video_transcript_trgm
  ON sm.youtube_video USING GIN (transcript gin_trgm_ops);

COMMIT;
