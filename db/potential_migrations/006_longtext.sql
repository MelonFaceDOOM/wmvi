-- Purpose:
--   - Create news.article (NO filtered_text; tsv_en based on text)
--   - Create longtext.content_unit (content_unit is the registry)
--   - Create matches.content_unit_match + matches.content_unit_match_state
--   - Create two views:
--       1) longtext.units_skinny      (unit-only + minimal fields)
--       2) longtext.units_with_meta   (unit + joined source metadata)
--   - Add indices, grouped by purpose

-- ==========================
-- 0) Extensions (for trigram indexes below)
-- ==========================
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ==========================
-- 1) News schema + table
-- ==========================
CREATE SCHEMA IF NOT EXISTS news;

CREATE TABLE IF NOT EXISTS news.article (
    id            BIGSERIAL PRIMARY KEY,
    date_entered  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- identity
    url           TEXT NOT NULL,
    url_hash      VARCHAR(32) GENERATED ALWAYS AS (MD5(url)) STORED,

    -- metadata
    publication   TEXT,   -- e.g., site / outlet name
    title         TEXT,

    -- content
    text          TEXT NOT NULL,
    tsv_en        tsvector GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,

    -- publication time if available; can be NULL
    created_at_ts TIMESTAMPTZ,
    is_en         BOOLEAN,

    CONSTRAINT news_article_url_uniq UNIQUE (url_hash)
);

-- ==========================
-- 2) longtext.content_unit (registry)
-- ==========================
CREATE SCHEMA IF NOT EXISTS longtext;

-- source_type examples:
--   'podcast_episode'  (source_key = podcasts.episodes.id)
--   'youtube_video'    (source_key = youtube.video.video_id)
--   'news_article'     (source_key = news.article.id::text)
CREATE TABLE IF NOT EXISTS longtext.content_unit (
    id            BIGSERIAL PRIMARY KEY,
    date_entered  TIMESTAMPTZ NOT NULL DEFAULT now(),

    source_type   TEXT NOT NULL,
    source_key    TEXT NOT NULL,
    unit_idx      INT  NOT NULL,   -- stable ordering within the source
    text          TEXT NOT NULL,
    tsv_en        tsvector GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,

    -- optional “source time” for dashboards / filtering
    created_at_ts TIMESTAMPTZ,
    is_en         BOOLEAN,

    CONSTRAINT content_unit_source_uniq UNIQUE (source_type, source_key, unit_idx)
);

-- ==========================
-- 3) Content-unit term matching
-- ==========================
CREATE TABLE IF NOT EXISTS matches.content_unit_match (
    unit_id         BIGINT NOT NULL REFERENCES longtext.content_unit(id) ON DELETE CASCADE,
    term_id         INT    NOT NULL REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE,
    matched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    matcher_version TEXT,
    confidence      NUMERIC,
    PRIMARY KEY (unit_id, term_id)
);

CREATE TABLE IF NOT EXISTS matches.content_unit_match_state (
    term_id              INT  NOT NULL REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE,
    matcher_version      TEXT NOT NULL,
    last_checked_unit_id BIGINT,
    last_run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (term_id, matcher_version)
);

-- ==========================
-- 4) Indices (grouped by purpose)
-- ==========================

-- ---- News search ----
CREATE INDEX IF NOT EXISTS news_article_tsv_en_gin
  ON news.article USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS news_article_text_trgm
  ON news.article USING GIN (text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS news_article_created_at_brin
  ON news.article USING BRIN (created_at_ts);

CREATE INDEX IF NOT EXISTS news_article_date_entered_brin
  ON news.article USING BRIN (date_entered);

-- ---- Content-unit search (primary matching substrate) ----
CREATE INDEX IF NOT EXISTS content_unit_tsv_en_gin
  ON longtext.content_unit USING GIN (tsv_en);

CREATE INDEX IF NOT EXISTS content_unit_text_trgm
  ON longtext.content_unit USING GIN (text gin_trgm_ops);

-- ---- Content-unit time filtering ----
CREATE INDEX IF NOT EXISTS content_unit_created_at_brin
  ON longtext.content_unit USING BRIN (created_at_ts);

CREATE INDEX IF NOT EXISTS content_unit_date_entered_brin
  ON longtext.content_unit USING BRIN (date_entered);

-- ---- Fast “units for a source in order” ----
CREATE INDEX IF NOT EXISTS content_unit_source_idx
  ON longtext.content_unit (source_type, source_key, unit_idx);

-- ---- Matching table access patterns ----
CREATE INDEX IF NOT EXISTS content_unit_match_term_idx
  ON matches.content_unit_match (term_id, unit_id);

CREATE INDEX IF NOT EXISTS content_unit_match_matched_at_brin
  ON matches.content_unit_match USING BRIN (matched_at);

-- ---- yt transcript ----
CREATE INDEX IF NOT EXISTS youtube_video_transcript_trgm
  ON youtube.video USING GIN (transcript gin_trgm_ops);

-- ==========================
-- 5) Views
--   - units_skinny: unit-level only
--   - units_with_meta: joins to source tables for common workflow metadata
-- ==========================

-- 5a) Skinny: no source joins
CREATE OR REPLACE VIEW longtext.units_skinny AS
SELECT
    u.id           AS unit_id,
    u.source_type  AS source_type,
    u.source_key   AS source_key,
    u.unit_idx     AS unit_idx,
    u.date_entered AS unit_date_entered,
    u.created_at_ts AS unit_created_at_ts,
    u.is_en        AS unit_is_en,
    u.text         AS text,
    u.tsv_en       AS tsv_en
FROM longtext.content_unit u;

-- 5b) With metadata: branch by source_type and join to the real source tables
--   - include primary_metric (NULL for podcast/news; youtube.video uses view_count)
--   - include publication string:
--       - youtube.video: channel_title
--       - news.article: publication
--       - podcast: show title
CREATE OR REPLACE VIEW longtext.units_with_meta AS
    -- Podcast episodes
    SELECT
        u.id            AS unit_id,
        u.source_type   AS source_type,
        u.source_key    AS source_key,
        u.unit_idx      AS unit_idx,

        u.date_entered  AS unit_date_entered,
        u.created_at_ts AS unit_created_at_ts,
        u.is_en         AS unit_is_en,

        u.text          AS text,
        u.tsv_en        AS tsv_en,

        -- common metadata
        'podcast'::text AS source_family,
        sh.title        AS publication,
        ep.title        AS title,
        ep.download_url AS url,
        ep.created_at_ts AS created_at_ts,
        NULL::BIGINT    AS primary_metric,

        -- extra useful fields
        ep.id           AS episode_id,
        sh.id::text     AS publication_key,
        ep.transcript_updated_at AS transcript_updated_at
    FROM longtext.content_unit u
    JOIN podcasts.episodes ep
      ON u.source_type = 'podcast_episode'
     AND u.source_key  = ep.id
    JOIN podcasts.shows sh
      ON ep.podcast_id = sh.id

    UNION ALL

    -- YouTube videos (long-form)
    SELECT
        u.id            AS unit_id,
        u.source_type   AS source_type,
        u.source_key    AS source_key,
        u.unit_idx      AS unit_idx,

        u.date_entered  AS unit_date_entered,
        u.created_at_ts AS unit_created_at_ts,
        u.is_en         AS unit_is_en,

        u.text          AS text,
        u.tsv_en        AS tsv_en,

        'youtube'::text AS source_family,
        yv.channel_title AS publication,
        yv.title        AS title,
        yv.url          AS url,
        yv.created_at_ts AS created_at_ts,
        yv.view_count   AS primary_metric,

        yv.video_id     AS video_id,
        yv.channel_id   AS publication_key,
        yv.transcript_updated_at AS transcript_updated_at
    FROM longtext.content_unit u
    JOIN youtube.video yv
      ON u.source_type = 'youtube_video'
     AND u.source_key  = yv.video_id

    UNION ALL

    -- News articles
    SELECT
        u.id            AS unit_id,
        u.source_type   AS source_type,
        u.source_key    AS source_key,
        u.unit_idx      AS unit_idx,

        u.date_entered  AS unit_date_entered,
        u.created_at_ts AS unit_created_at_ts,
        u.is_en         AS unit_is_en,

        u.text          AS text,
        u.tsv_en        AS tsv_en,

        'news'::text    AS source_family,
        a.publication   AS publication,
        a.title         AS title,
        a.url           AS url,
        a.created_at_ts AS created_at_ts,
        NULL::BIGINT    AS primary_metric,

        a.id::text      AS article_id,
        NULL::text      AS publication_key,
        NULL::timestamptz AS transcript_updated_at
    FROM longtext.content_unit u
    JOIN news.article a
      ON u.source_type = 'news_article'
     AND u.source_key  = a.id::text;
