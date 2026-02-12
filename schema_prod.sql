--
-- PostgreSQL database dump
--

\restrict yt5c3TGQcbl3CB7g8NbYMdd9P3CNZJ2ojc63PdW6ioLkbkZHIDJi8NDrdh9DnVQ

-- Dumped from database version 17.7
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: matches; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA matches;


--
-- Name: news; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA news;


--
-- Name: podcasts; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA podcasts;


--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: scrape; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA scrape;


--
-- Name: sm; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA sm;


--
-- Name: taxonomy; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA taxonomy;


--
-- Name: youtube; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA youtube;


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: trg_episode_reg_del(); Type: FUNCTION; Schema: podcasts; Owner: -
--

CREATE FUNCTION podcasts.trg_episode_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'podcast_episode'
     AND key1 = OLD.id::text
     AND key2 IS NULL;
  RETURN OLD;
END $$;


--
-- Name: trg_article_reg_del(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_article_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'news_article'
     AND key1 = OLD.id::text
     AND key2 IS NULL;
  RETURN OLD;
END $$;


--
-- Name: trg_article_reg_ins(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_article_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('news_article', NEW.id::text, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: trg_rc_reg_del(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_rc_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform='reddit_comment' AND key1=OLD.id AND key2 IS NULL;
  RETURN OLD;
END $$;


--
-- Name: trg_rc_reg_ins(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_rc_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('reddit_comment', NEW.id, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: trg_rs_reg_del(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_rs_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform='reddit_submission' AND key1=OLD.id AND key2 IS NULL;
  RETURN OLD;
END $$;


--
-- Name: trg_rs_reg_ins(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_rs_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('reddit_submission', NEW.id, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: trg_tg_reg_del(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_tg_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform='telegram_post'
     AND key1=OLD.channel_id::text
     AND key2=OLD.message_id::text;
  RETURN OLD;
END $$;


--
-- Name: trg_tg_reg_ins(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_tg_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('telegram_post', NEW.channel_id::text, NEW.message_id::text)
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: trg_tweet_reg_del(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_tweet_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform='tweet' AND key1=OLD.id::text AND key2 IS NULL;
  RETURN OLD;
END $$;


--
-- Name: trg_tweet_reg_ins(); Type: FUNCTION; Schema: sm; Owner: -
--

CREATE FUNCTION sm.trg_tweet_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('tweet', NEW.id::text, '')
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: set_duration_seconds(); Type: FUNCTION; Schema: youtube; Owner: -
--

CREATE FUNCTION youtube.set_duration_seconds() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.duration_iso IS NOT NULL THEN
        NEW.duration_seconds :=
            EXTRACT(EPOCH FROM NEW.duration_iso::interval)::integer;
    ELSE
        NEW.duration_seconds := NULL;
    END IF;
    RETURN NEW;
END $$;


--
-- Name: trg_comment_reg_del(); Type: FUNCTION; Schema: youtube; Owner: -
--

CREATE FUNCTION youtube.trg_comment_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'youtube_comment'
     AND key1 = OLD.video_id
     AND key2 = OLD.comment_id;
  RETURN OLD;
END $$;


--
-- Name: trg_comment_reg_ins(); Type: FUNCTION; Schema: youtube; Owner: -
--

CREATE FUNCTION youtube.trg_comment_reg_ins() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO sm.post_registry(platform, key1, key2)
  VALUES ('youtube_comment', NEW.video_id, NEW.comment_id)
  ON CONFLICT (platform, key1, key2) DO NOTHING;
  RETURN NEW;
END $$;


--
-- Name: trg_video_reg_del(); Type: FUNCTION; Schema: youtube; Owner: -
--

CREATE FUNCTION youtube.trg_video_reg_del() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM sm.post_registry
   WHERE platform = 'youtube_video'
     AND key1 = OLD.video_id
     AND key2 IS NULL;
  RETURN OLD;
END $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: post_term_hit; Type: TABLE; Schema: matches; Owner: -
--

CREATE TABLE matches.post_term_hit (
    id bigint NOT NULL,
    post_id bigint NOT NULL,
    term_id integer NOT NULL,
    match_start integer NOT NULL,
    match_end integer NOT NULL,
    matched_at timestamp with time zone DEFAULT now() NOT NULL,
    matcher_version text DEFAULT ''::text NOT NULL,
    CONSTRAINT post_term_hit_span_chk CHECK (((match_start >= 0) AND (match_end >= match_start)))
);


--
-- Name: post_term_hit_id_seq; Type: SEQUENCE; Schema: matches; Owner: -
--

CREATE SEQUENCE matches.post_term_hit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: post_term_hit_id_seq; Type: SEQUENCE OWNED BY; Schema: matches; Owner: -
--

ALTER SEQUENCE matches.post_term_hit_id_seq OWNED BY matches.post_term_hit.id;


--
-- Name: term_match_state; Type: TABLE; Schema: matches; Owner: -
--

CREATE TABLE matches.term_match_state (
    term_id integer NOT NULL,
    matcher_version text NOT NULL,
    last_checked_post_id bigint,
    last_run_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: article; Type: TABLE; Schema: news; Owner: -
--

CREATE TABLE news.article (
    id bigint NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    url text NOT NULL,
    url_hash character varying(32) GENERATED ALWAYS AS (md5(url)) STORED,
    publication text,
    title text,
    text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, text)) STORED,
    created_at_ts timestamp with time zone,
    is_en boolean
);


--
-- Name: article_id_seq; Type: SEQUENCE; Schema: news; Owner: -
--

CREATE SEQUENCE news.article_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: article_id_seq; Type: SEQUENCE OWNED BY; Schema: news; Owner: -
--

ALTER SEQUENCE news.article_id_seq OWNED BY news.article.id;


--
-- Name: episodes; Type: TABLE; Schema: podcasts; Owner: -
--

CREATE TABLE podcasts.episodes (
    id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    audio_path text NOT NULL,
    guid text NOT NULL,
    title text,
    description text,
    created_at_ts timestamp with time zone,
    download_url text,
    podcast_id integer NOT NULL,
    transcript text,
    transcript_updated_at timestamp with time zone,
    is_en boolean,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, COALESCE(transcript, ''::text))) STORED,
    transcription_started_at timestamp with time zone
);


--
-- Name: shows; Type: TABLE; Schema: podcasts; Owner: -
--

CREATE TABLE podcasts.shows (
    id integer NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    title text NOT NULL,
    rss_url text,
    rss_url_hash character varying(32) GENERATED ALWAYS AS (md5(rss_url)) STORED
);


--
-- Name: shows_id_seq; Type: SEQUENCE; Schema: podcasts; Owner: -
--

ALTER TABLE podcasts.shows ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME podcasts.shows_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: transcript_segments; Type: TABLE; Schema: podcasts; Owner: -
--

CREATE TABLE podcasts.transcript_segments (
    id bigint NOT NULL,
    episode_id text NOT NULL,
    seg_idx integer NOT NULL,
    start_s numeric NOT NULL,
    end_s numeric NOT NULL,
    text text,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, COALESCE(text, ''::text))) STORED,
    CONSTRAINT transcript_segments_time_chk CHECK ((end_s >= start_s))
);


--
-- Name: transcript_segments_id_seq; Type: SEQUENCE; Schema: podcasts; Owner: -
--

CREATE SEQUENCE podcasts.transcript_segments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transcript_segments_id_seq; Type: SEQUENCE OWNED BY; Schema: podcasts; Owner: -
--

ALTER SEQUENCE podcasts.transcript_segments_id_seq OWNED BY podcasts.transcript_segments.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    checksum text NOT NULL
);


--
-- Name: job; Type: TABLE; Schema: scrape; Owner: -
--

CREATE TABLE scrape.job (
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    description text,
    platforms text[] DEFAULT '{}'::text[] NOT NULL,
    time_range tstzrange,
    term_set_snapshot jsonb,
    config_snapshot jsonb,
    status text DEFAULT 'completed'::text NOT NULL,
    notes text
);


--
-- Name: job_id_seq; Type: SEQUENCE; Schema: scrape; Owner: -
--

ALTER TABLE scrape.job ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME scrape.job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: post_scrape; Type: TABLE; Schema: scrape; Owner: -
--

CREATE TABLE scrape.post_scrape (
    scrape_job_id integer NOT NULL,
    post_id bigint NOT NULL,
    linked_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: lang_label_state; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.lang_label_state (
    id text DEFAULT 'global'::text NOT NULL,
    last_checked_post_id bigint DEFAULT 0 NOT NULL,
    last_run_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: post_registry; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.post_registry (
    id bigint NOT NULL,
    platform text NOT NULL,
    key1 text NOT NULL,
    key2 text NOT NULL,
    post_key text GENERATED ALWAYS AS (
CASE platform
    WHEN 'tweet'::text THEN ((platform || ':'::text) || key1)
    WHEN 'reddit_submission'::text THEN ((platform || ':'::text) || key1)
    WHEN 'reddit_comment'::text THEN ((platform || ':'::text) || key1)
    WHEN 'youtube_video'::text THEN ((platform || ':'::text) || key1)
    WHEN 'youtube_comment'::text THEN ((((platform || ':'::text) || key1) || ':'::text) || COALESCE(key2, ''::text))
    WHEN 'telegram_post'::text THEN ((((platform || ':'::text) || key1) || ':'::text) || COALESCE(key2, ''::text))
    ELSE (((platform || ':'::text) || key1) || COALESCE((':'::text || key2), ''::text))
END) STORED,
    CONSTRAINT post_registry_platform_chk CHECK ((platform = ANY (ARRAY['tweet'::text, 'reddit_submission'::text, 'reddit_comment'::text, 'telegram_post'::text, 'youtube_video'::text, 'youtube_comment'::text, 'podcast_episode'::text, 'news_article'::text])))
);


--
-- Name: post_registry_id_seq; Type: SEQUENCE; Schema: sm; Owner: -
--

CREATE SEQUENCE sm.post_registry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: post_registry_id_seq; Type: SEQUENCE OWNED BY; Schema: sm; Owner: -
--

ALTER SEQUENCE sm.post_registry_id_seq OWNED BY sm.post_registry.id;


--
-- Name: reddit_comment; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.reddit_comment (
    id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    link_id text NOT NULL,
    parent_comment_id text,
    body text NOT NULL,
    filtered_text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, filtered_text)) STORED,
    permalink text NOT NULL,
    created_at_ts timestamp with time zone NOT NULL,
    subreddit_id text NOT NULL,
    subreddit_type text,
    total_awards_received integer NOT NULL,
    subreddit text NOT NULL,
    score integer NOT NULL,
    gilded integer NOT NULL,
    stickied boolean DEFAULT false NOT NULL,
    is_submitter boolean DEFAULT false NOT NULL,
    gildings jsonb,
    all_awardings jsonb,
    is_en boolean
);


--
-- Name: reddit_submission; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.reddit_submission (
    id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    url text NOT NULL,
    url_hash character varying(32) GENERATED ALWAYS AS (md5(url)) STORED,
    domain text NOT NULL,
    title text NOT NULL,
    filtered_text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, filtered_text)) STORED,
    permalink text,
    created_at_ts timestamp with time zone NOT NULL,
    url_overridden_by_dest text,
    subreddit_id text NOT NULL,
    subreddit text NOT NULL,
    upvote_ratio numeric NOT NULL,
    score integer NOT NULL,
    gilded integer NOT NULL,
    num_comments integer NOT NULL,
    num_crossposts integer NOT NULL,
    pinned boolean DEFAULT false NOT NULL,
    stickied boolean DEFAULT false NOT NULL,
    over_18 boolean DEFAULT false NOT NULL,
    is_created_from_ads_ui boolean DEFAULT false NOT NULL,
    is_self boolean DEFAULT false NOT NULL,
    is_video boolean DEFAULT false NOT NULL,
    media jsonb,
    gildings jsonb,
    all_awardings jsonb,
    is_en boolean
);


--
-- Name: telegram_post; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.telegram_post (
    channel_id bigint NOT NULL,
    message_id bigint NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    link text NOT NULL,
    link_hash character varying(32) GENERATED ALWAYS AS (md5(link)) STORED,
    created_at_ts timestamp with time zone NOT NULL,
    text text NOT NULL,
    filtered_text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, filtered_text)) STORED,
    views integer,
    forwards integer,
    replies integer,
    reactions_total integer,
    is_pinned boolean DEFAULT false NOT NULL,
    has_media boolean DEFAULT false NOT NULL,
    raw_type text,
    is_en boolean
);


--
-- Name: tweet; Type: TABLE; Schema: sm; Owner: -
--

CREATE TABLE sm.tweet (
    id bigint NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    source text NOT NULL,
    conversation_id bigint NOT NULL,
    created_at_ts timestamp with time zone NOT NULL,
    tweet_text text NOT NULL,
    filtered_text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, filtered_text)) STORED,
    retweet_count integer,
    like_count integer,
    reply_count integer,
    quote_count integer,
    is_en boolean
);


--
-- Name: comment; Type: TABLE; Schema: youtube; Owner: -
--

CREATE TABLE youtube.comment (
    video_id text NOT NULL,
    comment_id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    comment_url text NOT NULL,
    comment_url_hash character varying(32) GENERATED ALWAYS AS (md5(comment_url)) STORED,
    text text NOT NULL,
    filtered_text text NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, filtered_text)) STORED,
    created_at_ts timestamp with time zone NOT NULL,
    like_count bigint,
    raw jsonb,
    is_en boolean,
    parent_comment_id text,
    reply_count bigint
);


--
-- Name: video; Type: TABLE; Schema: youtube; Owner: -
--

CREATE TABLE youtube.video (
    video_id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    url text NOT NULL,
    url_hash character varying(32) GENERATED ALWAYS AS (md5(url)) STORED,
    title text NOT NULL,
    description text,
    created_at_ts timestamp with time zone NOT NULL,
    channel_id text NOT NULL,
    channel_title text,
    duration_iso text,
    view_count bigint,
    like_count bigint,
    comment_count bigint,
    is_en boolean,
    transcript text,
    transcript_updated_at timestamp with time zone,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, COALESCE(transcript, ''::text))) STORED,
    transcription_started_at timestamp with time zone,
    duration_seconds integer
);


--
-- Name: posts_all; Type: VIEW; Schema: sm; Owner: -
--

CREATE VIEW sm.posts_all AS
 SELECT pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    t.date_entered,
    t.created_at_ts,
    t.filtered_text AS text,
    t.tsv_en,
    t.is_en,
    (t.like_count)::bigint AS primary_metric,
    NULL::text AS url
   FROM (sm.post_registry pr
     JOIN sm.tweet t ON (((pr.platform = 'tweet'::text) AND (pr.key1 = (t.id)::text) AND (pr.key2 = ''::text))))
UNION ALL
 SELECT pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    rs.date_entered,
    rs.created_at_ts,
    rs.filtered_text AS text,
    rs.tsv_en,
    rs.is_en,
    (rs.score)::bigint AS primary_metric,
    rs.permalink AS url
   FROM (sm.post_registry pr
     JOIN sm.reddit_submission rs ON (((pr.platform = 'reddit_submission'::text) AND (pr.key1 = rs.id) AND (pr.key2 = ''::text))))
UNION ALL
 SELECT pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    rc.date_entered,
    rc.created_at_ts,
    rc.filtered_text AS text,
    rc.tsv_en,
    rc.is_en,
    (rc.score)::bigint AS primary_metric,
    rc.permalink AS url
   FROM (sm.post_registry pr
     JOIN sm.reddit_comment rc ON (((pr.platform = 'reddit_comment'::text) AND (pr.key1 = rc.id) AND (pr.key2 = ''::text))))
UNION ALL
 SELECT pr.id AS post_id,
    pr.platform,
    pr.key1,
    pr.key2,
    tp.date_entered,
    tp.created_at_ts,
    tp.filtered_text AS text,
    tp.tsv_en,
    tp.is_en,
    (tp.views)::bigint AS primary_metric,
    tp.link AS url
   FROM (sm.post_registry pr
     JOIN sm.telegram_post tp ON (((pr.platform = 'telegram_post'::text) AND (pr.key1 = (tp.channel_id)::text) AND (pr.key2 = (tp.message_id)::text))))
UNION ALL
 SELECT pr.id AS post_id,
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
   FROM (sm.post_registry pr
     JOIN youtube.video yv ON (((pr.platform = 'youtube_video'::text) AND (pr.key1 = yv.video_id) AND (pr.key2 = ''::text))))
UNION ALL
 SELECT pr.id AS post_id,
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
   FROM (sm.post_registry pr
     JOIN youtube.comment yc ON (((pr.platform = 'youtube_comment'::text) AND (pr.key1 = yc.video_id) AND (pr.key2 = yc.comment_id))))
UNION ALL
 SELECT pr.id AS post_id,
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
   FROM (sm.post_registry pr
     JOIN podcasts.episodes e ON (((pr.platform = 'podcast_episode'::text) AND (pr.key1 = e.id) AND (pr.key2 = ''::text))))
UNION ALL
 SELECT pr.id AS post_id,
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
   FROM (sm.post_registry pr
     JOIN news.article a ON (((pr.platform = 'news_article'::text) AND (pr.key1 = (a.id)::text) AND (pr.key2 = ''::text))));


--
-- Name: post_search_en; Type: VIEW; Schema: sm; Owner: -
--

CREATE VIEW sm.post_search_en AS
 SELECT post_id,
    tsv_en
   FROM sm.posts_all
  WHERE (is_en IS TRUE);


--
-- Name: vaccine_term; Type: TABLE; Schema: taxonomy; Owner: -
--

CREATE TABLE taxonomy.vaccine_term (
    id integer NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    type text NOT NULL
);


--
-- Name: vaccine_term_id_seq; Type: SEQUENCE; Schema: taxonomy; Owner: -
--

ALTER TABLE taxonomy.vaccine_term ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME taxonomy.vaccine_term_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: vaccine_term_subset; Type: TABLE; Schema: taxonomy; Owner: -
--

CREATE TABLE taxonomy.vaccine_term_subset (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    date_entered timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: vaccine_term_subset_id_seq; Type: SEQUENCE; Schema: taxonomy; Owner: -
--

ALTER TABLE taxonomy.vaccine_term_subset ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME taxonomy.vaccine_term_subset_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: vaccine_term_subset_member; Type: TABLE; Schema: taxonomy; Owner: -
--

CREATE TABLE taxonomy.vaccine_term_subset_member (
    subset_id integer NOT NULL,
    term_id integer NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: search_status; Type: TABLE; Schema: youtube; Owner: -
--

CREATE TABLE youtube.search_status (
    term_id integer NOT NULL,
    last_found_ts timestamp with time zone NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    last_updated timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: transcript_segments; Type: TABLE; Schema: youtube; Owner: -
--

CREATE TABLE youtube.transcript_segments (
    id bigint NOT NULL,
    video_id text NOT NULL,
    seg_idx integer NOT NULL,
    start_s numeric NOT NULL,
    end_s numeric NOT NULL,
    text text,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    tsv_en tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, COALESCE(text, ''::text))) STORED,
    CONSTRAINT yt_transcript_segments_time_chk CHECK ((end_s >= start_s))
);


--
-- Name: transcript_segments_id_seq; Type: SEQUENCE; Schema: youtube; Owner: -
--

ALTER TABLE youtube.transcript_segments ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME youtube.transcript_segments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: post_term_hit id; Type: DEFAULT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.post_term_hit ALTER COLUMN id SET DEFAULT nextval('matches.post_term_hit_id_seq'::regclass);


--
-- Name: article id; Type: DEFAULT; Schema: news; Owner: -
--

ALTER TABLE ONLY news.article ALTER COLUMN id SET DEFAULT nextval('news.article_id_seq'::regclass);


--
-- Name: transcript_segments id; Type: DEFAULT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.transcript_segments ALTER COLUMN id SET DEFAULT nextval('podcasts.transcript_segments_id_seq'::regclass);


--
-- Name: post_registry id; Type: DEFAULT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.post_registry ALTER COLUMN id SET DEFAULT nextval('sm.post_registry_id_seq'::regclass);


--
-- Name: post_term_hit post_term_hit_pkey; Type: CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.post_term_hit
    ADD CONSTRAINT post_term_hit_pkey PRIMARY KEY (id);


--
-- Name: post_term_hit post_term_hit_uniq; Type: CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.post_term_hit
    ADD CONSTRAINT post_term_hit_uniq UNIQUE (post_id, term_id, match_start, match_end, matcher_version);


--
-- Name: term_match_state term_match_state_pkey; Type: CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.term_match_state
    ADD CONSTRAINT term_match_state_pkey PRIMARY KEY (term_id, matcher_version);


--
-- Name: article article_pkey; Type: CONSTRAINT; Schema: news; Owner: -
--

ALTER TABLE ONLY news.article
    ADD CONSTRAINT article_pkey PRIMARY KEY (id);


--
-- Name: article news_article_url_hash_uniq; Type: CONSTRAINT; Schema: news; Owner: -
--

ALTER TABLE ONLY news.article
    ADD CONSTRAINT news_article_url_hash_uniq UNIQUE (url_hash);


--
-- Name: episodes episodes_pkey; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.episodes
    ADD CONSTRAINT episodes_pkey PRIMARY KEY (id);


--
-- Name: episodes episodes_podcast_guid_uniq; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.episodes
    ADD CONSTRAINT episodes_podcast_guid_uniq UNIQUE (podcast_id, guid);


--
-- Name: shows podcasts_rss_url_uniq; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.shows
    ADD CONSTRAINT podcasts_rss_url_uniq UNIQUE (rss_url);


--
-- Name: shows shows_pkey; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.shows
    ADD CONSTRAINT shows_pkey PRIMARY KEY (id);


--
-- Name: transcript_segments transcript_segments_ep_seg_uniq; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.transcript_segments
    ADD CONSTRAINT transcript_segments_ep_seg_uniq UNIQUE (episode_id, seg_idx);


--
-- Name: transcript_segments transcript_segments_pkey; Type: CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.transcript_segments
    ADD CONSTRAINT transcript_segments_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: job job_pkey; Type: CONSTRAINT; Schema: scrape; Owner: -
--

ALTER TABLE ONLY scrape.job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: post_scrape post_scrape_pkey; Type: CONSTRAINT; Schema: scrape; Owner: -
--

ALTER TABLE ONLY scrape.post_scrape
    ADD CONSTRAINT post_scrape_pkey PRIMARY KEY (scrape_job_id, post_id);


--
-- Name: job scrape_job_name_uniq; Type: CONSTRAINT; Schema: scrape; Owner: -
--

ALTER TABLE ONLY scrape.job
    ADD CONSTRAINT scrape_job_name_uniq UNIQUE (name);


--
-- Name: lang_label_state lang_label_state_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.lang_label_state
    ADD CONSTRAINT lang_label_state_pkey PRIMARY KEY (id);


--
-- Name: post_registry post_registry_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.post_registry
    ADD CONSTRAINT post_registry_pkey PRIMARY KEY (id);


--
-- Name: post_registry post_registry_uniq; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.post_registry
    ADD CONSTRAINT post_registry_uniq UNIQUE (platform, key1, key2);


--
-- Name: reddit_comment reddit_comment_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.reddit_comment
    ADD CONSTRAINT reddit_comment_pkey PRIMARY KEY (id);


--
-- Name: reddit_submission reddit_submission_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.reddit_submission
    ADD CONSTRAINT reddit_submission_pkey PRIMARY KEY (id);


--
-- Name: telegram_post telegram_post_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.telegram_post
    ADD CONSTRAINT telegram_post_pkey PRIMARY KEY (channel_id, message_id);


--
-- Name: tweet tweet_pkey; Type: CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.tweet
    ADD CONSTRAINT tweet_pkey PRIMARY KEY (id);


--
-- Name: vaccine_term vaccine_term_name_key; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term
    ADD CONSTRAINT vaccine_term_name_key UNIQUE (name);


--
-- Name: vaccine_term vaccine_term_name_unique; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term
    ADD CONSTRAINT vaccine_term_name_unique UNIQUE (name);


--
-- Name: vaccine_term vaccine_term_pkey; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term
    ADD CONSTRAINT vaccine_term_pkey PRIMARY KEY (id);


--
-- Name: vaccine_term_subset_member vaccine_term_subset_member_pkey; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term_subset_member
    ADD CONSTRAINT vaccine_term_subset_member_pkey PRIMARY KEY (subset_id, term_id);


--
-- Name: vaccine_term_subset vaccine_term_subset_name_key; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term_subset
    ADD CONSTRAINT vaccine_term_subset_name_key UNIQUE (name);


--
-- Name: vaccine_term_subset vaccine_term_subset_pkey; Type: CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term_subset
    ADD CONSTRAINT vaccine_term_subset_pkey PRIMARY KEY (id);


--
-- Name: comment comment_pkey; Type: CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.comment
    ADD CONSTRAINT comment_pkey PRIMARY KEY (video_id, comment_id);


--
-- Name: search_status search_status_pkey; Type: CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.search_status
    ADD CONSTRAINT search_status_pkey PRIMARY KEY (term_id);


--
-- Name: transcript_segments transcript_segments_pkey; Type: CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.transcript_segments
    ADD CONSTRAINT transcript_segments_pkey PRIMARY KEY (id);


--
-- Name: video video_pkey; Type: CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.video
    ADD CONSTRAINT video_pkey PRIMARY KEY (video_id);


--
-- Name: post_term_hit_matched_at_brin; Type: INDEX; Schema: matches; Owner: -
--

CREATE INDEX post_term_hit_matched_at_brin ON matches.post_term_hit USING brin (matched_at);


--
-- Name: post_term_hit_matcher_time_idx; Type: INDEX; Schema: matches; Owner: -
--

CREATE INDEX post_term_hit_matcher_time_idx ON matches.post_term_hit USING btree (matcher_version, matched_at);


--
-- Name: post_term_hit_post_idx; Type: INDEX; Schema: matches; Owner: -
--

CREATE INDEX post_term_hit_post_idx ON matches.post_term_hit USING btree (post_id);


--
-- Name: post_term_hit_term_post_idx; Type: INDEX; Schema: matches; Owner: -
--

CREATE INDEX post_term_hit_term_post_idx ON matches.post_term_hit USING btree (term_id, post_id);


--
-- Name: news_article_text_trgm; Type: INDEX; Schema: news; Owner: -
--

CREATE INDEX news_article_text_trgm ON news.article USING gin (text public.gin_trgm_ops);


--
-- Name: news_article_tsv_en_gin; Type: INDEX; Schema: news; Owner: -
--

CREATE INDEX news_article_tsv_en_gin ON news.article USING gin (tsv_en);


--
-- Name: episodes_created_at_idx; Type: INDEX; Schema: podcasts; Owner: -
--

CREATE INDEX episodes_created_at_idx ON podcasts.episodes USING btree (created_at_ts);


--
-- Name: episodes_date_entered_brin; Type: INDEX; Schema: podcasts; Owner: -
--

CREATE INDEX episodes_date_entered_brin ON podcasts.episodes USING brin (date_entered);


--
-- Name: episodes_transcript_tsv_en_gin; Type: INDEX; Schema: podcasts; Owner: -
--

CREATE INDEX episodes_transcript_tsv_en_gin ON podcasts.episodes USING gin (tsv_en);


--
-- Name: seg_date_entered_brin; Type: INDEX; Schema: podcasts; Owner: -
--

CREATE INDEX seg_date_entered_brin ON podcasts.transcript_segments USING brin (date_entered);


--
-- Name: seg_text_tsv_en_gin; Type: INDEX; Schema: podcasts; Owner: -
--

CREATE INDEX seg_text_tsv_en_gin ON podcasts.transcript_segments USING gin (tsv_en);


--
-- Name: post_scrape_linked_at_idx; Type: INDEX; Schema: scrape; Owner: -
--

CREATE INDEX post_scrape_linked_at_idx ON scrape.post_scrape USING btree (linked_at);


--
-- Name: post_scrape_post_idx; Type: INDEX; Schema: scrape; Owner: -
--

CREATE INDEX post_scrape_post_idx ON scrape.post_scrape USING btree (post_id);


--
-- Name: scrape_job_created_at_idx; Type: INDEX; Schema: scrape; Owner: -
--

CREATE INDEX scrape_job_created_at_idx ON scrape.job USING btree (created_at);


--
-- Name: post_registry_id_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX post_registry_id_brin ON sm.post_registry USING brin (id);


--
-- Name: post_registry_platform_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX post_registry_platform_idx ON sm.post_registry USING btree (platform);


--
-- Name: rc_body_trgm; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_body_trgm ON sm.reddit_comment USING gin (body public.gin_trgm_ops);


--
-- Name: rc_created_at_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_created_at_brin ON sm.reddit_comment USING brin (created_at_ts);


--
-- Name: rc_date_entered_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_date_entered_brin ON sm.reddit_comment USING brin (date_entered);


--
-- Name: rc_en_time_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_en_time_idx ON sm.reddit_comment USING btree (created_at_ts, id) WHERE (is_en IS TRUE);


--
-- Name: rc_is_en_null_date_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_is_en_null_date_idx ON sm.reddit_comment USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: rc_link_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_link_idx ON sm.reddit_comment USING btree (link_id);


--
-- Name: rc_parent_comment_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_parent_comment_idx ON sm.reddit_comment USING btree (parent_comment_id);


--
-- Name: rc_score_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_score_idx ON sm.reddit_comment USING btree (score);


--
-- Name: rc_subreddit_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_subreddit_idx ON sm.reddit_comment USING btree (subreddit);


--
-- Name: rc_tsv_en_gin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rc_tsv_en_gin ON sm.reddit_comment USING gin (tsv_en);


--
-- Name: rs_created_at_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_created_at_brin ON sm.reddit_submission USING brin (created_at_ts);


--
-- Name: rs_date_entered_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_date_entered_brin ON sm.reddit_submission USING brin (date_entered);


--
-- Name: rs_en_time_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_en_time_idx ON sm.reddit_submission USING btree (created_at_ts, id) WHERE (is_en IS TRUE);


--
-- Name: rs_is_en_null_date_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_is_en_null_date_idx ON sm.reddit_submission USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: rs_score_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_score_idx ON sm.reddit_submission USING btree (score);


--
-- Name: rs_subreddit_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_subreddit_idx ON sm.reddit_submission USING btree (subreddit);


--
-- Name: rs_title_trgm; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_title_trgm ON sm.reddit_submission USING gin (title public.gin_trgm_ops);


--
-- Name: rs_tsv_en_gin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX rs_tsv_en_gin ON sm.reddit_submission USING gin (tsv_en);


--
-- Name: telegram_text_trgm; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX telegram_text_trgm ON sm.telegram_post USING gin (text public.gin_trgm_ops);


--
-- Name: telegram_tsv_en_gin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX telegram_tsv_en_gin ON sm.telegram_post USING gin (tsv_en);


--
-- Name: tg_created_at_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tg_created_at_brin ON sm.telegram_post USING brin (created_at_ts);


--
-- Name: tg_date_entered_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tg_date_entered_brin ON sm.telegram_post USING brin (date_entered);


--
-- Name: tg_is_en_null_date_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tg_is_en_null_date_idx ON sm.telegram_post USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: tweet_conversation_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_conversation_idx ON sm.tweet USING btree (conversation_id);


--
-- Name: tweet_created_at_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_created_at_brin ON sm.tweet USING brin (created_at_ts);


--
-- Name: tweet_date_entered_brin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_date_entered_brin ON sm.tweet USING brin (date_entered);


--
-- Name: tweet_en_time_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_en_time_idx ON sm.tweet USING btree (created_at_ts, id) WHERE (is_en IS TRUE);


--
-- Name: tweet_is_en_null_date_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_is_en_null_date_idx ON sm.tweet USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: tweet_like_idx; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_like_idx ON sm.tweet USING btree (like_count);


--
-- Name: tweet_text_trgm; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_text_trgm ON sm.tweet USING gin (tweet_text public.gin_trgm_ops);


--
-- Name: tweet_tsv_en_gin; Type: INDEX; Schema: sm; Owner: -
--

CREATE INDEX tweet_tsv_en_gin ON sm.tweet USING gin (tsv_en);


--
-- Name: yc_created_at_brin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yc_created_at_brin ON youtube.comment USING brin (created_at_ts);


--
-- Name: yc_date_entered_brin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yc_date_entered_brin ON youtube.comment USING brin (date_entered);


--
-- Name: yc_is_en_null_date_idx; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yc_is_en_null_date_idx ON youtube.comment USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: yc_text_trgm; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yc_text_trgm ON youtube.comment USING gin (text public.gin_trgm_ops);


--
-- Name: yc_tsv_en_gin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yc_tsv_en_gin ON youtube.comment USING gin (tsv_en);


--
-- Name: youtube_search_status_last_found_idx; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX youtube_search_status_last_found_idx ON youtube.search_status USING btree (last_found_ts);


--
-- Name: youtube_video_duration_seconds_idx; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX youtube_video_duration_seconds_idx ON youtube.video USING btree (duration_seconds);


--
-- Name: youtube_video_transcript_trgm; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX youtube_video_transcript_trgm ON youtube.video USING gin (transcript public.gin_trgm_ops);


--
-- Name: yt_transcript_segments_tsv_en_gin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yt_transcript_segments_tsv_en_gin ON youtube.transcript_segments USING gin (tsv_en);


--
-- Name: yt_transcript_segments_video_idx; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yt_transcript_segments_video_idx ON youtube.transcript_segments USING btree (video_id);


--
-- Name: yv_created_at_brin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yv_created_at_brin ON youtube.video USING brin (created_at_ts);


--
-- Name: yv_date_entered_brin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yv_date_entered_brin ON youtube.video USING brin (date_entered);


--
-- Name: yv_is_en_null_date_idx; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yv_is_en_null_date_idx ON youtube.video USING btree (date_entered) WHERE (is_en IS NULL);


--
-- Name: yv_tsv_en_gin; Type: INDEX; Schema: youtube; Owner: -
--

CREATE INDEX yv_tsv_en_gin ON youtube.video USING gin (tsv_en);


--
-- Name: article article_reg_del; Type: TRIGGER; Schema: news; Owner: -
--

CREATE TRIGGER article_reg_del AFTER DELETE ON news.article FOR EACH ROW EXECUTE FUNCTION sm.trg_article_reg_del();


--
-- Name: article article_reg_ins; Type: TRIGGER; Schema: news; Owner: -
--

CREATE TRIGGER article_reg_ins AFTER INSERT ON news.article FOR EACH ROW EXECUTE FUNCTION sm.trg_article_reg_ins();


--
-- Name: episodes ep_reg_del; Type: TRIGGER; Schema: podcasts; Owner: -
--

CREATE TRIGGER ep_reg_del AFTER DELETE ON podcasts.episodes FOR EACH ROW EXECUTE FUNCTION podcasts.trg_episode_reg_del();


--
-- Name: reddit_comment rc_reg_del; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER rc_reg_del AFTER DELETE ON sm.reddit_comment FOR EACH ROW EXECUTE FUNCTION sm.trg_rc_reg_del();


--
-- Name: reddit_comment rc_reg_ins; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER rc_reg_ins AFTER INSERT ON sm.reddit_comment FOR EACH ROW EXECUTE FUNCTION sm.trg_rc_reg_ins();


--
-- Name: reddit_submission rs_reg_del; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER rs_reg_del AFTER DELETE ON sm.reddit_submission FOR EACH ROW EXECUTE FUNCTION sm.trg_rs_reg_del();


--
-- Name: reddit_submission rs_reg_ins; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER rs_reg_ins AFTER INSERT ON sm.reddit_submission FOR EACH ROW EXECUTE FUNCTION sm.trg_rs_reg_ins();


--
-- Name: telegram_post tg_reg_del; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER tg_reg_del AFTER DELETE ON sm.telegram_post FOR EACH ROW EXECUTE FUNCTION sm.trg_tg_reg_del();


--
-- Name: telegram_post tg_reg_ins; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER tg_reg_ins AFTER INSERT ON sm.telegram_post FOR EACH ROW EXECUTE FUNCTION sm.trg_tg_reg_ins();


--
-- Name: tweet tweet_reg_del; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER tweet_reg_del AFTER DELETE ON sm.tweet FOR EACH ROW EXECUTE FUNCTION sm.trg_tweet_reg_del();


--
-- Name: tweet tweet_reg_ins; Type: TRIGGER; Schema: sm; Owner: -
--

CREATE TRIGGER tweet_reg_ins AFTER INSERT ON sm.tweet FOR EACH ROW EXECUTE FUNCTION sm.trg_tweet_reg_ins();


--
-- Name: comment yc_reg_del; Type: TRIGGER; Schema: youtube; Owner: -
--

CREATE TRIGGER yc_reg_del AFTER DELETE ON youtube.comment FOR EACH ROW EXECUTE FUNCTION youtube.trg_comment_reg_del();


--
-- Name: comment yc_reg_ins; Type: TRIGGER; Schema: youtube; Owner: -
--

CREATE TRIGGER yc_reg_ins AFTER INSERT ON youtube.comment FOR EACH ROW EXECUTE FUNCTION youtube.trg_comment_reg_ins();


--
-- Name: video yt_video_duration_seconds_trg; Type: TRIGGER; Schema: youtube; Owner: -
--

CREATE TRIGGER yt_video_duration_seconds_trg BEFORE INSERT OR UPDATE OF duration_iso ON youtube.video FOR EACH ROW EXECUTE FUNCTION youtube.set_duration_seconds();


--
-- Name: video yv_reg_del; Type: TRIGGER; Schema: youtube; Owner: -
--

CREATE TRIGGER yv_reg_del AFTER DELETE ON youtube.video FOR EACH ROW EXECUTE FUNCTION youtube.trg_video_reg_del();


--
-- Name: post_term_hit post_term_hit_post_id_fkey; Type: FK CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.post_term_hit
    ADD CONSTRAINT post_term_hit_post_id_fkey FOREIGN KEY (post_id) REFERENCES sm.post_registry(id) ON DELETE CASCADE;


--
-- Name: post_term_hit post_term_hit_term_id_fkey; Type: FK CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.post_term_hit
    ADD CONSTRAINT post_term_hit_term_id_fkey FOREIGN KEY (term_id) REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE;


--
-- Name: term_match_state term_match_state_term_id_fkey; Type: FK CONSTRAINT; Schema: matches; Owner: -
--

ALTER TABLE ONLY matches.term_match_state
    ADD CONSTRAINT term_match_state_term_id_fkey FOREIGN KEY (term_id) REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE;


--
-- Name: episodes episodes_podcast_id_fkey; Type: FK CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.episodes
    ADD CONSTRAINT episodes_podcast_id_fkey FOREIGN KEY (podcast_id) REFERENCES podcasts.shows(id) ON DELETE CASCADE;


--
-- Name: transcript_segments transcript_segments_episode_id_fkey; Type: FK CONSTRAINT; Schema: podcasts; Owner: -
--

ALTER TABLE ONLY podcasts.transcript_segments
    ADD CONSTRAINT transcript_segments_episode_id_fkey FOREIGN KEY (episode_id) REFERENCES podcasts.episodes(id) ON DELETE CASCADE;


--
-- Name: post_scrape post_scrape_post_id_fkey; Type: FK CONSTRAINT; Schema: scrape; Owner: -
--

ALTER TABLE ONLY scrape.post_scrape
    ADD CONSTRAINT post_scrape_post_id_fkey FOREIGN KEY (post_id) REFERENCES sm.post_registry(id) ON DELETE CASCADE;


--
-- Name: post_scrape post_scrape_scrape_job_id_fkey; Type: FK CONSTRAINT; Schema: scrape; Owner: -
--

ALTER TABLE ONLY scrape.post_scrape
    ADD CONSTRAINT post_scrape_scrape_job_id_fkey FOREIGN KEY (scrape_job_id) REFERENCES scrape.job(id) ON DELETE CASCADE;


--
-- Name: reddit_comment reddit_comment_parent_comment_id_fkey; Type: FK CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.reddit_comment
    ADD CONSTRAINT reddit_comment_parent_comment_id_fkey FOREIGN KEY (parent_comment_id) REFERENCES sm.reddit_comment(id) ON DELETE CASCADE;


--
-- Name: reddit_comment reddit_comment_submission_fk; Type: FK CONSTRAINT; Schema: sm; Owner: -
--

ALTER TABLE ONLY sm.reddit_comment
    ADD CONSTRAINT reddit_comment_submission_fk FOREIGN KEY (link_id) REFERENCES sm.reddit_submission(id) ON DELETE CASCADE;


--
-- Name: vaccine_term_subset_member vaccine_term_subset_member_subset_fk; Type: FK CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term_subset_member
    ADD CONSTRAINT vaccine_term_subset_member_subset_fk FOREIGN KEY (subset_id) REFERENCES taxonomy.vaccine_term_subset(id) ON DELETE CASCADE;


--
-- Name: vaccine_term_subset_member vaccine_term_subset_member_term_fk; Type: FK CONSTRAINT; Schema: taxonomy; Owner: -
--

ALTER TABLE ONLY taxonomy.vaccine_term_subset_member
    ADD CONSTRAINT vaccine_term_subset_member_term_fk FOREIGN KEY (term_id) REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE;


--
-- Name: comment comment_video_fk; Type: FK CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.comment
    ADD CONSTRAINT comment_video_fk FOREIGN KEY (video_id) REFERENCES youtube.video(video_id) ON DELETE CASCADE;


--
-- Name: comment youtube_comment_parent_fk; Type: FK CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.comment
    ADD CONSTRAINT youtube_comment_parent_fk FOREIGN KEY (video_id, parent_comment_id) REFERENCES youtube.comment(video_id, comment_id) ON DELETE CASCADE;


--
-- Name: search_status youtube_search_status_term_fk; Type: FK CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.search_status
    ADD CONSTRAINT youtube_search_status_term_fk FOREIGN KEY (term_id) REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE;


--
-- Name: transcript_segments yt_transcript_segments_video_fk; Type: FK CONSTRAINT; Schema: youtube; Owner: -
--

ALTER TABLE ONLY youtube.transcript_segments
    ADD CONSTRAINT yt_transcript_segments_video_fk FOREIGN KEY (video_id) REFERENCES youtube.video(video_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict yt5c3TGQcbl3CB7g8NbYMdd9P3CNZJ2ojc63PdW6ioLkbkZHIDJi8NDrdh9DnVQ

