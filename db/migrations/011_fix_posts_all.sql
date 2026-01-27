-- Accidentally overwrote posts-all with a yt-only version in the previous migration
-- This re-adds all the tables

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

    -- Youtube videos
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
    FROM sm.post_registry pr
    JOIN youtube.video yv
      ON pr.platform = 'youtube_video'
     AND pr.key1 = yv.video_id
     AND pr.key2 IS NULL

    UNION ALL

    -- Youtube comments
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
    FROM sm.post_registry pr
    JOIN youtube.comment yc
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
