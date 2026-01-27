-- telegram indices i forgot to add before:

CREATE INDEX IF NOT EXISTS telegram_text_trgm ON sm.telegram_post USING GIN (text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS telegram_tsv_en_gin ON sm.telegram_post USING GIN (tsv_en);

CREATE TABLE IF NOT EXISTS matches.term_match_state (
    term_id				 INT NOT NULL
		REFERENCES taxonomy.vaccine_term(id)
		ON DELETE CASCADE,
    matcher_version      TEXT   NOT NULL,
    last_checked_post_id BIGINT,
    last_run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (term_id, matcher_version)
);

-- post search will make it easier for the term matcher to search posts
CREATE VIEW sm.post_search_en AS
    SELECT pr.id AS post_id, t.tsv_en
    FROM sm.tweet t
    JOIN sm.post_registry pr
      ON pr.platform = 'tweet'
     AND pr.key1 = t.id::text

    UNION ALL

    SELECT pr.id AS post_id, rs.tsv_en
    FROM sm.reddit_submission rs
    JOIN sm.post_registry pr
      ON pr.platform = 'reddit_submission'
     AND pr.key1 = rs.id

    UNION ALL

    SELECT pr.id AS post_id, rc.tsv_en
    FROM sm.reddit_comment rc
    JOIN sm.post_registry pr
      ON pr.platform = 'reddit_comment'
     AND pr.key1 = rc.id

    UNION ALL

	SELECT pr.id AS post_id, tp.tsv_en
    FROM sm.telegram_post tp
    JOIN sm.post_registry pr
      ON pr.platform = 'telegram_post'
     AND pr.key1 = tp.channel_id::text
	 AND pr.key2 = tp.message_id::text

    UNION ALL

    SELECT pr.id AS post_id, yv.tsv_en
    FROM sm.youtube_video yv
    JOIN sm.post_registry pr
      ON pr.platform = 'youtube_video'
     AND pr.key1 = yv.video_id

    UNION ALL

    SELECT pr.id AS post_id, yc.tsv_en
    FROM sm.youtube_comment yc
    JOIN sm.post_registry pr
      ON pr.platform = 'youtube_comment'
     AND pr.key1 = yc.video_id
     AND pr.key2 = yc.comment_id;
