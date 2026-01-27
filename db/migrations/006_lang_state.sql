CREATE TABLE IF NOT EXISTS sm.lang_label_state (
    platform TEXT PRIMARY KEY,
    last_checked_post_id BIGINT,
    last_run_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
