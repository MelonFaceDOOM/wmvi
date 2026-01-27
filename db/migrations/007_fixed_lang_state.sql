DROP TABLE IF EXISTS sm.lang_label_state;

CREATE TABLE IF NOT EXISTS sm.lang_label_state (
    id TEXT PRIMARY KEY DEFAULT 'global',
    last_checked_post_id BIGINT NOT NULL DEFAULT 0,
    last_run_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
