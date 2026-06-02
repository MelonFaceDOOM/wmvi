-- Tracks content sync export/import watermarks (unified dev -> prod bundles).

CREATE TABLE IF NOT EXISTS sm.content_sync_state (
    id                      TEXT PRIMARY KEY,
    last_exported_at        TIMESTAMPTZ,
    last_imported_bundle_at TIMESTAMPTZ
);

INSERT INTO sm.content_sync_state (id, last_exported_at, last_imported_bundle_at)
VALUES ('global', NULL, NULL)
ON CONFLICT (id) DO NOTHING;
