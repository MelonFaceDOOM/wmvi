# WMVI (Web Monitoring for Vaccine Information)

Brief: collect social-media posts about vaccines, normalize them, and track term-level matches for analysis.

## Database layout

- `sm.*`  
  - Per-platform tables: `tweet`, `reddit_submission`, `reddit_comment`, `youtube_video`, `youtube_comment`, `telegram_post`, etc.  
  - As part of ingestion, each data source removes personal info from the primary text attribute, discards the original text, and stores the filtered text in `filered_text`.
  - Each table also has a `tsv_en` full-text search column.

- `taxonomy.vaccine_term`  
  - Canonical list of vaccine-related terms.  
  - Columns: `id`, `name`, `type`, etc.

- `sm.post_registry`  
  - Single registry of all posts across platforms.  
  - Columns: `id` (global post id), `platform`, `key1`, `key2`, `post_key`.  
  - Each `sm.*` table row has a matching entry here.
  - Entry is automatic via triggers so this table doesn't need to be manually maintained.

- `sm.post_search_en` (view)  
  - Union of all `tsv_en` columns joined to `sm.post_registry`.  
  - Columns: `post_id`, `tsv_en`.  
  - Used as the unified search surface for term matching.

- `matches.post_term_match`  
  - Term–post matches.  
  - Columns: `post_id`, `term_id`, `matcher_version`, `matched_at`, `confidence`.  
  - PK: `(post_id, term_id)`.
  - Populated by a term matching service. This is the canonical way that terms are tied to posts.

- `matches.term_match_state`  
  - Per-term scan state.  
  - Columns: `term_id`, `matcher_version`, `last_checked_post_id`, `last_run_at`.  
  - Tracks how far each matcher version has scanned through `sm.post_registry.id`.

## Scripts

All scripts are run via `python -m` from the project root. There are many and some are suited to very specific circumstances, but here is a description of a few:

- `scripts/migrate_db.py`  
  - Apply SQL migrations in `db/migrations/`.  
  - Usage:  
    - `python -m scripts.migrate_db`

- `scripts/db_clone.py` (or similar)  
  - Clone/copy DB between environments (e.g. dev → prod).  
  - Usage pattern:  
    - `python -m scripts.db_clone`  
  - See script docstring / comments for exact flags.

- `python -m scripts.<script_name> [args...]`

## Services

### Reddit monitor

Module: `services.reddit_monitor.reddit_monitor`

Purpose: continuously scrape Reddit for configured vaccine-related search terms and insert new posts into `sm.reddit_submission` / `sm.reddit_comment` + `sm.post_registry`.

Key points:

- Uses a scheduler (`ScrapeScheduler`) that:
  - Loads search terms from the DB.
  - Tracks observed result rates per term.
  - Adapts scrape frequency per term based on activity.
- Writes raw posts into `sm` tables and maintains `sm.post_registry`.

Run from project root:

- `python -m services.reddit_monitor.reddit_monitor`

Service is intended to run 24/7 under a supervisor (systemd, etc.), but can also be run manually.

### Term matcher

Module: `services.term_matcher` (core logic in `term_matcher.py`, CLI in `cli.py`)

Purpose: scan posts for vaccine terms and populate `matches.post_term_match`, using `matches.term_match_state` to avoid reprocessing old posts.

Key behavior:

- For each term in `taxonomy.vaccine_term` and a given `matcher_version`:
  - Reads `last_checked_post_id` from `matches.term_match_state`.
  - Finds new posts in `(last_checked_post_id, max(sm.post_registry.id)]` via `sm.post_search_en` and `plainto_tsquery('english', term_name)`.
  - Inserts matches into `matches.post_term_match` with `ON CONFLICT DO NOTHING`.
  - Updates `last_checked_post_id` to the current max post id.

CLI entry (from project root):

- General form:

  - `python -m services.term_matcher.cli <command> [options]`

Examples:

- Run continuously over all terms:

  - `python -m services.term_matcher.cli run-loop`

- Single pass over all terms:

  - `python -m services.term_matcher.cli run-once`

- Run for specific term IDs:

  - `python -m services.term_matcher.cli run-ids --term-id 1 2 3`

- Print term list:

  - `python -m services.term_matcher.cli print-terms`
  - `python -m services.term_matcher.cli print-terms --filter covid`

- Stats for matches and coverage:

  - `python -m services.term_matcher.cli stats`
  - `python -m services.term_matcher.cli stats-top --limit 20`