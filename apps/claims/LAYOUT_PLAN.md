# Claims file layout plan

File-mode organization for corpora and pipeline artifacts. Status is **derived from which well-known files exist** (optionally later: stale if upstream hashes/mtimes disagree)—not a stored enum.

## Top-level tree

```
apps/claims/data/
  inputs/<corpus>/          # corpus identity + pipeline ladder
    NOTES.md
    meta.json               # optional: terms, since, until, active pointers
    posts.json              # fresh
    posts_trimmed.json      # optional prep
    posts_coref.json        # optional prep
    claims.json             # extracted
    claims_filtered.json    # filtered (when added)
    groups.json             # grouped

  models/<tag>/             # shared across corpora (embedders; later filters)
  labels/                   # triplets, eval queries (shared / global)
  prompts/                  # prompt archive; one canonical active prompt
    active.txt              # or defaults.json pointing at the active file

  runs/<corpus>__<model_tag>/     # embedded (not inside inputs/)
    vectors.npy
    index.json
    metrics.json
    manifest.json           # optional (export/import)

  experiments/<corpus>__<model_tag>/<exp>/   # clustered
    hierarchy_….json
    leaf_labels_….npy
    narrative_labels_….npy
    …
```

## Why this split

| Area | Role |
|------|------|
| `inputs/<corpus>/` | One folder per corpus: identity + claim ladder through `groups.json` |
| `runs/<corpus>__<tag>/` | Embed output is tied to **corpus + embedder tag**; keeps alternate embedders without nesting under inputs |
| `experiments/…` | Cluster/hierarchy outputs under a run; never mutate the run’s vectors |
| `models/`, `prompts/`, `labels/` | Shared across corpora; CLI points at one **active** embedder / prompt |

## Status checklist (derive from files)

| Stage | Green when |
|-------|------------|
| fresh | `inputs/<c>/posts.json` exists (and ideally `posts` non-empty) |
| prepared | optional: `posts_coref.json` or `posts_trimmed.json` (only if you track prep) |
| claims_extracted | `claims.json` exists |
| filtered | `claims_filtered.json` exists (when feature lands) |
| grouped | `groups.json` exists |
| embedded | active run has `vectors.npy` + `index.json` + `metrics.json` |
| clustered | at least one experiment dir under `experiments/<c>__<active_tag>/` (or a named “latest”) |

`corpus status` should print this checklist + the active model tag—not a single exclusive status value.

**Later (staleness):** compare `source_hash` / mtimes so groups/runs can show “exists but stale vs claims.”

## Write rules

1. **Each stage writes a downstream artifact**; do not mutate `posts.json` into claims in place.
2. **Overwrite only with `--force`** on the same path.
3. **Canonical filenames** (`claims.json`, `groups.json`, …) so status stays dumb.
4. **Archives** (old prompts, old experiments) live beside the canonical path, not instead of it.
5. **Shared assets** (models, prompts, labels) stay outside the corpus folder.

## Stage → artifact (what each step adds)

| Stage | Primary output | Notes |
|-------|----------------|-------|
| seed / copy-posts | `posts.json` | Header should carry `terms` (+ ideally `since` / `until`); NOTES for narrative |
| prepare trim / coref | `posts_trimmed.json` / `posts_coref.json` | Or overwrite `posts.json` only with `--force` |
| extract | `claims.json` | Stream-write posts-with-claims; does not edit `posts.json` |
| filter | `claims_filtered.json` | Planned; filter model lives under `models/` or labeler output |
| group | `groups.json` | Includes `source_hash` of input claims file |
| embed | `runs/<c>__<tag>/` | `metrics.source_hash` ties back to groups |
| cluster / hierarchy | `experiments/<c>__<tag>/<exp>/` | New dir per experiment; labels optional `.npy` |

## Active pointers (no plugin registry)

CLI uses one canonical choice per function:

- Active embedder: registered tag under `models/<tag>/` (or path), referenced as `--model-tag` / defaults
- Active extract prompt: `prompts/` archive + `active` pointer
- Active hierarchy preset: e.g. `--preset default` in code/config
- Active filter model (later): path or tag only—no “this came from labeler” taxonomy in claims

Iteration apps (refinement, labeler, train) write **archives**; the main CLI only reads the active pointer.
