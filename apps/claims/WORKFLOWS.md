# Claims CLI workflows

Entry point (from repo root):

```bash
python -m apps.claims <command> ...
```

Artifacts live under `apps/claims/data/`:

| Path | Role |
|------|------|
| `inputs/<corpus>/` | `posts.json`, `claims.json`, `groups.json`, `NOTES.md` |
| `models/<tag>/` | Registered embedders |
| `labels/` | Triplets, eval queries, unusable log |
| `runs/<corpus>__<tag>/` | `vectors.npy`, `index.json`, `metrics.json` |
| `experiments/<corpus>__<tag>/<exp>/` | Cluster / hierarchy / inspect outputs |

---

## 0. One-time setup

```bash
# Self-check deps (no model download)
python -m apps.claims doctor --skip-model

# Register a trained (or HF-local) embedder under a short tag
python -m apps.claims model register \
  --path /path/to/bge-large-500trips \
  --tag bge-large

python -m apps.claims model list
python -m apps.claims ls-artifacts
```

---

## 1. New corpus from DB (when you have DB access)

On a machine with Postgres (often not the same box as the claims CLI):

```bash
# Fetch posts and optionally PUT to nitwitch (needs NITWITCH_UPLOAD_* in .env)
python -m scripts.get_posts_for_search_term \
  --terms measles "mmr vaccine" "mmr autism" \
  --since 2024-01-01 --until 2025-01-01 \
  --prod --out measles_posts.json --upload

# Browse/download manually: https://nitwitch.com/dl/uploads/
# Then on the claims machine:
python -m apps.claims corpus copy-posts --name measles --create \
  --from measles_posts.json
```

Or seed directly from DB on a machine that has both DB and claims CLI:

```bash
python -m apps.claims corpus seed --name measles --create \
  --terms measles "mmr vaccine" "mmr autism" \
  --since 2024-01-01 --until 2025-01-01 \
  --prod

# Validate args only (no DB):
python -m apps.claims corpus seed --name covid --create \
  --terms covid --since 2020-01-01 --until 2021-01-01 --dry-run
```

`corpus seed` writes `data/inputs/<name>/posts.json` and appends to `NOTES.md`.
Upload env vars: `NITWITCH_UPLOAD_URL`, `NITWITCH_UPLOAD_USER`, `NITWITCH_UPLOAD_PASSWORD`.

---

## 2. New corpus from an existing posts JSON (no DB)

```bash
python -m apps.claims corpus copy-posts --name measles --create \
  --from data/posts_for_term.json

# Overwrite if posts.json already exists:
python -m apps.claims corpus copy-posts --name measles \
  --from /elsewhere/posts.json --force

python -m apps.claims corpus status --name measles
python -m apps.claims corpus list
```

---

## 2b. Prepare posts (trim → coref → extract)

Optional stages before extraction (when posts have term `hits`).

Implementations live in project-level [`nlp/`](../../nlp/README.md) (`nlp.trim`, `nlp.coref`);
the claims CLI only wraps posts JSON I/O.

```bash
CORPUS=measles

# Sentence windows around hits → posts_trimmed.json
python -m apps.claims prepare trim --corpus $CORPUS

# Coreference (heavy deps; see nlp/requirements-coref.txt) → posts_coref.json
python -m apps.claims prepare coref --posts apps/claims/data/inputs/$CORPUS/posts_trimmed.json \
  --out apps/claims/data/inputs/$CORPUS/posts_coref.json

# Then extract from the prepared file:
python -m apps.claims extract --corpus $CORPUS \
  --posts apps/claims/data/inputs/$CORPUS/posts_coref.json

# QA summary of claims.json
python -m apps.claims validate --corpus $CORPUS --human
```

Typical order: fetch → trim → coref → extract → group → embed.

---

## 3. Full pipeline: extract → group → embed → hierarchy

```bash
CORPUS=measles
TAG=bge-large

# Claims extraction (Azure/OpenAI; network)
python -m apps.claims extract --corpus $CORPUS
# Optional: cap posts while iterating
# python -m apps.claims extract --corpus $CORPUS --n-posts 50 --claims-only

# Collapse duplicate claim texts
python -m apps.claims group --corpus $CORPUS

# Embed (progress on stderr; refuses overwrite without --force)
python -m apps.claims embed --corpus $CORPUS --model $TAG --model-tag $TAG
# Smoke on first N groups:
# python -m apps.claims embed --corpus $CORPUS --model $TAG --model-tag $TAG --limit 200 --force

# Two-level clustering (known-good preset: kmeans-800 → agglo-25)
python -m apps.claims hierarchy --corpus $CORPUS --model-tag $TAG \
  --preset default --save-labels

python -m apps.claims corpus status --name $CORPUS
python -m apps.claims runs list --corpus $CORPUS

# Optional: zip a run for another machine
python -m apps.claims runs export --corpus $CORPUS --model-tag $TAG --out measles_bge.zip
# python -m apps.claims runs import --from measles_bge.zip --run-name measles__bge-large
```

Run dir: `data/runs/measles__bge-large/`  
Experiments: `data/experiments/measles__bge-large/hierarchy_default_<stamp>/`

---

## 4. Inspect clusters

After hierarchy with `--save-labels`, use the printed `leaf_labels_path` / `narrative_labels_path`:

```bash
CORPUS=measles
TAG=bge-large
EXP=apps/claims/data/experiments/measles__bge-large/hierarchy_default_XXXXXXXXXX

python -m apps.claims inspect \
  --corpus $CORPUS --model-tag $TAG \
  --labels $EXP/leaf_labels_XXXXXXXXXX.npy \
  --parent-labels $EXP/narrative_labels_XXXXXXXXXX.npy \
  --mode mixed --n-clusters 8 --n-per-cluster 5
```

Modes: `largest`, `loosest`, `tightest`, `mixed`, `noise`, `query`.

---

## 5. Flat cluster / sweep (tuning)

```bash
CORPUS=measles
TAG=bge-large

# One config
python -m apps.claims cluster --corpus $CORPUS --model-tag $TAG \
  --algorithm kmeans \
  --params-json '{"n_clusters":800,"reduce":"none"}' \
  --save-labels

# Many configs (JSON array of {algorithm, params, seed?})
python -m apps.claims sweep --corpus $CORPUS --model-tag $TAG \
  --configs /path/to/configs.json
```

Cache eval-query vectors once per model (optional; cluster/hierarchy also auto-cache):

```bash
python -m apps.claims prep-queries \
  --model bge-large \
  --out-dir apps/claims/data/experiments/query_cache
```

---

## 6. Triplet train / eval / discover

```bash
# Fine-tune → saved under data/models/<output-name>/
python -m apps.claims train \
  --triplets apps/claims/data/labels/triplets.json \
  --base-model BAAI/bge-large-en-v1.5 \
  --output-name bge-large-500trips \
  --epochs 3

python -m apps.claims model register \
  --path apps/claims/data/models/bge-large-500trips \
  --tag bge-large --force

# Score eval-pool anchors against a run's model id
python -m apps.claims eval-triplets \
  --corpus measles --model-tag bge-large \
  --triplets apps/claims/data/labels/triplets.json \
  --pool eval

# LLM discovery (network); merges into --out
python -m apps.claims discover-triplets \
  --corpus measles --model-tag bge-large \
  --model gpt-4o-mini \
  --out apps/claims/data/labels/triplets.json \
  --n-claims 20
```

---

## 7. Second corpus (same model)

```bash
python -m apps.claims corpus seed --name covid --create \
  --terms covid "covid vaccine" --since 2020-01-01 --until 2022-01-01 --prod

python -m apps.claims extract --corpus covid
python -m apps.claims group --corpus covid
python -m apps.claims embed --corpus covid --model bge-large --model-tag bge-large
python -m apps.claims hierarchy --corpus covid --model-tag bge-large --preset default --save-labels
```

Compare with `runs list` / `corpus status` per slug. Shared model tag keeps geometry comparable.

---

## Explicit paths (bypass `--corpus`)

```bash
python -m apps.claims group \
  --claims /tmp/claims.json --out /tmp/groups.json

python -m apps.claims embed \
  --groups /tmp/groups.json \
  --model /path/to/model \
  --run-name scratch_test \
  --force

python -m apps.claims hierarchy \
  --run-dir apps/claims/data/runs/scratch_test \
  --preset default \
  --out-dir /tmp/hier --save-labels
```

---

## Command index

| Command | Purpose |
|---------|---------|
| `corpus create/list/status` | Manage corpora |
| `corpus seed` | DB → `posts.json` (terms + date range) |
| `corpus copy-posts` | File → `posts.json` (no DB) |
| `model register/list/resolve` | Short tags for embedders |
| `extract` | Posts → claims (network) |
| `validate` | Summarize claims extraction QA |
| `prepare trim` / `prepare coref` | Pre-extract stages |
| `group` | Claims → unique claim groups |
| `embed` | Groups → run vectors |
| `cluster` / `sweep` / `hierarchy` | Clustering |
| `inspect` | Sample claim texts from labels |
| `prep-queries` | Cache eval query vectors |
| `runs list` / `export` / `import` | List or zip transfer runs |
| `train` / `eval-triplets` / `discover-triplets` | Embedder training loop |
| `doctor` / `ls-artifacts` | Sanity checks |

Before removing `apps/claim_extractor/`, follow [deletion_checklist.md](deletion_checklist.md).
