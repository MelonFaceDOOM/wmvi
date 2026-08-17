# Claims pipeline CLI

Post-extract file-mode pipeline: **group → label/annotate → embed → filter → cluster**.

Entry point (repo root):

```bash
python -m apps.claims <command> ...
```

**Input:** nested `claims.json` (posts→chunks→claims). Import with `corpus import-claims`.
Extraction lives outside this package: `scripts/get_posts_extract_upload.py`.
Prompt iteration UI (separate app): [`apps/prompt_refinement/`](../prompt_refinement/).

Join key: `claim_key` (stable hash of normalized claim text). Do not use `group_id` as a join key.
Derived scores go in annotations / selections — do not mutate `claims.json` / `groups.json` in place.

---

## Layout (`apps/claims/data/`)

```
corpora/<corpus>/
  claims.json, groups.json, NOTES.md
  annotations/<name>.jsonl + <name>.meta.json   # promoted scores (k/v)
  selections/<name>.json                        # optional named key sets

training/labelers/<intent>/
  spec.json
  labels.jsonl                                  # training log (may include blind probes)
  gold/<corpus>.jsonl                           # human-only eval yardstick
  runs/<run_id>.json                            # probe ledgers for labeling runs
  datasets/<version>/                           # frozen train-only manifests

training/embedders/<intent>/
  spec.json, triplets.jsonl, datasets/<version>/

models/{labelers,embedders}/<intent>/<version>/ # immutable artifacts + active.json aliases
models/registered/<tag>/                        # short embedder tags

runs/<corpus>/<tag>/                            # vectors.npy, index.json, metrics.json
experiments/clustering/...                      # cluster / hierarchy outputs
experiments/model_eval/...                      # candidate eval (not promoted annotations)
fixtures/                                       # eval queries, discovery logs
```

**Dependencies**

| Tool | Needs |
|------|-------|
| `labeler sample` / train / apply | `groups.json` (no embed run); sample requires gold gate |
| `browse` | `groups.json` (generic sampling; not for labeling probes) |
| `neighbors` / triplet discovery | embed run |
| `cluster` / `hierarchy` | embed run (+ optional `--filter`) |

```
claims.json → group → groups.json
                ├─ gold-sample / gold-add
                ├─ labeler sample → labels-add → freeze → train → apply
                └─ embed → runs/... → neighbors / cluster
```

---

## Typical workflows

### New corpus → group → embed → hierarchy

```bash
python -m apps.claims doctor --skip-model

python -m apps.claims corpus import-claims --name measles --create --force \
  --from data/measles_1.json
python -m apps.claims validate --corpus measles --human
python -m apps.claims group --corpus measles

python -m apps.claims embed --corpus measles --model bge-large --model-tag bge-large
python -m apps.claims hierarchy --corpus measles --model-tag bge-large \
  --preset default --save-labels
```

### Reddit-balanced derived corpus

Downsample Reddit posts so Reddit **claim** count ≈ non-Reddit claims (keeps all telegram/youtube/podcast posts):

```bash
python -m apps.claims corpus derive --from measles --name measles_bal --seed 0 --group
python -m apps.claims corpus derive --from resp --name resp_bal --seed 0 --group
```

`--target-ratio 1.0` (default) means reddit claims ≈ other claims. Sampling is post-level with a fixed seed; provenance is written under `claims.json` → `derived`.

### Labeler: human gold → agentic train → freeze → apply

```bash
CORPUS=measles
INTENT=epi_value

# 1) Build human gold (interactive loop; or gold-sample + gold-add)
python -m apps.claims labeler gold-label --intent $INTENT --corpus $CORPUS --n 50
python -m apps.claims labeler gold-status --intent $INTENT --corpus $CORPUS

# 2) Agentic training labels (blind probes injected)
python -m apps.claims labeler sample --intent $INTENT --corpus $CORPUS \
  --n 50 --run-size 1000 --seed 1 --human
# … labels-add for each claim; reuse --run-id across batches …
python -m apps.claims labeler agent-eval --intent $INTENT --corpus $CORPUS

# 3) Freeze (excludes gold keys) → train → apply
python -m apps.claims labeler dataset-freeze --intent $INTENT --version v3
python -m apps.claims labeler train --intent $INTENT --dataset v3 --version m3 --set-active
python -m apps.claims labeler apply --corpus $CORPUS --model $INTENT@active --name epi_value_pred_m3
python -m apps.claims labeler eval --intent $INTENT --model $INTENT@active --corpus $CORPUS
python -m apps.claims labeler annotation-eval --corpus $CORPUS --name epi_value_pred_m3 --intent $INTENT
```

### Standalone-only gold / sample / apply

Prefer a **named selection** for gold and training runs: it freezes the key set.
`--filter` re-resolves against a live annotation that may be re-applied later.

```bash
# Persist standalone=1 keys once
python -m apps.claims select --corpus measles --annotation standalone_pred_m1 \
  --name standalone_ok --low 0.5

# Gold and agentic sample only within that subset (probes also filtered)
python -m apps.claims labeler gold-label --intent epi_value2 --corpus measles \
  --n 50 --selection standalone_ok
python -m apps.claims labeler sample --intent epi_value2 --corpus measles \
  --n 100 --selection standalone_ok

# Or ephemeral: --filter 'standalone_pred_m1:eq=1' (same flags on apply)
python -m apps.claims labeler apply --corpus measles --model epi_value2@active \
  --name epi_value2_standalone --selection standalone_ok
```

### Quality filter → embed → stance-split cluster

```bash
CORPUS=resp
TAG=bge-large

python -m apps.claims browse --corpus $CORPUS --sample 10 --seed 0 --human \
  --filter epi_value_pred_m2:low=0.5 \
  --filter stance:low=0.875,high=1

python -m apps.claims embed --corpus $CORPUS --model $TAG --model-tag $TAG \
  --filter epi_value_pred_m2:low=0.5

python -m apps.claims cluster --corpus $CORPUS --model-tag $TAG \
  --filter stance:high=0.33 \
  --algorithm kmeans --params-json '{"n_clusters":25}' --save-labels
```

### Neighbors / embedder training (needs a run)

```bash
# 0) Embed the pool you will label (base model)
python -m apps.claims embed --corpus measles_bal \
  --model BAAI/bge-large-en-v1.5 --model-tag bge-large \
  --selection standalone_ok

# 1) Similarity intent + human gold (gate)
python -m apps.claims embedder intent-create --name belief_sim \
  --instructions "…" --rubric "…" --min-gold-total 20 --neighbor-k 15
python -m apps.claims embedder gold-sample --intent belief_sim --corpus measles_bal \
  --model-tag bge-large --n 20 --human
# … gold-add / gold-import / gold-label …
python -m apps.claims embedder gold-status --intent belief_sim --corpus measles_bal

# 2) Agentic triplets (blind probes; reuse --run-id)
python -m apps.claims embedder sample --intent belief_sim --corpus measles_bal \
  --model-tag bge-large --n 20 --run-size 500 --seed 1 --human
# judge neighbors → jsonl of {claim_key, pos:[…], neg:[…], reason}
python -m apps.claims embedder triplets-import --intent belief_sim --corpus measles_bal \
  --from judged.jsonl --run-id <run_id>
python -m apps.claims embedder agent-eval --intent belief_sim --corpus measles_bal \
  --run-id <run_id> --human

# 3) Freeze → train both losses → promote winner → gold eval
python -m apps.claims embedder dataset-freeze --intent belief_sim --version v1
python -m apps.claims embedder train-compare --intent belief_sim --dataset v1 --version m1 \
  --base-model BAAI/bge-large-en-v1.5 --corpus measles_bal --set-active --human
python -m apps.claims embedder eval --intent belief_sim --model belief_sim@active \
  --corpus measles_bal --human
```

### Qwen3-Embedding-8B (GPU)

Requires CUDA torch + `pip install -r apps/claims/requirements-embed.txt`.
Do **not** overwrite an existing `bge-large` run — use a new tag.

Use the **same** `--doc-instruction` for embed, gold-label neighbors (via the run), and LoRA train.

```bash
DOC=$'Instruct: Retrieve claims that express the same underlying proposition or narrative.\nQuery:'

python -m apps.claims embed --corpus measles_bal \
  --model Qwen/Qwen3-Embedding-8B --model-tag qwen3-emb-8b \
  --selection standalone_ok \
  --doc-instruction "$DOC" \
  --batch-size 16 --max-seq-length 512

python -m apps.claims embedder gold-label --intent belief_sim \
  --corpus measles_bal --model-tag qwen3-emb-8b --selection standalone_ok --n 20

# After freeze: LoRA fine-tune (full FT of 8B will OOM on 32GB)
python -m apps.claims embedder train-compare --intent belief_sim --dataset v1 --version m1 \
  --base-model Qwen/Qwen3-Embedding-8B --corpus measles_bal \
  --lora --batch-size 4 --learning-rate 1e-4 \
  --doc-instruction "$DOC" --max-seq-length 512 \
  --set-active --human
```

Expect `metrics.json` `vector_dim=4096` and `device` containing `cuda`.

Ad-hoc neighbor browse (not for training labels):

```bash
python -m apps.claims neighbors --corpus measles --model-tag bge-large \
  --text "MMR causes autism" --top-k 15 --human
```

---

## Command index

| Command | Purpose |
|---------|---------|
| `corpus create/list/status/import-claims` | Manage corpora |
| `corpus derive` | Reddit-deweighted derived corpus (claim-balanced) |
| `corpus seed` / `copy-posts` | Optional raw `posts.json` |
| `model register/list/resolve` | Embedder tags |
| `validate` / `group` / `embed` | Claims QA → groups → vectors |
| `labeler` | Intent / gold / sample / labels / freeze / train / eval / apply |
| `annotations` / `select` / `selections` | Sidecars and named key sets |
| `browse` | Generic claim sampling (filters OK; not for probe labeling) |
| `neighbors` | Nearest neighbors (needs run) |
| `cluster` / `sweep` / `hierarchy` / `inspect` | Clustering |
| `embedder` | Similarity intent / gold / sample / triplets / freeze / train-compare / eval |
| `runs` / `prep-queries` / `doctor` / `ls-artifacts` | Runs + sanity checks |

```bash
python -m apps.claims --help
python -m apps.claims labeler --help
```
