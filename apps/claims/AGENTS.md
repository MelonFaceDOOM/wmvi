# Claims app — agent notes

File-mode claims pipeline under `apps/claims`. Canonical labeler path:

```text
intent-show → labeler sample → labels-add → (repeat) → agent-eval
→ (human) gold-sample / gold-add → dataset-freeze → train → labeler apply
→ annotation-eval / labeler eval
```

Canonical **embedder / triplet** path (requires an embed run on the base model first):

```text
embed → embedder intent-show
→ (human) gold-sample / gold-add until gate_ok
→ embedder sample → judge neighbors → triplets-import  (repeat, same run_id)
→ agent-eval --human
→ dataset-freeze → train-compare --set-active
→ embedder eval --human
```

See [README.md](README.md) for layout and typical workflows.

## Agentic labeling loop (required)

When creating **training labels** for any intent:

1. **Load the rubric first**
   ```bash
   python -m apps.claims labeler intent-show --name <intent>
   ```
   Follow `spec.instructions` exactly (allowed values, decision order, examples).
   If the spec has `agent_batch_size` / `agent_model`, honor them for every labeling turn
   (batch cap and Cursor Task `model=` slug).

2. **Sample claims via intent-aware sampler** (injects blind gold probes; requires gold gate)
   ```bash
   python -m apps.claims labeler sample --intent <intent> --corpus <corpus> \
     --n N --run-size <full_run_n> --seed <seed> --human
   ```
   Echoed `run_id` should be reused across batches of the same labeling run.
   To restrict to a subpopulation (e.g. standalone claims), pass `--selection <name>`
   or `--filter 'ann:eq=1'` — ordinary draws and blind probes are both restricted.
   Prefer a named selection (frozen keys) over a live `--filter` for gold/training runs.
   Do **not** use `browse --exclude …/labels.jsonl` for labeling — that bypasses probe injection.

3. **Batch size and judgment quality**
   - Never label more than `spec.agent_batch_size` claims in one subagent turn
     (default **200** when set on the intent; otherwise prefer ≤50).
   - Every label must have a **unique, claim-specific** reason that refers to that claim's
     content. Reject generic bucket templates (e.g. five canned stems reused for every row)
     and VALUES-only dumps with no per-claim rationale.
   - Pin Cursor Task `model` to `spec.agent_model` when present
     (e.g. `cursor-grok-4.6-high-fast`). Do not use a more expensive model than the spec.

4. **Write labels via CLI only**
   ```bash
   python -m apps.claims labeler labels-add --intent <intent> \
     --text "…" --value <allowed> \
     --corpus <corpus> --claim-key <key> \
     --producer-type agent_label \
     --reason "<short claim-specific why>"
   ```
   Or batch with `labeler labels-import` from a curated jsonl of **already-judged**
   `(claim_key, value, reason)` triples for that batch only (≤ `agent_batch_size`).
   Pass `--probe-run-id` when known; otherwise attribution is inferred from the run ledger.

5. **Then stop and wait** for the user before freeze/train unless they explicitly asked to freeze/train.

6. After a run, user may call `labeler agent-eval --intent … --corpus … [--run-id …] [--human]`.

## Agentic embedder / triplet loop (required)

When creating **similarity training triplets**:

1. **Prerequisite:** an embed run exists for the corpus + base model
   (`python -m apps.claims embed --corpus … --model … --model-tag … [--selection …]`).
   For Qwen3-Embedding-8B (GPU): install `apps/claims/requirements-embed.txt`, use
   `--dtype auto` (bf16 on CUDA), `--max-seq-length 512`, and a `--doc-instruction`
   prompt; fine-tune later with `train-compare --lora` (full FT OOMs on 32GB).
   See README “Qwen3-Embedding-8B (GPU)”.

2. **Load the similarity rubric**
   ```bash
   python -m apps.claims embedder intent-show --name <intent>
   ```
   Honor `agent_batch_size` (default **20** — each item is an anchor plus neighbors),
   `agent_model`, and `neighbor_k`.

3. **Human gold first** (gate required before agentic sample)
   ```bash
   python -m apps.claims embedder gold-sample --intent <intent> --corpus <corpus> \
     --model-tag <tag> --n 20 --human
   # then gold-add / gold-import / gold-label
   python -m apps.claims embedder gold-status --intent <intent> --corpus <corpus>
   ```
   Gold lives under `training/embedders/<intent>/gold/<corpus>.jsonl`.
   **Do not read gold/** during agentic labeling.

4. **Sample anchors with numbered neighbors** (injects blind gold probes)
   ```bash
   python -m apps.claims embedder sample --intent <intent> --corpus <corpus> \
     --model-tag <tag> --n N --run-size <full_run_n> --seed <seed> --human
   ```
   Reuse echoed `run_id`. Prefer `--selection` over live `--filter`.
   Parent agents should pre-sample non-overlapping batches when running parallel subagents
   (concurrent `sample` calls race on the unlabeled pool).

5. **Judge in one pass** per anchor: list true-positive neighbor indices and true-negative
   indices (either side may be empty / nothing). Unmarked neighbors are unused, not silent
   negatives. Import judged jsonl only:
   ```bash
   python -m apps.claims embedder triplets-import --intent <intent> --corpus <corpus> \
     --from judged.jsonl --run-id <run_id> --sample sample.json
   ```
   Each row: `{"claim_key":"…","pos":[1,3],"neg":[8,11],"reason":"claim-specific why"}`.
   Reasons must be unique per batch. Never use `--auto-split` / heuristic neighbor splits.

6. Stop before freeze/train unless the user asked. Then:
   ```bash
   python -m apps.claims embedder agent-eval --intent … --corpus … --run-id … --human
   python -m apps.claims embedder dataset-freeze --intent … --version v1
   python -m apps.claims embedder train-compare --intent … --dataset v1 --version m1 \
     --base-model … --corpus … --set-active --human
   python -m apps.claims embedder eval --intent … --model <intent>@active --corpus … --human
   ```

## Gold labels (human-only)

- Labeler gold: `training/labelers/<intent>/gold/<corpus>.jsonl`.
- Embedder gold: `training/embedders/<intent>/gold/<corpus>.jsonl`.
- **Do not read or write the gold directory** during agentic labeling. Blind probes must stay blind.
- Gold is created only via `gold-label` (interactive), or `gold-sample` + `gold-add` / `gold-import`, with `producer.type=human`.
  ```bash
  python -m apps.claims labeler gold-label --intent epi_value --corpus measles --n 50
  # Subpopulation (preferred: named selection freezes keys):
  python -m apps.claims select --corpus measles --annotation standalone_pred_m1 \
    --name standalone_ok --low 0.5
  python -m apps.claims labeler gold-label --intent epi_value2 --corpus measles \
    --n 50 --selection standalone_ok
  ```

## Forbidden

- Regex / heuristic / keyword bulk labeling of training data
- VALUES-only dumps with canned/template reasons for every row
- Using `labeler apply` (or any model prediction) as gold or training labels
- Inventing label values outside the intent’s allowed set
- Skipping `intent-show` when the rubric matters for the batch
- Reading `gold/` or run ledgers to discover which sample claims are probes
- Labeling more than `agent_batch_size` claims in one subagent turn when the spec sets it
- Embedder `--auto-split` / heuristic neighbor pos/neg assignment for training data

## Apply vs train

- **`labeler apply`**: scores a corpus (or `--filter`/`--selection` subset) → annotation sidecar (inference).
- **`labels-add` / `labels-import`**: training log rows (may include blind probes).
- **`gold-add` / `gold-import`**: human eval yardstick only.
- **`dataset-freeze`**: train-only; excludes all gold claim_keys / gold anchors.
- **`embedder train-compare`**: trains MNRL and TripletLoss; promotes the better gold pairwise winner.

## Prompt Lab eval sample (agentic)

When curating a prompt-refinement eval set (see [`apps/prompt_refinement/README.md`](../prompt_refinement/README.md)):

1. `python -m apps.prompt_refinement export-pool --corpus measles_bal --out apps/prompt_refinement/data/samples/pool.json --human`
2. Sample batches of ~15 chunks from the pool; pick **30** total mixing (a) good-looking extracts, (b) `has_standalone_0`, (c) platform variety.
3. `write-sample --from-ids … --pool … --out apps/prompt_refinement/data/samples/eval30.json`
4. Stop — do not import/run extract unless asked. Sample path is gitignored (`data/`).

## NLI

Experimental NLI uses its own placeholder scorer under `apps/claims/nli/`. Do not confuse that with labeler training heuristics; leave NLI alone unless the task is about NLI.
