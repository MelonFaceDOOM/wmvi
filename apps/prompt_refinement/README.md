# Prompt Lab (`apps/prompt_refinement`)

Streamlit UI for iterating claims-only extraction prompts on a curated eval set.

## Run

From the repository root:

```bash
python -m apps.prompt_refinement
```

CLI helpers (no Streamlit):

```bash
python -m apps.prompt_refinement export-pool --help
python -m apps.prompt_refinement write-sample --help
python -m apps.prompt_refinement import-sample --help
```

## Environment

| Variable | Role |
|----------|------|
| `AZURE_OPENAI_KEY` | Required for connectivity check, extract runs, and optimize |
| `AZURE_OPENAI_ENDPOINT` | Required Azure OpenAI endpoint URL |
| `AZURE_OPENAI_API_VERSION` | Optional (default `2024-08-01-preview`) |
| `CLAIMS_TARGET_RPM` | Throttle (default 90) |
| `CLAIMS_429_COOLDOWN_S` | Rate-limit cooldown seconds (default 20) |

Shared LLM helpers come from `nlp.claim_extraction` (not the claims CLI package).

## Data (gitignored)

Root `.gitignore` ignores any `data/` path, including:

- Lab SQLite: `apps/prompt_refinement/data/refinement.sqlite`
- Eval samples: `apps/prompt_refinement/data/samples/` (`pool.json`, `eval30.json`)

Do **not** name samples `*_claims.json` (also ignored by a separate rule). Prefer `eval30.json`.

## Prompts (in git)

| Role | Path |
|------|------|
| Current (“old”) | `nlp/claim_extraction/prompts/extract_system.txt` + `extract_user.txt` |
| Next (“new”) | `nlp/claim_extraction/prompts/candidates/next_system.txt` + `next_user.txt` |

Same `{{text_input}}` / `{{max_claims}}` contract. Profiles → **Load current…** / **Load next…** converts to lab `{var}` placeholders.

## Eval loop (30 chunks, 2× current vs 2× next)

### 1) Export pool + agentic pick (laptop; sample stays local)

```bash
python -m apps.prompt_refinement export-pool \
  --corpus measles_bal \
  --out apps/prompt_refinement/data/samples/pool.json \
  --human
```

**Agent goals:** select **30 chunks** with:

- some where extraction looks good
- some with ≥1 claim where standalone pred is low (`has_standalone_0`; binary ≈ score `< 0.5`, since `standalone_pred_m1` is continuous)
- platform variety (reddit_submission / reddit_comment / telegram / youtube / podcast as available)

Agent should sample batches of ~15 from the pool (task_id, platform, flags, text, claims) — not dump the whole pool. Write chosen ids to a file, then:

```bash
python -m apps.prompt_refinement write-sample \
  --from-ids /tmp/eval30_ids.txt \
  --pool apps/prompt_refinement/data/samples/pool.json \
  --out apps/prompt_refinement/data/samples/eval30.json \
  --human
```

Stop; do not import/run extract unless asked.

### 2) Ship sample via nitwitch

```bash
cd /home/melon/wmvi
zip -r /tmp/prompt_eval30.zip apps/prompt_refinement/data/samples/eval30.json
set -a && source .env && set +a
BASE="${NITWITCH_UPLOAD_URL%/}/"
curl -u "${NITWITCH_UPLOAD_USER}:${NITWITCH_UPLOAD_PASSWORD}" \
  -H 'Content-Type: application/octet-stream' \
  -T /tmp/prompt_eval30.zip \
  "${BASE}prompt_eval30.zip"
```

### 3) Dest machine (after `git pull`)

Edit `nlp/claim_extraction/prompts/candidates/next_{system,user}.txt`, then:

```bash
mkdir -p apps/prompt_refinement/data/samples
curl -L -o /tmp/prompt_eval30.zip 'https://nitwitch.com/dl/uploads/prompt_eval30.zip'
unzip -o /tmp/prompt_eval30.zip
pip install -r apps/prompt_refinement/requirements.txt
python -m apps.prompt_refinement import-sample \
  --from apps/prompt_refinement/data/samples/eval30.json --clear --human
python -m apps.prompt_refinement
```

In the UI:

1. Create profiles `current` and `next` (or Duplicate).
2. **Load current from nlp prompts** / **Load next from candidates/**.
3. Run each profile with run label **`1`**, then again with **`2`** (four snapshots).
4. Browse → multi-select all four snapshots → judge side-by-side.
