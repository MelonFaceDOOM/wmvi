# Event extraction experiment

File-mode experiment: one LLM call per post-chunk from `claims.json`. Mentions
are independent (no catalog matching, no collapsing).
Not registered on `python -m apps.claims`.

```bash
python -m apps.claims.events.run --corpus measles2 --n 1000 --sample-seed 17
```

`--n` is how many **unlogged** chunks to sample (first run) or extract
(`--continue`). Already-logged `doc_id` values in `extract_log.jsonl` are
skipped. The first run writes a seeded, platform-stratified `documents.jsonl`
so the sample is replayable.

Progress goes to stderr. The last stdout line is a JSON object with `run_dir`
and counts. Default provider is personal OpenAI (`PERSONAL_OPENAI_API_KEY`).
`--llm-provider azure` uses `AZURE_OPENAI_*`.

`--estimate-only` samples (or reuses `documents.jsonl`) and prints how many
prompts `--n` would make, without calling the LLM.

LLM calls run in parallel through `nlp.claim_extraction.ConcurrentApiRequester`
(`--max-workers`, default 24). Pre-pacing is off (`CLAIMS_TARGET_RPM=0`) so
sends go as fast as workers allow; 429s still apply a global cooldown. Failed
calls are not logged as done, so `--continue` retries them.

Comment thread titles are stored as metadata and are **not** sent to the model.
Submission / video titles and native channels (telegram, podcast name) are.

Types: `outbreak`, `notable_case`, `cases` (infections), `vaccine_injury`
(public vaccine-attributed injury or death), `institutional_action`,
`study_publication`, `other`. Empty for personal shots/titers/coverage, journalism, FOIA requests,
patent filings, and historical setup. `role_in_post` is `focal` or `mentioned`
(stored for later analysis; not used to drop events). Finest place
including county; at most 4 events per chunk. `date_relation` is inferred
from stated dates.

Possible prompt changes (not applied until after grouping experiments):
[`prompt_improvements.md`](prompt_improvements.md).

Frozen 57-chunk set for repeated prompt tries:
[`prompt_sample/README.md`](prompt_sample/README.md).

## Inputs

- `apps/claims/data/corpora/<corpus>/claims.json` posts → chunks with non-empty text
  (claim extraction success is not required)

## Outputs (`apps/claims/events/runs/run_N/`)

| File | Use |
|---|---|
| `documents.jsonl` | Sampled chunks (prompt payload + metadata) |
| `mentions.jsonl` | One row per extracted mention |
| `extract_log.jsonl` | one LLM result per chunk (resume) |
| `browse.json` | posts → mentions with quotes |
| `metrics.json` | empty rate, type/place/date histograms, quote-drop |

## Mention merge attempts

Independent of extract `run_N`. Pointer + clustering only; no catalog writes.

```bash
python -m apps.claims.events.merge.run \
    --mentions apps/claims/events/runs/run_11/mentions.jsonl
```

Writes `apps/claims/events/merges/merge_N/` (`config.json` is the algo,
`metrics.json` / stdout summarize the output). Default strategy
`filter_place_key_v2`: drop vague unplaced waves, alias-clean place, titles
off for outbreak/cases. Date is not a grouping key.

`--strategy high_precision_v3` (merge_3) also drops country-only waves on
large countries (`national_status`), generic national institutions, leftover
`other`, and unanchored notable cases. See `merges/merge_3/assessment.md`.

Each merge also writes `browse.json` + `browse.html`: kept events with
year/place **children** and member cards, plus **include-candidate** buckets
(the dropped rows we would consider bringing back). Open `browse.html` in a
browser. Rebuild without re-clustering:

```bash
python -m apps.claims.events.merge.browse \
    --merge-dir apps/claims/events/merges/merge_3
```
