# Prep experiments (trim + punct + optional coref)

Local input copy: [`posts_for_term_raw.json`](posts_for_term_raw.json).

## One-shot CLI

```bash
# New run (next run_N); default phases: punct,chunk,report (coref off)
python -m nlp.experiments.do_experiment_run

# Explicit phases
python -m nlp.experiments.do_experiment_run --phases punct,chunk,report
python -m nlp.experiments.do_experiment_run --phases punct,chunk,coref,report

# Resume latest run_N: only fill missing automated products
python -m nlp.experiments.do_experiment_run --continue

# Smoke
python -m nlp.experiments.do_experiment_run --limit 50 --phases punct,chunk,report

# Status
python -m nlp.experiments.do_experiment_run --status
```

`--skip-coref` remains as an alias that drops `coref` from the phase set.

Punct install (once): `pip install -r nlp/requirements-punct.txt`

Intent: change trim/punct/coref in `nlp/`, run an experiment, read products, write `assessment.md`.

## Phases

| Phase | Effect |
|-------|--------|
| `punct` | Length-aware punct gate + restore; keep original `text`, set `text_punct` / remapped hits |
| `chunk` | `trim_sentence_boundary` on working text → `trimmed_chunks` |
| `coref` | Optional; off by default |
| `report` | `summary_chunks.md`; coref browse/summary if coref fields present |

Prepare order: load → optional punct → chunk → optional coref → `posts_prepared.json`.

## Run products

```
runs/run_N/
  posts_prepared.json
  browse_coref_edits.html   # if coref ran
  summary_coref.md          # if coref ran
  summary_chunks.md
  assessment.md             # human/AI conclusions (not auto-written)
```

## Runs

- [`runs/run_1/`](runs/run_1/) — trim + coref; coref dropped from pipeline (see assessment).
- [`runs/run_2/`](runs/run_2/) — punct + chunk + report (no coref); widened windows + far-hit split.
- [`runs/run_3/`](runs/run_3/) — length-aware punct gate + even-split `CHUNK_CHAR_LIMIT` trim.
