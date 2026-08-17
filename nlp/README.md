# `nlp/` — shared text prep + canonical claim extraction

Project-level helpers used by claims (and anything else) before / during claim extraction:

| Module | Role | Default deps |
|--------|------|----------------|
| [`trim.py`](trim.py) | Sentence-boundary spans + hit-window chunking (`syntok`) | root `requirements.txt` |
| [`punct.py`](punct.py) | Gated punctuation restore for low-punct text | [`requirements-punct.txt`](requirements-punct.txt) |
| [`coref.py`](coref.py) | Batch coreference rewrite (experimental; not in default prep) | [`requirements-coref.txt`](requirements-coref.txt) |
| [`claim_extraction/`](claim_extraction/) | Canonical prompts, defaults, prep, schemas, concurrent requester, OpenAI/Azure client factories | root `openai` + `python-dotenv` for LLM helpers |

```python
from nlp.trim import syntok_sentence_spans, trim_sentence_boundary
from nlp.punct import needs_punctuation, restore_punctuation, remap_hits_to_text
from nlp.coref import iter_coref_resolved_posts, process_payload
from nlp.claim_extraction import (
    MODEL_NAME,
    render_system,
    render_user,
    format_input_text,
    build_openai_claims_client,
    ConcurrentApiRequester,
)
from nlp.claim_extraction.prep import prepare_and_explode
from nlp.claim_extraction.nest import nest_posts_chunks_claims
```

Batch posts-JSON extract I/O is `nlp.claim_extraction.batch` (also used by `scripts.get_posts_extract_upload`). Default fetch→extract path uses Azure deployment `gpt-5.6-luna` and stores `claim_vaccine_alignment_score` on each claim. Prep helpers live under `nlp.claim_extraction.prep` / `nlp.trim` / `nlp.coref`. `apps.claims` is post-extract only (group → embed → annotate/select → cluster).

End-to-end fetch → punct → trim → extract → upload:

```bash
# How many posts would match (no files written):
python -m scripts.get_posts_extract_upload \
  --terms measles --since 2024-01-01 --until 2025-01-01 --prod --count-only

# Live smoke: 1 post → 1 chunk extract, print JSON (no --out):
python -m scripts.get_posts_extract_upload \
  --terms measles --since 2024-01-01 --until 2025-01-01 --prod --smoke

# Full run:
python -m scripts.get_posts_extract_upload \
  --terms measles --since 2024-01-01 --until 2025-01-01 \
  --prod --out measles_claims.json --upload
```

## Punct install

```bash
pip install -r nlp/requirements-punct.txt
```

Gate: length-aware — `punctuation_ratio` &lt; `NLP_PUNCT_RATIO_THRESHOLD` (default `0.004`),
or length ≥ `NLP_PUNCT_LONG_MIN_CHARS` (default `2000`) and ratio &lt; `NLP_PUNCT_LONG_RATIO_THRESHOLD`
(default `0.008`); and length ≥ `NLP_PUNCT_MIN_CHARS` (default `80`).

## Coref install

```bash
pip install -r nlp/requirements-coref.txt
python -m spacy download en_core_web_lg
```

Env knobs (see `coref.py`): `COREF_PIPE_BATCH_SIZE`, `COREF_MAX_CHARS`, `COREF_RESET_EVERY_BATCHES`, etc.

**Note:** Prep assessment (run_1) rejected default coref rewrite for claims; keep the module for experiments only until improved.

## Non-goals (for now)

- No batch “process all posts” service under `services/`
- No decision yet on persisting chunks vs calling these at runtime
- Corpus/file-mode I/O stays in `apps.claims` (not under `nlp/`)

## Experiments

Trim/punct/coref assessment: see [`experiments/`](experiments/README.md) (`python -m nlp.experiments.do_experiment_run --phases …`).
