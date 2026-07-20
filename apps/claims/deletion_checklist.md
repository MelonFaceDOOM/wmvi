# Deletion checklist: `apps/claim_extractor/`

Use this before removing `apps/claim_extractor/`. The file-mode successor is `apps/claims/` (`python -m apps.claims`).

## 1. Hard deps cleared

From repo root:

```bash
rg 'apps\.claim_extractor' --glob '!apps/claim_extractor/**' -g '!*.md'
```

**Cleared by this port (must be empty under `apps/claims/`):**

```bash
rg 'apps\.claim_extractor' apps/claims
```

**Shared text-prep (trim / coref) already lives in [`nlp/`](../../nlp/README.md).**  
`claim_extractor/trim_transcripts.py` and `coreference_resolution.py` are thin re-export shims; safe to delete with the package.

**Known external hits that remain until labs/Ridge are archived or tests rewritten:**

| Path | Status |
|------|--------|
| `tests/test_get_claims.py` / `tests/test_get_claims_claims_only.py` | Still points at claim_extractor; rewrite to `apps.claims.extraction` or drop when deleting |
| `tests/test_score_claims.py` | Ridge/score path — **abandoned**; delete with package |
| `tests/test_refinement_lab.py` | Streamlit refinement lab — **abandoned** |
| `tests/test_claim_dedup.py` | Labeler/normalize — rewrite or drop |
| `tests/test_resolve_enum_choice.py` | `model_common` helper — move or drop |
| `scripts/oneoffs/embedding_lab_transfer.py` | SQLite transfer — superseded by `claims runs export/import` |
| Docs under `apps/claim_extractor/demo/` | Historical; remove with tree |

## 2. Functional validation (Phases A–E)

Run from repo root with a real corpus (or smoke fixtures):

| Phase | Check |
|-------|--------|
| **A. Doctor** | `python -m apps.claims doctor --skip-model` |
| **B. Corpus** | `corpus create/list/status`, `copy-posts` or `seed`, optional `prepare trim` → `prepare coref` |
| **C. Extract** | `extract --corpus …` then `validate --corpus …` |
| **D. Group + embed** | `group`, `embed --model … --model-tag …` |
| **E. Hierarchy / inspect / triplets** | `hierarchy --preset default`, `inspect`, optional `train` / `eval-triplets` / `discover-triplets` |

Also: `runs export` / `runs import` round-trip on a small run dir.

## 3. Artifacts to relocate before delete

Copy anything you still need out of claim_extractor paths:

- Trained SentenceTransformer dirs / HF caches referenced by labs
- Triplet JSON / label exports under `embedding_lab/data/` or `labeler_lab`
- Valued experiment artifacts (`.cluster_opt/`, hierarchy inspect dumps)
- SQLite DBs only if you still need historical run metadata (file-mode uses zip + run dirs instead)

Register keepers under `apps/claims/data/models/` and `apps/claims/data/labels/`.

## 4. Explicitly abandoned

Do **not** port these unless a later product need appears:

- Streamlit **embedding_lab**, **labeler_lab**, **refinement_lab** UIs
- Ridge / `score_claims` / learned train heads for claim scoring
- SQLite-bound `embedding_lab/transfer.py` (replaced by `runs export|import`)

## 5. Final delete steps

1. Confirm `rg apps.claim_extractor apps/claims` is empty.
2. Relocate artifacts (section 3).
3. Update or delete leftover `tests/` and `scripts/` that import claim_extractor.
4. Remove or archive `apps/claim_extractor/` (git rm or tar + delete).
5. Grep CI / `requirements*.txt` / READMEs for `claim_extractor` and update.
6. Smoke `python -m apps.claims doctor --skip-model` and one corpus status.
