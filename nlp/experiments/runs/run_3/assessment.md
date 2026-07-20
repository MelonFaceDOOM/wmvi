# Assessment — run_3

Prepared from `posts_for_term_raw.json` (~18.7k posts) with `--phases punct,chunk,report` (no coref). Products: `posts_prepared.json`, `summary_chunks.md`, `run_meta.json`.

## Major changes since run_2

### Punct adder

| Change | Detail |
|--------|--------|
| Length-aware gate | Still restore when `ratio < 0.004` and `len ≥ 80`; **also** when `len ≥ 2000` and `ratio < 0.008` (env: `NLP_PUNCT_LONG_*`) |
| Long-text restore | Upstream model asserts on clipped HF batches; restore now word-slices (~180 words) with try/except fallback to original text |
| Rationale | Flat threshold raises (0.008/0.01) mostly hit short posts; length-aware bump adds ~13 large lightly-punctuated bodies with almost no volume growth |

### Chunker (landed between run_2 and run_3)

| Change | Detail |
|--------|--------|
| `CHUNK_CHAR_LIMIT = 4000` | Replaces `MAX_TRIMMED_CHARS` / `MAX_CONTEXT_CHARS` + overlap sliding windows |
| Even split | Oversized merged spans → `ceil(len/limit)` ~equal pieces; snap cuts to sentence ends (else whitespace / hard cut) |
| `MAX_SENTENCES` | Far-hit clustering / window shape only — no longer a length splitter |

## Results vs run_2

| Metric | run_2 | run_3 |
|--------|------:|------:|
| punct restored | 448 | **460** (+12 from length-aware bump; gate predicted 462, ~2 no-ops/failures) |
| chunk len p50 / p90 / p99 / max | 249 / 886 / 2338 / **12000** | 248 / 892 / 2673 / **3991** |
| chunks/post mean | 1.024 | 1.025 |
| `single_sentence_fallback` | 4142 | 4142 |
| `far_apart_still_one_chunk` | 23 | 46 |
| `near_*_char_limit` | 11 (at 12k) | 0 near 4k − 5 |

- **Char budget works:** max chunk ≈ 3991 (under 4000); no 12k monsters.
- **Punct bump is small:** +12 restores; `single_sentence_fallback` unchanged (those posts are mostly short/title-like, not the long lightly-punctuated set).
- **`far_apart_still_one_chunk` up:** expected — run_2 still sub-split big merges by `MAX_SENTENCES`; run_3 only splits when over `CHUNK_CHAR_LIMIT`, so a &lt;4k merge that spans &gt;2k of hit distance can stay one chunk. Far-hit **clustering** (gap ≥ 2000 between consecutive anchors) is unchanged.

## Verdict

**Ship this combo for claims prep:** length-aware punct + even-split trim at 4k, coref off.

- Punct: keep the length-aware defaults; further liberalism isn’t worth the short-post volume.
- Trim: 4k even-split is healthier than 12k hard-cap leftovers; distribution otherwise stable vs run_2.
- Optional follow-up: if extract quality suffers on multi-hit posts with hit span &gt;2k but consecutive gaps &lt;2k, tighten far-hit clustering (e.g. also split when first–last hit span &gt; `FAR_HIT_GAP_CHARS`).
