# Assessment — run_2

Prepared from `posts_for_term_raw.json` (~18.7k posts) with `--phases punct,chunk,report` (no coref). Products: `posts_prepared.json`, `summary_chunks.md`, `run_meta.json`.

## Major changes vs run_1

| Change | Detail |
|--------|--------|
| Punct gate | `nlp/punct.py`: restore via `deepmultilingualpunctuation` when punct ratio &lt; 0.004 and length ≥ 80; keep original `text`, set `text_punct` + remapped `hits_for_trim` |
| Trim windows | `SENTENCES_BEFORE/AFTER` 4→5; `MAX_SENTENCES` 16→20 |
| Far-hit split | Do not merge hit windows when anchor sentence gap &gt; `MAX_SENTENCES` or char gap ≥ 2000 |
| CLI phases | `do_experiment_run --phases punct,chunk,coref,report`; default `punct,chunk,report` |
| Coref | Skipped (run_1 verdict stands) |

## Chunk / punct results

From `run_meta.json` / `summary_chunks.md`:

- **Punct:** 448 posts restored (~2.4%); rest skipped (ratio OK or too short).
- **Lengths:** p50 ≈ 249 (≈ run_1 248); p90 ≈ 886 (was ~828); p99 ≈ 2338 (was ~1957). Mild widen as intended.
- **Chunks/post:** mean ≈ 1.02 (still mostly one chunk per post; far-apart posts are rare).
- **Quirks vs run_1:**

| Quirk | run_1 | run_2 | Notes |
|-------|------:|------:|-------|
| `single_sentence_fallback` | 4196 | 4142 | Small drop; many “≤1 sentence” bodies remain even after punct |
| `far_apart_hits` | 140 | 140 | Same posts; **`far_apart_still_one_chunk` = 23** (new) → most far-apart cases now split |
| `near_max_trimmed_chars` | 14 | 11 | Slightly better |
| `short_post_hit` | 4876 | 2521 | Still large; titles/tiny bodies expected (metric also tightened in this report) |

## Verdict

**Punct + widened trim + far-hit split is good enough to use for claims prep.** Coref stays out.

- Punct helps a small low-punctuation subset; it does not erase `single_sentence_fallback` (many posts are genuinely short or title-like).
- Far-hit force-split addresses the mega-window failure mode (117/140 far-apart posts now produce &gt;1 chunk).
- Context is a bit richer (p90/p99 up); hard caps remain rare.

**Follow-ups (optional):** tune punct threshold if more transcript-like text appears; revisit `single_sentence_fallback` only if extract quality shows missing sentence breaks after restore; wire claims `prepare` to optional punct (trim on, coref off).

**Claims prep default:** use trim ± gated punct; do not enable coref.
