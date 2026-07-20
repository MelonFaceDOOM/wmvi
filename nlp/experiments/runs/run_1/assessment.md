# Assessment — run_1

Prepared from `posts_for_term_raw.json` (~18.7k posts). Products: `posts_prepared.json`, `browse_coref_edits.html`, `summary_coref.md`, `summary_chunks.md`.

## Coreference

**Finding:** fastcoref resolve-and-rewrite often replaces short phrases / pronouns with long multi-clause noun phrases and repeats those phrases through the document (“description bleed”). Examples in `browse_coref_edits.html` (largest inserts / likely_bleed) show short antecedents expanding into multi-sentence gibberish and broken grammar (`public's's`, wrong speaker identity).

**Scale (this run):** ~57% of posts have `text ≠ text_coreference_resolved`. Almost all of those are real content edits (not whitespace). Delta among changed: p50 ≈ +25 chars, but max ≈ +4500 on long YouTube transcripts—the failure mode concentrates on long discourse.

**Decision:** **Remove coref from the claims / prep pipeline for now.** Do not spend a day tuning batch size or `COREF_MAX_CHARS`; that only changes who is damaged. Keep `nlp.coref` and this experiment as evidence. Revisit only with a different approach (e.g. short-post-only + hard reject on expansion, or a non-rewrite method).

## Chunking (sentence-boundary trim)

**Stats (this run):** chunk char length p50 ≈ 248, p90 ≈ 828, p99 ≈ 1957 (n ≈ 19k chunks). Quirks: many `short_post_hit` / `single_sentence_fallback` (title-like or ≤1 syntok sentence → char-window path); `far_apart_hits` and near trim/sentence caps are uncommon.

**Judgment (pending review):** Hit-window trim looks **usable** as a default before extract—lengths are mostly moderate, and extreme caps are rare. Open question: whether p90 (~800 chars) is enough context for claim extraction on multi-hit / long posts, or whether windows should be widened.

**Recommendation:** Keep trim in the pipeline; iterate on window knobs only if extract quality shows missing context—not blocked on coref.
