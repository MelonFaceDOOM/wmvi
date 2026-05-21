# Demo output index

| File | One-line | Suggested slide |
|------|----------|-----------------|
| `01_pipeline_overview.md` | Stages fetch→trim→coref→claims | Pipeline overview |
| `02_sentence_boundaries.md` | Real post from `posts_for_term_trimmed.json` + trim | Why sentence windows |
| `03_coreference.md` | Static before/after referents | Coref + chunking |
| `04_claims_stats.md` | Stats + charts from `posts_with_claims_full.json` | Dataset health |
| `05_claims_variables.md` | Score fields | Variables collected |
| `06_offline_models_plan.md` | Labeler lab Streamlit + Ridge path | Roadmap |
| `labeler_lab/app.py` | Streamlit UI (run separately) | Generic Ridge labeling |
| `img/claims_per_post.png` | Bar: claims per post | Quick viz |
| `img/llm_score_distributions.png` | Histograms: five LLM scores | Score spread |
