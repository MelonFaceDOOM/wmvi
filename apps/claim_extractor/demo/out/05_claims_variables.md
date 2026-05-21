# 05 — Variables collected (per claim)

| Field | Meaning (short) |
|-------|-------------------|
| `claim` | Direct proposition text (no “this post says…”) |
| `claim_vaccine_alignment_score` | 0 anti-vaccine … 0.5 neutral … 1 pro-vaccine (claim content) |
| `author_claim_agreement_score` | 0 author rejects … 0.5 unclear … 1 author supports claim |
| `attribution_anecdote_score` | 0–1 personal / relational anecdote framing |
| `attribution_authority_score` | 0–1 expert / study / institution framing |
| `attribution_common_knowledge_score` | 0–1 “obvious / everyone knows” framing |

## Predictions
Train generic BGE+Ridge heads and optional batch scoring in the **labeler lab** Streamlit app (not hardcoded to the five claim score names).

```bash
pip install -r apps/claim_extractor/requirements-learned.txt
streamlit run apps/claim_extractor/labeler_lab/app.py
```
