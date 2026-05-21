# 06 — Offline models + manual labeling (plan)

- **Labeler lab (Streamlit):** `streamlit run apps/claim_extractor/labeler_lab/app.py` — create Ridge heads by **name** + **input variable bank** (claim, post text, titles, etc.), manual labels in isolated SQLite, train BGE+Ridge artifacts under `data/models/ridge_lab/`, preview inference, score on held-out **eval** split.
- **Dependencies:** `pip install -r apps/claim_extractor/requirements-learned.txt` (torch, sentence-transformers, scikit-learn, streamlit, …).
- **Extraction unchanged:** `python -m apps.claim_extractor.get_claims` still produces LLM-filled claims JSON; the lab does not surface those five score columns in the UI (you label your own target `y` per head).

**No metrics fabricated here** — numbers come only from real runs on completed files.
