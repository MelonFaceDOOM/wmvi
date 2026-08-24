# Hierarchy title massage (demo)

Turn Exp2 v2 cluster ids into scannable narrative/leaf titles, then pack `measles2_demo.sqlite`.

Default experiment:

`apps/claims/data/experiments/clustering/measles2/qwen3-emb-8b/exp2_kmeans_orphan_v2`

## Commands

Personal OpenAI (`PERSONAL_OPENAI_API_KEY`), model **`gpt-5.6-luna`**:

```bash
python -m apps.claims demo massage name-narratives
python -m apps.claims demo massage name-leaves
python -m apps.claims demo massage reassign
python -m apps.claims demo pack
python -m apps.claims demo
python -m apps.claims demo --bundle measles2_demo.sqlite
```

`--exp-dir` overrides the default. `name-leaves` / `reassign` skip ids already in the JSON (resume).

## Outputs (gitignored data/)

- `names.json` — `{narratives:[{id,title,blurb}], leaves:[{id,title,blurb}]}`
- `membership.json` — `{leaf_id: narrative_id}` overrides only (`-1` = Unassigned)
- `measles2_demo.sqlite` — **the one file** the demo PC needs besides `git pull`

Do not invent new narrative ids. Do not overwrite `leaf_labels_*.npy` / `narrative_labels_*.npy`; pack applies membership as an overlay.

## Review bar

- Narrative titles are globally distinct topics (not “measles claims 3”).
- Leaf titles are short paraphrases of the medoid, not the parent topic name.
- Grab-bag narratives: reassign stray leaves to `-1` or a better parent; keep Unassigned off the homepage.
