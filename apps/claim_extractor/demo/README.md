# Claim pipeline — meeting demo

Regenerates short markdown (and optional charts) under `out/` for slide copy.

```bash
python -m apps.claim_extractor.demo.demo_summary
```

Re-run after `data/posts_with_claims_full.json` is ready to fill `04_claims_stats.md` and optional `out/img/` plots. Large files (>400MB) skip full JSON parse with a note in the stats file.
