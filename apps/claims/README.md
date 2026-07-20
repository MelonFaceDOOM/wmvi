# Claims pipeline CLI

File-mode claims pipeline. No SQLite, no Streamlit.

**Example commands and end-to-end workflows:** see [WORKFLOWS.md](WORKFLOWS.md).

**File layout / status derivation:** see [LAYOUT_PLAN.md](LAYOUT_PLAN.md).

**Before deleting `apps/claim_extractor/`:** see [deletion_checklist.md](deletion_checklist.md).

```bash
python -m apps.claims --help
python -m apps.claims doctor --skip-model
python -m apps.claims corpus list
```

`apps/claim_extractor/` is left untouched as reference; delete it later once this CLI is confirmed (checklist above).
