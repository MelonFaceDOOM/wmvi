from __future__ import annotations

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io


def cmd_ls_artifacts() -> int:
    models = sorted(p.name for p in claims_io.models_dir().iterdir()) if claims_io.models_dir().is_dir() else []
    labels = sorted(p.name for p in claims_io.labels_dir().iterdir()) if claims_io.labels_dir().is_dir() else []
    runs = sorted(p.name for p in claims_io.runs_dir().iterdir() if p.is_dir()) if claims_io.runs_dir().is_dir() else []
    experiments = (
        sorted(p.name for p in claims_io.experiments_dir().iterdir() if p.is_dir())
        if claims_io.experiments_dir().is_dir()
        else []
    )
    corpora = corpus_mod.list_corpora()
    claims_io.emit_json(
        {
            "data_root": str(claims_io.data_root()),
            "models": models,
            "labels": labels,
            "runs": runs,
            "experiments": experiments,
            "corpora": [c["slug"] for c in corpora],
        }
    )
    return 0
