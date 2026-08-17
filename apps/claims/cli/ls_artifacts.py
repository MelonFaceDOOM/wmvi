from __future__ import annotations

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import models as models_mod


def cmd_ls_artifacts() -> int:
    registered = models_mod.list_models()
    fixtures = (
        sorted(p.name for p in claims_io.fixtures_dir().iterdir() if not p.name.startswith("."))
        if claims_io.fixtures_dir().is_dir()
        else []
    )
    runs: list[str] = []
    if claims_io.runs_dir().is_dir():
        for corpus_dir in sorted(claims_io.runs_dir().iterdir()):
            if not corpus_dir.is_dir() or corpus_dir.name.startswith("."):
                continue
            for tag_dir in sorted(corpus_dir.iterdir()):
                if tag_dir.is_dir() and not tag_dir.name.startswith("."):
                    runs.append(f"{corpus_dir.name}/{tag_dir.name}")
    experiments = (
        sorted(p.name for p in claims_io.experiments_dir().iterdir() if p.is_dir())
        if claims_io.experiments_dir().is_dir()
        else []
    )
    corpora = corpus_mod.list_corpora()
    claims_io.emit_json(
        {
            "data_root": str(claims_io.data_root()),
            "models_registered": [m["tag"] for m in registered],
            "fixtures": fixtures,
            "runs": runs,
            "experiments": experiments,
            "corpora": [c["slug"] for c in corpora],
        }
    )
    return 0
