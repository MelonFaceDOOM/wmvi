"""
Single entry: regenerate meeting demo artifacts under ``demo/out/``.

Run: ``python -m apps.claim_extractor.demo.demo_summary``
"""

from __future__ import annotations

import json
import math
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
DEMO_DIR = Path(__file__).resolve().parent
OUT_DIR = DEMO_DIR / "out"
IMG_DIR = OUT_DIR / "img"
CLAIMS_JSON = REPO_ROOT / "data" / "posts_with_claims_full.json"
POSTS_TERM_JSON = REPO_ROOT / "data" / "posts_for_term_trimmed.json"
MAX_JSON_BYTES = 400 * 1024 * 1024  # skip full parse above this (OOM guard)
DEMO_MIN_TEXT_CHARS = 1000
DEMO_MAX_TEXT_CHARS = 1300  # narrow band for slide-sized example bodies
DEMO_MIN_HITS = 2
DEMO_BODY_DISPLAY_MAX = 9000  # omit middle of fenced body if longer (keeps markdown usable)


def _reset_out_dir() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)
    IMG_DIR.mkdir(parents=True)


def write_01_pipeline() -> None:
    text = """# 01 — Data cleaning pipeline (overview)

| Step | Module / command | Purpose |
|------|------------------|---------|
| Fetch | `run_term_pipeline --stage fetch` | Pull posts matching search terms from DB → `posts_for_term_raw.json` |
| Trim | `trim_sentence_boundary` | Sentence windows around hits → `posts_for_term_trimmed.json` |
| Coref | `run_term_pipeline --stage coref` | Resolve pronouns → `posts_for_term.json` (+ `.coref.jsonl` resume) |
| Claims | `python -m apps.claim_extractor.get_claims` | LLM extracts claims + scores → `posts_with_claims_full.json` |

```mermaid
flowchart LR
  fetch[Fetch_DB]
  trim[Sentence_trim]
  coref[Coreference]
  claims[LLM_claims]
  fetch --> trim --> coref --> claims
```

**Framing:** long transcripts (YT/podcast) need bounded context before coref/LLM — pipeline enforces chunk size and referential clarity.
"""
    (OUT_DIR / "01_pipeline_overview.md").write_text(text.strip() + "\n", encoding="utf-8")


def _pick_sentence_demo_post() -> tuple[dict[str, Any] | None, str]:
    """
    Choose a real post row: body length in [DEMO_MIN_TEXT_CHARS, DEMO_MAX_TEXT_CHARS], multiple
    hits; prefer more output chunks then more hits (then longer text among ties).
    Returns (post, err) where post is None if file missing or no qualifying row.
    """
    from apps.claim_extractor.trim_transcripts import trim_sentence_boundary

    if not POSTS_TERM_JSON.is_file():
        return None, f"file not found: {POSTS_TERM_JSON}"
    try:
        payload = json.loads(POSTS_TERM_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return None, str(e)
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return None, "expected top-level `posts` array"

    best: dict[str, Any] | None = None
    best_key: tuple[int, int, int] = (-1, -1, -1)
    for p in posts:
        if not isinstance(p, dict):
            continue
        body = p.get("text")
        hits = p.get("hits")
        if not isinstance(body, str) or not (DEMO_MIN_TEXT_CHARS <= len(body) <= DEMO_MAX_TEXT_CHARS):
            continue
        if not isinstance(hits, list) or len(hits) < DEMO_MIN_HITS:
            continue
        chunks = trim_sentence_boundary(body, hits)
        key = (len(chunks), len(hits), len(body))
        if key > best_key:
            best_key = key
            best = p
    if best is None:
        return None, (
            f"no post with {DEMO_MIN_TEXT_CHARS}<=text<={DEMO_MAX_TEXT_CHARS} chars "
            f"and >={DEMO_MIN_HITS} hits in {POSTS_TERM_JSON.name}"
        )
    return best, ""


def _fence_body_ellipsis(text: str, limit: int) -> str:
    """Full body for small posts; head/tail + marker when very long."""
    if len(text) <= limit:
        return text
    head = limit // 2
    tail = limit - head - 80
    if tail < 200:
        tail = 200
    gap = len(text) - head - tail
    return (
        text[:head]
        + f"\n\n… ({gap} characters omitted) …\n\n"
        + text[-tail:]
    )


def _chunk_union_coverage(text: str, chunks: list[str]) -> tuple[float, int]:
    """
    Approximate fraction of ``text`` covered by at least one chunk (chunks may overlap).

    Locates each chunk in document order with ``find`` from a sliding lower bound.
    Returns (fraction in [0,1], union_char_count).
    """
    if not text:
        return 0.0, 0
    intervals: list[tuple[int, int]] = []
    search_from = 0
    for ch in chunks:
        if not ch:
            continue
        pos = text.find(ch, max(0, search_from - 500))
        if pos < 0:
            pos = text.find(ch)
        if pos < 0:
            continue
        end = pos + len(ch)
        intervals.append((pos, end))
        search_from = pos + 1
    if not intervals:
        return 0.0, 0
    intervals.sort()
    merged: list[tuple[int, int]] = []
    for a, b in intervals:
        if not merged or a > merged[-1][1]:
            merged.append((a, b))
        else:
            lo, hi = merged[-1]
            merged[-1] = (lo, max(hi, b))
    union = sum(b - a for a, b in merged)
    return union / max(1, len(text)), union


def write_02_sentence_boundaries() -> None:
    from apps.claim_extractor.trim_transcripts import syntok_sentence_spans, trim_sentence_boundary

    post, err = _pick_sentence_demo_post()
    if post is None:
        body = "\n".join(
            [
                "# 02 — Sentence boundary trimming",
                "",
                "## Status",
                f"**No demo example loaded:** {err}",
                "",
                "Expected `data/posts_for_term_trimmed.json` from the term pipeline trim stage.",
                "",
                "## Feature (when data is present)",
                "Use **syntok** sentence spans to anchor search hits, then take N sentences before/after (merge overlaps, cap length).",
            ]
        )
        (OUT_DIR / "02_sentence_boundaries.md").write_text(body + "\n", encoding="utf-8")
        return

    text = post["text"] if isinstance(post.get("text"), str) else ""
    hits = post["hits"] if isinstance(post.get("hits"), list) else []
    spans = syntok_sentence_spans(text)
    chunks = trim_sentence_boundary(text, hits)
    n_spans = len(spans)
    chunk_lens = [len(c) for c in chunks]
    cov_frac, union_chars = _chunk_union_coverage(text, chunks)
    excluded = 1.0 - cov_frac
    post_id = post.get("post_id")
    url = post.get("url")
    platform = post.get("platform")
    rel_json = POSTS_TERM_JSON.relative_to(REPO_ROOT)

    lines = [
        "# 02 — Sentence boundary trimming",
        "",
        "## Feature",
        "Use **syntok** sentence spans to anchor search hits, then take N sentences before/after (merge overlaps, cap length).",
        "",
        "## Purpose",
        "Avoid naive **char windows** that cut mid-sentence or mid-clause — especially in transcripts with weak punctuation.",
        "",
        "## Real row from term pipeline",
        "",
        f"- **Source:** `{rel_json}` (same `text` + `hits` the trim stage uses).",
        f"- **`post_id`:** `{post_id}`  ·  **`platform`:** `{platform}`",
    ]
    if isinstance(url, str) and url:
        lines.append(f"- **URL:** {url}")
    lines += [
        "",
        f"- **Chars:** {len(text)}  ·  **Syntok sentences:** {n_spans}  ·  **DB hits:** {len(hits)}  ·  **Output chunks:** {len(chunks)}",
        f"- **Per-chunk lengths:** {' + '.join(str(x) for x in chunk_lens)} = **{sum(chunk_lens)}** (sum can exceed body when cap-split windows **reuse** overlap).",
        f"- **~Union coverage** (chars in ≥1 chunk, approximate): **{100 * cov_frac:.0f}%** (~**{100 * excluded:.0f}%** not in any chunk).",
        "- **Note:** Sum of chunk lengths can exceed the body when max-length splits **reuse** overlap; union coverage is the clearer “how much source text appears downstream” read.",
        "",
        "### Hits (DB-style)",
        "",
        "| # | term | match_start | match_end |",
        "|--:|------|-------------:|----------:|",
    ]
    for idx, h in enumerate(hits, start=1):
        if not isinstance(h, dict):
            continue
        tn = h.get("term_name", "")
        lines.append(
            f"| {idx} | `{tn}` | {h.get('match_start')} | {h.get('match_end')} |"
        )
    lines += [
        "",
        "### Merge / separate (this row)",
        "",
        f"- **{len(hits)}** substring hits → **{len(chunks)}** chunks after ±N sentences, **overlap merge**, and **length cap**.",
        "- Nearby hits often land in the same syntok window → **one** merged chunk; distant clusters → **multiple** chunks.",
        "",
        "### Input text",
        "",
        "```",
        _fence_body_ellipsis(text, DEMO_BODY_DISPLAY_MAX),
        "```",
        "",
        "## Sentence spans (first 6 + last 4; total = "
        + str(n_spans)
        + ")",
        "",
    ]
    for j, (a, b) in enumerate(spans[:6]):
        excerpt = text[a:b].replace("\n", " ")
        lines.append(f"{j}. [{a:4d},{b:4d})  {excerpt[:70]}{'…' if len(excerpt) > 70 else ''}")
    if n_spans > 10:
        lines.append("*(middle spans omitted for brevity)*")
        for j, (a, b) in enumerate(spans[-4:], start=n_spans - 4):
            excerpt = text[a:b].replace("\n", " ")
            lines.append(f"{j}. [{a:4d},{b:4d})  {excerpt[:70]}{'…' if len(excerpt) > 70 else ''}")

    lines += ["", "## `trim_sentence_boundary` output", ""]
    for k, ch in enumerate(chunks):
        lines.append(f"### Chunk {k} (len={len(ch)})")
        lines.append("```")
        lines.append(ch)
        lines.append("```")
        lines.append("")

    lines += [
        "## vs char before/after",
        "- **Char slice:** can start/end inside a clause → broken context for coref/LLM.",
        "- **Sentence window:** keeps grammatical units → more stable meaning in each chunk.",
        "- **Hard cap** (`MAX_TRIMMED_CHARS` / coref cap): prevents OOM on huge single “sentences”.",
    ]
    (OUT_DIR / "02_sentence_boundaries.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_03_coreference() -> None:
    text = """# 03 — Coreference resolution (illustrative)

## Feature
Model rewrites text so pronouns and vague NPs point to explicit entities (`text_coreference_resolved` on each post row).

## Purpose
Chunks sent to the claim LLM keep **who/what** clear — fewer dropped referents when only a slice of the post is in context.

## Example (toy — not live model output)

**Before (ambiguous):**  
*Dr. Lee published a study on measles immunity. She said it supports the current MMR schedule. Many parents still doubt them.*

**After (resolved style — schematic):**  
*Dr. Lee published a study on measles immunity. Dr. Lee said Dr. Lee's study supports the current MMR schedule. Many parents still doubt the MMR vaccines.*

## Why it helps chunking
- Each sentence-boundary chunk can stand alone better for downstream extraction.
- Reduces “important context lost” when the window does not include the original antecedent sentence.

## Live run
Implemented in `apps/claim_extractor/coreference_resolution.py`; orchestrated via `python -m apps.claim_extractor.run_term_pipeline --stage coref`.
"""
    (OUT_DIR / "03_coreference.md").write_text(text.strip() + "\n", encoding="utf-8")


def _coerce_score_float(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)) and math.isfinite(float(v)):
        return float(v)
    if isinstance(v, str) and v.strip():
        try:
            x = float(v.strip())
        except ValueError:
            return None
        return x if math.isfinite(x) else None
    return None


def write_04_claims_stats() -> None:
    from apps.claim_extractor.model_common import SCORE_FIELD_NAMES

    lines = [
        "# 04 — Claims file statistics",
        "",
        f"**Source:** `{CLAIMS_JSON}`",
        "",
    ]
    if not CLAIMS_JSON.is_file():
        lines += [
            "## Status",
            "**File not found** — extraction still running or path differs.",
            "",
            "Re-run:",
            "",
            "```bash",
            "python -m apps.claim_extractor.demo.demo_summary",
            "```",
            "",
            "after `posts_with_claims_full.json` exists.",
        ]
        (OUT_DIR / "04_claims_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    size = CLAIMS_JSON.stat().st_size
    lines.append(f"**File size:** {size / (1024 * 1024):.1f} MB")
    lines.append("")

    if size > MAX_JSON_BYTES:
        lines += [
            "## Status",
            f"**Skipped full parse** — file exceeds demo guard ({MAX_JSON_BYTES // (1024 * 1024)} MB).",
            "Use a subset copy on a workstation, or extend this script with streaming.",
        ]
        (OUT_DIR / "04_claims_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    try:
        payload = json.loads(CLAIMS_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        lines += ["## Status", f"**Read/parse failed:** `{e}`"]
        (OUT_DIR / "04_claims_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    posts = payload.get("posts") if isinstance(payload, dict) else None
    if not isinstance(posts, list):
        lines += ["## Status", "**Invalid JSON** — expected top-level `posts` array."]
        (OUT_DIR / "04_claims_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    status_c: Counter[str] = Counter()
    disp_c: Counter[str] = Counter()
    claim_hist: Counter[int] = Counter()
    total_claims = 0
    score_samples: dict[str, list[float]] = {n: [] for n in SCORE_FIELD_NAMES}

    for row in posts:
        if not isinstance(row, dict):
            continue
        st = str(row.get("claim_extraction_status") or "missing")
        status_c[st] += 1
        disp = str(row.get("claim_extraction_disposition") or "")
        if disp:
            disp_c[disp] += 1
        out = row.get("claim_extraction_output")
        if st == "success" and isinstance(out, dict):
            cl = out.get("claims")
            if isinstance(cl, list):
                nc = len(cl)
                claim_hist[min(nc, 5)] += 1
                total_claims += nc
                for claim in cl:
                    if not isinstance(claim, dict):
                        continue
                    for name in SCORE_FIELD_NAMES:
                        fv = _coerce_score_float(claim.get(name))
                        if fv is not None:
                            score_samples[name].append(fv)

    lines += [
        "## Counts",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| Post rows | {len(posts)} |",
        f"| `claim_extraction_status=success` | {status_c.get('success', 0)} |",
        f"| Other / failed statuses | {sum(v for k, v in status_c.items() if k != 'success')} |",
        f"| Total claims (success rows) | {total_claims} |",
        "",
        "## Claims per post (success rows; bucket 5 = “5+”)",
        "",
        "| #claims | posts |",
        "|--------:|------:|",
    ]
    for k in range(6):
        label = "5+" if k == 5 else str(k)
        lines.append(f"| {label} | {claim_hist.get(k, 0)} |")
    lines.append("")

    if disp_c:
        lines += ["## Disposition (if present)", ""]
        for k, v in disp_c.most_common(8):
            lines.append(f"- `{k}`: {v}")
        lines.append("")

    _maybe_chart_claims_per_post(claim_hist)
    chart_path = IMG_DIR / "claims_per_post.png"
    if chart_path.is_file():
        lines += [
            "## Chart — claims per post",
            "",
            f"![claims per post](img/claims_per_post.png) — copy `{chart_path.relative_to(OUT_DIR)}` into slides if useful.",
            "",
        ]

    _maybe_chart_llm_score_histograms(score_samples)
    score_chart = IMG_DIR / "llm_score_distributions.png"
    if score_chart.is_file():
        lines += [
            "## LLM score distributions (per claim)",
            "",
            "Histograms of the five continuous scores on **success** rows (`claim_extraction_output.claims[]`), one bin per 0.05 from 0 to 1.",
            "",
            f"![LLM score distributions](img/llm_score_distributions.png) — `{score_chart.relative_to(OUT_DIR)}`",
            "",
        ]

    (OUT_DIR / "04_claims_stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _maybe_chart_claims_per_post(hist: Counter[int]) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return
    labels = [str(i) if i < 5 else "5+" for i in range(6)]
    vals = [hist.get(i, 0) for i in range(6)]
    if sum(vals) == 0:
        return
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.bar(labels, vals, color="steelblue")
    ax.set_xlabel("Claims per post")
    ax.set_ylabel("Count")
    ax.set_title("Claims-per-post (success rows)")
    fig.tight_layout()
    path = IMG_DIR / "claims_per_post.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _maybe_chart_llm_score_histograms(score_samples: dict[str, list[float]]) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return
    from apps.claim_extractor.model_common import SCORE_FIELD_NAMES

    if sum(len(score_samples.get(n, [])) for n in SCORE_FIELD_NAMES) == 0:
        return

    bins = [i / 20 for i in range(21)]

    fig, axes = plt.subplots(2, 3, figsize=(11, 6.5))
    axes_flat = axes.flatten()
    for i, name in enumerate(SCORE_FIELD_NAMES):
        ax = axes_flat[i]
        vals = score_samples.get(name) or []
        if vals:
            ax.hist(vals, bins=bins, color="steelblue", edgecolor="white", linewidth=0.4)
        ax.set_title(name, fontsize=7)
        ax.set_xlim(0, 1)
        ax.set_xlabel("score")
        ax.set_ylabel("claim count")
        ax.tick_params(labelsize=8)
    axes_flat[5].axis("off")
    fig.suptitle("LLM score distributions (success rows, per claim)", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    path = IMG_DIR / "llm_score_distributions.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)


def write_05_variables() -> None:
    from apps.claim_extractor.model_common import SCORE_FIELD_NAMES

    rows = [
        "# 05 — Variables collected (per claim)",
        "",
        "| Field | Meaning (short) |",
        "|-------|-------------------|",
        "| `claim` | Direct proposition text (no “this post says…”) |",
    ]
    desc = {
        "claim_vaccine_alignment_score": "0 anti-vaccine … 0.5 neutral … 1 pro-vaccine (claim content)",
        "author_claim_agreement_score": "0 author rejects … 0.5 unclear … 1 author supports claim",
        "attribution_anecdote_score": "0–1 personal / relational anecdote framing",
        "attribution_authority_score": "0–1 expert / study / institution framing",
        "attribution_common_knowledge_score": "0–1 “obvious / everyone knows” framing",
    }
    for name in SCORE_FIELD_NAMES:
        rows.append(f"| `{name}` | {desc.get(name, '')} |")
    rows += [
        "",
        "## Predictions",
        "Train generic BGE+Ridge heads and optional batch scoring in the **labeler lab** Streamlit app (not hardcoded to the five claim score names).",
        "",
        "```bash",
        "cd /path/to/wmvi   # repository root (parent of apps/)",
        "pip install -r apps/claim_extractor/requirements-learned.txt",
        "streamlit run apps/claim_extractor/labeler_lab/app.py",
        "```",
    ]
    (OUT_DIR / "05_claims_variables.md").write_text("\n".join(rows) + "\n", encoding="utf-8")


def write_06_offline_plan() -> None:
    text = """# 06 — Offline models + manual labeling (plan)

- **Labeler lab (Streamlit):** from repo root, `streamlit run apps/claim_extractor/labeler_lab/app.py` — create Ridge heads by **name** + **input variable bank** (claim, post text, titles, etc.), manual labels in isolated SQLite, train BGE+Ridge artifacts under `data/models/ridge_lab/`, preview inference, score on held-out **eval** split.
- **Dependencies:** `pip install -r apps/claim_extractor/requirements-learned.txt` (torch, sentence-transformers, scikit-learn, streamlit, …).
- **Extraction unchanged:** `python -m apps.claim_extractor.get_claims` still produces LLM-filled claims JSON; the lab does not surface those five score columns in the UI (you label your own target `y` per head).

**No metrics fabricated here** — numbers come only from real runs on completed files.
"""
    (OUT_DIR / "06_offline_models_plan.md").write_text(text.strip() + "\n", encoding="utf-8")


def write_index() -> None:
    rows = [
        "# Demo output index",
        "",
        "| File | One-line | Suggested slide |",
        "|------|----------|-----------------|",
        "| `01_pipeline_overview.md` | Stages fetch→trim→coref→claims | Pipeline overview |",
        "| `02_sentence_boundaries.md` | Real post from `posts_for_term_trimmed.json` + trim | Why sentence windows |",
        "| `03_coreference.md` | Static before/after referents | Coref + chunking |",
        "| `04_claims_stats.md` | Stats + charts from `posts_with_claims_full.json` | Dataset health |",
        "| `05_claims_variables.md` | Score fields | Variables collected |",
        "| `06_offline_models_plan.md` | Labeler lab Streamlit + Ridge path | Roadmap |",
        "| `labeler_lab/app.py` | Streamlit UI (run separately) | Generic Ridge labeling |",
        "| `img/claims_per_post.png` | Bar: claims per post | Quick viz |",
        "| `img/llm_score_distributions.png` | Histograms: five LLM scores | Score spread |",
    ]
    (OUT_DIR / "INDEX.md").write_text("\n".join(rows) + "\n", encoding="utf-8")


def main() -> None:
    _reset_out_dir()
    write_01_pipeline()
    write_02_sentence_boundaries()
    write_03_coreference()
    write_04_claims_stats()
    write_05_variables()
    write_06_offline_plan()
    write_index()
    print(f"[demo_summary] wrote artifacts under {OUT_DIR.resolve()}", flush=True)


if __name__ == "__main__":
    main()
