from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Iterator

from apps.claim_extractor.get_claims import run


def _fake_extractor(
    tasks: Iterable[dict[str, Any]],
    *,
    max_workers: int,
    max_claims: int,
) -> Iterator[dict[str, Any]]:
    del max_workers, max_claims
    for t in tasks:
        yield {
            "task_id": t["task_id"],
            "input_text": t["input_text"],
            "output": {
                "claims": [
                    {
                        "claim": "Synthetic test claim",
                        "claim_stance_to_vaccines": "neutral",
                        "author_stance_to_claim": "support",
                        "attribution": "self",
                    }
                ]
            },
        }


def test_run_writes_jsonl_and_merges_outputs(tmp_path: Path) -> None:
    in_file = tmp_path / "sample_input.json"
    out_file = tmp_path / "sample_output.jsonl"

    if out_file.exists():
        raise RuntimeError(f"Refusing to overwrite existing test output: {out_file}")

    input_payload = {
        "posts": [
            {
                "post_id": 101,
                "platform": "reddit_comment",
                "contexts": [
                    {"text": "Measles can be severe in children."},
                    {"text": "Vaccines reduce severe outcomes."},
                ],
            },
            {
                "post_id": 202,
                "platform": "youtube_comment",
                "contexts": [
                    {"text": "MMR vaccine discussion context."},
                ],
            },
        ]
    }
    in_file.write_text(json.dumps(input_payload, ensure_ascii=False), encoding="utf-8")

    try:
        run(
            input_file=in_file,
            out_file=out_file,
            batch_count=1,
            max_workers=2,
            max_claims=3,
            max_tasks=0,
            extractor_fn=_fake_extractor,
        )

        assert out_file.exists(), "Expected output JSONL file to be created."
        lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        assert len(lines) == 2

        rows = [json.loads(ln) for ln in lines]
        for post in rows:
            contexts = post.get("contexts", [])
            assert isinstance(contexts, list)
            assert contexts, "Each post should retain contexts."
            for ctx in contexts:
                assert "output" in ctx
                assert isinstance(ctx["output"], dict)
                assert "claims" in ctx["output"]
    finally:
        if out_file.exists():
            out_file.unlink()


def test_run_resumes_and_skips_existing_task_ids(tmp_path: Path) -> None:
    in_file = tmp_path / "sample_input_resume.json"
    out_file = tmp_path / "sample_output_resume.jsonl"

    if out_file.exists():
        raise RuntimeError(f"Refusing to overwrite existing test output: {out_file}")

    input_payload = {
        "posts": [
            {
                "post_id": 301,
                "platform": "reddit_comment",
                "contexts": [
                    {"text": "ctx a"},
                    {"text": "ctx b"},
                ],
            },
            {
                "post_id": 302,
                "platform": "reddit_comment",
                "contexts": [
                    {"text": "ctx c"},
                ],
            },
        ]
    }
    in_file.write_text(json.dumps(input_payload, ensure_ascii=False), encoding="utf-8")

    called_ids: list[str] = []

    def tracking_extractor(
        tasks: Iterable[dict[str, Any]],
        *,
        max_workers: int,
        max_claims: int,
    ) -> Iterator[dict[str, Any]]:
        del max_workers, max_claims
        for t in tasks:
            called_ids.append(str(t["task_id"]))
            yield {
                "task_id": t["task_id"],
                "input_text": t["input_text"],
                "output": {
                    "claims": [
                        {
                            "claim": "Synthetic test claim",
                            "claim_stance_to_vaccines": "neutral",
                            "author_stance_to_claim": "support",
                            "attribution": "self",
                        }
                    ]
                },
            }

    try:
        # First run: small test (only 1 task).
        run(
            input_file=in_file,
            out_file=out_file,
            batch_count=2,
            max_workers=2,
            max_claims=3,
            max_tasks=1,
            extractor_fn=tracking_extractor,
        )
        first_called = list(called_ids)
        assert len(first_called) == 1

        # Second run: full run should skip the already completed task id.
        called_ids.clear()
        run(
            input_file=in_file,
            out_file=out_file,
            batch_count=2,
            max_workers=2,
            max_claims=3,
            max_tasks=0,
            extractor_fn=tracking_extractor,
        )
        second_called = list(called_ids)
        assert len(second_called) == 2
        assert first_called[0] not in second_called
    finally:
        if out_file.exists():
            out_file.unlink()
