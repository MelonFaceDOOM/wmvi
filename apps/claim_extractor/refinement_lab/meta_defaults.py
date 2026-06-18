"""Default optimizer objective + meta-prompt templates."""

from __future__ import annotations

import re
from typing import Any

_VAR_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

DEFAULT_OBJECTIVE = """\
You are helping optimize a claim-extraction prompt for a vaccine/epidemiology monitoring pipeline.
The goal is to extract claims of epidemiological value regarding vaccines, vaccination policy,
safety, effectiveness, and subjects relevant to vaccination discourse.
Claims must be atomic, direct propositions (no meta-framing like "the author says").
"""

META_PROMPT_SPECS: dict[str, dict[str, Any]] = {
    "diagnose_post": {
        "required": ("objective", "post_text", "reference_claims", "candidate_claims"),
        "template": """\
{objective}

Compare reference claims vs candidate (cheap model) claims for one post.

Post text:
{post_text}

Reference claims (JSON):
{reference_claims}

Candidate claims (JSON):
{candidate_claims}

Return JSON with:
- matched: list of {{reference_index, candidate_index, note}} for semantically equivalent claims
- missed: list of {{reference_index, issue_category, note}} for reference claims with no match
- extra: list of {{candidate_index, issue_category, note}} for candidate claims with no match
- issue_tags: list of short category strings seen in this post (e.g. missed_implicit_claim, hallucinated, over_split)

Issue categories: missed_implicit_claim, missed_explicit_claim, hallucinated, over_split, merged_claims, wrong_scope, meta_framing, other.
""",
    },
    "summarize_problems": {
        "required": ("objective", "issue_notes", "current_system_prompt"),
        "template": """\
{objective}

Per-post issue notes from comparing candidate extractions to reference:
{issue_notes}

Current system prompt:
{current_system_prompt}

Summarize systemic, prompt-fixable problems ranked by frequency/severity.
Return JSON: {{problems: [{{category, description, frequency, example_task_ids}}]}}
""",
    },
    "propose_prompt": {
        "required": ("objective", "current_system_prompt", "current_user_prompt", "problems", "constraints"),
        "template": """\
{objective}

Problems to address:
{problems}

Constraints:
{constraints}

Current system prompt:
{current_system_prompt}

Current user prompt:
{current_user_prompt}

Propose small, targeted prompt edits (not a full rewrite). Return JSON:
{{
  "system_prompt": "...",
  "user_prompt": "...",
  "changes": [{{"target": "system"|"user", "summary": "...", "addresses_category": "..."}}]
}}
Keep profile variable placeholders (text_input, max_claims, etc.) intact in the proposed prompts.
""",
    },
    "evaluate": {
        "required": ("objective", "targeted_problems", "metrics_before", "metrics_after", "diff_examples"),
        "template": """\
{objective}

Targeted problems we tried to fix:
{targeted_problems}

Metrics before: {metrics_before}
Metrics after: {metrics_after}

Example diffs: {diff_examples}

Assess whether the prompt change met the targeted improvements. Return JSON:
{{accepted: bool, summary: str, regressions: [str], improvements: [str]}}
""",
    },
}

DIAGNOSE_POST_SCHEMA: dict[str, Any] = {
    "name": "claim_alignment_diagnosis",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "matched": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "reference_index": {"type": "integer"},
                        "candidate_index": {"type": "integer"},
                        "note": {"type": "string"},
                    },
                    "required": ["reference_index", "candidate_index", "note"],
                },
            },
            "missed": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "reference_index": {"type": "integer"},
                        "issue_category": {"type": "string"},
                        "note": {"type": "string"},
                    },
                    "required": ["reference_index", "issue_category", "note"],
                },
            },
            "extra": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "candidate_index": {"type": "integer"},
                        "issue_category": {"type": "string"},
                        "note": {"type": "string"},
                    },
                    "required": ["candidate_index", "issue_category", "note"],
                },
            },
            "issue_tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["matched", "missed", "extra", "issue_tags"],
    },
}

SUMMARIZE_PROBLEMS_SCHEMA: dict[str, Any] = {
    "name": "summarize_problems",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "problems": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "category": {"type": "string"},
                        "description": {"type": "string"},
                        "frequency": {"type": "integer"},
                        "example_task_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["category", "description", "frequency", "example_task_ids"],
                },
            }
        },
        "required": ["problems"],
    },
}

PROPOSE_PROMPT_SCHEMA: dict[str, Any] = {
    "name": "propose_prompt",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "system_prompt": {"type": "string"},
            "user_prompt": {"type": "string"},
            "changes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "target": {"type": "string"},
                        "summary": {"type": "string"},
                        "addresses_category": {"type": "string"},
                    },
                    "required": ["target", "summary", "addresses_category"],
                },
            },
        },
        "required": ["system_prompt", "user_prompt", "changes"],
    },
}

EVALUATE_SCHEMA: dict[str, Any] = {
    "name": "evaluate_iteration",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "accepted": {"type": "boolean"},
            "summary": {"type": "string"},
            "regressions": {"type": "array", "items": {"type": "string"}},
            "improvements": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["accepted", "summary", "regressions", "improvements"],
    },
}


def validate_meta_prompt(name: str, template: str) -> None:
    if name == "objective":
        return
    spec = META_PROMPT_SPECS.get(name)
    if spec is None:
        raise ValueError(f"Unknown meta-prompt name: {name}")
    required = set(spec["required"])
    found = set(_VAR_PATTERN.findall(template))
    missing = required - found
    if missing:
        raise ValueError(f"Meta-prompt {name!r} missing placeholders: {', '.join(sorted(missing))}")


def render_meta_template(template: str, values: dict[str, str]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"Missing template value: {{{key}}}")
        return values[key]

    return _VAR_PATTERN.sub(repl, template)


def seed_default_meta_prompts(conn) -> None:
    """Insert default meta-prompts if missing (idempotent)."""
    for name, spec in META_PROMPT_SPECS.items():
        row = conn.execute("SELECT 1 FROM meta_prompts WHERE name = ?", (name,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO meta_prompts (name, template) VALUES (?, ?)",
                (name, spec["template"].strip()),
            )
    obj = conn.execute("SELECT 1 FROM meta_prompts WHERE name = 'objective'").fetchone()
    if obj is None:
        conn.execute(
            "INSERT INTO meta_prompts (name, template) VALUES ('objective', ?)",
            (DEFAULT_OBJECTIVE.strip(),),
        )
    _repair_meta_prompt_templates(conn)
    conn.commit()


def _repair_meta_prompt_templates(conn) -> None:
    """Reset any stored meta-prompt whose template has stray ``{placeholder}`` tokens.

    Legacy templates embedded examples like ``{{var}}`` which the renderer parses as
    a required ``{var}`` substitution, causing "Missing template value" at run time.
    Each meta-prompt's full set of valid placeholders equals its ``required`` set, so
    any extra token is invalid and the template is restored to the current default.
    """
    for name, spec in META_PROMPT_SPECS.items():
        row = conn.execute("SELECT template FROM meta_prompts WHERE name = ?", (name,)).fetchone()
        if row is None:
            continue
        found = set(_VAR_PATTERN.findall(str(row[0] or "")))
        extras = found - set(spec["required"])
        if extras:
            conn.execute(
                "UPDATE meta_prompts SET template = ? WHERE name = ?",
                (str(spec["template"]).strip(), name),
            )
