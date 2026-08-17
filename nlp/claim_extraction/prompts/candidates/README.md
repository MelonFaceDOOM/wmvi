# Candidate prompts

Edit next_system.txt / next_user.txt for A/B extract comparisons in Prompt Lab.

Archived snapshots (do not load unless you want that exact version):
- `negatives_v1_{system,user}.txt` — first next pair (luna eval, 2026-08-17)

Same placeholders as the canonical templates:
- {{text_input}} (user)
- {{max_claims}} (user; optional on system)

Each extracted claim also includes discrete `claim_vaccine_alignment_score`
(0 / 0.25 / 0.5 / 0.75 / 1), using the `alignment` labeler rubric.

The lab extract path stores that alignment field. Canonical ``extract_{system,user}.txt``
is the same alignment prompt (copy of next) used by ``scripts.get_posts_extract_upload``.
