# 03 — Coreference resolution (illustrative)

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
