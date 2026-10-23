# merge_2 assessment

Strategy: `filter_place_key_v2` — drop vague waves, alias-clean place,
titles off for outbreak/cases, actor/title kept for named types. Date not a
key; 2,560 news rows flagged `posted_at_proxy`.

Input: run_11 11,210 mentions → dropped 2,276 → grouped 8,934 → **3,198
clusters** (merge_1 was 8,636).

## What worked

- Vague unplaced outbreaks are gone (`Measles outbreak` ×186 etc.). Drop
  reasons: 2,249 unplaced wave, 27 vague-place title.
- State/country waves collapsed paraphrases: Texas 431, South Carolina 280,
  Samoa 227 (2019 titles joined), Alberta 174, Florida 170, England 111,
  Illinois/Chicago shelter 83.
- `USA` joined `United States`. England-as-country became UK + admin1 England
  (111 in that cluster).
- Named types did not collapse to one blob: vaccine_injury 48 mentions → 43
  clusters. Actor helped (Hannah Poling ×2, RFK Jr. ×2) but Hanna Paulings
  and a third Poling slug are still split.

## Over-merge (main problem)

- **Country-only US outbreak: 1,223 mentions.** Every “measles in the United
  States” / “a U.S. state” / 2024–2026 national totals, plus 1967–2019
  leftovers. Same pattern: Canada 154, UK 96. Samoa/Bangladesh country-only
  is probably one wave; US/CA/UK is not.
- **Texas 431** mixes West Texas 2025, Dilley 2026, and 2024/2020. Expected
  until a stated-year split. 15 Dilley mentions with locality only (no
  Texas) did **not** join this cluster (fallback key `admin1=Dilley`).
- **institutional_action US / no state: 127** keyed on actor `cdc` — schedule
  changes, elimination status, case confirmations in one pile. Actor is the
  org, not the act.

## Under-merge (named types)

- Texas measles deaths still many title slugs (`child-dies-from-measles-in-texas`
  vs Daisy Hildebrand vs “two children”).
- CDC/Trump/Florida actor aliases (`cdc` vs `centers-for-disease-control-and-prevention`,
  `donald-trump` vs `trump`).
- Poling / Hanna Paulings; Samoa vaccine-mixing deaths still separate from
  American Samoa admin-error.

## Unplaced kept

1,546 mentions in 1,239 clusters — institutional/study/injury with no
country, plus site fallbacks (The Lancet, Dilley). Correct that they were
not dropped.

## One rule to change next

Do **not** treat country-only as a wave for large countries. Split
`outbreak`/`cases` with country but empty admin1 into a **national-rollup /
review** bucket (or drop from candidate events), except a small-country
allowlist (Samoa, Bangladesh, …). Then stated-year split on
state-level waves (Texas 2025 vs 2026), leaving unknowns in a residual.

Optional same pass: actor aliases for CDC/Trump/RFK; map locality Dilley →
Texas so those 15 join the state wave before the year split.
