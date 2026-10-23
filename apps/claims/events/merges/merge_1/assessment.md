# merge_1 assessment

Strategy: `exact_key_v1` — `(event_type, country, admin1, title_slug)`. Date not a key.
Input: run_11 mentions (11,210) → 8,636 clusters.

## What this pass did

Exact titles. 7,863 / 8,636 clusters are singletons (91%). The key is too
tight on wording, too loose on generic unplaced titles.

## Over-merge

- Unplaced `outbreak` + slug `measles-outbreak` is the only 100+ cluster
  (186 mentions). No place, titles identical, years 2025+2026 mixed in.
  Same story for `measles-resurgence` (42), `measles-outbreak-reported` (31).
- Placed but generic: `measles-outbreak-in-texas` (87; 2024+2025),
  `measles-outbreak-in-south-carolina` (58; 2025+2026),
  `measles-outbreak-in-philadelphia` (28; 2023+2024). One slug, more than
  one wave is possible; undated majority cannot tell them apart yet.
- Mixed-year count is only 27 and almost all are these generic outbreak
  slugs. Date-as-split would not fix the 179 undated "Measles outbreak" rows.

## Under-merge

- Same place, paraphrased titles stay apart. Texas: `measles-outbreak-in-texas`
  (87) vs `texas-measles-outbreak` (13) vs `measles-outbreak-in-west-texas`
  (11) vs `west-texas-measles-outbreak` (7). South Carolina similarly
  split; Samoa `measles-outbreak-in-samoa` (74) vs `2019-measles-outbreak-in-samoa`
  (54) because the year is inside the title.
- National US outbreak is ~200 title variants (`measles-outbreak-in-the-united-states`,
  `measles-resurgence-…`, `u-s-measles-cases-reach-a-35-year-high`, …).
- vaccine_injury: 48 mentions → 46 clusters. Hannah Poling is three slugs;
  Samoa mixing deaths did not collapse. Almost all unplaced.

## Unplaced

3,765 mentions / 2,911 clusters. Top of the bucket is generic outbreak
wording, not named injury stories. "speaker's area / state" titles also
land here (model had no place).

## One rule to change next

Do not add date to the key. Next: coarsen `title_slug` (drop leading year
tokens; maybe drop "in {place}" when place is already a key field) so
paraphrases at the same type+place join, then re-check whether Texas /
Samoa / Poling collapse without swallowing Dilley or 2019-vs-2025 waves.

Do not merge unplaced generic `measles-outbreak` into placed state clusters.
That 186-row bucket is a discard/review pile, not a catalog event.
