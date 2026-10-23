# Extract prompt — possible improvements

Living list for `prompts.py`. Do **not** fold grouping/filter logic into the
prompt. Merge attempts live in `merges/`; this file is only extract-time
wording and schema.

Status: `later` = worth a prompt pass after merge_2; `idea` = unproven;
`wont` = would make merge worse.

The current `prompts.py` applies the prominence bar (news-publishable and
discussable by outsiders), the omit list for anecdotes, unresolved references,
votes with no outcome, standing schedules, routine operations, and
citation-as-evidence, plus `diseases` / `vaccines` / `people` /
`organizations` tags. Tags are not a drop filter. Grouping stays out of the
prompt. Default model is gpt-6-luna. A short or commentary chunk still keeps
a named act (withdrawal, retraction, public death, a dated schedule change,
a named place of a wave). Country-wide counts, a death year with no person
and no place, joke titles, speculative asides, and grievance lists are empty.
`place_scope` is `at_named_place`, `countrywide`, `unplaced`, or `global`.
`study_action` is `publication` or `retraction`. `admin2` is the division
under `admin1`. A city that is also that division fills `locality` and
`admin1`. Taiwan and Vatican City are countries.

## later

- **`date_basis`:** `stated` | `posted_at_proxy` | `unknown`. Prompt already
  resolves “yesterday” against `posted_at`, but “last week” / “in the last
  couple of months” still land as `date_precision=unknown` with leftover
  `stated_date_text`. Explicit basis stops those going silent. Use proxy only
  when the text is a news-like report of a current happening, not a comment
  about an old event.
- **Complete place when a site/city is set:** If `locality` or `site` is
  filled, require `country`, and `admin1` for US/CA/AU. Chicago shelters,
  Disneyland, Universal, Muscatine County currently sit with no country/admin1
  (535 site-only mentions on run_11).
- **Place grain (one value per slot, English short names):** The model often
  puts the right string in the wrong slot, or two countries in `country`.
  Prompt (not gazetteer) can enforce:
  - `country`: one sovereign country (`United States`, `United Kingdom`).
    Not `USA` / `US` / `Britain` / `England`. Not `Worldwide` / `Europe`.
    Not `United States and Mexico` — second country or state goes in
    `admin1_other`.
  - `admin1`: state / province / constituent country (`Texas`, `Alberta`,
    `England`, `Scotland`). Not a city (`London` is locality). Not a country
    (`United Kingdom` is country). `District of Columbia` not
    `Washington, D.C.` as admin1.
  - `admin2`: one county / equivalent (`Gaines County`). Not “Forsyth and
    Guilford counties” (split or use `admin1_other`).
  - `locality`: city. `site`: named facility (hospital, park, university,
    shelter). Give a US example stack: site=Disneyland, locality=Anaheim,
    admin1=California, country=United States.
  - `stated_start` / `stated_end`: date only `YYYY-MM-DD`, never a datetime.
- **Relative windows:** Spell out “last week” → day range or week precision,
  “last couple of months” → month, “this year” / “last year” vs `posted_at`.
  The yesterday example is not enough. Add “last week” and “this year”
  next to the yesterday example.
- **National status ≠ outbreak (country-only, large countries):** merge_2
  glued 1,223 US mentions with no `admin1` into one “outbreak” (CDC YTD
  totals, “measles is back in America”, “outbreak in a U.S. state” with no
  state). That is a national burden/status report, not a named-place wave
  with a start. Same pattern: Canada 154, UK 96. Future prompt should say:
  for large countries, `outbreak` requires `admin1` (or finer). Country-only
  case counts and “measles in the US/UK/Canada” are not outbreak — omit, or
  a later `national_status` type *only if* we want them on the timeline.
  Small countries where the country *is* the wave (Samoa, Bangladesh, Tonga)
  stay outbreak. Unspecified subnational (“a U.S. state”, “nationwide”,
  “multiple places”) stay empty. Do not use `cases` for this (that type is
  leftover isolated infections). Until a prompt pass, merge can bucket or
  drop these; do not re-extract 41k for this alone.

Alias files for leftover variants after extract:
`geo/country_aliases.json`, `geo/admin1_aliases.json`, `geo/admin2_aliases.json`,
`geo/not_a_country.json`, `geo/country_not_atomic.json`. `England` as country
maps to `United Kingdom`; grouping should set `admin1=England` when that
alias fires and admin1 is empty.

## idea

- **`place_certainty`:** `named` | `implied` | `unknown`. Only useful if merge
  treats implied differently from empty. Until then, empty vs filled is enough.
- **Do not emit an outbreak with neither place nor a named wave.** Might cut
  “huge measles outbreak” at extract instead of the merge filter. Risk: the
  model over-invents places to avoid empty. Prefer the merge drop unless a
  500-chunk A/B shows otherwise.
- **Actor required for `vaccine_injury` / `notable_case`.** Named person, court
  docket, or official descriptor. Helps Poling vs unnamed anecdotes; overlap
  with rules already in the type bullets.

## wont

- Infer `stated_start` from `posted_at` on comments / when the text is about a
  historical happening (Samoa 2019 talked about in 2026).
- Invent country/admin1 when the speaker says “my area”, “nationwide”,
  “multiple places”. Those stay empty and are dropped before grouping.
- Extra types or title-normalization instructions aimed at clustering.
  Paraphrase collapse is a merge key, not an extract job.
- `mention_focus`, `subjects`, NER/place_conflict (already removed).

## seen in run_11 (for the next prompt pass)

- `stated_date_text='Last week'` with `date_precision=unknown`
- `stated_date_text='in the last couple of months'` with precision unknown
- Disneyland / Universal as `site` only
- Chicago locality without Illinois / United States
- `country='USA'` / `'United States of America'` / `'England'` / `'Britain'`
- `country='United States and Mexico'` (and other compound country strings)
- `country='Worldwide'` / `'Global'` / `'Europe'` / `'Unitma'`
- `admin1='Hawaiʻi'` vs `'Hawaii'`; `'Québec'` vs `'Quebec'`;
  `'Washington, D.C.'` vs `'District of Columbia'`
- `stated_start` datetimes (`2026-04-04T10:27:00-07:00`)
- merge_2: country-only `outbreak` US ×1223 / Canada ×154 / UK ×96 (national
  status, not a regional wave); “Measles outbreak in a U.S. state” with no
  state; Samoa/Bangladesh country-level waves are the exception to keep
- A vote is an event only when the text says what was decided. “Voting on
  measles, mumps and chicken pox vaccines for kids 0-5” and “ACIP will vote…
  it’s not clear what the votes will entail” are not events.
- Do not resolve “it” / “the vaccine” / “that year” from outside the chunk.
  “It’s now been introduced with the MMR and is standard in Europe, Australia
  and Canada” does not name the product.
- A requirement the speaker lived under is not an institutional act (“my
  university needed everyone to be vaccinated,” “SDSU made me get an MMR,”
  a daycare door sign).
- A vague accusation is not an event (“got the COVID variant wrong again at
  Pfizer's behest”; an unsourced “CDC admitted they had been lying”).
- A standing schedule is not an event (“9 mandatory here in the Czech
  Republic,” “Hungary and Romania vaccinations are mandatory,” “Washington
  doesn’t have mandatory child vaccines except for MMR”). A reported change
  to a schedule is.
- merge_3: Dilley / London / Disneyland often lack `admin1` even when
  locality/site is filled (groups at country grain or as unplaced site).
  Named deaths (Daisy Hildebrand, Kayley Fehr) omitted `actor` and dropped
  as `unanchored_case`. “UK loses measles-free status” vs “elimination
  status” — prompt should treat measles-free as the same institutional act.

Prompt-revision posts (57 chunks, with why each was picked):
[`prompt_sample/README.md`](prompt_sample/README.md).

## User notes
one major issue right now is that the prompt is still including personal anecdotes and stuff. event definition definitely needs work. I think i'll want to review each issue and do a significant prompt re-write, while also designing the full pipeline before i actually do anything else. then do a brand new run and get significantly better results. Merging is quite challenging unless you really improve prompt performance.
i need a sample of posts that i label and validate through different prompt attempts. i.e. prompts with anecdotes, prompts with real events, prompts with the same event worded differently, etc.
I need a better way of grouping non-time/place events. regex heuristic needs to go. I could look through all of the ones that exist now and see if i can reliably create subcategories. I wonder if I should just use 1 prompt to merge subcategories or something like that. I definitely think using LLM judgement as a post-event-extraction step is pretty much a hack for getting this to work quickly, but i dont want to run another prompt on every post. if i could figure out how to reduce the call-count it would help a lot.
some measure of whether the event reference is unambiguous would be nice. a lot of these references are just too vague to ever tie to one event. Then in short term i would filter out the vague ones and later in the future i would find a way to process them and see if any can be tied to our known event list.
the prompt will definitely need a lot of work and especially a lot of examples.
prompt also needs event wording guidance, just like the claim extraction prompt. we dont want references like "this post says," "the user claims"
we need these categories: aneecdote, reference to ambiguous event, reference to clear event (where we clearly define what qualifies as an event)
also need event type clarity. events with place & time, events with just time. We need to be clear about when an event simply doesn't have a place or time, and when it should have one but that info is missing or uncertain
