# merge_3 assessment

Strategy: `high_precision_v3` — keep only mentions that can be tied to a
**named happening** (a place-anchored wave, a named person/place case, a
named policy act, a public injury story, or a publication). Extract run_11
is frozen; this is a keep/drop + grouping pass.

Input: 11,210 mentions → dropped 4,353 → grouped 6,857 → **2,955 clusters**.
(merge_2: drop 2,276, 3,198 clusters.)

Alignment: a timeline of events related to MMR vaccination, plus the posts
that talk about those happenings. Inclusion means “this mention is about a
thing that took place and we can name it.” Exclusion means “status
commentary, anonymous anecdote, or a happening we cannot locate.”

## How keep/drop was done

Same grouping as merge_2 once a mention is kept: waves by
`type + country + admin1` (titles off); named types by
`type + country + admin1 + actor_or_title_slug`. Date is still not a join
key. `posted_at_proxy` year on 2,013 news-like placed unknown-time rows.

V3 only changes **who is allowed into grouping**:

| Type | Keep | Drop |
|---|---|---|
| `outbreak` / `cases` | subnational place (admin1/admin2/locality/site), **or** country on the small-wave list (Samoa, American Samoa, Tonga, Fiji, Marshall Islands, Bangladesh, Singapore, Hong Kong) | no place; vague-place title (`nationwide`, speaker’s city); country-only on large countries |
| `notable_case` | any place hook **or** a filled `actor` | neither |
| `vaccine_injury`, `study_publication` | all | — |
| `institutional_action` | subnational place, **or** title/summary matches mandate / elimination / VICP / court / schedule / appoint / recall / license / exemption | country-only + actor in `generic_national_actors.json` (CDC, WHO, Trump, RFK, HHS, …) and no regex |
| `other` | — | all |

Drop reasons: `vague_unplaced_wave` 2,249, `national_status` 1,651,
`national_institution` 220, `unanchored_case` 133, `leftover_other` 73,
`vague_place_title` 27.

## What the keep set looks like (true positives)

Named-place waves are the core of the timeline and they did collapse
paraphrases:

- Texas 431, South Carolina 280, Samoa 227 (2019), Alberta 174, Florida 170,
  Pennsylvania 113, England 111, Utah 110, Bangladesh 97, Ontario 88,
  Illinois/Chicago shelter 83.
- Country-only US outbreak (1,223 in merge_2) is **gone** from the keep set.
- Canada / UK measles-elimination loss is kept (title regex `eliminat`).
- Florida mandate removal is kept (subnational + `mandate`).
- Vaccine-injury stays split (48 mentions → 43 clusters): Hannah Poling,
  Samoa mixing deaths, Syria dilution deaths, Violet Skye Rodela.

## False positives still in the keep set

These are the filters that still leak. Ordered by how much they pollute a
timeline.

### 1. State-level waves still mix distinct happenings (largest issue)

Same as merge_2; V3 does not split years or sites.

- **Texas 431:** West Texas 2025 + Dilley ICE 2026 + 2024/2020 leftovers.
- **South Carolina 280:** 2025 and 2026 in one cluster.
- **Florida 170:** university / Ave Maria / 1995.
- **California 73:** Disneyland 2014–15 + Shasta 2024/2026.
- **New York 38:** 2019 NYC/Rockland + later spillover.
- **Pennsylvania 113:** measles plus a **Legionnaires** title (wrong disease).

Fix without reopening national tallies: split **stated year** on
state-level waves after this filter (unknowns stay in a residual beside the
dated peak, not in the other year’s cluster). Optionally promote
`locality`/`site` (Dilley, Disneyland, Chicago shelter) to a child event
**after** the year split, only when that site has enough mentions.

### 2. Locality/site without admin1 groups at country grain

`has_subnational` is true if `site` or `locality` is set, so the mention is
**kept**. Grouping only falls back to finest place when **both** country and
admin1 are empty. Result:

- 32 “US outbreak, no admin1” mentions: Dilley ICE mixed with “ICE camps”,
  “public spaces”, “schools”, and “United States reports 588 confirmed cases
  in 2026” (site=`CDC` / `United States`).
- 16 “UK outbreak, no admin1”: Liverpool + North London + Birmingham +
  **polio in wastewater**.

This is a high-precision leak. Fix: (a) count `site` as a place hook only
when it looks like a facility, not an org or “public spaces”; (b) when
country is set and admin1 is empty, key waves on locality/admin2, not on
country alone. That recovers Dilley/London as their own events without
bringing back “measles in the United States”.

### 3. Institutional regex is too wide

`schedule`, `appoint`, `approv`, `licen`, `fired`, `eliminat` keep real MMR
policy **and** unrelated acts, then actor-key piles them:

- **CDC ×89:** US elimination loss (want) + childhood schedule cuts (want) +
  “CDC reports measles case counts” (national surveillance, should drop) +
  Abrysvo on the schedule (weak).
- **Trump ×28:** RFK/HHS appointment (borderline related) mixed with other
  appointments.
- `approv`/`licen` keep RSV pregnancy approval, anal-cancer immunotherapy,
  PRIORIX license (the last is MMR-related; the others are not).
- `fired` keeps generic CDC/NIH workforce cuts.

Fix without keeping CDC case-alerts: narrow the regex to
elimination / measles-free, mandate, VICP/vaccine court, ACIP+MMR/MMRV,
“split MMR”, executive order on childhood schedule. Drop even on regex match
if the title is surveillance (`confirms … cases`, `health alert`, `case
counts`). Stop grouping institutional by actor `cdc`; use title slug so
elimination ≠ schedule ≠ case counts.

### 4. `study_publication` and anonymous `vaccine_injury` are not timeline-grade

All 306 study mentions and all 48 injury mentions were kept. Wakefield /
Lancet retraction is a real happening. Most studies are “a paper exists”,
not something that took place in the world. Injury still includes unnamed
neurodevelopmental claims next to Poling / Samoa nurses.

For a high-precision timeline: keep study only if the title is a retraction,
a named landmark (Wakefield, Danish MMR-autism), or a court-cited review.
Keep injury only if actor is a named person or a named incident (Samoa
mixing, Syria dilution). That is a keep-rule, not a prompt change.

### 5. Generic site-only waves

192 unplaced outbreak mentions survived because a site/locality was filled
with no country: “measles outbreak at a daycare/school/hospital”, army
bases, a cruise-ship hantavirus. Disneyland ×4 and Dilley-without-country ×15
are real and currently split from the California/Texas clusters. Generic
“at a school” should drop; named sites can wait for the place-complete
prompt pass.

---

## Excluded buckets

### A. `vague_unplaced_wave` — 2,249 (outbreak 2,117, cases 132)

What: “Measles outbreak” ×186, “Measles resurgence”, “current measles
outbreak”, plus compound countries cleaned to empty (`Worldwide`,
`United States and Canada`).

Include on the timeline? **No.** There is no happening to attach a post to.

How to include a subset without the rest: don’t. If a later prompt fills
country+admin1, those rows will be kept on a re-merge; inventing place here
would recreate merge_2’s US blob.

### B. `vague_place_title` — 27

What: “nationwide”, “speaker’s city”, “multiple states”, even when country
is US.

Include? **No.** Same as A. A US nationwide title is a tally, not a wave.

### C. `national_status` — 1,651 (US 1,207, Canada 157, UK 81, Mexico 40, Romania 17, NZ 13, …)

What: country-only `outbreak`/`cases`. This is the merge_2 US 1,223 blob and
its cousins: “U.S. measles cases reach a 35-year high”, “measles outbreak in
Canada”.

Include US/Canada/UK/Australia/Japan country-only? **No.** Those posts are
about national burden or “measles is back,” not a single event. Putting them
on the timeline would steal posts from Texas/SC/Alberta/England waves.

Include some smaller-country rows? **Yes, selectively.** These look like
real country-as-wave happenings that the allowlist missed:

- **New Zealand 2019** (and a 2025 tally mixed in) — 13 mentions
- **Romania 2023–2025 epidemic** — 17
- **Mexico 2025–26** — 40 (year mix risk, same as Texas)
- **Philippines** deadly outbreak — 7
- **Ireland** (including a 3-child-death wave) — 6
- **Mongolia** >14,000 cases — 1
- Yemen / Madagascar / Uganda — small n, possibly real

How without the US blob:

1. Prefer an **allowlist add** (NZ, Romania, Ireland, Mongolia, maybe
   Mexico) — same mechanism as Samoa. Do **not** use “has a year in the
   title”: US “35-year high in 2026” would come back.
2. Or a **tally-language drop** (`cases reach`, `35-year high`,
   `nationwide`, `across the United States`) plus allowlist. Still do not
   keep US/CA/UK/AU country-only.
3. Do not treat `USA` as missing from aliases (7 leftover `USA` rows
   should have been `United States` then dropped as national_status anyway).

Mexico at 40 mentions will over-merge years if allowed as one country wave;
treat like Texas (year split after keep).

### D. `national_institution` — 220

What: institutional, no subnational place, actor in the generic-national
list, title missed the keep regex.

**Should include (real MMR-timeline acts):**

- **UK loses WHO measles-free status** ×7 — dropped because the regex is
  `eliminat`, not `measles-free`. Canada “elimination status” was kept;
  this is the same event family.
- **Trump executive order to split MMR / limit childhood vaccines** ×~14 —
  no `mandate`/`schedule`/`appoint` in many paraphrases.
- **ACIP votes** on MMR/MMRV, hep B birth dose, disbanded/reconvened —
  actor `acip` is generic-national and titles often lack `schedule`.

**Should stay out:**

- CDC confirms additional US cases / case counts / health alerts / WHO
  global case data — surveillance, same as national_status.

How without the alerts:

- Expand keep phrases: `measles-free`, `measles free`, `executive order`,
  `split (the )?mmr`, `acip` + (vote|recommend|disband|reconvene).
- Add a **surveillance veto** that wins over the keep regex:
  `confirms? … cases`, `health alert`, `case counts`, `issues? (a )?measles warning`.
- Key remaining institutional clusters on title slug, not `cdc`/`trump`.

That recovers measles-free loss and the MMR split order without bringing
back “CDC confirms US cases.”

### E. `unanchored_case` — 133

What: `notable_case` with no place and no actor. Almost all are “Child dies
of measles”, “first measles death in a decade”, “three measles deaths in
2025.” Also a dog, Angela Luff chickenpox.

**Should stay out:** anonymous deaths. They cannot be tied to a named event
(and often belong on the Texas/Alberta wave as mentions, not as their own
events).

**Should include:** named people whose `actor` was left empty:

- Daisy Hildebrand died in the 2025 measles outbreak
- Kayley Fehr died in the 2025 measles outbreak

How without “child dies of measles”: keep only if the title has a
person-name (two capitalized words) **or**, better, a later prompt fills
`actor` for named cases. Do not keep count-only deaths (“three measles
deaths reported in 2025”) — those are wave statistics.

Once actor is filled they should **join** the Texas/Alberta 2025 wave (or
sit as child notable_cases of that wave), not form a parallel anonymous
death event.

### F. `leftover_other` — 73

What: extract leftover. Mix of:

| Worth a timeline slot? | Examples | How to include without the rest |
|---|---|---|
| Yes, as local ops / consequence | Free MMR clinic New Albany; Leicester clinic; Kano campaign; Canada Priorix/MMR2 shortage; Texas vitamin A toxicity (Lubbock); Salt Lake / Boulder **exposure** events; Byron Bay measles parties; Cebu parents declining MR; Samoa tainted batch (2019 mixing) | Do **not** blanket-keep `other`. Recode at extract to `institutional_action` / `outbreak` / `cases`, **or** merge-keep `other` only when subnational place **and** title matches `clinic\|shortage\|exposure\|campaign\|measles part(y\|ies)\|vitamin A tox`. Exposures should have been `outbreak`/`cases`. |
| No | Glyphosate in vaccines, Wim Hof flu trial, Jay Varma party, Del Bigtree, RFK trademark, CDC shooting, salad parasite, gamblers on outbreaks, Corbevax, pancreatic mRNA trial | Leave dropped. |

Samoa “tainted batch” / nurses mixing is already in `vaccine_injury` under
other titles; including the `other` paraphrase is an under-merge fix
(alias), not a new keep bucket.

---

## Suggested next merge (not done)

High-precision tighten, still no re-extract:

1. Finest-place key when admin1 is empty (Dilley, London) so they are not
   a US/UK blob; site-hook only for facility-like sites.
2. Stated-year split on admin1 waves (Texas 2025 vs 2026).
3. Narrow institutional keep + surveillance veto; title-slug not actor for
   CDC/Trump.
4. Allowlist NZ / Romania / Ireland / Mongolia (Mexico optional) for
   country-as-wave.
5. Named-person rescue on `unanchored_case`; still drop anonymous deaths.
6. Drop or quarantine `study_publication` except retraction/landmark;
   require named actor on `vaccine_injury`.

Prompt notes (later, not this merge): complete country+admin1 when
locality/site is set; `actor` for named deaths; `measles-free` as
elimination wording; `USA` alias already exists — leftover `USA` country
rows are extract variants.
