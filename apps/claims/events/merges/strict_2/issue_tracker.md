# Strict-join issue tracker

Review of `run_14` through the grain-aware strict join (`strict_2`).
20,928 mentions, 853 joined groups. Patterns below, not individual mentions.

How this was read: every joined group of 20 or more mentions, then person keys whose slug is a role or a description, then a second pass for an agency used as the person and for several exposure sites inside one person group. A screen that compared years in the title to the stated year was dropped. Almost all of its hits are comparison years ("since 2000", "first since 2019", "outpace 2025").

## Working as intended

- Exact-title institutional joins are one wording each. "Measles declared eliminated in the United States" (2000, 53), "Canada loses measles elimination status" (32), "Andrew Wakefield loses his medical license" (45), "Measles vaccine introduced in 1963" (22).
- Wakefield publication (161) and retraction (163) stayed apart.
- South Carolina measles outbreak, 2025 (21) is one wave.
- US measles case counts for 2025 (134) and Pennsylvania case counts for 2026 (22) are running tallies of one place and year, including "highest since 2000".

## Patterns

### 1. A count of many outbreaks joined as one outbreak

Stage: extractor. The strict key then does what it is told.

The text says there were N outbreaks, and the mention is typed `outbreak` at country grain, so every such mention in that country and year becomes one event.

- United States, 2025: 85 mentions, 24 of them a count of outbreaks ("35 measles outbreaks", "49 measles outbreaks across 45 jurisdictions").
- United States, 2024: 76 mentions, 26 of them plural ("16 measles outbreaks", "outbreaks reported in several U.S. cities").
- United States, 2026: 57 mentions, 8 plural ("multiple measles outbreaks", "surge across multiple U.S. states").
- United States, 2019: 26 mentions, including "outbreaks drive a severe U.S. year".

### 2. A description is being used as a person's name

Stage: extractor, and the strict rule. These mentions have a complete person key, so they never reach the LLM.

The actor is "6-year-old girl", "a child", "parents", "unvaccinated child". Each wording becomes its own event, and different events that share a wording join.

Same Texas death, split across keys: `6-year-old-girl` (10), `six-year-old-girl` (6), `school-aged-child` (9), `a-child` (9), `parents` (5), `young-girl` (4), `daughter` (2).

Different events joined because they share a description:

- `children` (10): Texas measles deaths, a South Carolina brain-swelling report, and "more than 200 children die from flu".
- `two-children` (15): the Texas deaths, and "two children brought measles to New Orleans".
- `unvaccinated-child` (7): a Texas death, North Dakota's first case since 2011, a Bay Area first case, and an LAX exposure.
- `pregnant-woman` (3): a Utah measles birth, an HIV/AZT death, and a tuberculosis case.
- `two-infants` (3): Lancaster County measles deaths and two infants dead in a Louisiana pertussis outbreak.
- `6-year-old-girl` also contains "dies after contracting mycoplasma".

About 34 description keys have two or more mentions (147 mentions). Many more are singletons of the same kind (`her-daughter`, `texas-child`, `baby-daughter`), so the same death is also under-merged.

### 3. The actor is the agency, the official, or the place

Stage: extractor. The strict rule then treats that string as the patient. These also get a complete key and skip the LLM.

- `pennsylvania-department-of-health` (22), `pennsylvania-health-department` (16), and `pennsylvania-officials` (4) are the same Lancaster County deaths.
- `pennsylvania` (21) is that death again, keyed on the state name.
- `stephen-diamantoni` (9) and `diamantoni` (5) are the coroner, not the infant. `lancaster-county-coroner` (7) is the same dispute.
- `virginia-department-of-health` (6) is a measles exposure at Dallas airport.
- `north-carolina-department-of-health-and-human-services` (3) is the state's first case of 2025, not a person.
- `covenant-childrens-hospital` is a vitamin A toxicity report, keyed on the hospital.

### 4. One visitor description joins separate exposure sites

Stage: strict rule. The extractor often emitted one event per site, which is what the prompt asks, and the join undoes it because the actor string is the visitor.

- `a-person-who-visited-the-hospital` (4): CHOP neonatal ICU, family lounge, atrium, and cafeteria.
- `a-person-with-active-measles` (3): SF airport, a Trader Joe's, and a halal market.
- `person-with-measles` (4): Mitchell International and an Amtrak trip through six California counties.
- `man-who-caught-measles-after-travelling-from-overseas` (2): a restaurant in Shibuya and a hotel in Shinjuku.

### 5. The place on the mention is a neighbor of the event

Stage: extractor.

- Samoa 2019 (166) includes "New Zealand's 2019 measles outbreak spread to Samoa".
- American Samoa 2019 (3) uses the Samoa toll ("killed 83", "sickened 5,700").
- New York 2019 (23) includes "New York and New Jersey".
- Texas 2025 (96) includes "spread from western Texas into southeast New Mexico". That one may be the same wave crossing a border.
