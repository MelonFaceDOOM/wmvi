# Prompt revision sample

80 post-chunks. The first 57 are frozen from extract run_11 (`measles2`).
23 more, copied from run_12, cover the later prompt: place scope, two sites
in one city, a city that is also a state, cases versus outbreak, publication
versus retraction, and the keep/drop lines. Use this set for repeated prompt
tries. The chunks are copied out of the run `documents.jsonl` files, so a
prompt change does not depend on re-sampling the corpus.

Expected answers are in [`labels.jsonl`](labels.jsonl), in the same order as
`posts.jsonl`, except `9771068:0`, which is in the posts and has no label.
Each row's `events` list is empty when the
chunk should not produce a mention. An event with `"optional": true` may be
absent: leaving it out is not a miss, and emitting a match is not an extra.
`"also"` lists other place strings that also pass, besides the primary value.
`identity` joins the same happening
across different wording; titles are not labeled. Place and date fields are
only what that chunk's text supports, so two rows can share an identity and
still differ (Dilley with and without the word Texas). The notes below say
why the post is here and what run_11 did. They are not the labels.

`posts.jsonl` is the same shape as a run's `documents.jsonl` (no extra
fields). To score a new prompt, copy it into a fresh run directory and
extract only that file:

```bash
mkdir -p apps/claims/events/runs/prompt_try_1
cp apps/claims/events/prompt_sample/posts.jsonl \
   apps/claims/events/runs/prompt_try_1/documents.jsonl
python -m apps.claims.events.run --corpus measles2 \
    --phases extract,report \
    --run-dir apps/claims/events/runs/prompt_try_1
```

`--phases extract` loads the existing `documents.jsonl` and does not resample.

## What the set is for

The notes in [`prompt_improvements.md`](../prompt_improvements.md) split the
work into three reference kinds (anecdote, ambiguous event, clear event),
plus place/time that is truly absent versus place/time the text actually
gives, plus the same happening said more than one way. The first 43 chunks
hit those, and the concrete extract failures already seen (relative dates
left unknown, site with no state, `USA` / `England` / `U.S` / `Hawaiʻi`,
country-only US tallies called outbreaks, named deaths with no actor,
measles-free vs elimination). Fourteen more cover acts that are not
identifiable: a vote with no decision, an unresolved “it,” a personal
requirement, a vague accusation, and a standing vaccine list.

Posts are grouped by the question they answer. Several would also fail a
second check; the note says so.

## Same happening, different wording

A prompt that emits a different title or a different place slot for these
will not be joinable later. Label the event identity, not the wording.

**West Texas 2025 wave**

| doc | Why |
|---|---|
| `1523075:0` | News headline: rural West Texas, at least 49 cases, 200–300 suspected. Clear outbreak, state named, no calendar date. Run_11 kept it as Texas outbreak with `date_precision=unknown`. Do not copy `posted_at` (2025-02-15) into `stated_start`. |
| `1609747:0` | Same wave, finer place: “Gaines County last week”, 393 cases, Seminole. Tests “last week” against `posted_at` 2025-04-28, and county over state. Run_11 stored `stated_date_text=last week` and left precision unknown. |
| `1625673:0` | Comment, not a news lead: “I was living in Texas last year when we had that huge outbreak.” Posted 2026-02-05, so “last year” is 2025. Personal frame around a public wave. Run_11 did resolve year 2025. |

**Dilley detention outbreak (2026), not the 2025 West Texas wave**

| doc | Why |
|---|---|
| `1248675:0` | Headline only: “Measles outbreak reported at ICE’s Dilley family detention facility.” Run_11 filled Texas + Dilley. |
| `1248680:0` | The same headline on another post. Run_11 kept locality Dilley and dropped country/admin1. Pair with the previous row: identical text should not flip the place stack. |
| `1256967:0` | Different wording: “measles outbreak in the Dilley concentration camp.” |
| `1592419:0` | Morning digest. The event is one clause (“Texas facility hit by measles outbreak” / detention in Dilley, Texas) inside a longer newsletter. Tests buried clear events, and an explicit “Dilley, Texas” stack. |

**Canada / UK measles status** (one act family; wording is “measles-free” or “elimination status”)

| doc | Why |
|---|---|
| `1477362:0` | Telegram headline: “Canada Loses Measles-Free Status.” No calendar day in the text. |
| `4777146:0` | YouTube comment: “Canada just lost its measles elimination status,” plus a Health Canada aside that is not a second event. |
| `8076157:0` | Reddit headline, same Canada act, “elimination status” again. |
| `1239232:0` | Parallel act, other country: “UK loses WHO status as measles-free.” Run_11 typed this institutional, which is right; the point is the wording, not a new type. |

**Disneyland traveler case**

| doc | Why |
|---|---|
| `1246892:0` | One post, three Southern California infections “in the last week,” including LAX then Disneyland, and an Orange County traveler. Posted 2026-02-01. Run_11 split several mentions and left “in the last week” at precision unknown. Place stack for Disneyland should be site + California + United States, not site alone. |
| `1247688:0` | Shorter headline of the Orange County / Disneyland / California Adventure case. Same happening as one slice of the post above. One visit is required. A second event at California Adventure is optional. |

**Named Texas deaths**

| doc | Why |
|---|---|
| `7970758:0` | Daisy Hildebrand, died 2025-04-03, University Medical Center, Lubbock. Place and day are in the text. Actor is Daisy, not the hospital. |
| `3614434:0` | “Measles killed Daisy Hildebrand. Measles killed Kayley Fehr,” confirmed by Texas DSHS. Two public deaths, no dates. Run_11 sometimes left `actor` empty on these names and the merge then dropped them as unanchored. |

**Hannah Poling**

| doc | Why |
|---|---|
| `8599466:0` | Public case only: Hannah Poling, autism attributed to vaccines, family paid. `vaccine_injury`, actor Hannah Poling. |
| `9771068:0` | Same public case, after a first-person “my son got injured” anecdote. The anecdote is not an event. Hannah is. Run_11 emitted both the personal injury and Hannah. |

## Clear events (keep)

| doc | Why |
|---|---|
| `8741601:1` | Interview chunk: Samoa outbreak “83 deaths,” and “two deaths because local nurses injected babies with incorrectly mixed vaccines.” Two public happenings (outbreak + vaccine injury). Noisy transcript. Do not date them from `posted_at` 2024-08-23; the chunk does not say 2019. |
| `1597143:0` | Headline does say 2019: “2019 Samoa trip before measles outbreak,” posted 2026-02-06. Year is 2019. Samoa is the wave (small country). |
| `7954895:0` | “Explosive measles outbreak kills 46 children in Bangladesh.” Country is the place. No date in the text. |
| `271137:0` | ACIP voted to recommend MMR and varicella separately. Institutional act; no geography required. |
| `272798:0` | One post, several ACIP decisions (Hep B, MMRV, COVID) from “the last 2 days.” Tests the cap of 4, distinct acts, and a relative window. COVID votes are still institutional acts in the text; do not drop the MMRV one because others are present. |
| `4403786:0` | “The Lancet … fully retracted the paper.” Study retraction. `site=The Lancet` is the journal, not an outbreak place. |
| `9811853:0` | Miami County measles case, exposure at Englewood Kroger, clock time “12:45 to 4pm February 13th.” Run_11 copied a full datetime into `stated_start` and left country empty. Want a date `YYYY-MM-DD` and United States / Ohio if the text supports Ohio — Miami County and Englewood are in the text; country is not the word “United States,” so this is the place-completion case. |
| `1631378:0` | Snohomish exposure list: several named sites with clock times (Stellar Kids Dentistry, Pathfinder Kindergarten). Multi-event, datetime must not stay as `2026-01-08T09:30:00`. Run_11 emitted one notable_case per site with a datetime. |
| `1684294:0` | “DC Health warns” March for Life attendees may have been exposed. Admin1 is District of Columbia, not “Washington” and not “Washington, D.C.” |

## Anecdote — emit nothing

| doc | Why |
|---|---|
| `138801:0` | “My kid had a fever … from the MMR vaccine last week.” Run_11 correctly returned no events. “Last week” must not become an event date. Keep this as a regression: a rewrite that starts extracting personal fevers fails. |
| `174433:0` | “My 15 month old got the MMR vaccine yesterday” and might get a fever. Correctly empty in run_11. |
| `2235488:0` | “My mom and sister are anti-vax because two of my sister’s kids are autistic,” then a complaint that Disneyland lets people in without MMR. Run_11 minted `institutional_action` “Disneyland admits visitors without MMR” with site only. That is an anecdote, not an act. |
| `9771068:0` | Listed above. The “my son” half of this comment is the anecdote; Hannah Poling is not. |

## Ambiguous — do not invent a place or a wave

| doc | Why |
|---|---|
| `1660077:0` | “We had a statewide measles outbreak when my son was 9 months old.” No state. Run_11 still emitted an outbreak. Personal life context, not a nameable wave. |
| `1643098:0` | “We had an outbreak at a daycare right by the lab where my husband and I work.” No town. Run_11 emitted “Measles outbreak at a daycare.” |
| `1188626:0` | Relationship post: “a measles outbreak in our state” in “September of this year.” Run_11 titled it “speaker’s state” and left place empty, with a month date. Still not a timeline event until a state is named. |
| `195845:0` | Title only: “Families urged to get MMR jabs to stop measles outbreak.” No place. Run_11 emitted `outbreak` / “Measles outbreak”. |
| `1611342:0` | Dan Bongino chunk. “There’s a measles outbreak” is rhetoric about a New York Times headline on RFK and diet, not a located wave. Run_11 emitted a placeless outbreak. |
| `243401:0` | Parenting question: “in the US, where measles is making a comeback.” National mood, not a named outbreak. Run_11 called it “Measles outbreak in a U.S. state” with country United States and no state. |
| `1314884:0` | “CDC Warns of Significant Measles Uptick Across US.” Surveillance tally. Same failure mode as the 1,223 country-only US mentions in merge_2. |
| `326752:0` | “measles spreads nationwide,” “USA students,” vaccination rates. Nationwide is not a place. Run_11 kept country United States. Also the `USA` spelling. |
| `1625193:0` | Two different failures in one comment: UK “measles-free (eliminated)” status (real institutional act, measles-free wording) and “thousands of confirmed cases in countries like Mexico, the U.S, and Canada.” The second is not one outbreak. Run_11 put `country=Mexico, United States, Canada` on a single outbreak. `U.S` should be United States if a country is emitted at all; it should not be glued to Mexico and Canada in one `country` string. |

## Not an identifiable act

Run_11 turned each of these into `institutional_action`. A specific ACIP
decision stays in the clear-event rows above (`271137:0`, `272798:0`): those
say what was decided. These do not.

**Vote with no decision.** “Voting on” a product, or a meeting that “will
vote,” is not an event until the text says what passed or failed.

| doc | Why |
|---|---|
| `6156474:0` | “I screwed up and meant to link a specific article that also talked about them voting on measles, mumps and chicken pox vaccines for kids 0-5.” No panel name, no outcome. |
| `6155724:0` | Same failed link, different comment: “the panel is voting on recommendations for measles, mumps and chicken pox vaccines for kids 0-5,” plus a Spokesman URL. Still no decision. Run_11 titled it “Panel votes on vaccine recommendations for young children.” |
| `5908413:0` | “ACIP will vote on three shots… It’s not clear what the ACIP votes will entail.” Future meeting, and the comment says the substance is unknown. Run_11 emitted three institutional mentions anyway. |

**Unresolved “it” / “the vaccine.”** Do not name a product the chunk never names.

| doc | Why |
|---|---|
| `2849895:0` | “It’s now been introduced with the MMR and is standard in Europe, Australia and Canada. It’s a well established and safe vaccine.” The rest is about spots and nursery. The product is not in the chunk. Run_11 titled it “Vaccine introduced with MMR and standard in several regions.” |
| `1626274:0` | “That was when they decided the older single dose vaccines weren't effective enough. It was also the year Wisconsin decided to mandate the vaccine.” No year, no “they,” and “the vaccine” is only tied to measles later as commentary about elimination. Run_11 emitted “Wisconsin decided to mandate the vaccine.” |

**Personal requirement.** A rule the speaker lived under is not a public act.

| doc | Why |
|---|---|
| `3606703:0` | “my university needed everyone to either be vaccinated (just for serious stuff like measles) or submit a form.” No university name. Run_11: “University vaccination requirement with exemption form.” |
| `9411475:0` | “My college (San Diego State University) made me get an MMR.” The school is named; the happening is still the speaker’s shot, not a dated policy change. |
| `8493800:0` | “on the infant door at our daycare it says that everyone entering must have the MMR vaccine. I’m not sure what the actual rules are though.” A sign the speaker noticed. |

**Vague accusation.** An blame with no reported decision.

| doc | Why |
|---|---|
| `3123367:0` | “They also got the COVID variant wrong again at Pfizer's behest.” No date, no named decision, “again.” Run_11 made that sentence an FDA/Pfizer institutional act. Other sentences in the same comment are separate label calls: “The new FDA is requiring new studies…” is a reported change; “The MMRV … was recommended as separate shots” and “long-standing position” are a standing schedule, not a new act; “recently added language for Hep B” is optional. The FDA study requirement is required. |
| `4912054:0` | Comment asserts the CDC ordered MMR documents burned and then “came clean and admitted that they had been lying” about MMR and autism. No date, no source, a narrative accusation. Run_11 emitted both the document destruction and the admission as institutional acts. |

**Standing schedule.** “These shots are mandatory here” describes the current list. A change to that list is a different sentence.

| doc | Why |
|---|---|
| `6649994:0` | “there’s 9 mandatory here in the Czech Republic from approx 2 months old to 11 years old,” then the disease list. Run_11: “Mandatory childhood vaccination requirements in the Czech Republic.” The same comment’s “increase in TB cases here since 600k refugees” is also not a named MMR happening. |
| `6090750:0` | “In Hungary and Romania vaccinations against tuberculosis, diphtheria, pertussis, polio, measles and several other diseases are mandatory.” Current list, two countries, no change. |
| `7393115:0` | “Washington also doesn’t have mandatory child vaccines except for MMR.” A description of the rules, not a bill or a vote. |
| `4858190:0` | “Can't find dates,” then a quote that twelve countries “have universal varicella vaccination policies.” The comment is arguing about adoption, not reporting a decision. Run_11 still minted an institutional act on the Czech Republic among that list. |

## Not an event

| doc | Why |
|---|---|
| `1304078:0` | “Five childhood vaccines test positive for glyphosate.” A lab claim about vials, not a happening with a time and place. Run_11 used `other`. |
| `9995661:0` | Dorset public-health doctor: vaccines “here in England,” then “more cases of measles … popping up across the country.” PSA. Run_11 emitted “Measles cases increasing across England” with `country=England`. If anything is kept, England is admin1 of the United Kingdom, and “across the country” is not a named wave. |
| `321527:0` | “Free Measles Vaccine Clinic in New Albany.” A local clinic can be an event, but the text has a city and no state. Run_11 left country and admin1 empty. Tests “do not guess Ohio” versus “complete the stack.” The prompt note wants completion when the text supports it; New Albany alone does not say which state. |

## Place grain and date shape

Covered above, collected here so a place/date rewrite can be checked in one pass.

| Failure in run_11 | Posts |
|---|---|
| Relative window left `unknown` (“last week”, “in the last week”, “last 2 days”) | `1609747:0`, `1246892:0`, `272798:0` |
| “Last year” / “this year” against `posted_at` | `1625673:0`, `1188626:0` |
| Do not copy `posted_at` onto an old event | `1597143:0`, `8741601:1`, `1523075:0` |
| `stated_start` stored as a datetime | `9811853:0`, `1631378:0` |
| Site or city with no admin1/country | `1248680:0`, `2235488:0`, `321527:0`, `9811853:0` |
| Disneyland stack (site, California, United States) | `1246892:0`, `1247688:0` |
| Two counties in one `admin2` | `1624674:0` (“Waukesha County and Dane County”; the sentence also says this case is not linked to those outbreaks) |
| `country=England` | `9995661:0` |
| `USA` / nationwide | `326752:0` |
| `U.S` inside a multi-country string | `1625193:0` |
| `admin1=Hawaiʻi` (mumps case; okina vs Hawaii). Also a non-measles MMR-disease case, so the type decision is part of the label. | `312371:0` |
| District of Columbia, not Washington | `1684294:0` |
| Journal name in `site` | `4403786:0` |

## Added for the later prompt

These 23 are real run_12 chunks. One or two per gap is enough to see whether
the new rules fire. They are not a rate. Every gold event has `place_scope`
and `study_action`, and the scorer grades both.

`labels.jsonl` still has no row for `9771068:0`, which remains in
`posts.jsonl`.

**Place scope**

| doc | Why |
|---|---|
| `1625866:0` | A named outbreak in Canada (country only, `at_named_place`) and the 10 Nov 2025 measles-free loss (`countrywide`). The case totals stay inside those two. |
| `1489995:0` | “Worst measles outbreak in decades” in the US, no state: one `countrywide` outbreak. The Americas elimination, the worldwide surge, and the Latin America list are not events. |
| `168542:0` | Title is “Canada's Measles Outbreak.” The body never reports an outbreak. Empty. |
| `1677634:0` | US 1,267 and Canada 3,170 are country-wide counts (empty). Mexico’s cases centered in Chihuahua stay, as one state outbreak. |
| `3307008:0` | Efficacy list, including “eradicated smallpox worldwide in 1980.” Empty. Not a global event. |

**Two sites, and a city that is the state**

| doc | Why |
|---|---|
| `1670254:0` | Carbon County wave is an outbreak. Rawlins Walmart and Michael’s Big City Steak House on 29 July are two exposure notices, each a notable_case. |
| `1596009:0` | Headline only: Singapore orders isolation as infections rise. Country, admin1, and locality are all Singapore. |

**cases versus outbreak**

| doc | Why |
|---|---|
| `1681613:0` | Austin “hasn’t had an outbreak” and has reported 2 cases as of 14 July. `cases`. |
| `9768455:0` | “Cases of measles here in Oklahoma” today. No wording that the count is separate from a wave, so `outbreak`. |

**Publication versus retraction**

| doc | Why |
|---|---|
| `1669436:0` | Wakefield published a study. This sentence does not say retracted. |
| `213665:0` | 1998 publication, the retraction, and the lost licence are three events. The Denmark, Japan, U.S., and UK studies are citations. |

**Keep, including inside a rant or a headline**

| doc | Why |
|---|---|
| `8304472:0` | Two girls died in hospital in Texas, inside an RFK commentary. Those deaths are required. The Texas wave in the headline is optional. |
| `8545540:0` | Defunding Gavi, and Samoa 2019. The Disneyland clause is not that outbreak. |
| `7532555:0` | UK withdrawal of the licence for the separate vaccines. |
| `11404964:0` | Headline is the whole chunk: RFK Jr tells US families to vaccinate. |
| `7509061:0` | Second dose added in 1989 is required. The 1971 introduction is optional. The papers are not named publications. |
| `6701289:0` | Auckland 2019 cases, and GPs who could not get vaccine stock. A shortage is `institutional_action`. |
| `1638284:0` | Dr. Mark Joffe’s resignation in April. No year. The Hinshaw sentence is not an event. |
| `9836602:0` | Measles and rubella award, 6 March 2024, American Red Cross Headquarters, Washington, DC. |
| `9914208:2` | AstraZeneca compensation claims in the High Court, 20 January 2024. A lawsuit, not `vaccine_injury`. |

**Drop**

| doc | Why |
|---|---|
| `4302770:0` | Last child death was 2003. No person, no place. |
| `11624549:0` | Comma-separated healthcare grievances. |
| `10534036:6` | Title “Florida Welcomes Back the Measles.” The body does not report cases. |
