# Telegram channel discovery probe

Experiment to expand routine Telegram scraping beyond the original 22
hand-picked seed channels. Code: `python -m services.telegram_monitor.probe_channels`.
Full run artifacts (gitignored): `services/telegram_monitor/probe_results/`.

## What we tried

Three discovery approaches, then a relevance score on every unique channel:

1. **Forwards** — last 500 messages on each seed; tally `fwd_from` channel origins.
2. **Similar channels** — Telegram `GetChannelRecommendationsRequest` per seed.
3. **Keyword search** — public discovery. `messages.searchGlobal` only searches
   this account’s own dialogs (the monitor does not join channels), so it
   returned 0. Replaced with `channels.searchPosts` (paginated full-text,
   ~10 free queries/day, Premium), plus free hashtag search and
   `contacts.search` on short names.

Scoring sampled ~50 recent messages per channel and computed:

- **keyword hit rate** — share of sampled posts matching `taxonomy.vaccine_term`
  names used for this probe (30 curated measles/MMR + vaccine/anti-vax terms)
- English rate (ASCII-letter heuristic)
- messages/day over the sample window

The successful run (2026-09-14) scored **2667** channels: 157 from forwards,
581 from similar, 2130 from keyword search (with overlap).

## Selection criteria (first batch)

Keep a channel if all of:

- public `@username` (required by `CHANNEL_LIST`)
- not already in the seed list
- English rate ≥ 0.9
- keyword hit rate ≥ ~0.4
- still posting (≳ 0.2 messages/day in the sample)
- title is not a companion “Chat”
- on-topic vaccine / anti-vax / injury / CHD-NVIC-FLCCC-adjacent discussion
  (drop Q/medbed, generic MAGA news, and non-English names that slipped the
  English heuristic)

Prefer channels found by **more than one** approach. Keyword-search-only hits
are allowed when they clearly match the topic (injury/victim/choice orgs).

## First 20 added to monitoring

| Username | Why |
|---|---|
| `@TruthAboutCOVID` | forwards+similar, hit rate 0.96 |
| `@childrenshd` | CHD; forwards+similar |
| `@DiedSuddenlyConnecticut` | injury/sudden-death feed; forwards+similar |
| `@P_McCulloughMD` | distinct from existing `@PeterMcCulloughMD` |
| `@IMA_Health` | Independent Medical Alliance; forwards+similar |
| `@arkmedic` | high activity; forwards+similar |
| `@DrDavidMartin` | forwards+similar |
| `@covidvaccineadversereactions` | similar; hit rate 1.0 |
| `@covid19vaccinevictims` | similar; hit rate 1.0 |
| `@childcovidvaccineinjuriesuk` | similar |
| `@NVIC_VaccineFreedom` | NVIC |
| `@DrPaulMarik` | similar |
| `@HarveyRisch` | similar |
| `@TheHigh_Wire` | Del Bigtree; forwards |
| `@vaccinechoicecanada` | keyword search |
| `@CaliVaxChoice` | keyword search |
| `@CHDCanada` | keyword search |
| `@RemediesCV19VaxxVictims` | injury treatments; keyword search |
| `@antivaxxfacts` | keyword search |
| `@VigilantFox` | forwards+similar; slightly lower hit rate, high overlap with seeds |

## Expanding later

Do **not** sort the approach tables by forward count or raw keyword hits —
those lists are dominated by political news and Q channels.

To grow the list:

1. Lower hit-rate floor to **0.25–0.35** while keeping English ≥ 0.9, activity
   ≥ 0.2 msgs/day, and skipping `Chat` titles. That is the next ~20–40 from
   `probe_report.md` “Scored channels”.
2. Take **multi-method** channels first (`forwards`+`similar`), then
   keyword-search-only orgs (vaccine choice, CHD chapters, named physicians).
3. Spot-check duplicates (`@RogerHodkinson` vs `@RogerHodkinsonn`,
   `@HonestMedicine` vs `@IMA_Health`, extra McCullough handles) and skip
   near-idle feeds.
4. Re-run the probe later for new `searchPosts` pages (pagination is free;
   new queries cost 1 of ~10 daily slots). Scoring will resume from
   `checkpoint.json` and only score newly discovered channels.

After adding handles, run `python -m services.telegram_monitor --prod`. New
channels backfill from epoch on the first scrape.
