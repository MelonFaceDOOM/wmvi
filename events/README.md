# Event history

Curated history of vaccine-related real-world events (outbreaks, policy changes,
regulatory actions, study publications) plus the claim/article evidence for each.

**File mode today, DB later.** Record fields mirror
[`db/potential_migrations/026_event_history.sql`](../db/potential_migrations/026_event_history.sql)
so promoting the catalog is a load, not a translation. The migration is
*proposed, not applied*.

```python
from events import load_events, load_subjects, claim_key_links

events = load_events()                                  # validates on read
links = claim_key_links(events, corpus="measles2")      # claim_key -> event_key
```

---

## Why this lives outside `apps/claims/`

Everything under `apps/claims/data/` is reproducible and **gitignored**
(`.gitignore` line 35, `data/`): `claims.json` re-derives from the DB,
`groups.json` is deterministic from it, `vectors.npy` from a model, annotations
from a labeler, experiments are disposable.

The event catalog is the opposite. When an LLM proposes an outbreak and a human
corrects its dates, scope, or case count, that correction exists nowhere else.
It is also **cross-corpus** — one real outbreak is referenced by `measles`,
`measles2`, `measles_bal`, and later by news articles that have nothing to do
with claims. So the catalog is tracked in git and keyed independently of any
corpus.

| Artifact | Lifetime | Location | Tracked |
|---|---|---|---|
| Event catalog | Permanent, curated | `events/catalog/` | Yes |
| `claim_key` → `event_key` links | Derived, reproducible | corpus annotation | No |
| Resolution run logs | Audit / resume | corpus experiment dir | No |

Only the catalog is precious. The other two are rebuildable from it.

---

## Layout

```
events/
  schema.py              EventRecord / EventSource / EventSubject / Subject + validation
  store.py               catalog load / write / upsert / merge / link extraction
  catalog/
    events.jsonl         the event history
    subjects.jsonl       taxonomy.subject + subject_term seed
```

This package is stdlib-only and knows nothing about where events came from.
Provenance-specific drivers live with their source:

- `apps/claims/events/` — claim→event resolution (stage 1)
- news / web ingest — stage 2
- `services/events/` — routine extraction (stage 4)

Nothing here imports `apps.claims`, and nothing here belongs in `nlp/`, whose
README scopes that package to text primitives and explicitly disclaims
corpus/file-mode I/O.

---

## Event record

```json
{
  "event_key": "west-texas-measles-outbreak-2025",
  "event_type": "outbreak",
  "title": "West Texas measles outbreak",
  "summary": "Sustained measles transmission centered on Gaines County, Texas.",
  "start_date": "2025-01-01",
  "end_date": null,
  "date_precision": "month",
  "location": {"country": "US", "admin1": "TX", "label": "Gaines County"},
  "attributes": {"case_count": 700},
  "subjects": [{"slug": "measles", "relationship": "primary", "assigned_by": "llm"}],
  "status": "active",
  "merged_into": null,
  "review": {"state": "unreviewed"},
  "provenance": [
    {
      "source_type": "claim",
      "role": "primary",
      "is_discovery": true,
      "corpus": "measles2",
      "claim_key": "a1b2c3d4e5f60718",
      "claim_text": "There is a measles outbreak in West Texas.",
      "row_id": "4919:0",
      "post_id": 4919,
      "platform": "reddit_submission",
      "post_created_at": "2025-02-14T23:24:23+00:00",
      "confidence": 0.86,
      "recorded_at": "2026-09-14T14:00:00Z"
    }
  ],
  "created_at": "2026-09-14T14:00:00Z",
  "updated_at": "2026-09-14T14:00:00Z"
}
```

### `event_key` is the identity

A readable slug (same rule as `taxonomy.subject.slug`), minted from the title
plus the start year by `store.mint_event_key`.

**It is immutable once assigned.** Titles and dates get corrected constantly
during curation, so a content hash — the way `claim_key` works for immutable
claim text — would change on every correction and orphan every link. Correct the
`title` freely; never edit an `event_key` that has been referenced.

Duplicates are resolved by **merging, not deleting**: the losing record stays in
the catalog with `status: "merged"` and `merged_into: "<event_key>"`, so old
links still resolve via `store.resolve_key()`. This matches
`events.event.merged_into_event_id`.

### `provenance` is a typed list from day one

Each entry is discriminated on `source_type` (`claim`, `post`, `web_document`,
`api_record`), with only the locator fields for that type set — mirroring
`source_item_locator_chk`. Stage 2 appends `web_document` entries to the same
list with no restructuring.

`claim_text` is stored alongside `claim_key` on purpose: `claims.claim.normalized_text`
is `UNIQUE` in the proposed claims schema, so carrying the text is what lets a
future loader resolve file-mode claims to DB claim ids.

At most one entry carries `is_discovery` (the source that caused the event to be
created); `store.add_source` enforces this and dedupes on locator identity.

### Dates: do not invent them

A Reddit post mentioning an outbreak is an **upper bound** on the event start,
not the start. Set real dates only when a source states them; otherwise leave
`date_precision: "unknown"` with no dates and read `record.first_mention_at`
(earliest provenance timestamp) instead.

Most stage-1 events will honestly be `unknown`. That is fine — stage 1 exists to
group claims by event, not to build a timeline. Dates sharpen in stage 2 when
articles arrive with publication timestamps.

`date_precision` semantics, enforced by `validate_event`:

| Value | Meaning | Requires |
|---|---|---|
| `day` | Single known day | `start_date` |
| `month` | Known month; `start_date` is the 1st | `start_date` |
| `year` | Known year; `start_date` is Jan 1 | `start_date` |
| `range` | Spans a known interval | `start_date` **and** `end_date` |
| `unknown` | No reliable date | neither date set |

The SQL vocabulary also permits `exact`; file mode omits it as redundant with
`day`, keeping the file vocabulary a strict subset.

### Vocabularies

`event_type` is a fixed set (`schema.EVENT_TYPES`): `outbreak`,
`policy_change`, `regulatory_action`, `approval`, `recall`, `safety_signal`,
`study_publication`, `legal_action`, `public_statement`, `personnel_change`,
`funding_change`, `other`. Prefer `other` plus a note over inventing a type;
extending the tuple means revisiting the DB CHECK too.

`status`, `date_precision`, `subjects[].relationship`, `provenance[].role`, and
`provenance[].source_type` all mirror their SQL CHECK arrays. `review` is
file-mode only and has no DB counterpart.

---

## Editing the catalog

JSONL, one event per line, so a new event is a one-line diff. Reads validate and
raise `CatalogError` on duplicate keys, dangling merge targets, or constraint
violations; writes validate before touching disk and are atomic
(`tmp` + `replace`). `write_events` re-sorts chronologically with undated events
last, so rewrites are byte-stable.

Hand-edit carefully — or round-trip through the store, which reformats and
validates:

```python
from events import load_events, write_events
write_events(load_events())
```

---

## Integration with the claims pipeline

`claims.json` and `groups.json` are **never** modified; per the claims README,
derived values go to annotations. The link is an ordinary annotation:

```python
from apps.claims import annotations
annotations.write_annotation(
    corpus_root, "event_key", links,
    scope="group", value_type="string",
    producer="apps.claims.events.resolve", producer_kind="pipeline",
    source_hash=groups_hash,
)
```

That buys `--filter`, `select`, `join_into_groups`, and freshness checks for
free. String equality works in `filtering.matches`, so a corpus slices by event
with no new code:

```bash
python -m apps.claims browse --corpus measles2 \
  --filter 'event_key:eq=west-texas-measles-outbreak-2025'
```

Keep the filterable annotation **scalar**. If a claim maps to several events,
write a separate list-valued `event_keys` annotation for joins rather than
making `event_key` a list — the filter and threshold machinery assumes scalars.

### Resolution runs at cluster-leaf level

Per-claim LLM resolution against a mutating catalog is O(n) prompts over tens of
thousands of claims, order-dependent, and unparallelizable. A merged cluster
leaf ("there is a measles outbreak in Texas") already *is* an event-mention
group, so resolve leaves instead — hundreds of prompts for `measles2`.

1. **Gate with a labeler, not an LLM.** "Is this claim about a specific
   real-world event?" is a per-claim scalar judgment, which is what the labeler
   subsystem does: an `is_event_claim` intent, agentic labels plus human gold,
   ridge train, `labeler apply`. Scores the whole corpus at near-zero marginal
   cost.
2. **Retrieve candidates by embedding.** Embed each event's `title + summary`
   with the corpus's model tag; top-k events above a cosine threshold become the
   prompt's candidate list.
3. **One prompt per event-bearing leaf**, ordered by descending occurrence count
   so the largest events mint first and smaller ones match rather than
   duplicate. Returns match / create / not-an-event, appended to a JSONL resume
   log — the `clustering/refine.py` + `merge_log.jsonl` pattern, which solves the
   same problem.
4. **Dedupe pass** over near-duplicate minted events, reusing the union-find
   merge, writing `status: merged` / `merged_into`.

Event embeddings are reproducible, so they stay in ignored territory
(`apps/claims/data/events/embeddings/<tag>/`). Note `_events` would fail the
corpus-slug regex, so don't model this as a corpus.

---

## Roadmap

1. **Events from claims** *(current)* — catalog + schema here; resolution driver
   in `apps/claims/events/`; links as corpus annotations.
2. **Other provenance** — news/web articles scraped out-of-db, appended as
   `web_document` provenance. No schema change needed.
3. **Migrate to DB** — apply the claims and event migrations; load the catalog
   keyed on `event_key`.
4. **Routine services** — `services/events/` extracting from many sources on a
   timer.

### Fixes migration 026 needs before it is applied

- **`events.event` has no natural key.** Add `event_key TEXT NOT NULL UNIQUE`
  with the `taxonomy.subject.slug` CHECK. Without it the catalog load is not
  idempotent and file-mode links cannot resolve to DB ids.
- **`events.source_item` cannot represent claim evidence.** Its locator CHECK
  admits only `post`, `web_document`, and `api_record`, but stage-1 evidence is a
  *claim occurrence within a post*. Once claims are in the DB, a bridge table
  (`events.claim_event`) keyed on `claims.claim(id)` makes "claims tied to
  events" a single join instead of a text match.
- **`event_type` is unconstrained free text** while every comparable column uses
  a CHECK array. Mirror `schema.EVENT_TYPES` as a CHECK or lookup table.
- **`date_precision` overlaps** between `exact`, `day`, and `range`; adopt the
  table above.

Also: 026 changes the `news.article` registration trigger, so per
`.cursor/rules/data-flow.mdc`, `docs/data-flow.md` must be updated in the same
change with the migration marked proposed/dotted. Stage 1 does not touch it.

---

## Subjects

`catalog/subjects.jsonl` seeds `taxonomy.subject` + `taxonomy.subject_term`.
Search terms *discover* content; subjects *classify* events — so query an
event's `subjects`, never keyword hits. `terms` maps
`taxonomy.vaccine_term.name` → relationship (`canonical` / `alias` /
`discovery`), resolved to term ids at load time.

`store.unknown_subject_slugs()` reports subject slugs referenced by events but
missing from the catalog.

---

## Tests

```bash
pytest tests/test_events_store.py
```

Covers round-trip stability, every validation rule, key minting, merge-chain
resolution, and validity of the shipped catalog.
