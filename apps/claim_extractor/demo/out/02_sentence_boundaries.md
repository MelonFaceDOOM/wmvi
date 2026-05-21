# 02 — Sentence boundary trimming

## Feature
Use **syntok** sentence spans to anchor search hits, then take N sentences before/after (merge overlaps, cap length).

## Purpose
Avoid naive **char windows** that cut mid-sentence or mid-clause — especially in transcripts with weak punctuation.

## Real row from term pipeline

- **Source:** `data/posts_for_term_trimmed.json` (same `text` + `hits` the trim stage uses).
- **`post_id`:** `8527983`  ·  **`platform`:** `reddit_submission`
- **URL:** https://www.reddit.com/r/HypervitaminosisA/comments/1swxeor/after_rfk_jr_recommends_vitamin_a_as_a_measles/

- **Chars:** 1191  ·  **Syntok sentences:** 16  ·  **DB hits:** 6  ·  **Output chunks:** 2
- **Per-chunk lengths:** 638 + 520 = **1158** (sum can exceed body when cap-split windows **reuse** overlap).
- **~Union coverage** (chars in ≥1 chunk, approximate): **97%** (~**3%** not in any chunk).
- **Note:** Sum of chunk lengths can exceed the body when max-length splits **reuse** overlap; union coverage is the clearer “how much source text appears downstream” read.

### Hits (DB-style)

| # | term | match_start | match_end |
|--:|------|-------------:|----------:|
| 1 | `measles` | 40 | 47 |
| 2 | `measles` | 292 | 299 |
| 3 | `measles` | 412 | 419 |
| 4 | `measles` | 865 | 872 |
| 5 | `measles` | 1008 | 1015 |
| 6 | `measles` | 1130 | 1137 |

### Merge / separate (this row)

- **6** substring hits → **2** chunks after ±N sentences, **overlap merge**, and **length cap**.
- Nearby hits often land in the same syntok window → **one** merged chunk; distant clusters → **multiple** chunks.

### Input text

```
After RFK Jr. recommends vitamin A as a measles treatment, some Texas patients show signs of toxicity
[https://x.com/DittiePE/status/2042560976784945407](https://x.com/DittiePE/status/2042560976784945407)

>Children in Lubbock, Texas were admitted to the hospital with liver damage. Not from measles. From vitamin A toxicity — because their parents followed RFK Jr.’s advice and gave them cod liver oil to treat measles. He is the sitting U.S. Secretary of Health and Human Services. This is not the first time this has happened. In 2019, RFK Jr. traveled to Samoa and ran the same anti-vaccine campaign. Vaccination rates dropped to 31%. Five months later: 5,700 cases. 83 dead. Most of them children under 4. He called it a “natural experiment.” The Senate confirmed him as HHS Secretary anyway

[https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-measles-treatment-some-texas-patients-show-signs-of-toxicity-214353603.html](https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-measles-treatment-some-texas-patients-show-signs-of-toxicity-214353603.html)

>  
After RFK Jr. recommends vitamin A as a measles treatment, some Texas patients show signs of toxicity
```

## Sentence spans (first 6 + last 4; total = 16)

0. [   0, 204)  After RFK Jr. recommends vitamin A as a measles treatment, some Texas …
1. [ 206, 282)  >Children in Lubbock, Texas were admitted to the hospital with liver d…
2. [ 282, 300)   Not from measles.
3. [ 300, 420)   From vitamin A toxicity — because their parents followed RFK Jr.’s ad…
4. [ 420, 483)   He is the sitting U.S. Secretary of Health and Human Services.
5. [ 483, 529)   This is not the first time this has happened.
*(middle spans omitted for brevity)*
12. [ 747, 796)   The Senate confirmed him as HHS Secretary anyway
13. [ 798, 941)  [https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-mea…
14. [ 941,1084)  (https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-mea…
15. [1086,1191)  >   After RFK Jr. recommends vitamin A as a measles treatment, some Te…

## `trim_sentence_boundary` output

### Chunk 0 (len=638)
```
After RFK Jr. recommends vitamin A as a measles treatment, some Texas patients show signs of toxicity
[https://x.com/DittiePE/status/2042560976784945407](https://x.com/DittiePE/status/2042560976784945407)

>Children in Lubbock, Texas were admitted to the hospital with liver damage. Not from measles. From vitamin A toxicity — because their parents followed RFK Jr.’s advice and gave them cod liver oil to treat measles. He is the sitting U.S. Secretary of Health and Human Services. This is not the first time this has happened. In 2019, RFK Jr. traveled to Samoa and ran the same anti-vaccine campaign. Vaccination rates dropped to 31%.
```

### Chunk 1 (len=520)
```
83 dead. Most of them children under 4. He called it a “natural experiment.” The Senate confirmed him as HHS Secretary anyway

[https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-measles-treatment-some-texas-patients-show-signs-of-toxicity-214353603.html](https://www.yahoo.com/news/after-rfk-jr-recommends-vitamin-a-as-a-measles-treatment-some-texas-patients-show-signs-of-toxicity-214353603.html)

>  
After RFK Jr. recommends vitamin A as a measles treatment, some Texas patients show signs of toxicity
```

## vs char before/after
- **Char slice:** can start/end inside a clause → broken context for coref/LLM.
- **Sentence window:** keeps grammatical units → more stable meaning in each chunk.
- **Hard cap** (`MAX_TRIMMED_CHARS` / coref cap): prevents OOM on huge single “sentences”.
