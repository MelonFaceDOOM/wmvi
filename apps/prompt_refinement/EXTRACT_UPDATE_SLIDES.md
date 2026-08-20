# Extract update — slide paste copy

Short meeting update. Bullets only.

---

## Slide 1 — Context

- Measles claim-extract refresh
- Eval: 30 mixed chunks (same set across runs)
- Compared: model (5.4-mini vs luna) + prompt (old standard vs new)
- Production path now: new prompt + Azure `gpt-5.6-luna` + alignment score saved on each claim

---

## Slide 2 — Prompt: what changed (old → latest)

- Prefer central / distinctive propositions; empty output OK
- Drop speaker-meta (“the speaker is not anti-vax”, “my kids are vaccinated”)
- Ban narrator indexicals (“the speaker”, “the individual”, “speaker’s childhood”)
- Depersonalize stance: “I won’t take measles vax” → “measles vaccine should be refused”
- Generalize person/anecdote → regular pattern; keep named epi details (place / first case / outbreak)
- Condemned action → evaluative claim, not just dry event
- Reject truisms (“measles is a disease”, “people want to avoid measles”)
- New field: discrete `claim_vaccine_alignment_score` ∈ {0, 0.25, 0.5, 0.75, 1}

---

## Slide 3 — Eval setup (reminder)

- Same 30 chunks for all runs
- Models: `gpt-5.4-mini` vs `gpt-5.6-luna`
- Prompts: old standard vs new (“negatives” / “negatives+”)
- Luna + new prompt cleaner overall (less speaker-meta, fewer junk claims)

---

## Slide 4 — 5.4 → luna (same new prompt)

**Chunk (YouTube):**
> You're not an anti-vaccine? No, I'm not anti-vaccine. If you show me a vaccine that was saved and in fact, if it did what it was supposed to, I would have no problem with it. I'm talking about general vaccines. Which vaccine? Measles. Measles. What I take a measles vaccine? No. Are your kids vaccinated? Yeah, all my kids are vaccinated. And I'm fully vaccinated. Yeah, all my kids are vaccinated.

**Before (5.4 + new prompt):**
1. `[1.0]` The speaker is not anti-vaccine.
2. `[1.0]` A vaccine is acceptable if it is safe and works as intended.
3. `[0.25]` The speaker would not take a measles vaccine.
4. `[1.0]` The speaker's children are vaccinated.
5. `[1.0]` The speaker is fully vaccinated.

**After (luna + new prompt):**
1. `[0.75]` Vaccines are acceptable when they are shown to be safe and effective at their intended purpose.
2. `[0.0]` The measles vaccine should be refused even when other vaccines are accepted.

- So what: luna drops vaccination-card / speaker-meta; keeps the actual stance

---

## Slide 5 — Old prompt → new prompt (luna) #1

**Chunk (Reddit):**
> Anyone else feeling a little stressed about measles? I don’t vaccinate my kids. They are super healthy, and we hardly ever get sick. … I have read the inserts as well as done hours and hours of research about MMR. It’s one of the vaccines I’m the most scared of the side effects. But on the other hand, some of the effects of measles sound really bad too. … I just keep wondering if I’m really doing the right thing, both for my kids and for my community, by not vaccinating.

**Before (old + luna):**
1. Some parents choose not to vaccinate their children against measles despite their children being generally healthy.
2. Concern about potential MMR vaccine side effects can contribute to parental reluctance to vaccinate.
3. Measles can cause serious health effects in children.
4. Parents may question whether declining MMR vaccination is appropriate because it could affect both their children and the broader community.

**After (new + luna):**
1. `[0.25]` MMR vaccination can cause side effects that some parents consider concerning.
2. `[0.75]` Measles can cause serious health effects.
3. `[0.75]` Choosing not to vaccinate children against measles may put children and their communities at risk.

- So what: drops the “healthy unvaccinated family” card; keeps the MMR-fear vs measles-harm tradeoff, plus community risk

---

## Slide 6 — Old prompt → new prompt (luna) #2

**Chunk (YouTube comment):**
> Yeah, I almost died from a bad case of measles, and I'd like to thank my parents for not being bothered to have me vaccinated. My siblings got the measles vax… I'm still paying the price many decades later.

**Before (old + luna):**
1. Unvaccinated people can contract measles and develop life-threatening illness.
2. Severe measles can result in health consequences that persist for decades.

**After (new + luna):**
1. `[0.75]` Failure to vaccinate children against measles can expose them to potentially life-threatening illness.
2. `[0.75]` Severe measles infection can cause health consequences that persist for decades.

- So what: old flattens it to “unvaccinated people can get measles”; new keeps the missed-vax → severe/lasting harm link

---

## Slide 7 — Old prompt → new prompt (luna) #3

**Chunk (YouTube comment):**
> I would not vaccinate. When our babies were small we had them in close contact with other babies who had the measles or chicken pox

**Before (old + 5.4):**
1. The speaker would not vaccinate children.
2. Babies can be kept in close contact with other babies who have measles or chickenpox.

**After (new + luna):**
1. `[0.25]` Some parents deliberately expose their infants to others with measles or chickenpox.
2. `[0.0]` Deliberate infection with measles or chickenpox is a preferable alternative to vaccination for infants.

- So what: drops “I would not vaccinate”; extracts the implied alternative-to-vax claims

---

## Slide 8 — Alignment Ridge vs gold (misses)

Ridge `alignment_pred_m1` vs human gold (`measles_bal`, n≈196):

- Exact match ~0.24; κ ~0.18; MAE ~0.26
- Tail collapse: gold **0.0** / **1.0** recall ≈ **0** on gold eval
- Model hugs the middle (~0.5) even when gold is extreme

**Example A — gold anti, model mid:**
- Claim: “In some years, more people die from the MMR vaccine than from measles in the United States.”
- Gold: `0.0` · Ridge: `~0.64`

**Example B — gold anti, model mid:**
- Claim: “The daughter was born completely fine before developing autism after MMR vaccination.”
- Gold: `0.0` · Ridge: `~0.54`

**Example C — gold pro, model mid:**
- Claim: “Mercury exposure from vaccines is not associated with an increased risk of ASD.”
- Gold: `1.0` · Ridge: `~0.51`

- So what: Ridge not a reliable stance source of truth (especially tails)

---

## Slide 9 — Now / next

- Extractor: new prompt + luna in `get_posts_extract_upload`
- Each claim stores discrete `claim_vaccine_alignment_score` from the LLM extract
- Alignment Ridge stays weak on extremes — don’t treat `alignment_pred_m1` as gold stance
- Batch extract in flight (TPM-bound at Azure 250k/min on luna)
