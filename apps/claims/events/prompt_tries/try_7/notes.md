# try_7

Same 79 labeled chunks, gpt-6-luna, standard. Prompt adds: a reported wave beside a status loss; countrywide for a national act with the country filled; unplaced when no country is named; a title that only argues is empty; a bare month-day uses the post's year; a bulletin's today/yesterday does not date the wave; an agency requiring studies is kept; a catch-up reminder is not an act.

Composite 0.9396 → 0.9486. F1 0.8961 → 0.9277 (63/69 required matched, 3 optional matched, 1 optional omitted). Place 0.9808 → 0.9717. Date grain 0.9344 → 0.9697. place_scope 0.9016 → 0.9394. study_action 1.0. Emptiness 75/79.

Fixed: Canada wave beside the status loss, the diagnostic-essay title, Rawlins July 29, Chihuahua's bulletin day, the Canada tally and the global surge, the two vaccination reminders, Miami County on the Kroger case, the FDA study requirement, Samoa inside the Gavi rant, and the 1998 publication beside the retraction. ACIP, the Samoa halt, the UK licence withdrawal, and being struck off now have the gold scope.

New misses: `1669436:0` returned empty (publication-only Wakefield). `195845:0` emitted an outbreak with no place. `1188626:0` emitted a UK child death the gold leaves empty. `7509061:0` added the 1963 introduction beside the optional 1971 and the required 1989. `1489995:0` added an RFK policy reversal. Singapore's isolation order was marked countrywide. Oklahoma's "today" was left unknown. The AstraZeneca day was dropped. The Lancet retraction lost the United Kingdom. Seminole is still empty. The four Snohomish quotes and Dane County still failed the quote check.
