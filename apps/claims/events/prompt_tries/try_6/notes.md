# try_6

New prompt on the 80-chunk sample (79 labeled). gpt-6-luna, standard tier. `9771068:0` was extracted and not scored.

Rescored after optional gold, then after Rawlins sites were relabeled notable_case and either Englewood Kroger site string was accepted. Composite 0.9396. Core weighting, without place_scope and study_action, is 0.9381. Emptiness 76/79. Event F1 0.8961 (60/69 required gold matched, 1 optional matched, 3 optional omitted, 66 predicted). Place 0.9808. Date grain 0.9344. place_scope 0.9016. study_action 1.0 on matches.

First score, before those marks: composite 0.9237, F1 0.8406, 58/72 gold matched.

This sample is larger than try_4, so 0.9237 is not a regression from 0.9656.

Quote drops account for six gold misses: all four Snohomish exposure sites, Dane County, and Samoa inside the Gavi rant. The model emitted them and the quote check rejected them.

Rawlins Walmart and the steakhouse are now notable_case in the gold, which is what the model returned. Their July 29 date is still unmarked: gold uses 2025 from the post, the model left the year unknown.

Real omissions that are still required: the Canada 2025 wave beside the status loss, the 1998 Wakefield publication beside the retraction, and the FDA study requirement in `3123367:0` (the model returned empty). The Hep B line in that comment is optional and was omitted.

False extras that are still wrong: Canada's outbreak from the diagnostic-inflation essay, Canada's outbreak from the national-tally comment, a worldwide measles surge, and two "urges vaccination" lines (Miami County, Hawaiʻi). California Adventure as a second site matched the optional gold.

place_scope misses are one pattern. ACIP votes with no place in the text were marked countrywide. The Samoa vaccine halt, the UK licence withdrawal, and Wakefield being struck off were marked at_named_place. Gold has those three as countrywide.

Seminole is still missing on the Gaines County chunk. The AstraZeneca claims date is the year 2024, not 20 January. Chihuahua was given a day. Englewood Kroger lost Miami County, and the site string gained "store".
