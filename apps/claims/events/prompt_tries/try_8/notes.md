# try_8

Same prompt and 79 labeled chunks as try_7, gpt-6-luna, standard. Extract now repairs a quote that is not a substring of the post, once, without changing type, place, or date.

Composite 0.9486 → 0.9444. F1 0.9277 → 0.9167. Required matches 63/69 → 66/69. Precision 0.9429 → 0.8800 (75 predictions; no optional event was matched). Recall 0.9130 → 0.9565. Place 0.9717 → 0.9658. Date grain 0.9697 → 0.9394. place_scope 0.9394 → 0.9697. study_action 1.0. Emptiness 75/79. Quote drops 5 → 0. Repairs 2.

Snohomish’s four exposure sites and Dane County were kept. Both passed the quote check on the first response, so the repair call was not what recovered them. The two repairs that did run kept events gold does not want: Brad Karp’s resignation (`1592419:0`) and an RFK policy reversal (`1489995:0`).

Also gone from the try_7 miss list: the placeless outbreak on `195845:0`, the Dilley site-string miss, the Singapore isolation scope, and the 1963 vaccine introduction. The Lancet retraction (`4403786:0`) and the Samoa outbreak inside the Gavi rant (`8545540:0`) were not emitted. `1669436:0` is still empty.

New extras: three on the Dilley/ICE comment (`1592419:0`), a nationwide US spread (`326752:0`), a “last major” US outbreak (`1611342:0`), a Latin American vaccination drive beside the RFK line, and a government payment to the Poling family (`8599466:0`). The UK child-death extra on `1188626:0` remains. Oklahoma’s “today” is still undated. New date misses: a 2025 range on Gaines County, year 2019 on Samoa, and year 2025 on the Canada wave. The US “worst outbreak” scope flipped to at_named_place. All four optional events were omitted.
