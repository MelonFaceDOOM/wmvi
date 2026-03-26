"""
next steps:

1) posts -> claims
look at previous extraction prompt & output and decide how these should be modified
create new prompt including new output format
set up foundry & get key
create simple service, no service.toml. ssh to vm. python -m services.claim_extract -n=2000 -o="outfile.jsonl"
 - something like that
run that on 2k posts. pick 300 best

2) choose 300 claims
 - only claims, no input post
 - this means we aren't looking at null posts (no claims) at all
 - include opposite positions
 - include clear groups that involve different wordings

3) embedding

 
 
 
 
Major pipeline steps
Collect and preprocess posts
Start from raw posts and do your existing text cleaning, deduping, language filtering, and any vaccine-relevance filtering you already trust.
Extract claim candidates
From each post, extract one or more vaccine-related claims. Keep provenance so every claim points back to the source post and span if possible.

Normalize each claim into a stable claim record
Do light normalization, not hard categorization. For each extracted claim, store:

raw claim text
canonicalized claim text
optional fields like target, actor, stance direction, evidence mode, pathogen/product, date, source platform

The point here is to make claims more comparable without prematurely forcing them into your own label set.

Embed the claim
Generate an embedding for at least the canonical claim text. Sentence Transformers is specifically meant for embeddings and rerankers, and semantic search is one of its standard workflows.

Store vectors plus metadata in a vector index
Use a vector DB such as Qdrant and store:

claim ID
vector
metadata payload
optionally multiple named vectors later, such as raw_claim_vec and canonical_claim_vec

Qdrant supports vectors, payload metadata, filtering, and multiple named vectors per point.

Nearest-neighbor claim search
This is your first core product: type a claim, embed it, retrieve nearby claims, and inspect the neighborhood.
Optional reranking of top results
Retrieve with embeddings, then rerank the top-k with a stronger reranker or model if needed. Sentence Transformers supports rerankers alongside embedding models.
Offline theme discovery
Periodically run clustering on the claim embeddings to identify dense groups, fringe groups, and outliers. This is where you look for emerging themes instead of forcing everything into a fixed taxonomy.
Analyst review and naming of stable themes
Only after repeated discovery should you start assigning soft labels to recurring clusters. The taxonomy should emerge from the space, not be fully imposed at the start.
Evaluation and refinement
Build a judged set of claim-to-claim similarity examples and query-to-relevant-neighbors examples. Use that to compare models, normalization choices, and reranking strategies.
 
"""