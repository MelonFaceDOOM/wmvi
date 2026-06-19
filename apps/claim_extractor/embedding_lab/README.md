# Embedding lab

Streamlit UI for embedding extracted claims under reusable profiles, then
searching, graphing, clustering, and scoring an editable triplet eval set.

## Run

From the **repository root** (so `apps` is importable):

```bash
streamlit run apps/claim_extractor/embedding_lab/app.py
```

Do **not** run with `python apps/claim_extractor/embedding_lab/app.py` — Streamlit
needs its own server (`st.session_state`, caching, etc. will not work in bare
mode).

Install deps (from repo root):

```bash
pip install -r requirements.txt
pip install -r apps/claim_extractor/requirements-learned.txt
```

For the full claim-extractor stack (coref, LLM extraction, etc.) use
`apps/claim_extractor/requirements.txt` instead of `requirements-learned.txt`.

## Corporate network / SSL errors

On some corporate networks, the first embedding run fails when downloading a
BGE model from Hugging Face:

```
SSLError: certificate verify failed: self-signed certificate in certificate chain
```

The browser often works because Windows trusts the corporate proxy CA; Python
uses its own CA bundle (`certifi`) and does not.

### Fix: export the corporate root cert from Windows

No IT ticket required — the cert is already installed for the browser.

1. `Win+R` → `certmgr.msc`
2. **Trusted Root Certification Authorities** → **Certificates**
3. Find the corporate / proxy root (company name, Zscaler, Netskope, etc.)
4. Right-click → **All Tasks** → **Export…**
5. **Base-64 encoded X.509 (.CER)** → save e.g. `C:\projects\wmvi\corp-root.pem`

Point Python at it before starting Streamlit (CMD):

```cmd
set REQUESTS_CA_BUNDLE=C:\projects\wmvi\corp-root.pem
set SSL_CERT_FILE=C:\projects\wmvi\corp-root.pem
streamlit run apps\claim_extractor\embedding_lab\app.py
```

PowerShell:

```powershell
$env:REQUESTS_CA_BUNDLE = "C:\projects\wmvi\corp-root.pem"
$env:SSL_CERT_FILE = "C:\projects\wmvi\corp-root.pem"
streamlit run apps/claim_extractor/embedding_lab/app.py
```

To make this permanent: Windows **Environment Variables** (user or system) → add
`REQUESTS_CA_BUNDLE` and `SSL_CERT_FILE` with the same path.

### Alternatives (no Hugging Face download on the work PC)

- Copy the model cache from another machine:
  `%USERPROFILE%\.cache\huggingface\hub\models--BAAI--bge-base-en-v1.5\`
  (adjust model name), then set `HF_HUB_OFFLINE=1`.
- Copy a `huggingface-cli download … --local-dir …` folder and set the
  embedding profile model to that local path.

## Transfer embedding runs between machines

Embedding vectors and run metadata only (not model weights, clusters, or triplet
eval):

```bash
python -m apps.claim_extractor.embedding_lab.transfer list
python -m apps.claim_extractor.embedding_lab.transfer export --run-id 1 --out run.embed.zip
python -m apps.claim_extractor.embedding_lab.transfer import run.embed.zip
```

See `transfer.py` for `--on-conflict` options.
