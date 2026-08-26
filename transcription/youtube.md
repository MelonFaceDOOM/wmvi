# YouTube transcription (cookies, proxy, troubleshooting)

The YouTube transcriber downloads audio via **yt-dlp**, then runs **Whisper**. Downloads need two files under `private/`, **Deno ≥ 2.3.0** for JS challenges, and optionally a proxy (`YT_PROXY_URL` in `.env`).

**Service:** `python -m services.youtube.transcriber` — see [services/youtube/transcriber/README.md](../services/youtube/transcriber/README.md) for run flags and systemd install.

**Graphical session required** for cookie refresh only. Use [gpu-remote-desktop.md](gpu-remote-desktop.md) to connect via xrdp first.

---

## Files

| File / env | Purpose |
|------|---------|
| `private/youtube-cookies.txt` | Netscape-format cookies exported from Firefox (yt-dlp `--cookies`) |
| `private/youtube-agent.txt` | Single-line User-Agent string matching the Firefox session |
| `.env` → `YT_PROXY_URL` | Optional Proxidize HTTP proxy for yt-dlp only (not read by Firefox) |
| `.env` → `YT_DENO_BIN` | Optional path to Deno if it is not on PATH (systemd often needs this or a `/usr/local/bin/deno` symlink) |

`is_en` is **not** set at transcription time. The `label_en` service labels language after a transcript exists in `sm.posts_all`.

---

## Prerequisites

1. [xrdp session](gpu-remote-desktop.md) on gpu-pc (XFCE desktop).
2. **Firefox proxy** configured to the **same HTTP proxy** as `YT_PROXY_URL` (Proxidize dashboard). The browser does not read `.env`; set proxy manually under **Settings → General → Network Settings**.
3. **[cookies.txt](https://addons.mozilla.org/en-US/firefox/addon/cookies-txt/)** extension installed in Firefox ESR on gpu-pc.

---

## Cookie refresh procedure

1. Open a **new Private Window** in Firefox (Ctrl+Shift+P).
2. Go to `https://www.youtube.com` and **sign in** (use a dedicated automation Google account if you have one). Complete phone/2FA on your device if prompted.
3. Confirm YouTube works (home page loads, a video plays).
4. Visit `https://www.youtube.com/robots.txt` in the same window (plain text robots file).
5. On a youtube.com tab, click the **cookies.txt** extension → export **current site only** → save as:

   ```
   <repo-root>/private/youtube-cookies.txt
   ```

   Overwrite the existing file.

6. Open **Developer Tools** (F12) → **Network** → reload youtube.com → select any request to youtube.com → copy the full **User-Agent** from Request Headers.
7. Save that string (one line, no extra whitespace) to:

   ```
   <repo-root>/private/youtube-agent.txt
   ```

8. **Verify** (from repo root, transcription venv active):

   ```bash
   python transcription/transcription_checklist.py --group youtube
   ```

9. **Restart the YouTube transcriber** after deploying transcriber code (cookies and user-agent are read on each download). After a cookie-only refresh, a restart is optional:

   ```bash
   sudo systemctl restart <youtube-transcriber-unit>
   ```

10. **Log out** from the XFCE menu when finished (cleaner than only closing the RDP window).

---

## Proxy (`YT_PROXY_URL`)

Format (see [storage/yt_proxy.py](../storage/yt_proxy.py)):

- `http://user:pass@host:port`, or
- Proxidize line: `host:port:user:pass`

Set in repo `.env` on gpu-pc. yt-dlp picks it up via `yt_dlp_proxy_args()`.

**Important:** Export cookies through Firefox using the **same proxy** yt-dlp uses. Mismatched IP/session between cookies and downloads is a common cause of `auth` failures.

---

## yt-dlp flags (Deno + player clients)

`download_yt_audio.py` always passes:

- `--js-runtimes deno:<absolute path>` — Deno solves YouTube n-sig challenges (`yt-dlp-ejs`). Node 20 is unsupported (need Node ≥ 22 if you ever switch).
- `--extractor-args youtube:player_client=default,web_embedded,-tv_downgraded` — `tv_downgraded` plus logged-in cookies yields “The page needs to be reloaded”; `web_embedded` is an extra client.

Deno install: [setup.md](setup.md). After changing flags or Deno, restart the YouTube transcriber unit.

---

## Session behavior (transcriber)

- **Windows:** 09:00–12:00 and 18:00–21:00 Pacific, up to 100 videos per window.
- **Systemic failures** (proxy or auth): cooldown 10 minutes between retries; session aborts after 5 consecutive failures.
- End-of-session log line: `session_summary: saved=... rate_per_hour=...`

```bash
journalctl -u '<youtube-transcriber-unit>' --since '24 hours ago' \
  | grep -E 'session_summary|systemic (proxy|auth) failure|cooling down'
```

---

## Troubleshooting

| Symptom | Likely cause | What to do |
|--------|--------------|------------|
| `yt-dlp auth failure` / `Sign in to confirm` | Stale cookies or bot check | Refresh cookies (procedure above) through proxied Firefox |
| `yt-dlp proxy failure` / 407 | Proxidize creds, quota, or outage | Check Proxidize dashboard; fix `YT_PROXY_URL`; still re-export cookies via same proxy |
| Works for several videos then stops (old behavior) | Single systemic failure aborted session | Deploy cooldown fix; check logs for first `proxy` vs `auth` |
| `session_summary` low `saved`, high `systemic_cooldowns` | Intermittent proxy/auth | Fix proxy; refresh cookies; watch for `session_aborted=true` |
| Missing formats / `n challenge solving failed` / `node … (unsupported)` | Deno missing or too old; Node 20 is not a valid EJS runtime | Install Deno ≥ 2.3.0 ([setup.md](setup.md)); confirm checklist check 11 (`deno`) |
| `The page needs to be reloaded` / UNPLAYABLE | `tv_downgraded` client with account cookies | Already excluded in `download_yt_audio.py`; do not add that client back |
| Only images / SABR / no audio formats | n-sig failed or web client forced SABR | Fix Deno first; keep `web_embedded` extractor args |
| Missing formats / JS challenge errors with Deno present | Outdated yt-dlp / yt-dlp-ejs | Update in transcription venv (`yt-dlp[default]` in `requirements-transcription.txt`) |
| `Video unavailable` etc. | Permanent content error | Video marked failed; transcriber skips — not an env issue |

**Log classification:** `auth` matches bot-check / 403 on the final ERROR line. `proxy` matches 407 / tunnel / unable to connect in stderr. See [yt_download_errors.py](../services/youtube/transcriber/yt_download_errors.py).

**Checklist smoke test** downloads a fixed test URL with your cookies, UA, and proxy:

```bash
python transcription/transcription_checklist.py --group youtube
```
