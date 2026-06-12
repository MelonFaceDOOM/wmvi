# GPU box: remote desktop (xrdp)

Use this on the headless Debian GPU machine when you need a **graphical session** — for example Firefox to refresh YouTube cookies. Podcast transcription does not require a browser.

Use **SSH** for shells and services; use **Remote Desktop (xrdp)** only when you need a GUI.

**YouTube cookie refresh:** after connecting, see [youtube.md](youtube.md) (Firefox proxy + export steps).

## Install (on gpu-pc)

```bash
sudo apt update
sudo apt install -y xfce4 xfce4-goodies dbus-x11 xorgxrdp xrdp firefox-esr
```

On Debian Trixie the browser package is `firefox-esr` (not `firefox`).

Tell xrdp to start XFCE for your user:

```bash
echo xfce4-session > ~/.xsession
chmod +x ~/.xsession
```

Enable xrdp:

```bash
sudo systemctl enable --now xrdp
sudo systemctl status xrdp   # should be active (running)
```

Reboot once after the first install:

```bash
sudo reboot
```

## Connect from Windows

1. **Win + R** → `mstsc` (Remote Desktop Connection).
2. **Computer:** the GPU host IP or hostname (e.g. `gpu-pc` on your LAN).
3. Log in with your Linux user (same account as SSH).
4. You should see an **XFCE** desktop.

## Firefox and proxy (YouTube cookie export only)

Open Firefox from the XFCE menu or run:

```bash
firefox-esr https://www.youtube.com
```

**Proxy:** **Settings → General → Network Settings → Settings… → Manual proxy configuration**. Enter the **HTTP** proxy host, port, and credentials from your Proxidize dashboard (same proxy as `YT_PROXY_URL` in `.env`).

`YT_PROXY_URL` applies only to **yt-dlp** on the transcriber; Firefox does not read `.env` — configure the proxy in the browser before signing in and exporting cookies.

Full procedure: [youtube.md](youtube.md).

## Troubleshooting

| Symptom | What to try |
|--------|-------------|
| Cannot connect on port 3389 | `sudo systemctl status xrdp`; confirm the host is reachable on your LAN |
| Grey or black screen after login | Confirm `~/.xsession` contains `xfce4-session`; reboot; disconnect all RDP sessions and reconnect |
| Session closes right away | `journalctl -u xrdp-sesman -b --no-pager \| tail -50` |

Log out from the XFCE menu when finished (cleaner than only closing the RDP window).
