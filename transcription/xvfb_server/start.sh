# RUN FROM INSIDE THE SERVER
# Sets up xfvb (virtual framebuffer) server 

#!/usr/bin/env bash

# bash settings:
# -e: if any command returns a non-zero exit code, the script exits immediately.
# -u: using an unset variable is treated as an error (prevents typos like $DISPALY).
# -o pipefail: in a pipeline (a | b | c), the pipeline fails if any command in it fails (not just the last one).
set -euo pipefail

export DISPLAY=:99

# Kill any existing stack on :99/5900 (idempotent)
pkill -f "Xvfb :99" 2>/dev/null || true
pkill -f "x11vnc .*:99" 2>/dev/null || true
pkill -f "fluxbox" 2>/dev/null || true

# Start X server
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp >/tmp/xvfb99.log 2>&1 &

# Simple WM so apps behave
fluxbox >/tmp/fluxbox99.log 2>&1 &

# VNC bound to localhost only (tunnel required)
x11vnc -display :99 -forever -shared -localhost -rfbport 5900 >/tmp/x11vnc99.log 2>&1 &

echo "Started Xvfb(:99) + fluxbox + x11vnc on localhost:5900"
echo "Use: ssh -L 5900:localhost:5900 <server> then vncviewer localhost:5900"
