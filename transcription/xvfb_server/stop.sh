#!/usr/bin/env bash
set -euo pipefail

pkill -f "x11vnc .*:99" 2>/dev/null || true
pkill -f "fluxbox" 2>/dev/null || true
pkill -f "Xvfb :99" 2>/dev/null || true

echo "Stopped VNC/Xvfb stack on :99"
