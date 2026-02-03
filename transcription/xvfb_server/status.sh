#!/usr/bin/env bash
set -euo pipefail

echo "=== processes ==="
ps aux | grep -E "Xvfb :99|x11vnc .*:99|fluxbox" | grep -v grep || true

echo
echo "=== listening ports (5900) ==="
ss -ltnp | grep ":5900" || true

echo
echo "=== DISPLAY ==="
echo "${DISPLAY:-<not set>}"
