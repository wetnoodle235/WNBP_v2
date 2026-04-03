#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────
# SportStock V5.0 — Stop all services
# ──────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PIDFILES=(
    "/tmp/sportstock-website.pid:Website"
    "/tmp/sportstock-tunnel-watchdog.pid:Tunnel watchdog"
    "/tmp/sportstock-tunnel.pid:Cloudflare tunnel"
    "/tmp/sportstock-backend.pid:Backend"
)

log()  { echo -e "${GREEN}[stop]${NC} $*"; }
warn() { echo -e "${YELLOW}[stop]${NC} $*"; }

stopped=0

for entry in "${PIDFILES[@]}"; do
    pidfile="${entry%%:*}"
    name="${entry##*:}"

    if [[ ! -f "$pidfile" ]]; then
        warn "$name: no pidfile found (not running?)"
        continue
    fi

    pid=$(<"$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        log "Stopping $name (PID $pid)…"
        kill "$pid" 2>/dev/null || true

        # Wait up to 5s for graceful exit
        i=0
        while kill -0 "$pid" 2>/dev/null && (( i < 15 )); do
            sleep 0.3
            (( i++ ))
        done

        if kill -0 "$pid" 2>/dev/null; then
            warn "  Force-killing $name (PID $pid)"
            kill -9 "$pid" 2>/dev/null || true
        fi
        (( stopped++ ))
    else
        warn "$name (PID $pid): already stopped"
    fi

    rm -f "$pidfile"
done

# Clean up tunnel URL file
rm -f /tmp/sportstock-tunnel-url.txt

if (( stopped > 0 )); then
    log "Stopped $stopped service(s) ✓"
else
    warn "No running services found."
fi

# Restore local dev URL in .env.local
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
ENV_LOCAL="$ROOT/website/.env.local"

if [[ -f "$ENV_LOCAL" ]]; then
    if grep -q '^NEXT_PUBLIC_API_URL=.*trycloudflare\.com' "$ENV_LOCAL"; then
        sed -i 's|^NEXT_PUBLIC_API_URL=.*|NEXT_PUBLIC_API_URL=http://127.0.0.1:8000|' "$ENV_LOCAL"
        log "Restored .env.local API URL to http://127.0.0.1:8000"
    fi
fi
