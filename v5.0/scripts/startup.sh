#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────
# SportStock V5.0 — Full startup with Cloudflare tunnel
#
# Starts:
#   1. FastAPI backend (uvicorn) on port 8000
#   2. Cloudflare quick tunnel → localhost:8000
#   3. Next.js dev server on port 3000 (after tunnel URL is known)
#
# Logs:  /tmp/sportstock-backend.log
#        /tmp/sportstock-tunnel.log
#        /tmp/sportstock-website.log
# ──────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
BACKEND_DIR="$ROOT/backend"
WEBSITE_DIR="$ROOT/website"

PIDFILE_BACKEND="/tmp/sportstock-backend.pid"
PIDFILE_TUNNEL="/tmp/sportstock-tunnel.pid"
PIDFILE_WEBSITE="/tmp/sportstock-website.pid"

LOG_BACKEND="/tmp/sportstock-backend.log"
LOG_TUNNEL="/tmp/sportstock-tunnel.log"
LOG_WEBSITE="/tmp/sportstock-website.log"

TUNNEL_URL_FILE="/tmp/sportstock-tunnel-url.txt"
TUNNEL_TIMEOUT=30

# ── Colors ────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[startup]${NC} $*"; }
warn() { echo -e "${YELLOW}[startup]${NC} $*"; }
err()  { echo -e "${RED}[startup]${NC} $*" >&2; }

# ── Cleanup function ─────────────────────────────────────────────
cleanup() {
    log "Shutting down all services…"
    local pids=()
    for pidfile in "$PIDFILE_WEBSITE" "$PIDFILE_TUNNEL" "$PIDFILE_BACKEND"; do
        if [[ -f "$pidfile" ]]; then
            local pid
            pid=$(<"$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null && pids+=("$pid")
            fi
            rm -f "$pidfile"
        fi
    done
    # Wait briefly for graceful exit, then force-kill
    for pid in "${pids[@]}"; do
        local i=0
        while kill -0 "$pid" 2>/dev/null && (( i < 10 )); do
            sleep 0.3
            (( i++ ))
        done
        kill -9 "$pid" 2>/dev/null || true
    done
    log "All services stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# ── Kill any previous instances ──────────────────────────────────
kill_existing() {
    local name="$1" pidfile="$2"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(<"$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            warn "Killing existing $name (PID $pid)…"
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
}

# ── Pre-flight checks ───────────────────────────────────────────
preflight() {
    local ok=true

    if ! command -v python3 &>/dev/null; then
        err "python3 not found"; ok=false
    fi
    if ! command -v uvicorn &>/dev/null; then
        # Try via python module
        if ! python3 -m uvicorn --version &>/dev/null; then
            err "uvicorn not found (install: pip install 'uvicorn[standard]')"; ok=false
        fi
    fi
    if ! command -v cloudflared &>/dev/null; then
        err "cloudflared not found (install: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/)"
        ok=false
    fi
    if ! command -v node &>/dev/null; then
        err "node not found"; ok=false
    fi
    if [[ ! -f "$BACKEND_DIR/main.py" ]]; then
        err "Backend not found at $BACKEND_DIR/main.py"; ok=false
    fi
    if [[ ! -f "$WEBSITE_DIR/package.json" ]]; then
        err "Website not found at $WEBSITE_DIR/package.json"; ok=false
    fi

    $ok || { err "Pre-flight checks failed. Aborting."; exit 1; }
    log "Pre-flight checks passed ✓"
}

# ── 1. Start FastAPI backend ────────────────────────────────────
start_backend() {
    kill_existing "backend" "$PIDFILE_BACKEND"

    log "Starting FastAPI backend on port 8000…"
    cd "$BACKEND_DIR"
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload \
        >> "$LOG_BACKEND" 2>&1 &
    local pid=$!
    echo "$pid" > "$PIDFILE_BACKEND"
    log "  Backend PID: $pid  (log: $LOG_BACKEND)"

    # Wait for backend to be ready
    local i=0
    while (( i < 20 )); do
        if curl -sf http://localhost:8000/health &>/dev/null; then
            log "  Backend is healthy ✓"
            return 0
        fi
        sleep 1
        (( i++ ))
    done
    warn "  Backend did not respond to /health within 20s (may still be starting)"
}

# ── 2. Start Cloudflare tunnel ──────────────────────────────────
start_tunnel() {
    kill_existing "tunnel" "$PIDFILE_TUNNEL"
    rm -f "$TUNNEL_URL_FILE"

    log "Starting Cloudflare quick tunnel → localhost:8000…"
    cloudflared tunnel --url http://localhost:8000 \
        >> "$LOG_TUNNEL" 2>&1 &
    local pid=$!
    echo "$pid" > "$PIDFILE_TUNNEL"
    log "  Tunnel PID: $pid  (log: $LOG_TUNNEL)"

    # Parse the tunnel URL from log output
    log "  Waiting for tunnel URL (up to ${TUNNEL_TIMEOUT}s)…"
    local i=0 url=""
    while (( i < TUNNEL_TIMEOUT )); do
        if [[ -f "$LOG_TUNNEL" ]]; then
            url=$(grep -oP 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$LOG_TUNNEL" | head -1)
            if [[ -n "$url" ]]; then
                break
            fi
        fi
        sleep 1
        (( i++ ))
    done

    if [[ -z "$url" ]]; then
        err "  Failed to capture tunnel URL within ${TUNNEL_TIMEOUT}s"
        err "  Check $LOG_TUNNEL for details"
        return 1
    fi

    echo "$url" > "$TUNNEL_URL_FILE"
    log "  Tunnel URL: ${CYAN}${url}${NC} ✓"
}

# ── 3. Update website environment ───────────────────────────────
update_website_env() {
    local url
    url=$(<"$TUNNEL_URL_FILE")

    log "Updating website environment with tunnel URL…"

    for envfile in "$WEBSITE_DIR/.env.local" "$WEBSITE_DIR/.env.production"; do
        if [[ -f "$envfile" ]]; then
            # Replace existing NEXT_PUBLIC_API_URL or append
            if grep -q '^NEXT_PUBLIC_API_URL=' "$envfile"; then
                sed -i "s|^NEXT_PUBLIC_API_URL=.*|NEXT_PUBLIC_API_URL=${url}|" "$envfile"
            else
                echo "NEXT_PUBLIC_API_URL=${url}" >> "$envfile"
            fi
        else
            echo "NEXT_PUBLIC_API_URL=${url}" > "$envfile"
        fi
        log "  Updated: $envfile"
    done
}

# ── 4. Start Next.js dev server ─────────────────────────────────
start_website() {
    kill_existing "website" "$PIDFILE_WEBSITE"

    log "Starting Next.js dev server on port 3000…"
    cd "$WEBSITE_DIR"
    npx next dev --port 3000 \
        >> "$LOG_WEBSITE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PIDFILE_WEBSITE"
    log "  Website PID: $pid  (log: $LOG_WEBSITE)"

    # Wait for website to be ready
    local i=0
    while (( i < 30 )); do
        if curl -sf http://localhost:3000 &>/dev/null; then
            log "  Website is ready ✓"
            return 0
        fi
        sleep 1
        (( i++ ))
    done
    warn "  Website did not respond within 30s (may still be compiling)"
}

# ── Main ─────────────────────────────────────────────────────────
main() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  SportStock V5.0 — Full Startup${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Truncate old logs
    : > "$LOG_BACKEND"
    : > "$LOG_TUNNEL"
    : > "$LOG_WEBSITE"

    preflight
    start_backend
    start_tunnel
    update_website_env
    start_website

    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}  All services running!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Backend (local):  ${CYAN}http://localhost:8000${NC}"
    echo -e "  Backend (docs):   ${CYAN}http://localhost:8000/docs${NC}"
    echo -e "  Backend (tunnel): ${CYAN}$(cat "$TUNNEL_URL_FILE")${NC}"
    echo -e "  Website:          ${CYAN}http://localhost:3000${NC}"
    echo ""
    echo -e "  Logs:"
    echo -e "    Backend:  $LOG_BACKEND"
    echo -e "    Tunnel:   $LOG_TUNNEL"
    echo -e "    Website:  $LOG_WEBSITE"
    echo ""
    echo -e "  Press ${YELLOW}Ctrl+C${NC} to stop all services"
    echo ""

    # Keep script alive — wait for any child to exit
    wait
}

main "$@"
