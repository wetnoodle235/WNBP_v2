#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
WEBSITE_DIR="$ROOT/website"

PIDFILE_TUNNEL="/tmp/sportstock-tunnel.pid"
LOG_TUNNEL="/tmp/sportstock-tunnel.log"
LOG_WATCHDOG="/tmp/sportstock-tunnel-watchdog.log"
TUNNEL_URL_FILE="/tmp/sportstock-tunnel-url.txt"

CHECK_INTERVAL="${SPORTSTOCK_TUNNEL_CHECK_INTERVAL:-15}"
FAILURE_WINDOW="${SPORTSTOCK_TUNNEL_FAILURE_WINDOW:-45}"
TUNNEL_TIMEOUT="${SPORTSTOCK_TUNNEL_TIMEOUT:-30}"
BACKEND_HEALTH_URL="${SPORTSTOCK_BACKEND_HEALTH_URL:-http://localhost:8000/health}"
TUNNEL_TARGET_URL="${SPORTSTOCK_TUNNEL_TARGET_URL:-http://localhost:8000}"
URL_CHANGE_HOOK="${SPORTSTOCK_TUNNEL_URL_CHANGE_HOOK:-}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[tunnel-watchdog]${NC} $*" | tee -a "$LOG_WATCHDOG"; }
warn() { echo -e "${YELLOW}[tunnel-watchdog]${NC} $*" | tee -a "$LOG_WATCHDOG"; }
err()  { echo -e "${RED}[tunnel-watchdog]${NC} $*" | tee -a "$LOG_WATCHDOG" >&2; }

current_tunnel_url() {
    if [[ -f "$TUNNEL_URL_FILE" ]]; then
        tr -d '\r\n' < "$TUNNEL_URL_FILE"
        return 0
    fi
    if [[ -f "$LOG_TUNNEL" ]]; then
        grep -oP 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$LOG_TUNNEL" | tail -1 || true
        return 0
    fi
    return 1
}

update_env_local() {
    local url="$1"
    local envfile="$WEBSITE_DIR/.env.local"

    if [[ -f "$envfile" ]]; then
        if grep -q '^NEXT_PUBLIC_API_URL=' "$envfile"; then
            sed -i "s|^NEXT_PUBLIC_API_URL=.*|NEXT_PUBLIC_API_URL=${url}|" "$envfile"
        else
            echo "NEXT_PUBLIC_API_URL=${url}" >> "$envfile"
        fi
    else
        echo "NEXT_PUBLIC_API_URL=${url}" > "$envfile"
    fi
}

run_url_change_hook() {
    local old_url="$1"
    local new_url="$2"

    if [[ -z "$URL_CHANGE_HOOK" || "$old_url" == "$new_url" ]]; then
        return 0
    fi

    if [[ ! -x "$URL_CHANGE_HOOK" ]]; then
        warn "URL change hook is not executable: $URL_CHANGE_HOOK"
        return 0
    fi

    if ! "$URL_CHANGE_HOOK" "$old_url" "$new_url" >> "$LOG_WATCHDOG" 2>&1; then
        warn "URL change hook failed for $new_url"
    fi
}

capture_tunnel_url() {
    local i=0
    local url=""

    while (( i < TUNNEL_TIMEOUT )); do
        url="$(current_tunnel_url)"
        if [[ -n "$url" ]]; then
            printf '%s' "$url" > "$TUNNEL_URL_FILE"
            echo >> "$TUNNEL_URL_FILE"
            printf '%s\n' "$url"
            return 0
        fi
        sleep 1
        (( i++ ))
    done

    return 1
}

restart_tunnel() {
    local old_url
    old_url="$(current_tunnel_url)"

    if [[ -f "$PIDFILE_TUNNEL" ]]; then
        local pid
        pid="$(<"$PIDFILE_TUNNEL")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$PIDFILE_TUNNEL"
    fi

    : > "$LOG_TUNNEL"
    rm -f "$TUNNEL_URL_FILE"

    log "Restarting Cloudflare quick tunnel → localhost:8000"
    cloudflared tunnel --url "$TUNNEL_TARGET_URL" --no-autoupdate >> "$LOG_TUNNEL" 2>&1 &
    local pid=$!
    echo "$pid" > "$PIDFILE_TUNNEL"

    local new_url
    if ! new_url="$(capture_tunnel_url)"; then
        err "Failed to capture a fresh tunnel URL after restart"
        return 1
    fi

    update_env_local "$new_url"
    run_url_change_hook "$old_url" "$new_url"

    if [[ -n "$old_url" && "$old_url" != "$new_url" ]]; then
        warn "Quick tunnel hostname rotated from $old_url to $new_url"
        warn "Any production environment pinned to the old hostname must be updated separately"
    else
        log "Tunnel healthy at $new_url"
    fi
}

backend_healthy() {
    curl -fsS --max-time 5 "$BACKEND_HEALTH_URL" >/dev/null 2>&1
}

tunnel_healthy() {
    local url="$1"
    [[ -n "$url" ]] || return 1
    curl -fsS --max-time 10 "${url}/health" >/dev/null 2>&1
}

main() {
    : >> "$LOG_WATCHDOG"
    log "Watching tunnel every ${CHECK_INTERVAL}s with ${FAILURE_WINDOW}s failure window"

    local unhealthy_for=0
    while true; do
        sleep "$CHECK_INTERVAL"

        if ! backend_healthy; then
            unhealthy_for=0
            warn "Backend is not healthy; skipping tunnel restart cycle"
            continue
        fi

        local pid_ok=false
        if [[ -f "$PIDFILE_TUNNEL" ]]; then
            local pid
            pid="$(<"$PIDFILE_TUNNEL")"
            if kill -0 "$pid" 2>/dev/null; then
                pid_ok=true
            fi
        fi

        local url
        url="$(current_tunnel_url)"
        if [[ "$pid_ok" == true ]] && tunnel_healthy "$url"; then
            unhealthy_for=0
            continue
        fi

        unhealthy_for=$(( unhealthy_for + CHECK_INTERVAL ))
        warn "Tunnel unhealthy for ${unhealthy_for}s (pid_ok=${pid_ok}, url=${url:-none})"

        if (( unhealthy_for < FAILURE_WINDOW )); then
            continue
        fi

        if restart_tunnel; then
            unhealthy_for=0
        else
            err "Tunnel restart failed; will retry on next cycle"
        fi
    done
}

main "$@"