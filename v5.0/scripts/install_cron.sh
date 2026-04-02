#!/bin/bash
# ──────────────────────────────────────────────────────────
# SportStock V5.0 — Crontab Installer
# ──────────────────────────────────────────────────────────
#
# Installs all cron jobs for the V5.0 data pipeline.
#
# Usage:
#   ./scripts/install_cron.sh          # preview crontab entries
#   ./scripts/install_cron.sh --install # actually install them
#   ./scripts/install_cron.sh --remove  # remove SportStock entries
#   ./scripts/install_cron.sh --systemd # install systemd timers instead
#
# Schedule overview (all times EST):
#   12:05 AM  — Opening odds (daily)
#   Every hour 6 AM – 11 PM — Current odds snapshots
#   Every hour — Injuries imports (all supported providers)
#   Every min 12 PM – 12 AM — Closing odds (dynamic)
#   6:00 AM   — Full daily pipeline
#   Continuous — Live importer (systemd service)
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

# ── Color helpers ────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[cron]${NC} $*"; }
warn() { echo -e "${YELLOW}[cron]${NC} $*"; }
err()  { echo -e "${RED}[cron]${NC} $*" >&2; }

# ── Tag for identifying our cron entries ─────────────────
CRON_TAG="# SPORTSTOCK_V5"

# ── Generate crontab entries ─────────────────────────────
generate_crontab() {
    cat <<EOF
# ──────────────────────────────────────────────────────────
# SportStock V5.0 — Automated Data Pipeline
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# ──────────────────────────────────────────────────────────

SHELL=/bin/bash
PATH=/home/derek/.local/bin:/usr/local/bin:/usr/bin:/bin
PROJECT=$ROOT

# ── Opening Odds (12:05 AM EST daily) ───────────────────
# Captures the day's opening lines as early as possible
5 0 * * *  cd \$PROJECT && ./scripts/collect_odds.sh opening >> logs/cron_opening.log 2>&1 $CRON_TAG

# ── Current Odds Snapshots (every hour, 6 AM – 11 PM EST)
# Hourly snapshots to track line movement throughout the day
0 6-23 * * *  cd \$PROJECT && ./scripts/collect_odds.sh current >> logs/cron_current.log 2>&1 $CRON_TAG

# ── Injuries Imports (every hour) ───────────────────────
# Runs injuries-only import across all providers that expose injuries
0 * * * *  cd \$PROJECT/importers && npm run import:injuries:hourly >> ../logs/cron_injuries.log 2>&1 $CRON_TAG

# ── Closing Odds (every minute, peak game hours 12 PM – 12 AM EST)
# Polls for games starting soon; skips if no imminent games
* 12-23 * * *  cd \$PROJECT && ./scripts/collect_odds.sh closing >> logs/cron_closing.log 2>&1 $CRON_TAG

# ── Full Daily Pipeline (6:00 AM EST) ───────────────────
# Import all data, normalize, extract features, predictions
0 6 * * *  cd \$PROJECT && python3 scripts/daily_pipeline.py >> logs/cron_pipeline.log 2>&1 $CRON_TAG

# ── Recurring Curated + DuckDB Refresh (every 30 min) ───
# Auto-detects changed normalized inputs and bulk-refreshes curated + DuckDB once.
*/30 * * * *  cd \$PROJECT && /home/derek/Documents/stock/.venv/bin/python3 scripts/recurring_curated_refresh.py --mode auto >> logs/cron_curated_refresh.log 2>&1 $CRON_TAG

# ── Full Curated Rebuild (daily) ─────────────────────────
# Rebuild all categories across all sports and bulk-refresh DuckDB catalog.
25 6 * * *  cd \$PROJECT && /home/derek/Documents/stock/.venv/bin/python3 scripts/recurring_curated_refresh.py --mode full >> logs/cron_curated_refresh_full.log 2>&1 $CRON_TAG

# ── Log rotation (weekly, keep 30 days) ─────────────────
0 3 * * 0  find \$PROJECT/logs -name "*.log" -mtime +30 -delete $CRON_TAG
EOF
}

# ── Preview ──────────────────────────────────────────────
preview_crontab() {
    echo ""
    echo -e "${CYAN}═══ SportStock V5.0 Crontab Entries ═══${NC}"
    echo ""
    generate_crontab
    echo ""
    echo -e "${CYAN}═══ End of crontab entries ═══${NC}"
    echo ""
    echo "To install:  $0 --install"
    echo "To use systemd timers instead:  $0 --systemd"
}

# ── Install cron entries ─────────────────────────────────
install_crontab() {
    log "Installing SportStock V5.0 cron entries …"

    # Ensure logs directory exists
    mkdir -p "$ROOT/logs"

    # Ensure scripts are executable
    chmod +x "$ROOT/scripts/collect_odds.sh"
    chmod +x "$ROOT/scripts/live_importer.py"
    chmod +x "$ROOT/scripts/game_schedule.py"

    # Remove existing SportStock entries
    local existing
    existing=$(crontab -l 2>/dev/null || true)
    local cleaned
    cleaned=$(echo "$existing" | grep -v "$CRON_TAG" || true)

    # Append new entries
    local new_entries
    new_entries=$(generate_crontab)

    {
        echo "$cleaned"
        echo ""
        echo "$new_entries"
    } | crontab -

    log "Crontab installed successfully!"
    echo ""
    log "Verify with:  crontab -l"
    echo ""

    # Summary
    echo "  Schedule (EST):"
    echo "    12:05 AM daily   — Opening odds"
    echo "    Every hour 6-11  — Current odds"
    echo "    Every hour       — Injuries imports"
    echo "    Every min 12-12  — Closing odds (dynamic)"
    echo "    6:00 AM daily    — Full pipeline"
    echo "    Every 30 min     — Curated auto refresh + DuckDB bulk refresh"
    echo "    6:25 AM daily    — Full curated rebuild + DuckDB bulk refresh"
    echo "    3:00 AM Sunday   — Log cleanup"
}

# ── Remove cron entries ──────────────────────────────────
remove_crontab() {
    log "Removing SportStock V5.0 cron entries …"

    local existing
    existing=$(crontab -l 2>/dev/null || true)

    if ! echo "$existing" | grep -q "$CRON_TAG"; then
        warn "No SportStock cron entries found"
        return
    fi

    local cleaned
    cleaned=$(echo "$existing" | grep -v "$CRON_TAG")

    # Also remove blank comment blocks left behind
    echo "$cleaned" | sed '/^# ─.*SportStock/,/^$/d' | crontab -

    log "SportStock cron entries removed"
}

# ── Install systemd timers ───────────────────────────────
install_systemd() {
    local user_dir="$HOME/.config/systemd/user"
    mkdir -p "$user_dir"

    log "Installing systemd timers to $user_dir …"

    # Copy service and timer files
    local files=(
        "odds-opening.service"
        "odds-opening.timer"
        "odds-closing.service"
        "odds-closing.timer"
        "sgo-poller.service"
        "sgo-snapshot-opening.service"
        "sgo-snapshot-opening.timer"
        "sgo-snapshot-closing.service"
        "sgo-snapshot-closing.timer"
        "oddsapi-snapshot-opening.service"
        "oddsapi-snapshot-opening.timer"
        "oddsapi-snapshot-closing.service"
        "oddsapi-snapshot-closing.timer"
        "injuries-hourly.service"
        "injuries-hourly.timer"
        "curated-refresh.service"
        "curated-refresh.timer"
        "curated-refresh-full.service"
        "curated-refresh-full.timer"
        "autobet.service"
    )

    for f in "${files[@]}"; do
        if [[ -f "$ROOT/scripts/$f" ]]; then
            cp "$ROOT/scripts/$f" "$user_dir/"
            log "  Installed $f"
        else
            err "  Missing $ROOT/scripts/$f"
        fi
    done

    # Create live-importer service
    cat > "$user_dir/live-importer.service" <<SVCEOF
[Unit]
Description=SportStock V5.0 — Live Data Importer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT
ExecStart=/usr/bin/python3 scripts/live_importer.py --log-level info
Environment=PATH=/home/derek/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=HOME=/home/derek
EnvironmentFile=-$ROOT/.env
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
SVCEOF
    log "  Created live-importer.service"

    # Reload systemd
    systemctl --user daemon-reload
    log "Reloaded systemd user daemon"

    # Enable timers
    systemctl --user enable odds-opening.timer 2>/dev/null || true
    systemctl --user enable odds-closing.timer 2>/dev/null || true
    systemctl --user enable sgo-snapshot-opening.timer 2>/dev/null || true
    systemctl --user enable sgo-snapshot-closing.timer 2>/dev/null || true
    systemctl --user enable oddsapi-snapshot-opening.timer 2>/dev/null || true
    systemctl --user enable oddsapi-snapshot-closing.timer 2>/dev/null || true
    systemctl --user enable injuries-hourly.timer 2>/dev/null || true
    systemctl --user enable curated-refresh.timer 2>/dev/null || true
    systemctl --user enable curated-refresh-full.timer 2>/dev/null || true
    log "Enabled all timers"

    # Start timers
    systemctl --user start odds-opening.timer 2>/dev/null || true
    systemctl --user start odds-closing.timer 2>/dev/null || true
    systemctl --user start sgo-snapshot-opening.timer 2>/dev/null || true
    systemctl --user start sgo-snapshot-closing.timer 2>/dev/null || true
    systemctl --user start oddsapi-snapshot-opening.timer 2>/dev/null || true
    systemctl --user start oddsapi-snapshot-closing.timer 2>/dev/null || true
    systemctl --user start injuries-hourly.timer 2>/dev/null || true
    systemctl --user start curated-refresh.timer 2>/dev/null || true
    systemctl --user start curated-refresh-full.timer 2>/dev/null || true
    log "Started all timers"

    # Enable continuous services
    systemctl --user enable sgo-poller.service 2>/dev/null || true
    systemctl --user enable autobet.service 2>/dev/null || true
    log "Enabled continuous services (sgo-poller, autobet)"

    echo ""
    log "systemd timers & services installed!"
    echo ""
    echo "  Commands:"
    echo "    systemctl --user list-timers                          # view all timers"
    echo "    systemctl --user status sgo-snapshot-opening.timer    # check SGO opening"
    echo "    systemctl --user status oddsapi-snapshot-closing.timer # check OddsAPI closing"
    echo "    systemctl --user status injuries-hourly.timer          # check hourly injuries"
    echo "    systemctl --user start sgo-poller.service             # start SGO odds poller"
    echo "    systemctl --user start autobet.service                # start autobet scheduler"
    echo "    systemctl --user start live-importer.service          # start live importer"
    echo "    journalctl --user -u sgo-poller -f                    # tail SGO poller logs"
    echo "    journalctl --user -u autobet -f                       # tail autobet logs"
}

# ── Main ─────────────────────────────────────────────────
case "${1:-}" in
    --install)
        install_crontab
        ;;
    --remove)
        remove_crontab
        ;;
    --systemd)
        install_systemd
        ;;
    --help|-h)
        echo "Usage: $0 [--install | --remove | --systemd | --help]"
        echo ""
        echo "  (no args)   Preview crontab entries"
        echo "  --install   Install crontab entries"
        echo "  --remove    Remove SportStock crontab entries"
        echo "  --systemd   Install systemd user timers + live-importer service"
        echo "  --help      Show this help"
        ;;
    *)
        preview_crontab
        ;;
esac
