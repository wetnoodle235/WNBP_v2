# Windows Scripts — SportStock V5.0

Windows shim scripts that mirror every Linux/macOS script in `../scripts/`.
Both `.bat` (Command Prompt) and `.ps1` (PowerShell) versions are provided.
**Prefer the `.ps1` versions** — they are more robust.

## Prerequisites

| Tool | Install |
|------|---------|
| Python 3.10+ | https://python.org |
| Node.js 18+ | https://nodejs.org |
| uvicorn | `pip install "uvicorn[standard]"` |
| cloudflared | https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/ |
| curl (for .bat scripts) | Built into Windows 10 1803+ |

> **PowerShell Execution Policy** — if `.ps1` scripts are blocked, run once as Administrator:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

## Script Reference

| Windows script | Linux equivalent | What it does |
|----------------|-----------------|--------------|
| `start.bat` / `start.ps1` | `scripts/start.sh` | Start backend + website (no tunnel) |
| `startup.bat` / `startup.ps1` | `scripts/startup.sh` | Full startup with Cloudflare tunnel |
| `stop.bat` / `stop.ps1` | `scripts/stop.sh` | Stop all running services |
| `daily-pipeline.bat` | `scripts/daily-pipeline.sh` | Import → normalize → extract features |
| `collect_odds.bat` | `scripts/collect_odds.sh` | Odds collection (opening/closing/current) |
| `injuries_hourly.bat` | `importers/src/live/injuries_hourly.ts` | Injuries-only import across supported providers |
| `backfill.bat` | `scripts/backfill.sh` | Backfill historical data 2023–2026 |
| `live_model.bat` | `scripts/live_model.py --daemon` | Live win-probability model (daemon) |
| `season_simulator.bat` | `scripts/season_simulator.py` | Monte Carlo season projections |
| `install_scheduler.bat` | `scripts/install_cron.sh` | Register Windows Task Scheduler jobs |

## Quick Start

```bat
REM Start everything (simple — no tunnel)
cd v5.0\windows
start.bat

REM Start everything with Cloudflare tunnel
startup.bat

REM Stop all services
stop.bat
```

```powershell
# PowerShell versions (preferred)
.\start.ps1
.\startup.ps1
.\stop.ps1
```

## Scheduled Tasks (replaces Linux cron)

Run once as Administrator to register all pipelines with Windows Task Scheduler:

```bat
install_scheduler.bat --preview    # see what would be created
install_scheduler.bat --install    # actually register tasks
install_scheduler.bat --remove     # unregister all tasks
```

The following tasks are registered under the `SportStock_V5` prefix:

| Task | Schedule | Description |
|------|----------|-------------|
| `SportStock_V5_OpeningOdds` | Daily 00:05 | Opening lines collection |
| `SportStock_V5_CurrentOdds_*` | Hourly 06:00–23:00 | Hourly odds snapshots |
| `SportStock_V5_InjuriesHourly` | Hourly | Injuries-only refresh across providers |
| `SportStock_V5_ClosingOdds` | Every 1 min, 12:00–00:00 | Closing lines (skips if no games) |
| `SportStock_V5_DailyPipeline` | Daily 06:00 | Full data → normalize → features |
| `SportStock_V5_SeasonSimulator` | Tuesday 08:00 | Monte Carlo projections |
| `SportStock_V5_LiveModel` | Daily 12:00 | Live model daemon |
| `SportStock_V5_LogCleanup` | Sunday 03:00 | Delete logs older than 30 days |

Manage tasks:
```bat
schtasks /query /fo LIST /v /tn "SportStock_V5*"   # list all tasks
schtasks /run /tn "SportStock_V5_DailyPipeline"    # run pipeline now
taskschd.msc                                        # open GUI
```

## Odds Collection Modes

```bat
collect_odds.bat opening           # grab opening lines (12:05 AM)
collect_odds.bat current           # current snapshot (hourly)
collect_odds.bat closing           # closing lines (checks schedule first)
collect_odds.bat closing nba,nfl   # closing lines for specific sports
```

## Logs

All logs land in `v5.0\logs\` (same as Linux). Service startup logs go to `%TEMP%\sportstock-*.log`.
