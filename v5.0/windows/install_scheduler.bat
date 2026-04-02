@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Windows Task Scheduler Installer
:: Windows equivalent of scripts/install_cron.sh
::
:: Registers Windows Scheduled Tasks for all V5.0 pipelines.
:: Must be run as Administrator.
::
:: Usage:
::   install_scheduler.bat             (preview tasks only)
::   install_scheduler.bat --install   (create all scheduled tasks)
::   install_scheduler.bat --remove    (remove all SportStock tasks)
::
:: Schedule (all times local / EST):
::   12:05 AM daily   — Opening odds
::   Every hour 6-23  — Current odds snapshots
::   Every hour       — Injuries imports (all supported providers)
::   Every 1 min 12-23 — Closing odds (dynamic skip if no games)
::   6:00 AM daily    — Full daily pipeline
::   8:00 AM Tuesday  — Season simulator
::   12:00 PM daily   — Live model (runs for 13 hours)
::   3:00 AM Sunday   — Log cleanup
:: ──────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."
set "WINDOWS_DIR=%SCRIPT_DIR%"
set "LOGDIR=%ROOT%\logs"

set "ACTION=%~1"
if "!ACTION!"=="" set "ACTION=--preview"

:: ── Task name prefix ──────────────────────────────────────
set "PREFIX=SportStock_V5"

echo.
echo ════════════════════════════════════════════════════════
echo   SportStock V5.0 — Windows Task Scheduler
echo ════════════════════════════════════════════════════════
echo.

if /i "!ACTION!"=="--preview" goto :preview
if /i "!ACTION!"=="--install" goto :install
if /i "!ACTION!"=="--remove"  goto :remove
if /i "!ACTION!"=="--help"    goto :help
if /i "!ACTION!"=="-h"        goto :help

:help
echo Usage: %~nx0 [--install ^| --remove ^| --preview ^| --help]
echo.
echo   (no args)    Preview tasks that would be created
echo   --install    Create all scheduled tasks (requires Admin)
echo   --remove     Remove all SportStock scheduled tasks
echo   --help       Show this help
goto :eof

:: ── Preview ───────────────────────────────────────────────
:preview
echo [scheduler] Tasks that would be registered:
echo.
echo   %PREFIX%_OpeningOdds
echo     Trigger: Daily at 00:05
echo     Action:  %WINDOWS_DIR%collect_odds.bat opening
echo.
echo   %PREFIX%_CurrentOdds
echo     Trigger: Daily, hourly 06:00-23:00
echo     Action:  %WINDOWS_DIR%collect_odds.bat current
echo.
echo   %PREFIX%_InjuriesHourly
echo     Trigger: Hourly (every hour)
echo     Action:  %WINDOWS_DIR%injuries_hourly.bat
echo.
echo   %PREFIX%_ClosingOdds
echo     Trigger: Daily, every minute 12:00-23:59
echo     Action:  %WINDOWS_DIR%collect_odds.bat closing
echo.
echo   %PREFIX%_DailyPipeline
echo     Trigger: Daily at 06:00
echo     Action:  %WINDOWS_DIR%daily-pipeline.bat
echo.
echo   %PREFIX%_SeasonSimulator
echo     Trigger: Weekly on Tuesday at 08:00
echo     Action:  %WINDOWS_DIR%season_simulator.bat --sport all --simulations 10000
echo.
echo   %PREFIX%_LiveModel
echo     Trigger: Daily at 12:00 (runs 13 hours via timeout)
echo     Action:  %WINDOWS_DIR%live_model.bat
echo.
echo   %PREFIX%_LogCleanup
echo     Trigger: Weekly on Sunday at 03:00
echo     Action:  PowerShell — delete logs older than 30 days
echo.
echo To install: %~nx0 --install  (run as Administrator)
goto :eof

:: ── Install ───────────────────────────────────────────────
:install
:: Check for admin privileges
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [scheduler] ERROR: Must run as Administrator.
    echo             Right-click install_scheduler.bat ^> Run as administrator
    exit /b 1
)

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

echo [scheduler] Installing scheduled tasks...
echo.

:: ── Opening odds: 12:05 AM daily ─────────────────────────
schtasks /create /f /tn "%PREFIX%_OpeningOdds" ^
    /tr "\"%WINDOWS_DIR%collect_odds.bat\" opening" ^
    /sc DAILY /st 00:05 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_OpeningOdds  (00:05 daily)

:: ── Current odds: every hour 6 AM – 11 PM ────────────────
:: Windows Task Scheduler can't do "every hour between X and Y" natively.
:: We create one task that runs hourly and the script itself is a no-op
:: outside business hours. For full fidelity, we create individual tasks.
for %%H in (06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23) do (
    schtasks /create /f /tn "%PREFIX%_CurrentOdds_%%H00" ^
        /tr "\"%WINDOWS_DIR%collect_odds.bat\" current" ^
        /sc DAILY /st %%H:00 ^
        /rl HIGHEST /ru "%USERNAME%" >nul
)
echo [scheduler]   Created: %PREFIX%_CurrentOdds_*  (hourly 06:00-23:00)

:: ── Injuries import: every hour ─────────────────────────
schtasks /create /f /tn "%PREFIX%_InjuriesHourly" ^
    /tr "\"%WINDOWS_DIR%injuries_hourly.bat\"" ^
    /sc HOURLY /mo 1 /st 00:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_InjuriesHourly  (hourly)

:: ── Closing odds: every minute 12 PM – 12 AM ─────────────
:: Use a single task with /ri (repeat interval) 1 minute, duration 12 hours
schtasks /create /f /tn "%PREFIX%_ClosingOdds" ^
    /tr "\"%WINDOWS_DIR%collect_odds.bat\" closing" ^
    /sc DAILY /st 12:00 /ri 1 /du 0012:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_ClosingOdds  (every 1 min, 12:00-00:00)

:: ── Full daily pipeline: 6:00 AM ─────────────────────────
schtasks /create /f /tn "%PREFIX%_DailyPipeline" ^
    /tr "\"%WINDOWS_DIR%daily-pipeline.bat\"" ^
    /sc DAILY /st 06:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_DailyPipeline  (06:00 daily)

:: ── Season simulator: Tuesday 8 AM ───────────────────────
schtasks /create /f /tn "%PREFIX%_SeasonSimulator" ^
    /tr "\"%WINDOWS_DIR%season_simulator.bat\" --sport all --simulations 10000" ^
    /sc WEEKLY /d TUE /st 08:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_SeasonSimulator  (Tuesday 08:00)

:: ── Live model: 12 PM daily (timeout 13h via PowerShell) ─
set "LIVE_CMD=powershell -Command \"Start-Process -FilePath '%WINDOWS_DIR%live_model.bat' -RedirectStandardOutput '%LOGDIR%\cron_live.log' -Wait\""
schtasks /create /f /tn "%PREFIX%_LiveModel" ^
    /tr "cmd /c \"%WINDOWS_DIR%live_model.bat\"" ^
    /sc DAILY /st 12:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_LiveModel  (12:00 daily)

:: ── Log cleanup: Sunday 3 AM ──────────────────────────────
set "CLEANUP_CMD=powershell -Command \"Get-ChildItem -Path '%LOGDIR%' -Filter *.log | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } | Remove-Item -Force\""
schtasks /create /f /tn "%PREFIX%_LogCleanup" ^
    /tr "%CLEANUP_CMD%" ^
    /sc WEEKLY /d SUN /st 03:00 ^
    /rl HIGHEST /ru "%USERNAME%" >nul
echo [scheduler]   Created: %PREFIX%_LogCleanup  (Sunday 03:00)

echo.
echo [scheduler] All tasks installed! ✓
echo.
echo   Manage tasks:
echo     schtasks /query /fo LIST /v /tn "%PREFIX%*"  (list all)
echo     schtasks /run /tn "%PREFIX%_DailyPipeline"   (run now)
echo     Task Scheduler GUI: taskschd.msc
echo.
goto :eof

:: ── Remove ────────────────────────────────────────────────
:remove
echo [scheduler] Removing SportStock V5.0 scheduled tasks...

for /f "delims=" %%T in ('schtasks /query /fo csv /nh 2^>nul ^| findstr /i "%PREFIX%"') do (
    for /f "tokens=1 delims=," %%N in ("%%T") do (
        set "TASK_NAME=%%~N"
        schtasks /delete /f /tn "!TASK_NAME!" >nul 2>&1
        echo [scheduler]   Removed: !TASK_NAME!
    )
)

echo [scheduler] All SportStock tasks removed.
goto :eof
