@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Odds Collection Wrapper
:: Windows equivalent of scripts/collect_odds.sh
::
:: Usage:
::   collect_odds.bat                   (default: current mode)
::   collect_odds.bat opening           (opening odds)
::   collect_odds.bat closing           (closing odds — checks schedule)
::   collect_odds.bat current           (current snapshot)
::   collect_odds.bat opening nba,nfl   (specific sports)
:: ──────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."

set "MODE=%~1"
if "!MODE!"=="" set "MODE=current"

set "SPORTS=%~2"
if "!SPORTS!"=="" set "SPORTS=nba,nfl,nhl,mlb,epl"

set "LOGDIR=%ROOT%\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set "LOGFILE=%LOGDIR%\odds_!MODE!.log"

cd /d "%ROOT%"

:: Load .env if present
if exist "%ROOT%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%ROOT%\.env") do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
    )
)

:: Helper: append timestamped log line
call :log "Starting odds collection: mode=!MODE!, sports=!SPORTS!"

:: For closing mode, check if any games are starting soon
if /i "!MODE!"=="closing" (
    for /f "delims=" %%R in ('python scripts\game_schedule.py --closing-window 10 --sports "!SPORTS!" --json 2^>nul') do set "CLOSING_CHECK=%%R"
    if "!CLOSING_CHECK!"=="[]" (
        :: No games starting soon — skip silently
        exit /b 0
    )
    for /f %%C in ('python -c "import json; data=!CLOSING_CHECK!; print(len(data))" 2^>nul') do set GAME_COUNT=%%C
    if "!GAME_COUNT!"=="" set GAME_COUNT=0
    call :log "Closing mode: !GAME_COUNT! game(s) starting within 10 minutes"
)

python scripts\odds_scheduler.py --mode "!MODE!" --sport "!SPORTS!" --once --normalize >> "%LOGFILE%" 2>&1
set EXIT_CODE=%errorlevel%

if %EXIT_CODE% equ 0 (
    call :log "Odds collection (!MODE!) completed successfully"
) else (
    call :log "ERROR: Odds collection (!MODE!) failed with exit code %EXIT_CODE%"
)

exit /b %EXIT_CODE%

:log
    for /f %%T in ('powershell -Command "Get-Date -Format '[yyyy-MM-dd HH:mm:ss]'"') do set TS=%%T
    echo %TS% %~1
    echo %TS% %~1 >> "%LOGFILE%"
    goto :eof
