@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Season Simulator
:: Windows equivalent of running scripts/season_simulator.py
::
:: Runs Monte Carlo projections for championships, awards, brackets,
:: and draft lottery.
::
:: Usage:
::   season_simulator.bat                        (all sports, 10000 sims)
::   season_simulator.bat --sport nba            (one sport)
::   season_simulator.bat --simulations 50000    (more simulations)
:: ──────────────────────────────────────────────────────────

setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."
set "LOGDIR=%ROOT%\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

:: Load .env if present
if exist "%ROOT%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%ROOT%\.env") do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
    )
)

cd /d "%ROOT%"

echo [season_simulator] Starting season simulator...

if "%~1"=="" (
    python scripts\season_simulator.py --sport all --simulations 10000 >> "%LOGDIR%\season_simulator.log" 2>&1
) else (
    python scripts\season_simulator.py %* >> "%LOGDIR%\season_simulator.log" 2>&1
)

echo [season_simulator] Done. Log: %LOGDIR%\season_simulator.log
