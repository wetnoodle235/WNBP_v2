@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Hourly Injuries Import
:: Windows equivalent of importers live injuries hourly runner
::
:: Usage:
::   injuries_hourly.bat
::   injuries_hourly.bat --dry-run
:: ──────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."
set "LOGDIR=%ROOT%\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set "LOGFILE=%LOGDIR%\injuries_hourly.log"

cd /d "%ROOT%\importers"

echo [%date% %time%] Starting hourly injuries import >> "%LOGFILE%"
call npm run import:injuries:hourly -- %* >> "%LOGFILE%" 2>&1
set EXIT_CODE=%errorlevel%

if %EXIT_CODE% equ 0 (
    echo [%date% %time%] Hourly injuries import complete >> "%LOGFILE%"
) else (
    echo [%date% %time%] ERROR: injuries import failed with code %EXIT_CODE% >> "%LOGFILE%"
)

exit /b %EXIT_CODE%
