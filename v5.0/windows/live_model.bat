@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Live Model Runner
:: Windows equivalent of running scripts/live_model.py --daemon
::
:: Runs the live win-probability model continuously during game hours.
:: Usage:
::   live_model.bat               (default: 60s polling interval)
::   live_model.bat --interval 30 (custom interval in seconds)
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

echo [live_model] Starting live model daemon...
echo [live_model] Log: %LOGDIR%\live_model.log
echo [live_model] Press Ctrl+C to stop.
echo.

python scripts\live_model.py --daemon --interval 60 %* >> "%LOGDIR%\live_model.log" 2>&1
