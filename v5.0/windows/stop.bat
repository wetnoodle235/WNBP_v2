@echo off
:: ──────────────────────────────────────────────────────────────────
:: SportStock V5.0 — Stop all services
:: Windows equivalent of scripts/stop.sh
:: ──────────────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."

set "PID_BACKEND=%TEMP%\sportstock-backend.pid"
set "PID_TUNNEL=%TEMP%\sportstock-tunnel.pid"
set "PID_WEBSITE=%TEMP%\sportstock-website.pid"
set "TUNNEL_URL_FILE=%TEMP%\sportstock-tunnel-url.txt"

set stopped=0

echo [stop] Stopping SportStock V5.0 services...

:: Kill by window title (most reliable approach for start /b processes)
for %%N in ("SportStock-Website" "SportStock-Tunnel" "SportStock-Backend") do (
    taskkill /fi "windowtitle eq %%~N" /f >nul 2>&1
    if !errorlevel! equ 0 (
        echo [stop] Stopped %%~N
        set /a stopped+=1
    ) else (
        echo [stop] %%~N: not running
    )
)

:: Also kill any lingering node / python processes spawned by uvicorn/next on those ports
:: (best-effort — only if they match our ports)
for /f "tokens=5" %%P in ('netstat -aon 2^>nul ^| findstr ":8000 " ^| findstr "LISTENING"') do (
    echo [stop] Killing process on port 8000 (PID %%P)
    taskkill /pid %%P /f >nul 2>&1
)
for /f "tokens=5" %%P in ('netstat -aon 2^>nul ^| findstr ":3000 " ^| findstr "LISTENING"') do (
    echo [stop] Killing process on port 3000 (PID %%P)
    taskkill /pid %%P /f >nul 2>&1
)

:: Clean up pid / tunnel URL files
del /q "%PID_BACKEND%" 2>nul
del /q "%PID_TUNNEL%" 2>nul
del /q "%PID_WEBSITE%" 2>nul
del /q "%TUNNEL_URL_FILE%" 2>nul

:: Restore .env.local API URL back to localhost
set "ENV_LOCAL=%ROOT%\website\.env.local"
if exist "%ENV_LOCAL%" (
    powershell -Command "(Get-Content '%ENV_LOCAL%') -replace '^NEXT_PUBLIC_API_URL=.*trycloudflare\.com.*', 'NEXT_PUBLIC_API_URL=http://127.0.0.1:8000' | Set-Content '%ENV_LOCAL%'"
    echo [stop] Restored .env.local API URL to http://127.0.0.1:8000
)

if %stopped% gtr 0 (
    echo [stop] Stopped %stopped% service(s) ✓
) else (
    echo [stop] No running services found.
)
