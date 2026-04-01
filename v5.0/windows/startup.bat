@echo off
:: ──────────────────────────────────────────────────────────────────
:: SportStock V5.0 — Full startup with Cloudflare tunnel
:: Windows equivalent of scripts/startup.sh
::
:: Starts:
::   1. FastAPI backend (uvicorn) on port 8000
::   2. Cloudflare quick tunnel -> localhost:8000
::   3. Next.js dev server on port 3000
::
:: Logs: %TEMP%\sportstock-backend.log
::        %TEMP%\sportstock-tunnel.log
::        %TEMP%\sportstock-website.log
:: ──────────────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."
set "BACKEND_DIR=%ROOT%\backend"
set "WEBSITE_DIR=%ROOT%\website"

set "LOG_BACKEND=%TEMP%\sportstock-backend.log"
set "LOG_TUNNEL=%TEMP%\sportstock-tunnel.log"
set "LOG_WEBSITE=%TEMP%\sportstock-website.log"
set "TUNNEL_URL_FILE=%TEMP%\sportstock-tunnel-url.txt"
set "PID_BACKEND=%TEMP%\sportstock-backend.pid"
set "PID_TUNNEL=%TEMP%\sportstock-tunnel.pid"
set "PID_WEBSITE=%TEMP%\sportstock-website.pid"

echo.
echo ════════════════════════════════════════════════════════
echo   SportStock V5.0 — Full Startup
echo ════════════════════════════════════════════════════════
echo.

:: ── Pre-flight checks ─────────────────────────────────────
echo [startup] Running pre-flight checks...

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [startup] ERROR: python not found
    exit /b 1
)

where uvicorn >nul 2>&1
if %errorlevel% neq 0 (
    python -m uvicorn --version >nul 2>&1
    if !errorlevel! neq 0 (
        echo [startup] ERROR: uvicorn not found. Run: pip install "uvicorn[standard]"
        exit /b 1
    )
    set "UVICORN=python -m uvicorn"
) else (
    set "UVICORN=uvicorn"
)

where cloudflared >nul 2>&1
if %errorlevel% neq 0 (
    echo [startup] ERROR: cloudflared not found. Download from https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/
    exit /b 1
)

where node >nul 2>&1
if %errorlevel% neq 0 (
    echo [startup] ERROR: node not found
    exit /b 1
)

if not exist "%BACKEND_DIR%\main.py" (
    echo [startup] ERROR: Backend not found at %BACKEND_DIR%\main.py
    exit /b 1
)

if not exist "%WEBSITE_DIR%\package.json" (
    echo [startup] ERROR: Website not found at %WEBSITE_DIR%\package.json
    exit /b 1
)

echo [startup] Pre-flight checks passed ✓

:: ── Truncate old logs ─────────────────────────────────────
type nul > "%LOG_BACKEND%"
type nul > "%LOG_TUNNEL%"
type nul > "%LOG_WEBSITE%"

:: ── Load .env if present ──────────────────────────────────
if exist "%ROOT%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%ROOT%\.env") do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
    )
)

:: ── 1. Start FastAPI backend ──────────────────────────────
echo [startup] Starting FastAPI backend on port 8000...
cd /d "%BACKEND_DIR%"
start "SportStock-Backend" /b cmd /c "%UVICORN% main:app --host 0.0.0.0 --port 8000 --reload >> "%LOG_BACKEND%" 2>&1"

:: Give it a moment to spawn then capture PID via WMIC
timeout /t 2 /nobreak >nul
for /f "tokens=2" %%P in ('tasklist /fi "windowtitle eq SportStock-Backend" /fo list ^| findstr "PID:"') do echo %%P>"%PID_BACKEND%"

:: Wait for backend health
set /a i=0
:wait_be
timeout /t 1 /nobreak >nul
curl -sf http://localhost:8000/health >nul 2>&1
if %errorlevel% equ 0 (
    echo [startup]   Backend is healthy ✓
    goto be_ready
)
set /a i+=1
if %i% lss 20 goto wait_be
echo [startup]   WARNING: Backend did not respond within 20s

:be_ready

:: ── 2. Start Cloudflare tunnel ────────────────────────────
echo [startup] Starting Cloudflare quick tunnel ^-^> localhost:8000...
start "SportStock-Tunnel" /b cmd /c "cloudflared tunnel --url http://localhost:8000 >> "%LOG_TUNNEL%" 2>&1"
timeout /t 2 /nobreak >nul

:: Wait for tunnel URL in log
echo [startup]   Waiting for tunnel URL (up to 30s)...
set /a i=0
set "TUNNEL_URL="
:wait_tunnel
timeout /t 1 /nobreak >nul
for /f "tokens=*" %%L in ('findstr /r "https://.*\.trycloudflare\.com" "%LOG_TUNNEL%" 2^>nul') do (
    for /f "tokens=*" %%U in ('echo %%L ^| grep -oP "https://[a-zA-Z0-9-]+\.trycloudflare\.com" 2^>nul') do set "TUNNEL_URL=%%U"
    :: Fallback: parse with PowerShell
    if "!TUNNEL_URL!"=="" (
        for /f "delims=" %%U in ('powershell -Command "if (Test-Path '%LOG_TUNNEL%') { (Get-Content '%LOG_TUNNEL%' -Raw) -match 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' | Out-Null; $Matches[0] }"') do set "TUNNEL_URL=%%U"
    )
)
if not "!TUNNEL_URL!"=="" goto tunnel_found
set /a i+=1
if %i% lss 30 goto wait_tunnel
echo [startup]   ERROR: Failed to capture tunnel URL. Check %LOG_TUNNEL%
goto :skip_tunnel

:tunnel_found
echo !TUNNEL_URL!>"%TUNNEL_URL_FILE%"
echo [startup]   Tunnel URL: !TUNNEL_URL! ✓

:: ── 3. Update website .env.local ─────────────────────────
echo [startup] Updating website environment with tunnel URL...
set "ENV_LOCAL=%WEBSITE_DIR%\.env.local"
set "ENV_PROD=%WEBSITE_DIR%\.env.production"

for %%E in ("%ENV_LOCAL%" "%ENV_PROD%") do (
    if exist "%%~E" (
        powershell -Command "(Get-Content '%%~E') -replace '^NEXT_PUBLIC_API_URL=.*', 'NEXT_PUBLIC_API_URL=!TUNNEL_URL!' | Set-Content '%%~E'"
        findstr /c:"NEXT_PUBLIC_API_URL=" "%%~E" >nul 2>&1
        if !errorlevel! neq 0 echo NEXT_PUBLIC_API_URL=!TUNNEL_URL!>>"%%~E"
    ) else (
        echo NEXT_PUBLIC_API_URL=!TUNNEL_URL!>"%%~E"
    )
    echo [startup]   Updated: %%~E
)

:skip_tunnel

:: ── 4. Start Next.js dev server ───────────────────────────
echo [startup] Starting Next.js dev server on port 3000...
cd /d "%WEBSITE_DIR%"
start "SportStock-Website" /b cmd /c "npx next dev --port 3000 >> "%LOG_WEBSITE%" 2>&1"

set /a i=0
:wait_web
timeout /t 1 /nobreak >nul
curl -sf http://localhost:3000 >nul 2>&1
if %errorlevel% equ 0 (
    echo [startup]   Website is ready ✓
    goto web_ready
)
set /a i+=1
if %i% lss 30 goto wait_web
echo [startup]   WARNING: Website did not respond within 30s

:web_ready
echo.
echo ════════════════════════════════════════════════════════
echo   All services running!
echo ════════════════════════════════════════════════════════
echo.
echo   Backend (local):  http://localhost:8000
echo   Backend (docs):   http://localhost:8000/docs
if exist "%TUNNEL_URL_FILE%" (
    set /p TURL=<"%TUNNEL_URL_FILE%"
    echo   Backend (tunnel): !TURL!
)
echo   Website:          http://localhost:3000
echo.
echo   Logs:
echo     Backend: %LOG_BACKEND%
echo     Tunnel:  %LOG_TUNNEL%
echo     Website: %LOG_WEBSITE%
echo.
echo   Run stop.bat to stop all services.
echo.
