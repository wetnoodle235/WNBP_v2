@echo off
:: ──────────────────────────────────────────────────────────
:: SportStock V5.0 — Start all services (backend + website)
:: Windows equivalent of scripts/start.sh
:: ──────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."

echo Starting V5.0 services...

:: Check for uvicorn
where uvicorn >nul 2>&1
if %errorlevel% neq 0 (
    python -m uvicorn --version >nul 2>&1
    if !errorlevel! neq 0 (
        echo ERROR: uvicorn not found. Run: pip install "uvicorn[standard]"
        exit /b 1
    )
    set "UVICORN=python -m uvicorn"
) else (
    set "UVICORN=uvicorn"
)

:: Start backend
echo   ^-^> Starting backend (port 8000)...
cd /d "%ROOT%\backend"
start "SportStock-Backend" /b cmd /c "%UVICORN% main:app --host 127.0.0.1 --port 8000 --reload > "%TEMP%\sportstock-backend.log" 2>&1"

:: Wait for backend
echo     Waiting for backend...
set /a tries=0
:wait_backend
timeout /t 1 /nobreak >nul
curl -sf http://127.0.0.1:8000/docs >nul 2>&1
if %errorlevel% equ 0 (
    echo     Backend ready
    goto backend_ready
)
set /a tries+=1
if %tries% lss 20 goto wait_backend
echo     WARNING: Backend did not respond within 20s

:backend_ready

:: Start website
echo   ^-^> Starting website (port 3000)...
cd /d "%ROOT%\website"
if exist ".next-dev" rmdir /s /q ".next-dev"
start "SportStock-Website" /b cmd /c "npm run dev -- --hostname 0.0.0.0 > "%TEMP%\sportstock-website.log" 2>&1"

:: Wait for website
echo     Waiting for website...
set /a tries=0
:wait_website
timeout /t 1 /nobreak >nul
curl -sf http://127.0.0.1:3000 >nul 2>&1
if %errorlevel% equ 0 (
    echo     Website ready
    goto website_ready
)
set /a tries+=1
if %tries% lss 40 goto wait_website
echo     WARNING: Website did not respond within 40s

:website_ready
echo.
echo Services running:
echo   Backend:  http://localhost:8000  (docs: http://localhost:8000/docs)
echo   Website:  http://localhost:3000
echo.
echo Logs:
echo   Backend:  %TEMP%\sportstock-backend.log
echo   Website:  %TEMP%\sportstock-website.log
echo.
echo Run stop.bat to stop all services.
