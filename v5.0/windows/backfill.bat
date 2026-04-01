@echo off
:: ──────────────────────────────────────────────────────────
:: V5.0 — Backfill historical data (2023–2026)
:: Windows equivalent of scripts/backfill.sh
:: Usage: backfill.bat [--providers=espn,nbastats]
:: ──────────────────────────────────────────────────────────

setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."

echo ═══════════════════════════════════════════════════
echo   V5.0 Historical Backfill — 2023–2026
echo ═══════════════════════════════════════════════════

cd /d "%ROOT%\importers"

call npx tsx src/cli.ts --all --seasons=2023,2024,2025,2026 %*
if %errorlevel% neq 0 (
    echo ERROR: Backfill failed
    exit /b 1
)

echo.
echo Backfill complete!
