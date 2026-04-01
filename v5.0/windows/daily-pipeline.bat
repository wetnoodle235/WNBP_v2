@echo off
:: ──────────────────────────────────────────────────────────
:: V5.0 — Daily data collection pipeline
:: Windows equivalent of scripts/daily-pipeline.sh
:: Usage: daily-pipeline.bat [--sports=nba,nfl] [--providers=espn,oddsapi]
:: ──────────────────────────────────────────────────────────

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%.."

:: Get current year
for /f "tokens=1 delims=/" %%Y in ("%date%") do set YEAR=%%Y
for /f %%Y in ('powershell -Command "Get-Date -Format yyyy"') do set YEAR=%%Y

echo ═══════════════════════════════════════════════════
for /f %%T in ('powershell -Command "Get-Date -Format 'yyyy-MM-dd HH:mm'"') do echo   V5.0 Daily Pipeline — %%T
echo ═══════════════════════════════════════════════════

:: Load .env if present
if exist "%ROOT%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%ROOT%\.env") do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
    )
)

:: Step 1: Import data
echo.
echo ── Step 1: Importing data ──────────────────────────
cd /d "%ROOT%\importers"
call npx tsx src/cli.ts --all --seasons="%YEAR%" %*
if %errorlevel% neq 0 (
    echo ERROR: Data import failed
    exit /b 1
)

:: Step 2: Normalize
echo.
echo ── Step 2: Normalizing data ────────────────────────
cd /d "%ROOT%\backend"
python -c "from normalization import Normalizer; from config import ALL_SPORTS; n = Normalizer(); n.run_all(ALL_SPORTS, [%YEAR%]); print('Normalization complete')"
if %errorlevel% neq 0 (
    echo ERROR: Normalization failed
    exit /b 1
)

:: Step 3: Extract features
echo.
echo ── Step 3: Extracting features ─────────────────────
python -c "from features import extract_features; from config import ALL_SPORTS; from pathlib import Path; data_dir = Path(r'%ROOT%\data'); [print(f'  {s}: {len(df)} games, {len(df.columns)} features') if (df := extract_features(s, %YEAR%, data_dir)) is not None and len(df) > 0 else print(f'  {s}: skipped') for s in ALL_SPORTS]; print('Feature extraction complete')"

echo.
echo ═══════════════════════════════════════════════════
for /f %%T in ('powershell -Command "Get-Date -Format HH:mm"') do echo   Pipeline complete — %%T
echo ═══════════════════════════════════════════════════
