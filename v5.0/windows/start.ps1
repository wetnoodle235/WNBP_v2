# ──────────────────────────────────────────────────────────────────
# SportStock V5.0 — Start all services (PowerShell)
# Windows equivalent of scripts/start.sh
#
# Usage: .\start.ps1
# ──────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path $MyInvocation.MyCommand.Path -Parent
$Root = Split-Path $ScriptDir -Parent
$BackendDir = Join-Path $Root "backend"
$WebsiteDir = Join-Path $Root "website"

Write-Host "Starting V5.0 services..." -ForegroundColor Cyan

# Resolve uvicorn
$uvicorn = if (Get-Command uvicorn -ErrorAction SilentlyContinue) { "uvicorn" } `
           elseif (python -m uvicorn --version 2>$null) { "python -m uvicorn" } `
           else { throw "uvicorn not found. Run: pip install 'uvicorn[standard]'" }

# ── Start backend ─────────────────────────────────────────
Write-Host "  -> Starting backend (port 8000)..." -ForegroundColor Yellow
$backend = Start-Process -FilePath "cmd" `
    -ArgumentList "/c", "cd /d `"$BackendDir`" && $uvicorn main:app --host 127.0.0.1 --port 8000 --reload" `
    -PassThru -WindowStyle Minimized

Write-Host "     PID: $($backend.Id)"

$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 500
    try { if ((Invoke-WebRequest http://127.0.0.1:8000/docs -UseBasicParsing -TimeoutSec 1).StatusCode -eq 200) { $ready = $true; break } }
    catch {}
}
if ($ready) { Write-Host "     Backend ready ✓" -ForegroundColor Green }
else { Write-Warning "Backend did not respond within 10s (may still be starting)" }

# ── Start website ─────────────────────────────────────────
Write-Host "  -> Starting website (port 3000)..." -ForegroundColor Yellow
$nextDev = Join-Path $WebsiteDir ".next-dev"
if (Test-Path $nextDev) { Remove-Item $nextDev -Recurse -Force }

$website = Start-Process -FilePath "cmd" `
    -ArgumentList "/c", "cd /d `"$WebsiteDir`" && npm run dev -- --hostname 0.0.0.0" `
    -PassThru -WindowStyle Minimized

Write-Host "     PID: $($website.Id)"

$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    try { if ((Invoke-WebRequest http://127.0.0.1:3000 -UseBasicParsing -TimeoutSec 1).StatusCode -eq 200) { $ready = $true; break } }
    catch {}
}
if ($ready) { Write-Host "     Website ready ✓" -ForegroundColor Green }
else { Write-Warning "Website did not respond within 20s (may still be compiling)" }

# ── Save PIDs ─────────────────────────────────────────────
$backend.Id  | Out-File "$env:TEMP\sportstock-backend.pid"
$website.Id  | Out-File "$env:TEMP\sportstock-website.pid"

Write-Host ""
Write-Host "Services running:" -ForegroundColor Green
Write-Host "  Backend:  http://localhost:8000  (docs: http://localhost:8000/docs)"
Write-Host "  Website:  http://localhost:3000"
Write-Host ""
Write-Host "Run stop.ps1 to stop all services."

# Keep script alive
try { Wait-Process -Id $backend.Id, $website.Id }
catch { }
