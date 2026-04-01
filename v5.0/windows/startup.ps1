# ──────────────────────────────────────────────────────────────────
# SportStock V5.0 — Full startup with Cloudflare tunnel (PowerShell)
# Windows equivalent of scripts/startup.sh
# ──────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"
$ScriptDir   = Split-Path $MyInvocation.MyCommand.Path -Parent
$Root        = Split-Path $ScriptDir -Parent
$BackendDir  = Join-Path $Root "backend"
$WebsiteDir  = Join-Path $Root "website"

$LogBackend  = "$env:TEMP\sportstock-backend.log"
$LogTunnel   = "$env:TEMP\sportstock-tunnel.log"
$LogWebsite  = "$env:TEMP\sportstock-website.log"
$TunnelUrl   = "$env:TEMP\sportstock-tunnel-url.txt"
$PidBackend  = "$env:TEMP\sportstock-backend.pid"
$PidTunnel   = "$env:TEMP\sportstock-tunnel.pid"
$PidWebsite  = "$env:TEMP\sportstock-website.pid"

function Log    { param($msg) Write-Host "[startup] $msg" -ForegroundColor Green }
function Warn   { param($msg) Write-Host "[startup] $msg" -ForegroundColor Yellow }
function Err    { param($msg) Write-Host "[startup] $msg" -ForegroundColor Red -BackgroundColor Black }

Write-Host ""
Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  SportStock V5.0 — Full Startup" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── Load .env ─────────────────────────────────────────────
$envFile = Join-Path $Root ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | Where-Object { $_ -match "^\s*[^#]" -and $_ -match "=" } | ForEach-Object {
        $kv = $_ -split "=", 2
        [System.Environment]::SetEnvironmentVariable($kv[0].Trim(), $kv[1].Trim(), "Process")
    }
}

# ── Pre-flight checks ─────────────────────────────────────
Log "Running pre-flight checks..."
$ok = $true
@("python", "node", "cloudflared") | ForEach-Object {
    if (-not (Get-Command $_ -ErrorAction SilentlyContinue)) { Err "$_ not found"; $ok = $false }
}
$uvicorn = if (Get-Command uvicorn -ErrorAction SilentlyContinue) { "uvicorn" } else { "python -m uvicorn" }
if (-not (Test-Path (Join-Path $BackendDir "main.py")))     { Err "Backend not found at $BackendDir\main.py"; $ok = $false }
if (-not (Test-Path (Join-Path $WebsiteDir "package.json"))) { Err "Website not found at $WebsiteDir\package.json"; $ok = $false }
if (-not $ok) { Err "Pre-flight checks failed."; exit 1 }
Log "Pre-flight checks passed ✓"

# Truncate logs
"", "", "" | ForEach-Object { $_ | Out-File $LogBackend; $_ | Out-File $LogTunnel; $_ | Out-File $LogWebsite }

# ── 1. Start FastAPI backend ──────────────────────────────
Log "Starting FastAPI backend on port 8000..."
$be = Start-Process -FilePath "cmd" `
    -ArgumentList "/c", "cd /d `"$BackendDir`" && $uvicorn main:app --host 0.0.0.0 --port 8000 --reload >> `"$LogBackend`" 2>&1" `
    -PassThru -WindowStyle Minimized
$be.Id | Out-File $PidBackend
Log "  Backend PID: $($be.Id)  (log: $LogBackend)"

for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep 1
    try { if ((Invoke-WebRequest http://localhost:8000/health -UseBasicParsing -TimeoutSec 1).StatusCode -eq 200) { Log "  Backend is healthy ✓"; break } } catch {}
}

# ── 2. Start Cloudflare tunnel ────────────────────────────
Log "Starting Cloudflare quick tunnel -> localhost:8000..."
$tn = Start-Process -FilePath "cmd" `
    -ArgumentList "/c", "cloudflared tunnel --url http://localhost:8000 >> `"$LogTunnel`" 2>&1" `
    -PassThru -WindowStyle Minimized
$tn.Id | Out-File $PidTunnel
Log "  Tunnel PID: $($tn.Id)  (log: $LogTunnel)"

Log "  Waiting for tunnel URL (up to 30s)..."
$tunnelUrl = $null
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 1
    if (Test-Path $LogTunnel) {
        $m = (Get-Content $LogTunnel -Raw) -match 'https://[a-zA-Z0-9-]+\.trycloudflare\.com'
        if ($m) { $tunnelUrl = $Matches[0]; break }
    }
}
if ($tunnelUrl) {
    $tunnelUrl | Out-File $TunnelUrl -Encoding ascii
    Log "  Tunnel URL: $tunnelUrl ✓"
} else {
    Warn "  Failed to capture tunnel URL. Check $LogTunnel"
}

# ── 3. Update website .env ────────────────────────────────
if ($tunnelUrl) {
    Log "Updating website environment with tunnel URL..."
    foreach ($envPath in @("$WebsiteDir\.env.local", "$WebsiteDir\.env.production")) {
        if (Test-Path $envPath) {
            $content = Get-Content $envPath
            if ($content -match "^NEXT_PUBLIC_API_URL=") {
                $content = $content -replace "^NEXT_PUBLIC_API_URL=.*", "NEXT_PUBLIC_API_URL=$tunnelUrl"
            } else {
                $content += "NEXT_PUBLIC_API_URL=$tunnelUrl"
            }
            $content | Set-Content $envPath
        } else {
            "NEXT_PUBLIC_API_URL=$tunnelUrl" | Out-File $envPath -Encoding ascii
        }
        Log "  Updated: $envPath"
    }
}

# ── 4. Start Next.js dev server ───────────────────────────
Log "Starting Next.js dev server on port 3000..."
$ws = Start-Process -FilePath "cmd" `
    -ArgumentList "/c", "cd /d `"$WebsiteDir`" && npx next dev --port 3000 >> `"$LogWebsite`" 2>&1" `
    -PassThru -WindowStyle Minimized
$ws.Id | Out-File $PidWebsite
Log "  Website PID: $($ws.Id)  (log: $LogWebsite)"

for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 1
    try { if ((Invoke-WebRequest http://localhost:3000 -UseBasicParsing -TimeoutSec 1).StatusCode -eq 200) { Log "  Website is ready ✓"; break } } catch {}
}

# ── Summary ───────────────────────────────────────────────
Write-Host ""
Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  All services running!" -ForegroundColor Green
Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Backend (local):  http://localhost:8000"
Write-Host "  Backend (docs):   http://localhost:8000/docs"
if ($tunnelUrl) { Write-Host "  Backend (tunnel): $tunnelUrl" -ForegroundColor Cyan }
Write-Host "  Website:          http://localhost:3000"
Write-Host ""
Write-Host "  Logs:"
Write-Host "    Backend: $LogBackend"
Write-Host "    Tunnel:  $LogTunnel"
Write-Host "    Website: $LogWebsite"
Write-Host ""
Write-Host "  Run stop.ps1 to stop all services." -ForegroundColor Yellow
Write-Host ""

try { Wait-Process -Id $be.Id, $tn.Id, $ws.Id } catch {}
