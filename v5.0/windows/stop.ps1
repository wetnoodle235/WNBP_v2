# ──────────────────────────────────────────────────────────────────
# SportStock V5.0 — Stop all services (PowerShell)
# Windows equivalent of scripts/stop.sh
# ──────────────────────────────────────────────────────────────────

$PidFiles = @{
    "Backend"          = "$env:TEMP\sportstock-backend.pid"
    "Cloudflare Tunnel"= "$env:TEMP\sportstock-tunnel.pid"
    "Website"          = "$env:TEMP\sportstock-website.pid"
}

$ScriptDir  = Split-Path $MyInvocation.MyCommand.Path -Parent
$Root       = Split-Path $ScriptDir -Parent
$EnvLocal   = Join-Path $Root "website\.env.local"
$TunnelFile = "$env:TEMP\sportstock-tunnel-url.txt"

$stopped = 0

Write-Host "[stop] Stopping SportStock V5.0 services..." -ForegroundColor Cyan

foreach ($entry in $PidFiles.GetEnumerator()) {
    $name    = $entry.Key
    $pidFile = $entry.Value
    if (Test-Path $pidFile) {
        $pid_ = [int](Get-Content $pidFile)
        try {
            Stop-Process -Id $pid_ -Force -ErrorAction Stop
            Write-Host "[stop] Stopped $name (PID $pid_)" -ForegroundColor Green
            $stopped++
        } catch {
            Write-Host "[stop] $name (PID $pid_): already stopped" -ForegroundColor Yellow
        }
        Remove-Item $pidFile -Force
    } else {
        Write-Host "[stop] $name: no PID file found (not running?)" -ForegroundColor Yellow
    }
}

# Kill anything still holding our ports (best-effort)
foreach ($port in @(8000, 3000)) {
    $conn = netstat -aon | Select-String ":$port\s.*LISTENING" | Select-Object -First 1
    if ($conn) {
        $pid_ = ($conn -split "\s+")[-1]
        try { Stop-Process -Id ([int]$pid_) -Force; Write-Host "[stop] Killed process on port $port (PID $pid_)" } catch {}
    }
}

# Cleanup tunnel URL file
if (Test-Path $TunnelFile) { Remove-Item $TunnelFile -Force }

# Restore .env.local API URL
if (Test-Path $EnvLocal) {
    $content = Get-Content $EnvLocal
    if ($content -match "trycloudflare\.com") {
        $content = $content -replace "^NEXT_PUBLIC_API_URL=.*", "NEXT_PUBLIC_API_URL=http://127.0.0.1:8000"
        $content | Set-Content $EnvLocal
        Write-Host "[stop] Restored .env.local API URL to http://127.0.0.1:8000" -ForegroundColor Green
    }
}

if ($stopped -gt 0) {
    Write-Host "[stop] Stopped $stopped service(s) ✓" -ForegroundColor Green
} else {
    Write-Host "[stop] No running services found." -ForegroundColor Yellow
}
