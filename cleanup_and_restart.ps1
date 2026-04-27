# Wholesaler-Control-Center cleanup and restart script
#
# Steps:
#   1. Stop only this project's run.py / scheduler.py processes
#      (other Python processes such as streamlit are preserved)
#   2. Remove stale files (server.pid, scheduler lock)
#   3. Start Flask server in background
#   4. Start Scheduler in background
#   5. Verify port 5000 LISTEN and report status
#
# Logs are saved under logs/ folder.

$ErrorActionPreference = "Continue"
$root   = "C:\dev\Wholesaler-Control-Center"
$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

Write-Host ""
Write-Host "=== Wholesaler cleanup and restart ===" -ForegroundColor Cyan
Write-Host ""

# 1) Find and stop project-related python processes only (CommandLine match)
#    NOTE: trailing space 가 commandline 에 붙는 경우가 있어 $ anchor 만 쓰면 누락됨.
#    공백 또는 EOL 모두 매치하도록 (\s|$) 사용.
$targets = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '\brun\.py(\s|$)|\bscheduler\.py(\s|$)' }

if ($targets) {
    Write-Host "[1] Stopping $($targets.Count) project process(es):"
    foreach ($t in $targets) {
        $script = ($t.CommandLine -split ' ')[-1]
        Write-Host ("    PID {0} - {1}" -f $t.ProcessId, $script)
        Stop-Process -Id $t.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
} else {
    Write-Host "[1] No matching processes (already clean)"
}

# 2) Remove stale lock/pid files
Write-Host "[2] Cleaning stale files"
Remove-Item (Join-Path $root "server.pid") -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $env:TEMP "wholesaler_scheduler.lock") -Force -ErrorAction SilentlyContinue

# 3) Start Flask in a VISIBLE new window (so the red identification banner shows up)
#    이 창을 닫으면 Flask 가 종료됨 — 다른 프로젝트 서버와 헷갈리지 않게 빨간 배너로 식별
Write-Host "[3] Starting Flask (새 창에서 보이게 띄움)..."
Start-Process -FilePath "py" -ArgumentList "-3.12", "run.py" `
    -WorkingDirectory $root
Start-Sleep -Seconds 6

$port5000 = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue
if ($port5000) {
    Write-Host ("    OK - port 5000 LISTEN (PID {0})" -f $port5000.OwningProcess) -ForegroundColor Green
} else {
    Write-Host "    [FAIL] check $logDir\flask.err.log" -ForegroundColor Red
}

# 4) Start Scheduler in background
Write-Host "[4] Starting Scheduler..."
Start-Process -FilePath "py" -ArgumentList "-3.12", "scheduler.py" `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $logDir "scheduler.log") `
    -RedirectStandardError  (Join-Path $logDir "scheduler.err.log")
Start-Sleep -Seconds 6

# 정규식: trailing 공백 케이스도 매치
$sched = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '\bscheduler\.py(\s|$)' }
if ($sched) {
    $cnt = ($sched | Measure-Object).Count
    if ($cnt -gt 1) {
        Write-Host ("    [경고] scheduler.py 가 {0}개 떠있음 — 중복 발생" -f $cnt) -ForegroundColor Yellow
        foreach ($s in $sched) { Write-Host ("       PID {0}" -f $s.ProcessId) }
    } else {
        Write-Host ("    OK - PID {0}" -f $sched.ProcessId) -ForegroundColor Green
    }
} else {
    Write-Host "    [FAIL] check $logDir\scheduler.err.log" -ForegroundColor Red
}

# 5) Final summary
Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Cyan
Write-Host "Dashboard:      http://localhost:5000/dashboard"
Write-Host "Flask 창:       빨간색 배너의 'Wholesaler 도매처 통합 관리 서버' 창" -ForegroundColor Red
Write-Host "Scheduler log:  $logDir\scheduler.log (백그라운드 실행)"
Write-Host ""
Write-Host "* 이 창(현재 cleanup 창)은 닫아도 됨 — Flask/Scheduler 는 살아있음" -ForegroundColor Gray
Write-Host "* 단, 빨간색 'Wholesaler 도매처 통합 관리 서버' 창을 닫으면 Flask 가 종료됨" -ForegroundColor Yellow
Write-Host ""
