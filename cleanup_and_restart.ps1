# Wholesaler-Control-Center 청소 + 재시작 스크립트
#
# 동작:
#   1. 이 프로젝트의 run.py / scheduler.py 프로세스만 종료 (streamlit 등 다른 프로젝트 보존)
#   2. stale 파일(server.pid, scheduler lock) 정리
#   3. Flask 서버 새로 1개 백그라운드 기동
#   4. Scheduler 새로 1개 백그라운드 기동
#   5. 포트 5000 LISTEN 확인 + 결과 보고
#
# 로그는 logs/ 폴더에 저장됨.

$ErrorActionPreference = "Continue"
$root   = "C:\dev\Wholesaler-Control-Center"
$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

Write-Host ""
Write-Host "=== Wholesaler 청소 + 재시작 ===" -ForegroundColor Cyan
Write-Host ""

# 1) 프로젝트 관련 python 프로세스만 종료 (CommandLine 기준)
$targets = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '\brun\.py$|\bscheduler\.py$' }

if ($targets) {
    Write-Host "[1] 종료 대상: $($targets.Count)개"
    foreach ($t in $targets) {
        $script = ($t.CommandLine -split ' ')[-1]
        Write-Host ("    PID {0} - {1}" -f $t.ProcessId, $script)
        Stop-Process -Id $t.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
} else {
    Write-Host "[1] 종료할 프로세스 없음 (이미 깨끗)"
}

# 2) stale 파일 정리
Write-Host "[2] stale 파일 정리"
Remove-Item (Join-Path $root "server.pid") -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $env:TEMP "wholesaler_scheduler.lock") -Force -ErrorAction SilentlyContinue

# 3) Flask 서버 시작 (백그라운드)
Write-Host "[3] Flask 시작..."
Start-Process -FilePath "py" -ArgumentList "-3.12", "run.py" `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $logDir "flask.log") `
    -RedirectStandardError  (Join-Path $logDir "flask.err.log")
Start-Sleep -Seconds 6

$port5000 = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue
if ($port5000) {
    Write-Host ("    OK - 포트 5000 LISTEN (PID {0})" -f $port5000.OwningProcess) -ForegroundColor Green
} else {
    Write-Host "    [실패] $logDir\flask.err.log 확인" -ForegroundColor Red
}

# 4) Scheduler 시작 (백그라운드)
Write-Host "[4] Scheduler 시작..."
Start-Process -FilePath "py" -ArgumentList "-3.12", "scheduler.py" `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $logDir "scheduler.log") `
    -RedirectStandardError  (Join-Path $logDir "scheduler.err.log")
Start-Sleep -Seconds 3

$sched = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '\bscheduler\.py$' }
if ($sched) {
    Write-Host ("    OK - PID {0}" -f $sched.ProcessId) -ForegroundColor Green
} else {
    Write-Host "    [실패] $logDir\scheduler.err.log 확인" -ForegroundColor Red
}

# 5) 최종 상태
Write-Host ""
Write-Host "=== 완료 ===" -ForegroundColor Cyan
Write-Host "대시보드: http://localhost:5000/dashboard"
Write-Host "Flask 로그:    $logDir\flask.log"
Write-Host "Scheduler 로그: $logDir\scheduler.log"
Write-Host ""
Write-Host "이 창은 닫아도 됩니다. Flask·Scheduler는 백그라운드에서 계속 동작." -ForegroundColor Gray
Write-Host ""
