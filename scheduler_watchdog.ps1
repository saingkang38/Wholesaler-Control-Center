# Wholesaler scheduler watchdog
#
# 5분마다 실행되어:
#  1. scheduler.py 프로세스가 살아있는지 확인
#  2. 없으면 logs/scheduler.log + scheduler.err.log로 출력 redirect 후 재시작
#  3. 결과를 logs/watchdog.log에 1줄 기록
#
# Windows Task Scheduler에 등록되어 자동 실행됨.
# (수동 실행도 가능 — `powershell -File .\scheduler_watchdog.ps1`)

$ErrorActionPreference = "Continue"
$root = "C:\dev\Wholesaler-Control-Center"
$logDir = Join-Path $root "logs"
$watchdogLog = Join-Path $logDir "watchdog.log"

# 로그 디렉토리 보장
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

function Write-WatchdogLog($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts $msg" | Out-File -FilePath $watchdogLog -Append -Encoding utf8
}

# scheduler.py 프로세스 검색 (CommandLine 기반)
$schedProc = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match '\bscheduler\.py(\s|$)' }

if ($schedProc) {
    # 살아있음 — 5분 호출 횟수가 많으니 로그는 1시간마다 1번만 (정시 0~4분 범위)
    $minute = (Get-Date).Minute
    if ($minute -lt 5) {
        Write-WatchdogLog "ALIVE PID=$($schedProc.ProcessId) (정상)"
    }
    return
}

Write-WatchdogLog "DEAD - scheduler.py 프로세스 없음, 재시작 시도"

# 잔여 lock 파일 정리 (있으면)
$lockPath = Join-Path $env:TEMP "wholesaler_scheduler.lock"
Remove-Item $lockPath -Force -ErrorAction SilentlyContinue

# 시작 — cleanup_and_restart.ps1과 동일한 패턴 (히든 + 로그 redirect)
try {
    Start-Process -FilePath "py" -ArgumentList "-3.12", "scheduler.py" `
        -WorkingDirectory $root -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir "scheduler.log") `
        -RedirectStandardError  (Join-Path $logDir "scheduler.err.log")

    # 점진 체크: 1초 간격 최대 30초까지 살아나기를 기다림
    # (scheduler.py가 Flask app 로드 등으로 시작에 10초+ 걸릴 수 있음)
    $schedProc = $null
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        $schedProc = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
            Where-Object { $_.CommandLine -match '\bscheduler\.py(\s|$)' }
        if ($schedProc) { break }
    }

    if ($schedProc) {
        Write-WatchdogLog "RESTARTED OK PID=$($schedProc.ProcessId) (시작까지 $($i+1)초)"
    } else {
        Write-WatchdogLog "RESTART FAILED - 30초 대기 후에도 안 떠있음, logs\scheduler.err.log 확인 필요"
    }
} catch {
    Write-WatchdogLog "RESTART EXCEPTION - $($_.Exception.Message)"
}
