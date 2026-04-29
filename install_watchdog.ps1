# 스케줄러 워치독을 Windows 작업 스케줄러에 등록
#
# 실행 후:
#  - 5분마다 워치독이 자동 실행되어 scheduler.py 살아있는지 체크
#  - 죽었으면 자동 재시작
#  - 사장님 로그인 5분 안에 스케줄러가 살아남
#
# 한 번만 실행하면 됩니다. 관리자 권한 필요 없음.

$taskName = "WholesalerSchedulerWatchdog"
$root = "C:\dev\Wholesaler-Control-Center"
$watchdogPath = Join-Path $root "scheduler_watchdog.ps1"

if (-not (Test-Path $watchdogPath)) {
    Write-Host "[ERROR] 워치독 스크립트 없음: $watchdogPath" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== 스케줄러 워치독 등록 ===" -ForegroundColor Cyan
Write-Host "작업명: $taskName"
Write-Host "스크립트: $watchdogPath"
Write-Host ""

$schtasks = Join-Path $env:SystemRoot "System32\schtasks.exe"

# 기존 task 제거 (있으면)
& $schtasks /Query /TN $taskName 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[1] 기존 작업 제거"
    & $schtasks /Delete /TN $taskName /F 2>&1 | Out-Null
}

# 액션 명령: powershell이 워치독 ps1 파일을 히든으로 실행
$tr = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$watchdogPath`""

# 5분마다 실행 (사용자 권한, 영구 반복)
Write-Host "[2] schtasks로 5분 간격 작업 등록 시도"
$result = & $schtasks /Create /TN $taskName /TR $tr /SC MINUTE /MO 5 /RL LIMITED /F 2>&1
$exit = $LASTEXITCODE

if ($exit -eq 0) {
    Write-Host "[3] 등록 성공" -ForegroundColor Green
    Write-Host ""
    Write-Host "동작 요약:" -ForegroundColor Cyan
    Write-Host "  - 등록 시점부터 5분마다 워치독 자동 실행"
    Write-Host "  - 워치독이 scheduler.py 살아있는지 체크"
    Write-Host "  - 죽어있으면 자동 재시작 (logs\watchdog.log에 기록)"
    Write-Host "  - PC 재부팅 후 로그인하면 5분 안에 자동 재개"
    Write-Host ""
    Write-Host "확인 방법:" -ForegroundColor Cyan
    Write-Host "  - 작업 스케줄러 GUI: taskschd.msc 실행 → 라이브러리에서 '$taskName' 확인"
    Write-Host "  - 다음 실행 시간 보기: $env:SystemRoot\System32\schtasks.exe /Query /TN $taskName /V /FO LIST"
    Write-Host "  - 워치독 로그: $root\logs\watchdog.log"
    Write-Host ""
    Write-Host "지금 즉시 1회 실행해보려면:" -ForegroundColor Yellow
    Write-Host "  $env:SystemRoot\System32\schtasks.exe /Run /TN $taskName"
    Write-Host ""
    Write-Host "제거하려면:" -ForegroundColor Yellow
    Write-Host "  $env:SystemRoot\System32\schtasks.exe /Delete /TN $taskName /F"
    Write-Host ""
} else {
    Write-Host "[FAIL] 등록 실패 (exit=$exit)" -ForegroundColor Red
    Write-Host $result
    exit 1
}
