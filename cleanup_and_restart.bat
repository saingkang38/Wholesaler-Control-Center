@echo off
REM Wholesaler-Control-Center 청소 + 재시작 (더블클릭용)
REM 실제 작업은 cleanup_and_restart.ps1 에서 수행
REM PowerShell 실행 정책 우회는 -ExecutionPolicy Bypass 로 처리

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0cleanup_and_restart.ps1"

echo.
echo 아무 키나 누르면 창이 닫힙니다.
pause >nul
