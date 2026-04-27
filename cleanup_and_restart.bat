@echo off
REM Wholesaler-Control-Center cleanup and restart launcher
REM Delegates to cleanup_and_restart.ps1 with ExecutionPolicy Bypass

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0cleanup_and_restart.ps1"

echo.
echo Press any key to close...
pause >nul
