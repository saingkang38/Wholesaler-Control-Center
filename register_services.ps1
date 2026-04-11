$python  = "C:\Users\saing\AppData\Local\Programs\Python\Python312\python.exe"
$workdir = "C:\Dev\Wholesaler-Control-Center"

$trigger  = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

# Flask 서버
$action1 = New-ScheduledTaskAction -Execute $python -Argument "run.py" -WorkingDirectory $workdir
Register-ScheduledTask -TaskName "WCC-Flask" -Action $action1 -Trigger $trigger -Settings $settings -RunLevel Highest -Force
Write-Output "WCC-Flask 등록 완료"

# 스케줄러
$action2 = New-ScheduledTaskAction -Execute $python -Argument "scheduler.py" -WorkingDirectory $workdir
Register-ScheduledTask -TaskName "WCC-Scheduler" -Action $action2 -Trigger $trigger -Settings $settings -RunLevel Highest -Force
Write-Output "WCC-Scheduler 등록 완료"

Write-Output "완료. 엔터를 누르면 창이 닫힙니다."
Read-Host
