# register_task.ps1  —  작업 스케줄러 등록 스크립트
# PowerShell 관리자 권한으로 실행하세요.
# 실행: .\register_task.ps1

# ── 설정 (실제 경로로 수정) ───────────────────────────────────
$ProjectDir = "C:\Users\Jin\test_feed"
$PythonExe  = "$ProjectDir\venv\Scripts\python.exe"
$ScriptFile = "$ProjectDir\run_scheduler.py"
$TaskName   = "NewsCrawler"
$Interval   = 10   # 수집 간격 (분)

# ── 기존 작업 제거 후 재등록 ─────────────────────────────────
$existing = schtasks /Query /TN $TaskName 2>$null
if ($existing) {
    Write-Host "기존 작업 제거 중..."
    schtasks /Delete /TN $TaskName /F | Out-Null
}

# ── 환경변수 로드를 포함한 실행 커맨드 ───────────────────────
# .env 파일을 읽어서 환경변수로 설정 후 python 실행
$WrapperScript = "$ProjectDir\start_crawler.bat"

@"
@echo off
cd /d $ProjectDir
for /f "tokens=1,2 delims==" %%a in (.env) do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"$PythonExe" "$ScriptFile" --interval $Interval
"@ | Set-Content -Path $WrapperScript -Encoding UTF8

Write-Host "래퍼 스크립트 생성: $WrapperScript"

# ── 작업 스케줄러 등록 ───────────────────────────────────────
$Action   = New-ScheduledTaskAction -Execute $WrapperScript -WorkingDirectory $ProjectDir
$Trigger1 = New-ScheduledTaskTrigger -AtStartup
$Trigger2 = New-ScheduledTaskTrigger -AtLogOn

$Settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 10 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable

$Principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger @($Trigger1, $Trigger2) `
    -Settings $Settings `
    -Principal $Principal `
    -Description "뉴스 크롤러 자동 실행 및 재시작" `
    -Force | Out-Null

Write-Host ""
Write-Host "작업 등록 완료: $TaskName"
Write-Host ""
Write-Host "확인:"
Write-Host "  schtasks /Query /TN $TaskName /FO LIST"
Write-Host ""
Write-Host "수동 실행:"
Write-Host "  schtasks /Run /TN $TaskName"
Write-Host ""
Write-Host "중지:"
Write-Host "  schtasks /End /TN $TaskName"
