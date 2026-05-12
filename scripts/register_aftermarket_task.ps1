# register_aftermarket_task.ps1  —  시간외 단일가 수집 작업 스케줄러 등록
# PowerShell 관리자 권한으로 실행하세요.
# 실행: .\scripts\register_aftermarket_task.ps1

# ── 설정 ─────────────────────────────────────────────────────────
$ProjectDir = "C:\Users\1\test_feed"
$PythonExe  = "$ProjectDir\venv\Scripts\python.exe"
$ScriptFile = "$ProjectDir\kiwoom_aftermarket_sync.py"
$TaskName   = "KiwoomAftermarketSync"
$RunHour    = 16   # 실행 시각 (시)
$RunMinute  = 5    # 실행 시각 (분) — 시간외 단일가 마감(16:00) 후 5분

# ── BAT 래퍼 생성 (.env 로드 포함) ───────────────────────────────
$WrapperScript = "$ProjectDir\run_aftermarket_sync.bat"

@"
@echo off
cd /d $ProjectDir
for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%b"=="" (
        if not "%%a:~0,1%"=="#" set "%%a=%%b"
    )
)
"$PythonExe" "$ScriptFile" --incremental >> "$ProjectDir\logs\aftermarket_sync.log" 2>&1
"@ | Set-Content -Path $WrapperScript -Encoding UTF8

Write-Host "래퍼 스크립트 생성: $WrapperScript"

# ── logs 디렉터리 생성 ───────────────────────────────────────────
$LogDir = "$ProjectDir\logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
    Write-Host "로그 디렉터리 생성: $LogDir"
}

# ── 기존 작업 제거 후 재등록 ─────────────────────────────────────
$existing = schtasks /Query /TN $TaskName 2>$null
if ($existing) {
    Write-Host "기존 작업 제거 중..."
    schtasks /Delete /TN $TaskName /F | Out-Null
}

# ── 트리거: 평일(월~금) 16:05 ────────────────────────────────────
$Action = New-ScheduledTaskAction `
    -Execute $WrapperScript `
    -WorkingDirectory $ProjectDir

$Trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At "$($RunHour):$($RunMinute.ToString('D2'))"

$Settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable

$Principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "키움 REST API 시간외 단일가 수집 (매일 16:05, 월~금)" `
    -Force | Out-Null

Write-Host ""
Write-Host "작업 등록 완료: $TaskName"
Write-Host "실행 시각: 평일 $($RunHour):$($RunMinute.ToString('D2'))"
Write-Host "로그: $LogDir\aftermarket_sync.log"
Write-Host ""
Write-Host "확인:    schtasks /Query /TN $TaskName /FO LIST"
Write-Host "수동실행: schtasks /Run /TN $TaskName"
Write-Host "중지:    schtasks /End /TN $TaskName"
Write-Host "제거:    schtasks /Delete /TN $TaskName /F"
