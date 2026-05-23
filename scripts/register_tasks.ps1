# register_tasks.ps1 — Windows 작업 스케줄러 일괄 등록
# PowerShell 관리자 권한으로 실행하세요.
# 실행: .\scripts\register_tasks.ps1 [-Task all|crawler|aftermarket]

param(
    [ValidateSet("all", "crawler", "aftermarket", "dashboard")]
    [string]$Task = "all"
)

$ProjectDir = "C:\Users\1\test_feed"
$PythonExe  = "$ProjectDir\venv\Scripts\python.exe"
$ScriptsDir = "$ProjectDir\scripts"
$LogDir     = "$ProjectDir\logs"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
    Write-Host "로그 디렉터리 생성: $LogDir"
}

# ── NewsCrawler ──────────────────────────────────────────────────────
function Register-CrawlerTask {
    $TaskName      = "NewsCrawler"
    $WrapperScript = "$ScriptsDir\start_crawler.bat"
    $ScriptFile    = "$ProjectDir\run_scheduler.py"

    # "@ 는 반드시 열 0에 위치해야 함
    @"
@echo off
cd /d $ProjectDir
for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"$PythonExe" "$ScriptFile" --interval 10
"@ | Set-Content -Path $WrapperScript -Encoding UTF8

    $existing = schtasks /Query /TN $TaskName 2>$null
    if ($existing) { schtasks /Delete /TN $TaskName /F | Out-Null }

    $Action   = New-ScheduledTaskAction -Execute $WrapperScript -WorkingDirectory $ProjectDir
    $Triggers = @(
        (New-ScheduledTaskTrigger -AtStartup),
        (New-ScheduledTaskTrigger -AtLogOn)
    )
    $Settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances IgnoreNew `
        -RestartCount 10 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RunOnlyIfNetworkAvailable `
        -StartWhenAvailable
    $Principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

    Register-ScheduledTask `
        -TaskName $TaskName -Action $Action -Trigger $Triggers `
        -Settings $Settings -Principal $Principal `
        -Description "뉴스 크롤러 자동 실행 및 재시작" -Force | Out-Null

    Write-Host "[NewsCrawler] 등록 완료 (시작 시 / 로그온 시)"
    Write-Host "  래퍼:     $WrapperScript"
    Write-Host "  확인:     schtasks /Query /TN $TaskName /FO LIST"
    Write-Host "  수동실행: schtasks /Run /TN $TaskName"
    Write-Host "  중지:     schtasks /End /TN $TaskName"
}

# ── KiwoomAftermarketSync ────────────────────────────────────────────
function Register-AftermarketTask {
    $TaskName      = "KiwoomAftermarketSync"
    $WrapperScript = "$ScriptsDir\run_aftermarket_sync.bat"
    $ScriptFile    = "$ProjectDir\data\kiwoom_aftermarket_sync.py"
    $RunHour       = 16
    $RunMinute     = 5

    # "@ 는 반드시 열 0에 위치해야 함
    @"
@echo off
cd /d $ProjectDir
for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%b"=="" (
        if not "%%a:~0,1%"=="#" set "%%a=%%b"
    )
)
"$PythonExe" "$ScriptFile" --incremental >> "$LogDir\aftermarket_sync.log" 2>&1
"@ | Set-Content -Path $WrapperScript -Encoding UTF8

    $existing = schtasks /Query /TN $TaskName 2>$null
    if ($existing) { schtasks /Delete /TN $TaskName /F | Out-Null }

    $Action   = New-ScheduledTaskAction -Execute $WrapperScript -WorkingDirectory $ProjectDir
    $Trigger  = New-ScheduledTaskTrigger `
        -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
        -At "$($RunHour):$($RunMinute.ToString('D2'))"
    $Settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
        -RunOnlyIfNetworkAvailable `
        -StartWhenAvailable
    $Principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

    Register-ScheduledTask `
        -TaskName $TaskName -Action $Action -Trigger $Trigger `
        -Settings $Settings -Principal $Principal `
        -Description "키움 REST API 시간외 단일가 수집 (매일 16:05, 월~금)" -Force | Out-Null

    Write-Host "[KiwoomAftermarketSync] 등록 완료 (평일 $($RunHour):$($RunMinute.ToString('D2')))"
    Write-Host "  래퍼:     $WrapperScript"
    Write-Host "  로그:     $LogDir\aftermarket_sync.log"
    Write-Host "  확인:     schtasks /Query /TN $TaskName /FO LIST"
    Write-Host "  수동실행: schtasks /Run /TN $TaskName"
    Write-Host "  중지:     schtasks /End /TN $TaskName"
}

# ── TradingDashboard ────────────────────────────────────────────────
function Register-DashboardTask {
    $TaskName      = "TradingDashboard"
    $BackendDir    = "$ProjectDir\dashboard\backend"
    $WrapperScript = "$ScriptsDir\start_dashboard_service.bat"

    # pause 없는 서비스 전용 래퍼 — task scheduler가 종료를 감지해 재시작 가능
    @"
@echo off
chcp 65001 >nul
cd /d $BackendDir
for /f "usebackq tokens=1,* delims==" %%a in ("$ProjectDir\.env") do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"$PythonExe" -m uvicorn main:app --host 0.0.0.0 --port 8000 --log-level warning >> "$LogDir\dashboard.log" 2>&1
"@ | Set-Content -Path $WrapperScript -Encoding UTF8

    $existing = schtasks /Query /TN $TaskName 2>$null
    if ($existing) { schtasks /Delete /TN $TaskName /F | Out-Null }

    $Action   = New-ScheduledTaskAction -Execute $WrapperScript -WorkingDirectory $BackendDir
    $Triggers = @(
        (New-ScheduledTaskTrigger -AtStartup),
        (New-ScheduledTaskTrigger -AtLogOn)
    )
    $Settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances IgnoreNew `
        -RestartCount 10 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -StartWhenAvailable
    $Principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

    Register-ScheduledTask `
        -TaskName $TaskName -Action $Action -Trigger $Triggers `
        -Settings $Settings -Principal $Principal `
        -Description "Trading Dashboard uvicorn 자동 실행 및 재시작 (Caddy → localhost:8000)" -Force | Out-Null

    Write-Host "[TradingDashboard] 등록 완료 (시작 시 / 로그온 시)"
    Write-Host "  래퍼:     $WrapperScript"
    Write-Host "  로그:     $LogDir\dashboard.log"
    Write-Host "  확인:     schtasks /Query /TN $TaskName /FO LIST"
    Write-Host "  수동실행: schtasks /Run /TN $TaskName"
    Write-Host "  중지:     schtasks /End /TN $TaskName"
}

# ── 실행 ────────────────────────────────────────────────────────────
Write-Host ""
switch ($Task) {
    "crawler"     { Register-CrawlerTask }
    "aftermarket" { Register-AftermarketTask }
    "dashboard"   { Register-DashboardTask }
    "all" {
        Register-CrawlerTask
        Write-Host ""
        Register-AftermarketTask
        Write-Host ""
        Register-DashboardTask
    }
}
Write-Host ""

