# start_dashboard.ps1 — 대시보드 백엔드 시작/재시작 (단일 진입점)
# 용도: 수동 실행 및 재시작. 자동 시작은 Task Scheduler(TradingDashboard)가 담당.
# 실행: .\scripts\start_dashboard.ps1

$ProjectDir = "C:\Users\1\test_feed"
$BatFile    = "$ProjectDir\scripts\start_dashboard_service.bat"
$LogFile    = "$ProjectDir\logs\dashboard.log"

# 로그 디렉터리 생성
if (-not (Test-Path "$ProjectDir\logs")) {
    New-Item -ItemType Directory -Path "$ProjectDir\logs" | Out-Null
}

# 포트 8000 선점 프로세스 종료
$conn = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($conn) {
    Write-Host "포트 8000 점유 프로세스 종료 (PID $($conn.OwningProcess))..."
    Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

# uvicorn 시작 (BAT 파일 경유 — 파이프라인 없이 직접 리다이렉션으로 안정적)
Write-Host "Dashboard 시작 중..."
Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"$BatFile`"" -WindowStyle Hidden

Write-Host "완료. 로그: $LogFile"
Write-Host "확인: Get-NetTCPConnection -LocalPort 8000 -State Listen"
