@echo off
:: NewsCrawler (run_scheduler.py) 재시작 스크립트
:: - Windows Task Scheduler 태스크를 중지 후 재시작
:: - DB RLS 등 init_db() 변경사항 반영 시 필요

echo [%TIME%] NewsCrawler 중지 중...
schtasks /End /TN "NewsCrawler" >nul 2>&1

:: 프로세스가 완전히 종료될 때까지 대기
timeout /t 3 /nobreak >nul

:: 혹시 남아있는 python 프로세스 정리 (run_scheduler.py 한정)
for /f "tokens=2" %%i in ('tasklist /fi "IMAGENAME eq python.exe" /fo csv 2^>nul ^| findstr /i "run_scheduler"') do (
    taskkill /PID %%i /F >nul 2>&1
)

timeout /t 2 /nobreak >nul

echo [%TIME%] NewsCrawler 시작 중...
schtasks /Run /TN "NewsCrawler"

echo [%TIME%] 완료. Telegram에서 /status 로 정상 여부 확인하세요.
