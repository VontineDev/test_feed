@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

title Trading Dashboard - Restart

echo.
echo ============================================================
echo   Trading Dashboard 재시작
echo ============================================================
echo.

:: ── 설정 ─────────────────────────────────────────────────────
set ROOT=C:\Users\1\test_feed
set PYTHON=%ROOT%\venv\Scripts\python.exe
set BACKEND=%ROOT%\dashboard\backend
set ENV_FILE=%ROOT%\.env
set PORT=8000

:: python 존재 확인
if not exist "%PYTHON%" (
    echo [경고] venv python 없음. 시스템 python 사용.
    set PYTHON=python
)

:: ── 1. 기존 프로세스 종료 ────────────────────────────────────
echo [1/3] 포트 %PORT% 기존 서버 종료 중...
set KILLED=0

:: 부모 PID 확인
for /f "tokens=5" %%i in ('netstat -ano 2^>nul ^| findstr ":%PORT% "^| findstr "LISTENING"') do (
    if not "%%i"=="" (
        set PARENT_PID=%%i
        echo   포트 점유 PID: %%i

        :: 프로세스 트리 전체 종료 (/T 플래그가 자식까지 포함)
        taskkill /T /F /PID %%i >nul 2>&1
        set KILLED=1
    )
)

:: python.exe 중 uvicorn 실행 중인 것도 추가 정리 (for/f 더블쿼트 이슈 → PowerShell 사용)
powershell -NoProfile -Command "Get-WmiObject Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*uvicorn*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1

if "!KILLED!"=="0" (
    echo   실행 중인 서버 없음.
) else (
    timeout /t 2 /nobreak >nul
    echo   종료 완료.
)

:: ── 2. .env 환경변수 로드 ────────────────────────────────────
echo [2/3] 환경변수 로드 중...
if not exist "%ENV_FILE%" (
    echo   [경고] .env 파일 없음 ^(경로: %ENV_FILE%^)
    goto :start_server
)
for /f "usebackq tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    set "_k=%%a"
    :: 공백 제거
    set "_k=!_k: =!"
    :: 빈 줄 / 주석(#) 건너뜀
    if not "!_k!"=="" (
        set "_first=!_k:~0,1!"
        if not "!_first!"=="#" (
            set "!_k!=%%b"
        )
    )
)
echo   환경변수 로드 완료.

:start_server
:: ── 3. uvicorn 시작 ──────────────────────────────────────────
echo [3/3] 서버 시작...
echo.
cd /d "%BACKEND%"

echo   주소  : http://localhost:%PORT%
echo   외부  : http://0.0.0.0:%PORT%
echo   종료  : Ctrl+C
echo.
echo ============================================================
echo.

"%PYTHON%" -m uvicorn main:app --host 0.0.0.0 --port %PORT% --log-level warning

echo.
echo 서버가 종료되었습니다.
pause
endlocal
