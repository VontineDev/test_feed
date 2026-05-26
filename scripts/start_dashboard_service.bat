@echo off
chcp 65001 >nul
cd /d C:\Users\1\test_feed\dashboard\backend
for /f "usebackq tokens=1,* delims==" %%a in ("C:\Users\1\test_feed\.env") do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
:restart
echo [%date% %time%] uvicorn start >> "C:\Users\1\test_feed\logs\dashboard.log"
"C:\Users\1\test_feed\venv\Scripts\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8000 --log-level warning >> "C:\Users\1\test_feed\logs\dashboard.log" 2>&1
echo [%date% %time%] uvicorn exited, restarting in 5s >> "C:\Users\1\test_feed\logs\dashboard.log"
timeout /t 5 /nobreak >nul
goto restart
