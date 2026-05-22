@echo off
cd /d C:\Users\1\test_feed
for /f "tokens=1,2 delims==" %%a in (.env) do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"C:\Users\1\test_feed\venv\Scripts\python.exe" "C:\Users\1\test_feed\run_scheduler.py" --interval 10
