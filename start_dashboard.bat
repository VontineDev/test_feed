@echo off
cd /d C:\Users\1\test_feed\dashboard\backend
for /f "tokens=1,2 delims==" %%a in (C:\Users\1\test_feed\.env) do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"C:\Users\1\test_feed\venv\Scripts\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8000 --log-level warning
