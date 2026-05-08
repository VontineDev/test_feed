@echo off
cd /d "%~dp0"
for /f "tokens=1,2 delims==" %%a in (.env) do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)
"%~dp0venv\Scripts\python.exe" "%~dp0run_scheduler.py" --interval 3
