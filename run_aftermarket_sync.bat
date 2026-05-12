@echo off
cd /d C:\Users\1\test_feed
for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%b"=="" (
        if not "%%a:~0,1%"=="#" set "%%a=%%b"
    )
)
"C:\Users\1\test_feed\venv\Scripts\python.exe" "C:\Users\1\test_feed\kiwoom_aftermarket_sync.py" --incremental >> "C:\Users\1\test_feed\logs\aftermarket_sync.log" 2>&1
