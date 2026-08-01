@echo off
cd /d C:\Users\1\test_feed

tasklist /FI "IMAGENAME eq tor.exe" 2>NUL | find /I "tor.exe" >NUL
if errorlevel 1 (
    start "" /min "C:\Users\1\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe" -f "C:\Users\1\test_feed\tor-daemon\torrc"
)

for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%b"=="" set %%a=%%b
)

:restart
"C:\Users\1\test_feed\venv\Scripts\python.exe" "C:\Users\1\test_feed\run_scheduler.py" --interval 10 --no-summary
timeout /t 5 /nobreak >nul
goto restart
