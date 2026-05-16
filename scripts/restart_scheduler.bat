@echo off
echo [%TIME%] Stopping NewsCrawler...
schtasks /End /TN "NewsCrawler" >nul 2>&1

ping -n 4 127.0.0.1 >nul

echo [%TIME%] Starting NewsCrawler...
schtasks /Run /TN "NewsCrawler"

echo [%TIME%] Done. Check Telegram /status to confirm.
