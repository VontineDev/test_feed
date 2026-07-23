@echo off
cd /d C:\Users\1\test_feed
"C:\Users\1\test_feed\venv\Scripts\python.exe" -m data.kiwoom_aftermarket_sync --incremental >> "C:\Users\1\test_feed\logs\aftermarket_sync.log" 2>&1
