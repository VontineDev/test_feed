Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "powershell.exe -ExecutionPolicy Bypass -NonInteractive -WindowStyle Hidden -File ""C:\Users\1\test_feed\scripts\start_dashboard.ps1""", 0, False
