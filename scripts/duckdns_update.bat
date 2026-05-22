@echo off
for /f "tokens=1,2 delims==" %%a in (C:\Users\1\test_feed\.env) do (
    if "%%a"=="DUCK_DNS_DOMAIN" set DUCK_DNS_DOMAIN=%%b
    if "%%a"=="DUCK_DNS_TOKEN" set DUCK_DNS_TOKEN=%%b
)
curl -s "https://www.duckdns.org/update?domains=%DUCK_DNS_DOMAIN%&token=%DUCK_DNS_TOKEN%&ip=" >> C:\Users\1\test_feed\logs\duckdns.log 2>&1
