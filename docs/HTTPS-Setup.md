# HTTPS 설정 가이드

Caddy를 사용해 `vtrading.duckdns.org`에 Let's Encrypt HTTPS를 자동으로 적용하는 방법을 설명합니다.

## 아키텍처

```
인터넷
  │ :443 HTTPS (Let's Encrypt 자동 인증서)
  ▼
Caddy (C:\caddy\)
  │ basicauth — 브라우저 네이티브 인증 다이얼로그
  │ reverse_proxy localhost:8000
  ▼
FastAPI (localhost:8000)
  │ localhost → auth 면제
  ▼
Trading Dashboard (React dist)
```

**왜 Caddy인가?**  
Caddy는 Let's Encrypt 인증서 발급·갱신을 자동으로 처리합니다. 90일 갱신을 직접 관리할 필요가 없습니다. DuckDNS DNS 플러그인이 내장되어 있어 포트 80 없이 DNS-01 챌린지로 인증서를 발급합니다.

**왜 basicauth를 FastAPI가 아닌 Caddy에서 처리하는가?**  
FastAPI는 `localhost`에서 오는 요청을 auth 없이 통과시킵니다(코드: `main.py:149`). Caddy가 프록시를 통해 FastAPI에 연결하면 FastAPI 입장에서는 모두 localhost 접속이므로 인증이 우회됩니다. 따라서 인증은 반드시 Caddy 레이어에서 처리해야 합니다.

---

## 설치 현황

```
C:\caddy\
  caddy.exe          — DuckDNS 플러그인 포함 Caddy v2.11.3
  Caddyfile          — 서버 설정
  start-caddy.ps1    — 수동 시작 스크립트
  caddy.log          — 런타임 로그

C:\Users\1\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\
  CaddyHTTPS.vbs     — 로그인 시 자동 시작
```

인증서 저장 위치: `C:\Users\1\AppData\Roaming\Caddy\`

---

## 현재 Caddyfile

```caddyfile
vtrading.duckdns.org {
    tls {
        dns duckdns {env.DUCKDNS_TOKEN}
        propagation_delay 10s
        propagation_timeout -1
    }

    basicauth {
        admin $2a$14$tHdZT7fIobl/Pc/IPmLmCu279QRjarPifyVEUhxHJF8dtX8Sjzpcq
    }

    reverse_proxy localhost:8000
}
```

`propagation_timeout -1`은 DNS 전파 확인을 건너뜁니다. ISP가 DuckDNS 네임서버(35.182.183.211:53)로의 아웃바운드 포트 53를 차단하기 때문입니다. 챌린지 레코드는 DuckDNS API가 정상 생성하므로 실제 발급에는 문제없습니다.

---

## How-to: Caddy 수동 시작/재시작

```powershell
# 기존 Caddy 종료
Stop-Process -Name "caddy" -Force -ErrorAction SilentlyContinue

# 재시작
$proc = Start-Process -FilePath "cmd.exe" `
    -ArgumentList '/c "set DUCKDNS_TOKEN=<token>&& C:\caddy\caddy.exe run --config C:\caddy\Caddyfile >> C:\caddy\caddy.log 2>&1"' `
    -WorkingDirectory "C:\caddy" -WindowStyle Hidden -PassThru
Write-Output "PID: $($proc.Id)"
```

또는 `start-caddy.ps1`을 직접 실행합니다(토큰이 이미 내장되어 있음).

---

## How-to: basicauth 비밀번호 변경

1. 새 bcrypt 해시 생성:

   ```powershell
   & "C:\caddy\caddy.exe" hash-password --plaintext "새비밀번호"
   ```

2. `C:\caddy\Caddyfile`의 `basicauth` 블록 수정 — `$2a$...` 해시를 교체합니다.

3. Caddy 재시작 (위 명령 참조).

---

## How-to: 새 서버에 동일 설정 재현

### 전제 조건

- DuckDNS 도메인과 토큰 보유
- 라우터에서 포트 443 → 이 PC 포트포워딩 완료
- FastAPI 백엔드(포트 8000) 실행 중

### 1단계: Caddy 다운로드 (DuckDNS 플러그인 포함)

```powershell
New-Item -ItemType Directory -Path "C:\caddy" -Force
$url = "https://caddyserver.com/api/download?os=windows&arch=amd64&p=github.com/caddy-dns/duckdns"
Invoke-WebRequest -Uri $url -OutFile "C:\caddy\caddy.exe" -UseBasicParsing
```

### 2단계: Caddyfile 작성

```powershell
# 비밀번호 해시 생성
$hash = & "C:\caddy\caddy.exe" hash-password --plaintext "원하는비밀번호"

# Caddyfile 생성
@"
yourdomain.duckdns.org {
    tls {
        dns duckdns {env.DUCKDNS_TOKEN}
        propagation_delay 10s
        propagation_timeout -1
    }

    basicauth {
        admin $hash
    }

    reverse_proxy localhost:8000
}
"@ | Out-File "C:\caddy\Caddyfile" -Encoding UTF8
```

### 3단계: 시작 스크립트 작성

```powershell
@"
Set objShell = CreateObject("WScript.Shell")
objShell.Run "cmd /c set DUCKDNS_TOKEN=<YOUR_TOKEN>&& C:\caddy\caddy.exe run --config C:\caddy\Caddyfile >> C:\caddy\caddy.log 2>&1", 0, False
"@ | Out-File "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\CaddyHTTPS.vbs" -Encoding ASCII
```

### 4단계: 방화벽 허용 (관리자 PowerShell 필요)

```powershell
netsh advfirewall firewall add rule name="Caddy HTTPS 443" dir=in action=allow protocol=TCP localport=443
netsh advfirewall firewall add rule name="Caddy HTTP 80" dir=in action=allow protocol=TCP localport=80
```

### 5단계: 첫 실행 및 인증서 발급 확인

```powershell
$env:DUCKDNS_TOKEN = "YOUR_TOKEN"
& "C:\caddy\caddy.exe" run --config "C:\caddy\Caddyfile"
```

로그에서 이 메시지를 확인하면 성공입니다:

```
certificate obtained successfully  identifier=yourdomain.duckdns.org
```

---

## 트러블슈팅

### 증상: 토큰 길이 오류 (`got: '...' ` — 공백 포함)

```
DuckDNS API token must be a 36 characters long UUID, got: '...' 
```

**원인:** `.env` 파일에서 읽은 토큰에 후행 공백 또는 줄바꿈이 포함됨.  
**해결:** 스크립트에서 `.Trim()`으로 제거하거나, 토큰을 하드코딩된 문자열로 직접 설정합니다.

### 증상: DNS 전파 확인 타임아웃

```
dial tcp 35.182.183.211:53: i/o timeout
```

**원인:** ISP가 DuckDNS 네임서버로의 아웃바운드 DNS 쿼리를 차단.  
**해결:** Caddyfile에 `propagation_timeout -1` 추가 — DNS 전파 검증을 건너뜁니다. 인증서 발급은 정상 완료됩니다.

### 증상: 포트 2019 이미 사용 중

```
listen tcp 127.0.0.1:2019: bind: Only one usage of each socket address
```

**원인:** 이전 Caddy 프로세스가 남아있음.  
**해결:**
```powershell
Get-NetTCPConnection -LocalPort 2019 | Select-Object OwningProcess | ForEach-Object {
    Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
}
```

### 증상: HTTPS 접속은 되는데 API 호출이 401

**원인:** URL에 크리덴셜을 넣는 방식(`https://user:pass@host`)은 현대 브라우저에서 API fetch 호출에 자동 전달되지 않음.  
**해결:** 브라우저 주소창에 `https://yourdomain.duckdns.org`를 직접 입력하면 브라우저 네이티브 인증 다이얼로그가 나타납니다. 여기서 ID/PW를 입력하면 세션 동안 모든 API 호출에 자동 포함됩니다.

---

## 운영 참고

| 항목 | 내용 |
|------|------|
| 인증서 자동갱신 | Caddy가 만료 30일 전부터 자동 갱신 (조작 불필요) |
| 로그 위치 | `C:\caddy\caddy.log` |
| 자동시작 | 로그인 시 `CaddyHTTPS.vbs` 실행 → Caddy 백그라운드 시작 |
| 포트 | 443(HTTPS), 80(HTTP→HTTPS 리다이렉트), 2019(Caddy 관리 API) |
| 포트포워딩 | 라우터: 443 → 172.30.1.5:443, 80 → 172.30.1.5:80 |
