> 최종 검증 버전: v0.6.0.0 | Last verified against: v0.6.0.0

# 한국 주식 뉴스 신호 알림 시스템 — 사용자 매뉴얼

국내외 금융 뉴스를 실시간 수집하여 로컬 LLM으로 매매 신호를 감지하고 텔레그램으로 전달하는 자가 호스팅 도구입니다.

---

## 목차

1. [시스템 소개](#1-시스템-소개)
2. [요구 사항](#2-요구-사항)
3. [설치 가이드](#3-설치-가이드)
4. [환경 설정](#4-환경-설정)
5. [첫 실행](#5-첫-실행)
6. [텔레그램 명령어](#6-텔레그램-명령어)
7. [주봉 차트 스크리너](#7-주봉-차트-스크리너)
8. [일봉 3단계 분류기](#8-일봉-3단계-분류기)
9. [백테스트](#9-백테스트)
10. [스케줄러](#10-스케줄러)
11. [문제 해결](#11-문제-해결)
12. [FAQ](#12-faq)

---

## 1. 시스템 소개

### 무엇을 하는가

이 시스템은 다음 파이프라인을 자동으로 실행합니다.

```
[주봉 스크리너 — 일요일 20:30 KST]
  KOSPI + KOSDAQ 전 종목 Ichimoku 스크리닝 (7조건)
  → 통과 종목 DB 저장 + 텔레그램 발송
  → 통과 종목 목록을 "스크리너 게이트" 캐시에 저장

[일봉 3단계 분류기 — 평일 16:30 KST]
  KOSPI + KOSDAQ 전 종목 일봉 Stage 분류 (Stage 1 / 2 / 3)
  → 분류 결과 DB 저장 (stage_classifications)
  → Ichimoku 주봉 결과와 비교
  → 텔레그램 비교 메시지 발송 (양쪽 통과 종목 최우선)

[뉴스 파이프라인 — 7분 간격]
  RSS 피드 수집
  → 기사 본문 크롤링
  → 로컬 LLM 한글 요약
  → LLM 매매 신호 감지 (BUY / SELL / WATCH)
  → yfinance 시세 교차분석 + Naver Finance 펀더멘털 스코어링
  → PostgreSQL 저장
  → 스크리너 게이트 확인 → 이번 주 통과 종목 아니면 알림 억제
  → 텔레그램 알림 전송 (스크리너 통과 종목만)
```

**스크리너 우선 설계 (v0.5.0.0~):** 뉴스 신호는 해당 주 주봉 스크리너를 통과한 종목에 대해서만 텔레그램으로 발송됩니다. 스크리너를 통과하지 않은 종목의 뉴스 신호는 DB에는 저장되지만 알림은 억제됩니다. 뉴스가 먼저가 아니라 기술적 구조가 먼저입니다.

지원 뉴스 소스:
- 영문: Reuters, CNBC, Yahoo Finance, Bloomberg
- 한국어: 연합뉴스, 한국경제, 매일경제

매주 일요일 20:30 KST에 KOSPI + KOSDAQ 전 종목 주봉 차트 스크리닝이 자동 실행됩니다.

### 무엇을 하지 않는가

- **자동 주문 없음.** 모든 매매 결정은 사용자가 직접 합니다.
- **투자 조언 아님.** 신호는 참고용이며 투자 결과에 대한 책임은 사용자에게 있습니다.
- **실시간 시세 없음.** yfinance 데이터는 15~20분 지연이 있습니다.

---

## 2. 요구 사항

### 필수 소프트웨어

| 항목 | 버전 | 비고 |
|------|------|------|
| Python | **3.11.x** | 3.12는 미검증 — 3.11 사용 권장 |
| PostgreSQL | 14 이상 | |
| Ollama 또는 LM Studio | 최신 | 로컬 LLM 실행 환경 |
| Telegram Bot | — | @BotFather 에서 발급 |

### 운영 환경

- **Linux 또는 WSL2 (Windows Subsystem for Linux)** 권장
- Windows 네이티브는 현재 미지원 (v1 기준)
- macOS에서도 동작하지만 공식 검증되지 않음

### 하드웨어 (최소)

| 항목 | 권장 |
|------|------|
| RAM | 8GB 이상 (LLM 실행 포함) |
| 디스크 | 15GB 이상 (모델 파일 4-8GB 포함) |
| CPU | 4코어 이상 |

> **참고:** 서버/VPS에서 실행하는 경우 Ollama 모델 다운로드에 상당한 시간이 걸릴 수 있습니다 (7B 모델 기준 약 4GB).

---

## 3. 설치 가이드

### 3-1. 저장소 복제

```bash
git clone https://github.com/VontineDev/test_feed.git
cd test_feed
```

### 3-2. Python 가상환경 및 의존성 설치

```bash
python3.11 -m venv venv
source venv/bin/activate          # Linux/macOS/WSL
# Windows WSL: source venv/bin/activate 동일

pip install --upgrade pip
pip install -r requirements.txt
```

### 3-3. PostgreSQL 설정

PostgreSQL이 설치되어 있지 않다면:

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib -y
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

데이터베이스와 사용자 생성:

```bash
sudo -u postgres psql << 'EOF'
CREATE USER news_user WITH PASSWORD '여기에강력한비밀번호입력';
CREATE DATABASE news_db OWNER news_user;
GRANT ALL PRIVILEGES ON DATABASE news_db TO news_user;
\q
EOF
```

연결 테스트:

```bash
psql -h localhost -U news_user -d news_db -c "SELECT version();"
# 비밀번호 입력 후 PostgreSQL 버전이 출력되면 성공
```

### 3-4. 로컬 LLM 설치 (Ollama — 기본)

**이 단계가 가장 오래 걸립니다.** 모델 파일이 4~8GB이므로 인터넷 속도에 따라 10분에서 1시간 이상 걸릴 수 있습니다. 커피 한 잔 준비하세요.

**Step 1. Ollama 설치:**

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

설치 완료 후 Ollama 서버가 자동으로 백그라운드에서 시작됩니다 (포트 11434).

서버 작동 확인:

```bash
curl http://localhost:11434/
# "Ollama is running" 이 표시되면 성공
```

> **포트 충돌 주의:** 이미 다른 프로세스가 11434 포트를 사용 중이라면 Ollama 서버가 시작되지 않습니다.  
> 아래 [증상 7: Ollama 포트 충돌](#증상-7-ollama-서버가-시작되지-않는다--포트-충돌) 항목을 참고하세요.

**Step 2. 모델 다운로드:**

```bash
ollama pull qwen3.5:9b
```

다운로드 중 터미널에 진행 상황이 실시간으로 표시됩니다:

```
pulling manifest
pulling 0e9b4b8d8e3d... 4.7 GB / 4.7 GB ████████████ 100%
verifying sha256 digest
writing manifest
success
```

- **예상 용량:** 약 4~8GB (모델 버전에 따라 다름)
- **예상 시간:** 100Mbps 환경에서 약 7분, 10Mbps VPS에서 약 60분
- **다운로드 중단 시:** 걱정 없습니다. 같은 명령어(`ollama pull qwen3.5:9b`)를 다시 실행하면 중단된 위치에서 이어받습니다.
- **디스크 공간 부족 시:** 10GB 이상 여유 공간을 확보한 후 다시 시도합니다.

**Step 3. 모델 설치 확인:**

```bash
ollama list
```

출력 예시:

```
NAME              ID              SIZE    MODIFIED
qwen3.5:9b        a7d3ab2c4e7f    5.2 GB  3 minutes ago
```

`qwen3.5:9b` 가 목록에 있으면 준비 완료입니다. 목록이 비어 있으면 Step 2를 다시 실행하세요.

**LM Studio를 사용하는 경우** (Ollama 대신):

LM Studio를 실행하고 원하는 한국어 모델을 로드한 후, Local Server를 시작합니다 (기본 포트 1234).

### 3-5. Telegram 봇 생성

1. 텔레그램에서 `@BotFather` 를 검색합니다.
2. `/newbot` 명령어로 봇을 생성하고 **Bot Token**을 받습니다.
   형식: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`
3. 본인의 Chat ID를 확인합니다.
   - `@userinfobot` 에 메시지를 보내면 Chat ID를 알려줍니다.
   - 또는 봇에게 메시지를 보낸 후 `https://api.telegram.org/bot<TOKEN>/getUpdates` 로 확인합니다.

### 3-6. 환경 변수 설정

```bash
cp .env.example .env
```

`.env` 파일을 텍스트 편집기로 열고 값을 채웁니다 (다음 섹션 참고).

```bash
nano .env   # 또는 vim .env
```

> **보안 주의:** `.env` 파일에는 실제 자격증명이 포함됩니다. `.gitignore`에 등록되어 있으므로 절대 git에 커밋하지 마세요.

### 3-7. 데이터베이스 초기화

```bash
source venv/bin/activate
python -c "import asyncio; from db import init_db; import asyncpg; asyncio.run(init_db(asyncio.run(asyncpg.create_pool(dsn='postgresql://news_user:비밀번호@localhost:5432/news_db'))))"
```

또는 시스템 첫 실행 시 자동으로 테이블이 생성됩니다.

### 3-8. 시스템 상시 실행 방법

#### 방법 A: tmux (간단, 추천 — 빠른 시작)

```bash
# tmux 설치 (없는 경우)
sudo apt install tmux -y

# 새 세션 시작
tmux new -s feed

# 세션 내에서 실행
source venv/bin/activate
python run_scheduler.py

# 세션에서 분리 (백그라운드 유지)
# Ctrl+B, D 키 조합

# 나중에 재접속
tmux attach -t feed
```

#### 방법 B: systemd 서비스 (운영 환경, 자동 재시작)

`/etc/systemd/system/test_feed.service` 파일 생성:

```ini
[Unit]
Description=한국 주식 뉴스 신호 알림 시스템
After=network.target postgresql.service

[Service]
Type=simple
User=여기에리눅스사용자이름
WorkingDirectory=/path/to/test_feed
EnvironmentFile=/path/to/test_feed/.env
ExecStart=/path/to/test_feed/venv/bin/python run_scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

서비스 등록 및 시작:

```bash
sudo systemctl daemon-reload
sudo systemctl enable test_feed
sudo systemctl start test_feed

# 상태 확인
sudo systemctl status test_feed

# 로그 확인
sudo journalctl -u test_feed -f
```

---

## 4. 환경 설정

`.env` 파일의 모든 변수 설명입니다.

### PostgreSQL 연결

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `DB_HOST` | `localhost` | PostgreSQL 서버 주소 |
| `DB_PORT` | `5432` | PostgreSQL 포트 |
| `DB_NAME` | `news_db` | 데이터베이스 이름 |
| `DB_USER` | `news_user` | 사용자 이름 |
| `DB_PASSWORD` | _(없음)_ | **필수.** 강력한 비밀번호 설정 필요 |
| `DATABASE_URL` | _(없음)_ | 위 개별 변수 대신 사용 가능. 형식: `postgresql://user:password@host:port/dbname` |

### Telegram

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TELEGRAM_TOKEN` | _(없음)_ | **필수.** @BotFather 에서 발급받은 봇 토큰 |
| `ALLOWED_CHAT_IDS` | _(없음)_ | **필수.** 봇 명령어를 허용할 Chat ID 목록 (쉼표 구분). 예: `123456789,987654321` |
| `TELEGRAM_CHAT_ID` | _(없음)_ | `ALLOWED_CHAT_IDS` 대신 단일 Chat ID로 사용 가능 |
| `TELEGRAM_CHANNEL_ID` | _(없음)_ | 선택사항. 주봉 스크리너 결과를 채널에도 발송할 경우 설정. 예: `@yourchannel` 또는 `-100123456789`. 미설정 시 DM으로만 발송. |

> `ALLOWED_CHAT_IDS` 와 `TELEGRAM_CHAT_ID` 중 하나는 반드시 설정해야 합니다.
> 둘 다 설정 시 `ALLOWED_CHAT_IDS` 가 우선합니다.

### 로컬 LLM — Ollama (기본)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `OLLAMA_BASE` | `http://localhost:11434` | Ollama 서버 URL. 기본값으로 로컬 실행 시 그대로 사용 가능 |
| `OLLAMA_MODEL` | `qwen3.5:9b` | 사용할 모델 이름. `ollama list` 에서 확인한 이름과 일치해야 함 |

### 로컬 LLM — LM Studio (대체)

Ollama 대신 LM Studio를 사용하는 경우 설정합니다. Ollama가 실행 중이지 않으면 자동으로 LM Studio로 폴백합니다.

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `LM_STUDIO_BASE` | `http://localhost:1234` | LM Studio 서버 URL |
| `LM_STUDIO_MODEL` | `eeve-korean-instruct-10.8b-v1.0` | LM Studio에 로드된 모델 식별자 |

### 한국 매크로 데이터

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KOREA_BASE_RATE` | `3.50` | 한국은행 기준금리 (%). 한은이 변경할 때마다 수동으로 업데이트 |

### 성능 튜닝 (선택사항)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `SCREENER_WORKERS` | `8` | 병렬 워커 수. **8 권장** — 일요일 주봉 스크리닝(20:30 KST)과 평일 일봉 3단계 분류기(16:30 KST) 모두에 적용됩니다. CPU 코어가 4개 이하면 4로 낮추세요 |
| `BACKTEST_MIN_SIGNALS` | `10` | `/backtest` 종목별 정확도 리포트에서 종목을 표시하기 위한 최소 신호 건수 |

> **v0.5.0.0 변경:** `SCREENER_WORKERS` 기본값 권장치가 1→8로 변경되었습니다. 일요일 20:30 KST 스크리닝 완료 예산(30분)을 맞추려면 `.env`에 `SCREENER_WORKERS=8`을 추가하세요.
>
> **v0.6.0.0 변경:** `SCREENER_WORKERS` 가 평일 16:30 KST 일봉 3단계 분류기에도 적용됩니다. 워커 수가 낮으면 16:30 이후 텔레그램 비교 메시지 발송이 지연될 수 있습니다.

### 최소 `.env` 예시

```dotenv
DB_PASSWORD=강력한비밀번호여기입력
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_db
DB_USER=news_user

TELEGRAM_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
ALLOWED_CHAT_IDS=123456789

OLLAMA_BASE=http://localhost:11434
OLLAMA_MODEL=qwen3.5:9b

KOREA_BASE_RATE=3.50
```

---

## 5. 첫 실행

### 실행

```bash
source venv/bin/activate
python run_scheduler.py
```

옵션:

```bash
python run_scheduler.py --interval 1    # 1분 간격 (빠른 테스트용)
python run_scheduler.py --no-summary    # 요약 없이 수집만 (LLM 비활성)
```

### 정상 시작 확인 — 로그 메시지

실행 후 30초 이내에 다음 로그 메시지가 순서대로 출력되어야 합니다:

```
뉴스 크롤러 시작 — 수집 7분 간격          ← 스케줄러 시작
Telegram 봇 시작 — /status /signals ...   ← 봇 폴링 시작
▶ [수집] 시작  YYYY-MM-DD HH:MM:SS        ← 첫 번째 RSS 수집 시작
```

30초 이내에 이 메시지가 보이지 않으면 [11. 문제 해결](#11-문제-해결)을 참고하세요.

### 봇 동작 확인

텔레그램에서 등록한 봇에게 `/status` 명령어를 보냅니다.

첫 번째 RSS 수집 사이클(기본 7분) 이내에 다음과 같은 응답이 옵니다:

```
📡 크롤러 상태

🕐 업타임: 0시간 3분
📰 누적 수집: 47건
📊 최근 24h 수집: 12건
🎯 최근 24h 신호: 2건
🌐 피드: Reuters + Investing + CNBC
```

`/status` 가 응답하면 DB 연결, 봇 폴링, RSS 수집이 모두 정상입니다.

> **참고:** 매매 신호 알림은 신호가 감지될 때만 전송됩니다. 조용한 장세에서는 알림이 없을 수 있습니다. `/status` 응답을 기준으로 동작 여부를 판단하세요.

---

## 6. 텔레그램 명령어

### /status

크롤러 현재 상태를 조회합니다.

```
/status
```

**응답 예시:**

```
📡 크롤러 상태

🕐 업타임: 2시간 14분
📰 누적 수집: 183건
📊 최근 24h 수집: 47건
🎯 최근 24h 신호: 8건
🌐 피드: Reuters + Investing + CNBC
```

---

### /signals

최근 매매 신호 10건을 조회합니다. 방향 필터를 지정할 수 있습니다.

```
/signals              # 전체 (BUY + SELL + WATCH 혼합)
/signals buy          # BUY 신호만
/signals sell         # SELL 신호만
/signals watch        # WATCH 신호만
```

**응답 예시 (`/signals buy`):**

```
🎯 최근 BUY 신호 10건

🟢 BUY ⬛⬛⬛⬛⬜
   Samsung Electronics cuts chip output amid weak demand
   💬 삼성전자 반도체 감산 발표 — 공급 감소로 가격 반등 기대
   🕐 14:23 KST

🟢 BUY ⬛⬛⬛⬜⬜
   Korea EV battery exports surge in Q1
   💬 국내 배터리 수출 급증 — LG에너지솔루션 실적 기대
   🕐 11:05 KST
```

신호 강도 바 (`⬛⬛⬛⬛⬜`): 1~5칸, 많을수록 강한 신호.

---

### /today

오늘 카테고리별 수집 건수와 최신 기사 5건을 조회합니다.

```
/today
```

**응답 예시:**

```
📰 오늘 수집 현황 (2026-04-18)

reuters: 23건
yfinance_news: 15건
yonhap: 8건
hankyung: 6건
maeil: 4건
합계: 56건

📌 최신 기사 5건
1. [Reuters] Fed signals patience on rate cuts...
2. [한국경제] 삼성전자, HBM4 양산 일정 확정...
...
```

---

### /backtest

교차분석 백테스팅 리포트를 조회합니다.

```
/backtest
```

**응답 예시:**

```
📊 교차분석 백테스팅 리포트

🎯 판정별 적중률 (1d 체크포인트)
✅ CONFIRM: 52건 / 적중률 71.2%
⚠️ CAUTION: 18건 / 적중률 44.4%
🔴 FILTER: 12건 / 역방향 적중 66.7%
➖ NEUTRAL: 31건 / 적중률 54.8%

⏱️ 체크포인트별 CONFIRM 적중률
1d: 71.2% | 3d: 68.5% | 1w: 65.3%

📈 종목별 정확도 (CONFIRM 1d, 10건 이상)
삼성전자 (005930): 15건 80.0%
SK하이닉스 (000660): 11건 72.7%
```

판정 의미:
- **CONFIRM** — 뉴스 신호와 시세 방향이 일치. 신호 강화.
- **CAUTION** — 뉴스 신호와 시세 방향이 반대. 주의.
- **FILTER** — 강한 역방향 시세. 해당 신호는 노이즈로 판단.
- **NEUTRAL** — 시세 데이터 없거나 방향성 불분명.

`/backtest` 는 신호가 10건 이상 누적되어야 의미 있는 결과가 출력됩니다.

---

### /screener

최신 주봉 차트 스크리닝 결과를 조회합니다.

```
/screener
```

매주 일요일 20:30 KST에 자동 갱신됩니다. `/screener` 명령으로 언제든 최신 결과를 조회할 수 있습니다.

**응답 예시:**

```
📊 주봉 차트 스크리너 (2026-W16)
통과 종목: 12개

🏭 반도체
  • 삼성전자 (005930) 78,500원
  • SK하이닉스 (000660) 182,000원

🔋 이차전지
  • LG에너지솔루션 (373220) 385,000원
```

> **v0.5.0.0 변경:** `/screener` 명령어는 명령을 보낸 사용자의 DM으로만 결과를 전송합니다. 이전 버전에서는 채널(`TELEGRAM_CHANNEL_ID`)로도 발송되는 버그가 있었습니다. 일요일 자동 스크리닝은 이전과 동일하게 DM + 채널 동시 발송됩니다.

자세한 스크리닝 조건은 [7. 주봉 차트 스크리너](#7-주봉-차트-스크리너)를 참고하세요.

---

### /volume

종목의 시간대별 거래량 패턴을 분석합니다.

```
/volume <종목명 또는 티커>

예)
/volume 삼성전자
/volume 005930
/volume AAPL
/volume TSLA
```

**응답 예시:**

```
📊 삼성전자 (005930.KS) 거래량 패턴

오전 (09:00-11:00): 32.4% ████████░░
점심 (11:00-13:00): 18.1% █████░░░░░
오후 (13:00-15:00): 28.7% ███████░░░
마감 (15:00-15:30): 20.8% █████░░░░░

최근 5일 평균 거래량: 12,847,233주
```

한국 종목은 종목명(한글) 또는 6자리 종목코드로 조회할 수 있습니다.
해외 종목은 yfinance 심볼(영문)로 조회합니다.

---

### /help / /start

사용 가능한 명령어 목록을 표시합니다.

```
/help
/start    # /help 와 동일
```

**응답 예시:**

```
📋 사용 가능한 명령어

/status — 크롤러 상태 (업타임, 수집 건수)
/signals — 최근 매매 신호 10건
/signals buy — BUY 신호만 조회
/signals sell — SELL 신호만 조회
/signals watch — WATCH 신호만 조회
/today — 오늘 수집 현황 + 최신 기사
/backtest — 교차분석 백테스팅 리포트
/volume <종목명|티커> — 시간대별 거래량 패턴 분석
/screener — 최신 주봉 차트 스크리닝 결과 (DM + 채널)
/help — 이 도움말
```

---

## 7. 주봉 차트 스크리너

### 스크리닝 조건

7개 조건을 모두 충족해야 통과합니다 (AND 조건).

```
A = 이번 주 종가 > max(선행스팬A, 선행스팬B)   — 구름 상향 돌파
B = 직전 주 종가 ≤ 직전 주 구름 상단            — 직전 주 구름 내/하부 (돌파 신선도)
C = 이번 주 종가 > 20주 이동평균선
D = 이번 주 종가 > 60주 이동평균선
E = 20주선 > 직전 주 20주선                     — 20주선 우상향
F = 60주선 > 직전 주 60주선                     — 60주선 우상향
G = 이번 주 종가 > 120주선 (데이터 부족 시 통과) — 장기 추세 상위
```

상장 100주 미만인 신규 상장 종목은 G 조건 데이터가 없으므로 자동 통과합니다.

### 결과 해석

결과는 KIND 섹터별로 그룹화되어 표시됩니다.

```
🏭 반도체
  • 삼성전자 (005930) 78,500원
  • SK하이닉스 (000660) 182,000원
```

- 섹터 분류는 KRX/KIND 공식 데이터 기반
- 종목코드는 KRX 표준 6자리
- 가격은 해당 주 금요일 종가 기준

### 뉴스 신호 게이팅 (v0.5.0.0~)

스크리너를 통과한 종목 목록은 메모리 캐시(`_screener_tickers`)에 보관되며, 뉴스 파이프라인이 이를 확인합니다.

- **스크리너 통과 종목 뉴스**: 텔레그램 알림 정상 발송
- **스크리너 미통과 종목 뉴스**: DB 저장은 되지만 텔레그램 알림 억제

이 게이팅은 세 가지 경우에 비활성화됩니다:
1. 캐시가 비어 있을 때 (첫 설치 직후, 일요일 스크리닝 전)
2. 뉴스 신호에 종목 티커가 없을 때 (거시 경제 기사 등)
3. 신호 자체가 `is_actionable=False` 일 때 (항상 억제)

캐시는 시스템 시작 시 DB에서 이번 주 스크리닝 결과를 불러와 워밍업되며, 매주 일요일 스크리닝 완료 후 자동 갱신됩니다.

### HTML 보고서

스크리너 실행 후 `reports/screener/screener_YYYYMMDD_HHMM.html` 파일이 생성됩니다. 브라우저로 열면 다음을 확인할 수 있습니다:

- **정배열 / 일반** 섹션으로 구분된 테이블
- **추세 스파크라인**: 각 종목 행의 우측에 12주 가격 추이를 인라인 SVG 미니차트로 표시. 외부 라이브러리 없이 브라우저에서 바로 렌더링됩니다.
- **업종별 섹션**: 테이블 하단에 KIND 섹터별 그룹 뷰 추가. 이번 주 돌파가 특정 섹터에 집중됐는지 한눈에 파악할 수 있습니다.
- **외국인 / 기관 순매수 칸**: 스크리너 통과 종목의 당일 외국인·기관 순매수 금액 표시 (Sprint 2에서 실데이터 연동 예정 — 현재는 `—` 표시).

### 차트 스크리너 백테스트 (v0.5.0.0~)

매주 일요일 스크리닝이 완료된 직후, 시스템은 과거 스크리닝 신호들의 사후 수익률을 자동으로 계산합니다.

**계산 항목:**
- 신호 발생 후 1주 / 4주 / 13주 수익률
- KOSPI 대비 초과수익률 (같은 기간 코스피 대비)
- 정배열(★) 신호와 일반 신호의 수익률 분리

**결과 파일:** `~/.gstack/backtest/test_feed/chart_backtest_latest.html`

브라우저로 열면 월별 · 연간 수익률 테이블과 신호 건수 추이를 볼 수 있습니다. 데이터는 증분 업데이트 방식으로 쌓이며, 13주가 지난 신호부터 의미 있는 완성 데이터가 됩니다.

### 실행 일정

매주 일요일 20:30 KST에 자동 실행됩니다.
`/screener` 명령어로 가장 최근 결과를 언제든 조회할 수 있습니다.

### 수동 실행

```bash
source venv/bin/activate
python generate_report.py
# 두 파일이 생성됩니다:
#   screening_report_YYYYMMDD_HHMM.txt   (텍스트, 현재 디렉터리)
#   reports/screener/screener_YYYYMMDD_HHMM.html  (브라우저용 HTML)
```

수천 개 종목을 처리하므로 수십 분이 걸릴 수 있습니다 (`SCREENER_WORKERS` 값에 따라 다름).

단독 테스트만 하려면:

```bash
python chart_screener.py    # 스크리닝 로직만 실행, 파일 저장 없음
```

---

## 8. 일봉 3단계 분류기

### 개요

매일 장 마감 후 **16:30 KST (UTC 07:30)** 에 KOSPI + KOSDAQ 전 종목(~2,770개)을 대상으로 자동 실행됩니다. 주봉 Ichimoku 스크리너와 완전히 독립된 시스템이며, 수급 흐름을 3단계로 분류합니다.

- **자동 실행 전용** — 수동 트리거 명령어 없음
- 분류 결과는 `stage_classifications` 테이블에 저장됩니다
- 실행 완료 후 텔레그램 비교 메시지를 자동 발송합니다

### 분류 단계

분류 우선순위: **Stage 3 > Stage 2 > Stage 1**

#### Stage 1 — 랠리 초입

5개 조건을 모두 충족해야 합니다 (AND 조건).

| 조건 | 기준 |
|------|------|
| 일일 상승률 | KOSPI ≥ +5%, KOSDAQ ≥ +7% |
| 거래량 | 당일 거래량 ≥ 20일 평균 × 2 |
| 이동평균 | 종가 > MA20 AND 종가 > MA60 |
| 52주 위치 | (52주 고점 − 종가) / 52주 고점 ≤ 20% |
| 수급 | 외국인 순매수 > 0 **OR** 기관 순매수 > 0 |

#### Stage 2 — 중간 조정·재매집

직전 14일 이내에 Stage 1 이력이 있는 종목을 대상으로 추가 판별합니다.

| 조건 | 기준 |
|------|------|
| 가격 위치 | Stage 1 고가 대비 −5% ~ −20% 구간 |
| 지지선 | 종가 ≥ MA20 × 0.95 |
| 거래량 수축 | 당일 거래량 = Stage 1 스파이크의 30% ~ 60% |
| 기관 수급 | 기관 순매수 연속일(inst_streak) ≥ 0 |

#### Stage 3 — 과열 재가속

| 조건 | 기준 |
|------|------|
| 돌파 | 종가 > 최근 10일 고가 최대값 |
| 일일 상승률 | ≥ +5% |
| RSI | RSI(14) ≥ 70 |
| 거래량 | 당일 거래량 ≥ 30일 평균 × 1.5 |
| 수급 | 외국인 순매수 > 0 **AND** 기관 순매수 > 0 |

### 피크아웃 신호

Stage 3 종목 중 다음 조건이 충족되면 `⚠️ Stage 3 피크아웃 주의` 경고가 함께 발송됩니다.

- **조건 1:** 외국인 순매도 연속일 ≤ −2 AND 기관 순매도 연속일 ≤ −2 (동시 순매도)
- **조건 2:** 윗꼬리 캔들 — `(고가 − 종가) > 0.5 × (고가 − 저가)` — AND 당일 거래량 ≥ 20일 평균 × 1.5

### 텔레그램 비교 메시지

매일 16:30 KST에 자동 발송됩니다. 형식:

```
📊 스크리너 비교 (2026-04-26)
주봉 Ichimoku + 일봉 Stage 교차 분석

✅ 양쪽 통과 (3종목) — 최우선 감시
  삼성전자 (005930) · Stage 1
  SK하이닉스 (000660) · Stage 3
  POSCO홀딩스 (005490) · Stage 2

📌 Ichimoku만 통과: 9종목
📌 Stage만 해당: 14종목
  └ Stage 1: 8 / Stage 2: 4 / Stage 3: 2

⚠️ Stage 3 피크아웃 주의: 카카오 (035720)
```

- **양쪽 통과** — Ichimoku 주봉 7조건 + 일봉 Stage 조건을 모두 충족. 가장 높은 신뢰도.
- **Ichimoku만 통과** — 장기 추세는 좋지만 당일 수급/모멘텀 조건 미충족.
- **Stage만 해당** — 당일 급등·수급은 포착됐지만 주봉 추세 기반 아님.

### 주의 사항

- 이 분류는 기계적 조건 충족 여부만 판단합니다. 투자 결정의 참고 자료로만 활용하세요.
- Stage 1/2/3 중 복수 조건을 동시에 충족하면 높은 Stage가 반환됩니다 (Stage 3 우선).
- 분류 실행에 `SCREENER_WORKERS` 값이 적용됩니다 — 워커 수가 낮으면 16:30 이후 늦게 발송될 수 있습니다.

---

## 9. 백테스트

이 시스템에는 두 개의 독립된 백테스트가 있습니다.

| 구분 | 대상 | 조회 방법 | 데이터 소스 |
|------|------|-----------|-------------|
| 뉴스 신호 백테스트 | LLM이 감지한 BUY/SELL/WATCH 신호 | `/backtest` 텔레그램 명령어 | `price_outcomes` 테이블 |
| 차트 스크리너 백테스트 | 주봉 Ichimoku 스크리닝 통과 종목 | HTML 파일 (브라우저) | `~/.gstack/backtest/test_feed/` |

---

### 8-1. 뉴스 신호 백테스트 (`/backtest`)

#### 작동 원리

신호가 감지될 때마다 해당 종목의 시세를 1일 후, 3일 후, 1주일 후에 자동으로 체크합니다. 이를 통해 각 판정(CONFIRM/CAUTION/FILTER/NEUTRAL)의 실제 적중률을 추적합니다.

```
신호 감지
  → 교차분석 판정 (CONFIRM/CAUTION/FILTER/NEUTRAL)
  → price_outcomes 테이블에 체크포인트 등록
  → 30분마다 미채운 체크포인트 가격 조회 (price_tracker)
  → /backtest 에서 적중률 계산
```

#### 판정 기준

| 판정 | 의미 | 적중 조건 |
|------|------|-----------|
| CONFIRM | 뉴스 방향 = 시세 방향 일치 | 신호 방향으로 가격 이동 |
| CAUTION | 뉴스 신호와 시세가 약하게 반대 | 신호 방향으로 가격 이동 |
| FILTER | 강한 역방향 시세 (노이즈 판정) | 신호 반대 방향으로 가격 이동 |
| NEUTRAL | 방향성 불분명 | 신호 방향으로 가격 이동 |

#### 결과 읽기

- **적중률 50% 이상**: 해당 판정 유형이 유효한 신호
- **FILTER 역방향 적중률 높음**: 필터 기능이 정상 작동 중
- **신호 수 10건 미만**: 통계적으로 불충분 — 축적 대기 필요

신호가 10건 이상 누적된 이후부터 의미 있는 데이터가 출력됩니다. 처음 설치 후 몇 주가 지나야 충분한 데이터가 쌓입니다.

---

### 8-2. 차트 스크리너 백테스트 (HTML 리포트)

매주 일요일 스크리닝 완료 후 자동 실행됩니다. 과거 스크리닝 신호들의 사후 수익률을 계산하여 HTML로 출력합니다.

#### 열람 방법

```bash
# 브라우저로 직접 열기 (경로는 OS 홈 디렉터리 기준)
# Linux/macOS
open ~/.gstack/backtest/test_feed/chart_backtest_latest.html

# Windows (WSL)
explorer.exe "$(wslpath -w ~/.gstack/backtest/test_feed/chart_backtest_latest.html)"
```

#### 리포트 내용

- **연간 집계**: 전체 / 정배열(★) / 일반 신호 승률, 평균 수익률, 중위 수익률, KOSPI 초과수익률
- **월별 집계**: 월별 신호 건수와 수익률 추이
- **수익률 기준**: 신호 발생 주 종가 대비 1주 후 / 4주 후 / 13주 후 종가

#### 데이터 축적 주기

신호 발생 후 13주(약 3개월)가 지나야 13주 수익률 항목이 채워집니다. 초기에는 `—` 로 표시되며, 실행 기간이 쌓일수록 데이터가 풍부해집니다.

> **참고:** 스크리너 백테스트 데이터는 `~/.gstack/backtest/test_feed/signals.json` 에 로컬 저장됩니다. DB를 초기화해도 백테스트 이력은 유지됩니다.

---

## 10. 스케줄러

`run_scheduler.py` 실행 시 다음 7개 작업이 등록됩니다.

| 작업 | 실행 주기 | 설명 |
|------|-----------|------|
| `news_collect` | 7분마다 (기본값) | RSS 수집 → 요약 → 신호 감지 → 스크리너 게이트 확인 → 텔레그램 전송 |
| `price_tracker` | 30분마다 | 체크포인트 가격 조회 (뉴스 백테스트 데이터 채움) |
| `weekly_backtest` | 매주 일요일 20:00 KST | 주간 뉴스 신호 백테스팅 리포트 자동 전송 |
| `krx_daily_refresh` | 매일 20:00 KST | KRX 전체 종목 리스트 갱신 (~2,500종목) |
| `weekly_chart_screener` | 매주 일요일 20:30 KST | KOSPI/KOSDAQ 주봉 차트 스크리닝 → 스크리너 게이트 갱신 → 차트 스크리너 백테스트 증분 업데이트 → HTML 리포트 재생성 |
| `daily_volume_report` | 평일 15:40 KST | 거래량 패턴 일일 배치 리포트 |
| `daily_stage_classifier` | 평일 16:30 KST | KOSPI/KOSDAQ 전 종목 일봉 Stage 1/2/3 분류 → DB 저장 → Ichimoku 비교 메시지 발송 (v0.6.0.0~) |

RSS 수집 간격 변경:

```bash
python run_scheduler.py --interval 3    # 3분 간격
python run_scheduler.py --interval 15   # 15분 간격
```

APScheduler가 PostgreSQL에 연결되면 Postgres jobstore를 사용합니다. 연결 실패 시 MemoryJobStore로 폴백합니다 (재시작 시 일정 초기화).

---

## 11. 문제 해결

### 증상 1: LLM이 응답하지 않는다 / 요약이 생성되지 않는다

**확인 방법:**

```bash
# Ollama 실행 중인지 확인
curl http://localhost:11434/api/tags
# 응답: {"models":[{"name":"qwen3.5:9b",...}]}

# 모델 목록 확인
ollama list
```

**해결 방법:**

1. Ollama 서버가 실행 중인지 확인:
   ```bash
   ollama serve   # 별도 터미널에서 실행, 이미 실행 중이면 에러 무시
   ```

2. 모델이 설치되어 있는지 확인:
   ```bash
   ollama list
   # 목록에 없으면:
   ollama pull qwen3.5:9b
   ```

3. `.env`의 `OLLAMA_MODEL` 값이 `ollama list` 의 모델 이름과 정확히 일치하는지 확인.

4. LM Studio 폴백 확인: Ollama가 안 되면 `.env`에 `LM_STUDIO_BASE`와 `LM_STUDIO_MODEL` 설정.

---

### 증상 7: `ollama list` 에 모델이 없다 (목록 비어 있음)

```bash
ollama list
# 출력: (아무것도 없음)
```

**원인:** 모델 다운로드가 완료되지 않았거나, 다른 디렉터리에 설치된 Ollama를 사용 중입니다.

**해결 방법:**

```bash
# 모델 다운로드 (이미 일부 받았다면 이어받기)
ollama pull qwen3.5:9b

# 완료 후 확인
ollama list
# qwen3.5:9b 가 표시되어야 함
```

> 디스크가 부족한 경우: `df -h` 로 여유 공간을 확인하세요. 최소 8GB 이상 필요합니다.

---

### 증상 8: Ollama 서버가 시작되지 않는다 / 포트 충돌

**로그 메시지 또는 에러:**

```
Error: listen tcp 127.0.0.1:11434: bind: address already in use
```

**원인:** 다른 프로세스가 이미 11434 포트를 점유하고 있습니다.

**해결 방법:**

```bash
# 11434 포트를 사용 중인 프로세스 확인
sudo lsof -i :11434

# 해당 프로세스가 이전 Ollama 인스턴스라면 종료
sudo pkill ollama

# 다시 시작
ollama serve
```

다른 프로세스가 11434를 사용 중이라면 Ollama 포트를 변경합니다:

```bash
# 환경 변수로 포트 지정 (예: 11435)
OLLAMA_HOST=127.0.0.1:11435 ollama serve
```

`.env` 파일의 `OLLAMA_BASE` 도 같은 포트로 변경해야 합니다:

```dotenv
OLLAMA_BASE=http://localhost:11435
```

---

### 증상 9: 모델 다운로드가 중단됐다

`ollama pull` 실행 중 네트워크 오류, 시스템 종료, 또는 Ctrl+C 로 중단된 경우:

**해결 방법:** 같은 명령어를 다시 실행하면 중단된 위치에서 자동으로 이어받습니다.

```bash
ollama pull qwen3.5:9b
# "pulling manifest" 이후 중단된 지점부터 재개됩니다
```

처음부터 다시 다운로드하지 않습니다. 안심하고 재실행하세요.

---

### 증상 2: DB 연결 실패 (startup 또는 /status 에서 오류)

**로그 메시지:** `DB 연결 실패: ...`

**해결 방법:**

1. PostgreSQL이 실행 중인지 확인:
   ```bash
   sudo systemctl status postgresql
   sudo systemctl start postgresql   # 정지 상태라면
   ```

2. 연결 정보 확인:
   ```bash
   psql -h localhost -U news_user -d news_db
   # 접속되면 정상
   ```

3. `.env` 의 `DB_PASSWORD`, `DB_HOST`, `DB_NAME`, `DB_USER` 값을 재확인.

4. PostgreSQL이 외부 연결을 허용하는지 확인 (원격 서버의 경우 `pg_hba.conf` 설정 필요).

---

### 증상 3: 텔레그램 봇이 응답하지 않는다

**해결 방법:**

1. `.env`의 `TELEGRAM_TOKEN` 이 올바른지 확인:
   ```bash
   curl "https://api.telegram.org/bot${TELEGRAM_TOKEN}/getMe"
   # {"ok":true,"result":{"username":"your_bot_name",...}} 가 반환되면 토큰 정상
   ```

2. `ALLOWED_CHAT_IDS` 또는 `TELEGRAM_CHAT_ID` 에 본인의 Chat ID가 등록되어 있는지 확인.
   Chat ID 확인 방법: `@userinfobot` 에 `/start` 전송.

3. 봇에게 먼저 메시지를 한 번 보낸 적 있는지 확인 (봇은 먼저 메시지를 시작할 수 없음).

---

### 증상 4: 신호가 전혀 오지 않는다

정상적인 상황일 수 있습니다. 확인 방법:

1. `/status` 로 수집이 정상인지 확인 (수집 건수 > 0).

2. `/today` 로 오늘 기사가 수집되고 있는지 확인.

3. 신호는 LLM이 `strength >= 2` 로 판단한 기사에서만 발송됩니다. 조용한 장세에서는 하루에 0~2건일 수 있습니다.

4. `--no-summary` 옵션 없이 실행 중인지 확인 (요약 없이 실행 시 신호도 감지되지 않음).

---

### 증상 5: /screener 결과가 없다 / "스크리닝 결과가 없습니다"

첫 설치 직후에는 DB에 스크리닝 결과가 없습니다.

**해결 방법:**

1. 매주 일요일 20:30 KST 자동 실행을 기다리거나,

2. 수동으로 스크리너 실행:
   ```bash
   source venv/bin/activate
   python generate_report.py
   ```
   수천 개 종목을 처리하므로 수십 분이 걸릴 수 있습니다 (`SCREENER_WORKERS` 값에 따라 다름).

---

### 증상 6: APScheduler MemoryJobStore 경고

```
[스케줄러] APScheduler jobstore: MemoryJobStore (Postgres 연결 실패: ...)
```

이 경고는 스케줄러가 메모리 기반으로 실행됨을 의미합니다. 시스템 재시작 시 실행 중인 작업 기록이 초기화됩니다. DB 연결 문제가 없다면 이 경고는 무시해도 됩니다.

---

## 12. FAQ

**Q: 자동으로 주식을 사거나 팔 수 있나요?**

아니요. 이 시스템은 알림 도구입니다. 자동 주문 기능은 없으며, 모든 매매 결정은 사용자가 직접 증권사 앱에서 실행합니다.

**Q: 신호가 얼마나 정확한가요?**

`/backtest` 에서 실제 적중률을 확인할 수 있습니다. 신호는 뉴스 기반 LLM 판단이므로 기술적 분석 지표 기반 시스템과 다른 성격을 가집니다. 투자 판단의 참고 자료로만 활용하세요.

**Q: 어떤 뉴스 소스를 사용하나요?**

영문: Reuters, CNBC, Yahoo Finance, Bloomberg (RSS 기반)
한국어: 연합뉴스, 한국경제, 매일경제 (RSS 기반)

실시간 API가 아닌 RSS 피드이므로 기사 게재 후 수 분의 지연이 있습니다.

**Q: yfinance 시세가 실시간이 아닌가요?**

yfinance는 거래소 시세를 15~20분 지연하여 제공합니다. 교차분석과 백테스트에 사용되는 데이터는 이 지연을 포함합니다.

**Q: 한국 주식 외에 해외 주식도 신호가 옵니까?**

네. 영문 소스 기사에서 감지된 해외 종목(AAPL, TSLA 등)도 신호로 발송됩니다. `/volume` 명령어도 해외 종목을 지원합니다.

**Q: 매일 16:30 KST에 텔레그램에 비교 메시지가 오는데 Stage 1/2/3이 무엇인가요?**

일봉 3단계 분류기의 결과입니다. 매일 장 마감 후 전 종목을 수급·모멘텀 조건으로 분류합니다.

- **Stage 1 (랠리 초입):** 당일 급등(+5%/+7%)과 거래량 폭증이 확인된 종목. 랠리의 첫 번째 신호.
- **Stage 2 (중간 조정·재매집):** Stage 1 이후 눌림목 구간에 있는 종목. 재매집 신호.
- **Stage 3 (과열 재가속):** 눌림목 이후 다시 고점을 돌파하는 종목. RSI 70 이상 + 외국인·기관 동시 순매수.

"양쪽 통과" 종목은 주봉 Ichimoku 7조건 + 일봉 Stage 조건을 모두 충족한 최우선 감시 대상입니다. 자세한 조건은 [8. 일봉 3단계 분류기](#8-일봉-3단계-분류기)를 참고하세요.

---

**Q: 뉴스 신호가 갑자기 줄었습니다 (v0.5.0.0 업그레이드 후)**

정상 동작입니다. v0.5.0.0부터 스크리너 통과 종목에 대한 뉴스만 알림이 옵니다. 첫 일요일 스크리닝이 실행되기 전에는 캐시가 비어 있어 모든 신호가 발송되지만, 스크리닝 완료 후에는 통과 종목만 필터링됩니다. 이는 의도된 동작입니다.

**Q: 차트 스크리너 백테스트 HTML 파일이 없습니다**

첫 일요일 스크리닝 실행 후 생성됩니다. 이전 스크리닝 이력이 없으면 빈 파일이 만들어집니다. 신호가 축적될수록 데이터가 채워집니다.

**Q: Python 3.12를 사용해도 되나요?**

공식 검증되지 않았습니다. 일부 의존 라이브러리의 호환성 문제가 있을 수 있습니다. Python 3.11.x 사용을 강력히 권장합니다.

**Q: Windows에서 직접 실행할 수 있나요?**

v1 기준으로 Linux 또는 WSL2(Windows Subsystem for Linux) 환경을 권장합니다. WSL2 설치 후 이 매뉴얼의 Linux 절차를 그대로 따라하시면 됩니다.

**Q: 서버를 종료하면 어떻게 되나요?**

tmux 세션 방식: tmux 세션이 살아있는 한 계속 실행됩니다. 서버 재부팅 시에는 세션이 사라지므로 다시 실행해야 합니다.
systemd 서비스 방식: 서버 재부팅 후 자동으로 재시작됩니다 (`WantedBy=multi-user.target` 설정 시).

---

> 버전 업데이트 시 이 매뉴얼의 상단 버전 정보(`Last verified: vX.Y.Z`)를 함께 갱신해 주세요.
