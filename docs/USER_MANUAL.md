> 최종 검증 버전: **v0.9.0.0** | 운영 환경: Windows 11 + Python 3.11

# 한국 주식 퀀트 신호 시스템 — 사용 설명서

KOSPI/KOSDAQ 전 종목을 자동 분석하여 매매 시그널을 Telegram으로 전달하고, 실제 거래를 기록·추적하는 자가 호스팅 도구입니다.

---

## 목차

1. [시스템이 하는 일](#1-시스템이-하는-일)
2. [최초 설치](#2-최초-설치)
3. [환경변수 설정](#3-환경변수-설정)
4. [시스템 시작](#4-시스템-시작)
   - [4-b. 웹 대시보드](#4-b-웹-대시보드)
5. [Telegram 명령어](#5-telegram-명령어)
   - 정보 조회: /status · /signals · /today · /screener · /top
   - 분석: /backtest · /scan
   - 거래 저널: /buy · /sell · /port · /pnl
   - 모의투자: /paper · /paper_perf · /paper_exit
6. [자동 실행 일정](#6-자동-실행-일정)
7. [시그널 해석 가이드](#7-시그널-해석-가이드)
8. [거래 저널 사용법](#8-거래-저널-사용법)
9. [문제 해결](#9-문제-해결)

---

## 1. 시스템이 하는 일

```
자동 실행 (손댈 것 없음)                   Telegram 명령어 (수동)
────────────────────────────────────      ───────────────────────
평일 09:05 모의투자 T+1 매수주문           /status   — 시스템 상태
평일 15:40 거래량 일보 전송                /signals  — 최근 매매 신호
평일 16:10 모의투자 Exit 체크              /screener — 주봉 스크리닝 결과
평일 16:30 전 종목 Stage 분류              /backtest — 전략 백테스트
평일 16:40 모의투자 신호 샘플링            /buy /sell /port /pnl — 거래 기록
평일 17:00 감시 종목 워치리스트 전송       /paper /paper_perf — 모의투자 현황
매일 16:05 시간외 단일가 수집
매일 20:00 KRX 종목 리스트 갱신
일요일 20:30 주봉 Ichimoku 스크리닝
7분마다 뉴스 수집 + 신호 감지
```

**실거래 자동화:** 없음. 실제 매매는 사용자가 직접 증권사 앱에서 실행합니다.

**모의투자 자동화 (KIWOOM_MOCK_APPKEY 설정 시):** 키움 모의투자 서버(mockapi.kiwoom.com)에 자동 주문을 제출하여 T+0 종가 진입 가정과 T+1 시가 실제 체결가의 슬리피지를 측정합니다.

---

## 2. 최초 설치

### 요구 사항

| 항목 | 버전 | 비고 |
|------|------|------|
| Python | **3.11.x** | 3.12 미검증 |
| PostgreSQL | 14 이상 | |
| Ollama (또는 LM Studio) | 최신 | 로컬 LLM |
| Kiwoom REST API 계정 | — | 시간외 단일가 수집용 |

### 2-1. 패키지 설치

```powershell
python -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2-2. PostgreSQL 설정

PostgreSQL 설치 후 데이터베이스와 사용자를 만듭니다.

```sql
-- psql 접속 후 실행
CREATE USER news_user WITH PASSWORD '강력한비밀번호';
CREATE DATABASE news_db OWNER news_user;
GRANT ALL PRIVILEGES ON DATABASE news_db TO news_user;
```

연결 확인:
```powershell
psql -h localhost -U news_user -d news_db -c "SELECT version();"
```

### 2-3. LLM 설치 (Ollama)

```powershell
# Ollama 설치 후 (https://ollama.com)
ollama pull qwen3.5:9b    # 약 5GB, 네트워크 속도에 따라 10분~1시간

# 설치 확인
ollama list
```

### 2-4. Telegram 봇 생성

1. Telegram에서 `@BotFather` 검색 → `/newbot` → **Bot Token** 획득
2. `@userinfobot` 에 메시지 → **Chat ID** 획득

### 2-5. DB 초기화

```powershell
venv\Scripts\activate
python -c "import asyncio, asyncpg; from db import init_db; asyncio.run(init_db(asyncio.run(asyncpg.create_pool('postgresql://news_user:비밀번호@localhost:5432/news_db'))))"
```

처음 `python run_scheduler.py` 실행 시 자동으로 테이블이 생성되므로, 이 단계는 생략해도 됩니다.

---

## 3. 환경변수 설정

`.env` 파일을 프로젝트 루트에 생성합니다.

```dotenv
# ── PostgreSQL ──────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_db
DB_USER=news_user
DB_PASSWORD=강력한비밀번호여기입력

# ── Telegram ────────────────────────────────────────────────
TELEGRAM_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789           # 본인 Chat ID
# ALLOWED_CHAT_IDS=111,222,333       # 여러 명 허용 시 사용 (쉼표 구분)
# TELEGRAM_CHANNEL_ID=-100123456789  # 채널로도 발송 시 설정

# ── 로컬 LLM (Ollama) ────────────────────────────────────────
OLLAMA_BASE=http://localhost:11434
OLLAMA_MODEL=qwen3.5:9b

# ── Kiwoom REST API (시간외 단일가 수집용) ───────────────────
KIWOOM_APP_KEY=여기에입력
KIWOOM_APP_SECRET=여기에입력

# ── Kiwoom 모의투자 (paper trading, 선택) ────────────────────
# 발급: open.kiwoom.com → 모의투자 앱키 신청
KIWOOM_MOCK_APPKEY=여기에입력
KIWOOM_MOCK_APPSECRET=여기에입력

# ── 기타 ─────────────────────────────────────────────────────
KOREA_BASE_RATE=3.50       # 한국은행 기준금리 (수동 업데이트)
SCREENER_WORKERS=8         # 병렬 워커 수 (CPU 4코어 이하면 4로 낮추세요)
```

> `.env` 파일은 `.gitignore`에 등록되어 있습니다. 절대 git에 커밋하지 마세요.

---

## 4. 시스템 시작

```powershell
venv\Scripts\activate
python run_scheduler.py
```

30초 이내에 아래 메시지가 보이면 정상입니다:

```
뉴스 크롤러 시작 — 수집 7분 간격
Telegram 봇 시작 — /status /signals ...
▶ [수집] 시작  2026-05-12 09:00:00
```

Telegram에서 `/status` 를 보내 응답이 오면 완전히 정상입니다.

### 옵션

```powershell
python run_scheduler.py --interval 3     # 수집 간격 3분으로 변경
python run_scheduler.py --no-summary    # LLM 없이 수집만
python run_scheduler.py --once watchlist  # 워치리스트 일보 1회 실행 후 종료
python run_scheduler.py --once stage      # Stage 분류 1회 실행 후 종료
```

### 백그라운드 실행 (Windows)

별도 PowerShell 창을 띄우거나, 작업 스케줄러에 등록하세요.

---

## 4-b. 웹 대시보드

브라우저에서 시스템 상태를 시각적으로 확인하고, 스케줄러 잡을 수동으로 실행할 수 있는 웹 인터페이스입니다.

### 구성 요소

| 컴포넌트 | 설명 |
|----------|------|
| **히트맵** | 오늘 Stage 1/2/3 분류 결과를 거래대금 기준 타일 히트맵으로 표시 (30분 캐시) |
| **포지션** | 모의투자 오픈/대기 포지션 목록과 미실현 수익률 |
| **신호 피드** | 뉴스 매매 신호 실시간 SSE 스트림 (15초 폴링) |
| **스케줄러** | Stage 분류·차트 스크리너·모의투자 샘플링 잡 수동 트리거 + 최근 실행 이력 |

### 시작 방법

**방법 1 — 개발 모드 (백엔드 + 프런트엔드 분리 실행)**

```powershell
# 터미널 1: FastAPI 백엔드
venv\Scripts\activate
cd dashboard\backend
uvicorn main:app --reload --port 8000

# 터미널 2: Vite 프런트엔드 (핫 리로드)
cd dashboard\frontend
npm install          # 최초 1회
npm run dev          # http://localhost:5173
```

**방법 2 — 프로덕션 모드 (정적 빌드 → FastAPI 서빙)**

```powershell
# 1. 프런트엔드 빌드
cd dashboard\frontend
npm install
npm run build        # dist/ 폴더 생성

# 2. 백엔드만 기동 (http://localhost:8000)
cd ..\backend
venv\Scripts\activate
uvicorn main:app --port 8000
```

### 백엔드 환경변수

백엔드는 프로젝트 루트의 `.env`를 그대로 사용합니다 (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`).

### 스케줄러 트리거 동작 원리

대시보드에서 버튼을 누르면 `scheduler_triggers` 테이블에 `pending` 행이 삽입됩니다.  
`run_scheduler.py` 가 30초마다 이 행을 감지해 해당 잡을 실행하고 `done`으로 갱신합니다.  
따라서 **`run_scheduler.py`가 실행 중이어야** 버튼이 실제로 동작합니다.

### API 엔드포인트 요약

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/heatmap` | Stage 히트맵 데이터 (30분 캐시) |
| `GET` | `/api/positions` | 오픈·대기 포지션 목록 |
| `GET` | `/api/signals/stream` | 신호 SSE 스트림 |
| `POST` | `/api/scheduler/trigger` | 잡 수동 트리거 (`{"job": "stage"\|"screener"\|"paper_sample"}`) |
| `GET` | `/api/scheduler/status` | 최근 트리거 이력 10건 |

---

## 5. Telegram 명령어

### /status

시스템 상태를 확인합니다.

```
/status
```

```
📡 크롤러 상태

업타임: 2시간 14분
누적 수집: 183건
최근 24h 수집: 47건 / 신호: 8건
```

---

### /signals

최근 매매 신호 10건을 조회합니다.

```
/signals              # BUY + SELL + WATCH 전체
/signals buy          # BUY만
/signals sell         # SELL만
/signals watch        # WATCH만
```

```
🟢 BUY ⬛⬛⬛⬛⬜
   삼성전자 반도체 감산 발표 — 공급 감소로 가격 반등 기대
   🕐 14:23 KST
```

신호 강도 바(⬛): 5칸 만점, 많을수록 강한 신호.

---

### /today

오늘 수집된 기사 현황을 조회합니다.

```
/today
```

```
📰 오늘 수집 현황 (2026-05-12)

reuters: 23건 / yfinance_news: 15건 / yonhap: 8건
합계: 56건

📌 최신 기사 5건
1. [Reuters] Fed signals patience...
2. [한국경제] 삼성전자 HBM4 양산...
```

---

### /screener

최신 주봉 Ichimoku 스크리닝 결과를 조회합니다. 매주 일요일 20:30 KST에 자동 갱신됩니다.

```
/screener
```

```
📊 주봉 차트 스크리너 (2026-W20)
통과 종목: 12개

🏭 반도체
  • 삼성전자 (005930) 78,500원
  • SK하이닉스 (000660) 182,000원

🔋 이차전지
  • LG에너지솔루션 (373220) 385,000원
```

> 이 목록에 있는 종목의 뉴스 신호만 Telegram 알림이 옵니다 (스크리너 게이팅).

---

### /scan

주봉 스크리닝을 즉시 실행합니다. 전 종목(~2,700개)을 처리하므로 10~20분 소요됩니다.

```
/scan
```

---

### /top

당일 거래금액 상위 10개 종목을 조회합니다. KOSPI와 KOSDAQ을 합산하여 순위를 산정합니다.

```
/top
```

```
📊 거래금액 상위 10  (05/16  KOSPI+KOSDAQ)

 1. SK하이닉스    1,819,000    -7.66%   14.1조
 2. 삼성전자        270,500    -8.61%   10.7조
 3. 현대차          700,000    -1.69%    3.1조
 4. LG전자          240,500   +10.83%    1.4조
 5. 삼성전기      1,010,000    -1.37%    1.4조
```

---

### /backtest

차트 전략(이치모쿠/Stage/교차)을 지정 기간으로 백테스트합니다. 결과는 Telegram + HTML 파일(`reports/backtest/`)로 저장됩니다. HTML 보고서는 종목별 상세 결과(1차 익절일·수익, 최종 청산일·수익, blended 가중수익)를 포함합니다.

```
/backtest <mode> <start> <end> [market] [--max N] [--tx-cost F] [--tp1 F] [--tp1-ratio F] [--trail F] [--stop F]
```

| 파라미터 | 설명 | 기본값 |
|---------|------|--------|
| `mode` | `ichimoku` \| `stage` \| `stage2` \| `cross` | 필수 |
| `start` | 시작일 (YYYY-MM-DD) | 필수 |
| `end` | 종료일 (YYYY-MM-DD) | 필수 |
| `market` | `KOSPI` \| `KOSDAQ` \| `ALL` | ALL |
| `--max N` | 최대 종목 수 (0=전종목) | 200 |
| `--tx-cost F` | 왕복 거래비용 비율 | ~0.0021 |
| `--tp1 F` | 1차 익절 목표 (예: 0.25 = +25%) | 0 (미사용) |
| `--tp1-ratio F` | 1차 익절 시 청산 비율 | 0.5 |
| `--trail F` | 트레일링 스탑 (고점 대비, 예: 0.10 = -10%) | 0 (미사용) |
| `--stop F` | 하드 손절 비율 | 0.08 |

**예시 (그리드서치 최적 파라미터 적용):**

```
# Stage/KOSPI — tp1=25%, trail=10%, stop=10%
/backtest stage 2024-01-01 2026-05-12 KOSPI --tp1 0.25 --trail 0.10 --stop 0.10

# Stage/KOSDAQ — KOSDAQ은 trail을 15%로 넓게
/backtest stage 2024-01-01 2026-05-12 KOSDAQ --tp1 0.25 --trail 0.15 --stop 0.10

# Cross (Ichimoku × Stage) — 승률 54%, tp1=15%가 과적합 없이 최적
/backtest cross 2024-01-01 2026-05-12 ALL --tp1 0.15 --trail 0.10 --stop 0.10 --max 100

# 기존 방식 (분할 청산 없음, MA20 이탈 기준)
/backtest ichimoku 2025-01-01 2026-01-01
```

데이터 다운로드 포함 2~20분 소요됩니다. 완료 후 결과가 전송됩니다.

**mode 설명 및 검증된 최적 파라미터:**

| mode | 전략 | tp1 | trail | stop | 검증 승률 | CAGR |
|------|------|-----|-------|------|---------|------|
| `stage` (KOSPI) | 일봉 Stage 1 진입 + 분할 청산 | 25% | 10% | 10% | 45.7% | 46.5% |
| `stage` (KOSDAQ) | KOSDAQ — 변동성 크므로 trail 넓게 | 25% | 15% | 10% | 46.7% | 67.7% |
| `cross` | Ichimoku × Stage 동시 통과 — 신호 적지만 승률 최고 | 15% | 10% | 10% | 54.3% | 46.6% |
| `stage2` | Stage 2 재매집 구간 진입 전략 | — | — | — | 미검증 | — |
| `ichimoku` | 주봉 Ichimoku 7조건 단독 | 25% | 10% | 10% | **55.8%** | — |

> 파라미터는 `backtest_engine.OPTIMAL_EXIT_PARAMS` / `OPTIMAL_EXIT_PARAMS_KOSDAQ` / `OPTIMAL_EXIT_PARAMS_CROSS` / `OPTIMAL_EXIT_PARAMS_ICHIMOKU`로 코드에서 import 가능합니다.

---

### /buy

진입을 기록합니다. 종목 코드 6자리를 입력하면 자동으로 `.KS` 심볼로 변환됩니다.

```
/buy <종목코드> <매수가> <수량> [YYYYMMDD]
```

```
/buy 005930 70000 100           # 오늘 날짜로 진입
/buy 005930 70000 100 20260512  # 날짜 지정
```

```
✅ 진입 기록 완료 (#42)
종목: 005930.KS
날짜: 2026-05-12
가격: 70,000원 × 100주 = 700만원
```

진입 시 `stage_classifications` 에서 가장 최근 Stage 정보를 자동으로 조회합니다.

---

### /sell

FIFO(선입선출) 방식으로 가장 오래된 미청산 포지션을 청산합니다.

```
/sell <종목코드> <매도가>
```

```
/sell 005930 73500
```

```
✅ 청산 완료
종목: 005930.KS
날짜: 2026-05-15
매도가: 73,500원 × 100주
P&L: 🟢 35만원 (+5.00%)
```

---

### /port

현재 보유 중인 미청산 포지션과 미실현 손익을 조회합니다. 현재가는 yfinance로 실시간 조회합니다.

```
/port
```

```
📊 보유 현황 (2026-05-12 14:30)

005930.KS (S1)
  진입: 70,000원 × 100주 (2026-05-12)
  미실현: 🟢 35만원
```

> yfinance 시세는 15~20분 지연이 있습니다.

---

### /pnl

실현 손익 요약을 조회합니다.

```
/pnl             # 전체 기간
/pnl week        # 직전 주
/pnl month       # 이번 달
```

```
📈 P&L 요약 (전체)

거래: 12건  승 9 / 패 3  (승률 75%)
총 P&L: 🟢 245만원
평균 수익: 37만원
평균 손실: 15만원

Stage별:
  Stage 1: 7건 → 승률 71%
  Stage 2: 3건 → 승률 100%
  Stage 3: 2건 → 승률 50%
```

---

### /help

전체 명령어 목록을 표시합니다.

```
/help
```

---

### /paper

키움 모의투자 현재 포지션 현황을 조회합니다. 모델별(stage/kosdaq/cross/ichimoku)로 open·pending 포지션을 표시하며, 실시간 가격과 미실현 수익률을 보여줍니다.

```
/paper
```

```
📋 모의투자 포지션 현황 (2026-05-16)

[stage] 3/10 슬롯
  005930 삼성전자  진입 78,500 → 현재 81,200  +3.44%  TP1 미도달
  000660 SK하이닉스  진입 182,000 → 현재 179,000  -1.65%

[cross] 1/5 슬롯
  035420 NAVER  진입 215,000 → 현재 228,000  +6.05%  TP1 도달✅

[kosdaq] 0/10 슬롯 (pending 2)
  263750 펄어비스  pending (진입 대기)
```

> KIWOOM_MOCK_APPKEY가 설정되지 않은 경우 "모의투자 비활성화" 메시지가 표시됩니다.

---

### /paper_perf

모의투자 누적 성과를 조회합니다. 모델별 실전 승률·평균 수익률·슬리피지를 백테스트 이론값과 비교합니다.

```
/paper_perf
```

```
📊 모의투자 누적 성과

[stage] 청산 12건
  실전 승률: 58.3%  평균 수익: +7.2%
  평균 슬리피지: +0.12% (이론→실전 진입가 차이)

[cross] 청산 4건
  실전 승률: 75.0%  평균 수익: +11.4%
  평균 슬리피지: +0.08%

목표 슬리피지 범위: -0.5% ~ +0.5%
```

---

### 모의투자 모델별 파라미터

4개 모델 각각의 진입 신호 소스와 검증된 청산 파라미터입니다.

| 모델 | 신호 소스 | 슬롯 | 포지션 | tp1 | tp1_ratio | trail | stop | val_sharpe | val_승률 |
|------|----------|------|--------|-----|-----------|-------|------|-----------|---------|
| `stage` | 일봉 Stage 1 (KOSPI) | 10 | 1,000만원 | 25% | 50% | 10% | 10% | 4.70 | 45.7% |
| `kosdaq` | 일봉 Stage 1 (KOSDAQ) | 10 | 1,000만원 | 25% | 50% | **15%** | 10% | 5.48 | 46.7% |
| `cross` | Stage 1 ∩ Ichimoku | 5 | 2,000만원 | **15%** | 50% | 10% | 10% | 5.11 | 54.3% |
| `ichimoku` | 주봉 Ichimoku 7조건 | 10 | 1,000만원 | 25% | **70%** | 10% | 10% | **7.50** | **55.8%** |

- `tp1_ratio 70%` (ichimoku): 주봉 전략 특성상 TP1 도달 시 70%를 조기 청산하는 것이 그리드서치에서 최적으로 검증됨.
- `trail 15%` (kosdaq): KOSDAQ 변동성이 커서 트레일링 스탑을 넓게 설정.
- `tp1 15%` (cross): 신호 수가 적어 과적합을 피하기 위해 보수적으로 설정.

---

### /paper_exit

특정 종목을 모의투자에서 즉시 강제 청산합니다. 시장가 매도주문을 키움 모의투자 서버에 제출합니다.

```
/paper_exit <종목코드>
```

```
/paper_exit 005930
```

```
✅ 005930 강제 청산 완료
  모델: stage
  청산가: 81,200원
  수익률: +3.44%
  주문번호: 20260516-00123
```

---

## 6. 자동 실행 일정

`run_scheduler.py` 가 실행 중일 때 다음 작업이 자동으로 실행됩니다.

| 작업 | 시각 | 설명 |
|------|------|------|
| 뉴스 수집 + 신호 감지 | 7분마다 | RSS → LLM 요약 → 신호 → Telegram |
| **모의투자 T+1 진입** | **평일 09:05 KST** | **pending 포지션 → 시가로 키움 모의투자 매수주문** |
| 거래량 일보 | 평일 15:40 KST | 감시 종목 당일 등락률 상·하위 3종목 |
| **모의투자 Exit 체크** | **평일 16:10 KST** | **보유 포지션 EOD 가격 체크 → 손절/익절/트레일 매도주문** |
| 시간외 단일가 수집 | 평일 16:05 KST | Kiwoom ka10098 API → DB 저장 |
| Stage 분류 | 평일 16:30 KST | 전 종목 Stage 1/2/3 분류 + Telegram |
| **모의투자 EOD 샘플러** | **평일 16:40 KST** | **Stage1/Ichimoku/Cross 신호 샘플링 → pending 삽입** |
| 워치리스트 일보 | 평일 17:00 KST | Stage 1 감시 종목 현황 + 전환 알림 |
| KRX 종목 갱신 | 매일 20:00 KST | 전체 종목 리스트 최신화 |
| 주봉 스크리닝 | 일요일 20:30 KST | Ichimoku 7조건 전 종목 스캔 + HTML |

> **모의투자 잡 3개**는 `KIWOOM_MOCK_APPKEY`가 `.env`에 설정된 경우에만 활성화됩니다.

---

## 7. 시그널 해석 가이드

### 주봉 Ichimoku 스크리너 (7조건 AND)

| 조건 | 의미 |
|------|------|
| 종가 > 구름 상단 | 이번 주 구름 상향 돌파 |
| 직전 주 종가 ≤ 직전 주 구름 | 돌파 신선도 확인 |
| 종가 > MA20 | 중기 추세 상위 |
| 종가 > MA60 | 장기 추세 상위 |
| MA20 우상향 | 중기 추세 가속 |
| MA60 우상향 | 장기 추세 가속 |
| 종가 > MA120 | 초장기 추세 상위 (신규 상장 제외) |

### 일봉 3단계 분류기

#### Stage 1 — 랠리 초입

5조건 ALL 충족:

| 조건 | 기준 |
|------|------|
| 일일 상승률 | KOSPI ≥ +5%, KOSDAQ ≥ +7% |
| 거래량 | ≥ 20일 평균 × 2 |
| 이동평균 | 종가 > MA20 AND MA60 |
| 52주 위치 | 52주 고점 대비 −20% 이내 |
| 수급 | 외국인 또는 기관 순매수 |

#### Stage 2 — 눌림목 재매집

Stage 1 이후 14일 이내 종목:

| 조건 | 기준 |
|------|------|
| 가격 | Stage 1 고가 대비 −5% ~ −20% |
| 지지 | 종가 ≥ MA20 × 0.95 |
| 거래량 수축 | Stage 1 스파이크의 30% ~ 60% |
| 기관 수급 | 기관 순매수 연속일 ≥ 0 |

#### Stage 3 — 과열 재가속

| 조건 | 기준 |
|------|------|
| 돌파 | 종가 > 최근 10일 고가 |
| 일일 상승률 | ≥ +5% |
| RSI(14) | ≥ 70 |
| 거래량 | ≥ 30일 평균 × 1.5 |
| 수급 | 외국인 AND 기관 동시 순매수 |

**피크아웃 경고 (`⚠️`):** Stage 3 종목에서 외국인·기관 동시 순매도 2일 연속, 또는 윗꼬리 캔들 + 거래량 급증 시 발송.

### 텔레그램 일별 비교 메시지 (16:30 KST)

```
📊 스크리너 비교 (2026-05-12)

✅ 양쪽 통과 (3종목) — 최우선 감시
  삼성전자 (005930) · Stage 1
  SK하이닉스 (000660) · Stage 3

📌 Ichimoku만 통과: 9종목
📌 Stage만 해당: 14종목 (Stage 1: 8 / Stage 2: 4 / Stage 3: 2)
```

- **양쪽 통과:** 주봉 Ichimoku 7조건 + 일봉 Stage 동시 충족 → 가장 높은 신뢰도
- **Ichimoku만:** 장기 추세 우량이지만 당일 모멘텀 미충족
- **Stage만:** 당일 급등·수급 포착이지만 장기 추세 미확인

---

## 8. 거래 저널 사용법

### 기본 흐름

```
1. Stage 분류 메시지에서 관심 종목 확인
2. 직접 매수 (증권사 앱에서 집행)
3. /buy 005930 70000 100  ← 기록
4. 보유 중 /port 로 수시 확인
5. 직접 매도 (증권사 앱에서 집행)
6. /sell 005930 73500  ← 기록
7. /pnl 로 성과 분석
```

### 종목코드 입력 규칙

| 입력 | 변환 결과 | 비고 |
|------|-----------|------|
| `005930` | `005930.KS` | 기본값 KOSPI |
| `005930.KS` | `005930.KS` | 그대로 유지 |
| `005930.KQ` | `005930.KQ` | KOSDAQ 지정 |

### trade_log DB 구조

거래 기록은 PostgreSQL `trade_log` 테이블에 저장됩니다.

| 컬럼 | 설명 |
|------|------|
| `entry_delay_days` | 신호일 → 진입일 거래일 수 (자동 계산) |
| `pnl` | (매도가 − 매수가) × 수량 (자동 계산) |
| `pnl_pct` | 수익률 % 소수점 3자리 (자동 계산) |
| `stage_at_entry` | 진입 시 Stage (자동 조회) |

---

## 9. 문제 해결

### Telegram 봇이 응답하지 않는다

```powershell
# 토큰 확인
curl "https://api.telegram.org/bot$env:TELEGRAM_TOKEN/getMe"
# {"ok":true} 가 나오면 토큰 정상
```

`.env`의 `TELEGRAM_CHAT_ID`가 본인의 ID와 일치하는지 확인하세요 (`@userinfobot` 에서 조회).

---

### LLM이 응답하지 않는다 / 신호가 오지 않는다

```powershell
# Ollama 실행 확인
curl http://localhost:11434/api/tags

# 모델 목록 확인
ollama list
```

`qwen3.5:9b`가 목록에 없으면 `ollama pull qwen3.5:9b` 재실행.

`.env`의 `OLLAMA_MODEL` 값이 `ollama list` 결과와 정확히 일치해야 합니다.

---

### DB 연결 실패

```powershell
# PostgreSQL 서비스 확인
Get-Service -Name postgresql*

# 연결 테스트
psql -h localhost -U news_user -d news_db
```

`.env`의 `DB_PASSWORD`, `DB_HOST`, `DB_NAME`, `DB_USER`를 재확인하세요.

---

### /screener 결과가 없다

첫 설치 후 일요일 20:30 KST 자동 실행 전에는 DB가 비어 있습니다.

```
/scan   ← 즉시 실행 (10~20분 소요)
```

---

### Stage 분류 메시지가 오지 않는다

평일 16:30 KST에 실행됩니다. 수동으로 즉시 실행하려면:

```powershell
python run_scheduler.py --once stage
```

---

### /port에서 현재가 조회 실패

yfinance 일시 장애 또는 네트워크 문제입니다. 미실현 손익 계산은 생략되지만 보유 포지션 목록은 정상 표시됩니다. 잠시 후 재시도하세요.

---

### 뉴스 신호 알림이 갑자기 줄었다

정상입니다. 주봉 스크리너를 통과한 종목의 뉴스만 Telegram 알림이 옵니다. 일요일 스크리닝 전에는 전체 알림, 이후에는 통과 종목만 필터링됩니다.

---

### /backtest 실행 중 오류

데이터 다운로드 타임아웃이 원인인 경우가 많습니다. `--max` 값을 줄여 재시도하세요.

```
/backtest ichimoku 2025-06-01 2026-01-01 KOSPI --max 50
```

---

## FAQ

**Q. 자동으로 주식을 사거나 팔 수 있나요?**
실거래는 불가합니다. 단, `KIWOOM_MOCK_APPKEY`를 설정하면 키움 **모의투자** 서버에 자동 주문을 제출합니다. 이 주문은 가상 계좌에서만 체결되며, 실제 자산에 영향을 주지 않습니다.

**Q. 모의투자와 백테스트의 차이는 무엇인가요?**
백테스트는 과거 yfinance 데이터로 시뮬레이션합니다 (T+0 종가 진입 가정). 모의투자는 키움 모의투자 서버에 실제 주문을 내어 T+1 시가 체결가를 기록합니다. 두 진입가의 차이(슬리피지)를 측정하여 백테스트 엣지가 실전에서도 유지되는지 검증합니다.

**Q. /pnl의 "직전 주"는 어떻게 계산되나요?**
지난 월요일 00:00 ~ 지난 일요일 23:59 기준입니다.

**Q. KOSDAQ 종목을 /buy로 기록하려면?**
종목코드에 `.KQ`를 명시합니다: `/buy 263750.KQ 15000 200`

**Q. /sell을 두 번 하면 포지션 두 개가 청산되나요?**
FIFO 방식으로 가장 오래된 포지션 하나씩 청산됩니다. 전량 청산하려면 보유 수량만큼 반복 실행하세요.

**Q. 뉴스 신호 적중률은 얼마나 되나요?**
뉴스 신호는 시장이 이미 반영한 후에 생성되는 후행 지표 특성상 방향성 예측 도구로는 유의미하지 않습니다. 시스템의 실제 알파는 Stage 분류기 + Ichimoku 스크리너에 있으며, `/backtest` 로 차트 전략 백테스트를 실행할 수 있습니다.

**Q. Kiwoom API 없이도 시스템이 동작하나요?**
네. `KIWOOM_APP_KEY` 없이는 시간외 단일가 수집(16:05 KST)만 비활성화됩니다. `KIWOOM_MOCK_APPKEY` 없이는 모의투자 잡 3개(09:05/16:10/16:40 KST)가 비활성화됩니다. 나머지 기능은 모두 정상입니다.

**Q. 모의투자가 평일에만 동작한다고 하는데, 주말에 테스트하려면?**
키움 모의투자 서버는 KRX 영업일에만 주문이 체결됩니다. 주말·공휴일에 주문을 제출하면 "모의투자 영업일이 아닙니다(RC4010)" 오류가 반환됩니다. 이는 정상 동작이며, 다음 영업일까지 대기하면 됩니다.

---

> 버전 업데이트 시 상단의 버전 정보를 함께 갱신하세요.
