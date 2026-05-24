# 한국 주식 뉴스 기반 매매 신호 알림 시스템

국내외 금융 뉴스를 실시간 수집하여 로컬 LLM으로 한글 요약 및 매매 신호를 추출하고 텔레그램으로 알려주는 도구입니다. 자동 주문 기능 없음 — 투자 결정은 사용자가 직접 합니다.

한국어 소스 포함: 연합뉴스, 한국경제, 매일경제 RSS 피드 (한국어 원문 본문 수집)

## 요구 사항

- Python 3.11+
- PostgreSQL 14+
- Ollama (Qwen2.5-7B 이상 권장) 또는 LM Studio (Qwen3-8B)
- Telegram Bot Token + Chat ID

## 빠른 시작

```bash
# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정
cp .env.example .env
# .env 편집 — DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

# 로컬 LLM 실행 (Ollama 기준)
ollama serve
ollama pull qwen3.5:9b

# 실행
python run_scheduler.py
```

## 텔레그램 명령어

| 명령어 | 설명 |
|--------|------|
| `/status` | 업타임, 누적 수집 건수, 최근 24h 수집·신호 건수 |
| `/signals` | 최근 매매 신호 10건 |
| `/signals buy` | BUY 신호만 조회 |
| `/signals sell` | SELL 신호만 조회 |
| `/signals watch` | WATCH 신호만 조회 |
| `/today` | 오늘 카테고리별 수집 건수 + 최신 기사 5건 |
| `/backtest <mode> <start> <end>` | 통합 백테스트 — ichimoku / stage / stage2 / cross 모드 |
| `/watchlist` | 거래대금 워치리스트 즉시 조회 (온디맨드) |
| `/screener` | 최신 강세 후보 발굴 결과 (DM + 채널) |
| `/scan` | 강세 후보 즉시 스캔 (전 종목 실시간 스캔, 약 10~20분) |
| `/paper` | 모의투자 오픈 포지션 현황 |
| `/paper_perf` | 모의투자 누적 성과 (승률·수익·슬리피지) |
| `/paper_exit <코드>` | 수동 강제 청산 |
| `/top` | 당일 거래금액 상위 10 (KOSPI+KOSDAQ) |
| `/buy <코드> <가격> <수량>` | 진입 기록 |
| `/sell <코드> <가격>` | 청산 기록 (FIFO) |
| `/port` | 보유 현황 + 미실현 P&L |
| `/pnl [week\|month\|all]` | 실현 P&L 요약 |
| `/help` | 명령어 목록 |

## 데이터 흐름

```
RSS 피드 수집 → 기사 본문 크롤링 → LLM 한글 요약
→ LLM 매매 신호 감지 (BUY/SELL/WATCH)
→ PostgreSQL 저장 → 텔레그램 알림
```

## 프로젝트 구조

```
run_scheduler.py          # 메인 실행 — RSS 루프 + 봇 병렬 실행 / --once watchlist|stage 지원 (892줄, 잡 로직은 jobs/에 위임)

jobs/                     # 스케줄러 잡 패키지 (run_scheduler.py 에서 import)
  stage_job.py            # 일봉 3단계 분류 잡 (OHLCV 수집 + stage_classifications 저장)
  screener_job.py         # 주봉 Ichimoku 스크리너 잡 (전 종목 스캔 + HTML 리포트)
  infra_jobs.py           # KRX 종목 갱신 + 외국인·기관 순매수 증분 sync 잡
  watchlist_job.py        # 거래대금 워치리스트 일보 잡 + build_watchlist_entries 헬퍼
  paper_jobs.py           # 모의투자 3종 잡 — EOD exit, 샘플링, T+1 시가 진입

core/                     # 공유 유틸리티
  db.py                   # PostgreSQL 연동 (asyncpg)
  ticker_cache.py         # 종목명 → yfinance 심볼 인메모리 캐시
  ohlcv_cache.py          # OHLCV DB 캐시 레이어
  article_fetcher.py      # 기사 본문 크롤링

data/                     # 데이터 수집·동기화
  market_data.py          # MacroContext(USD/KRW·기준금리), calc_rsi, fetch_daily_flow
  krx_sync.py             # KRX 전체 종목 DB 동기화 (KOSPI+KOSDAQ ~2500종목)
  krx_openapi.py          # KRX Open API REST 클라이언트 — OHLCV·종목마스터·지수 시세
  krx_flow_sync.py        # 외국인·기관 순매수 파이프라인 → daily_flow 테이블
  krx_aftermarket_sync.py # KRX 장후 데이터 동기화
  kiwoom_aftermarket_sync.py  # Kiwoom REST API 클라이언트
  kiwoom_paper_trader.py  # 모의투자 Kiwoom 연동

analysis/                 # 분석·전략
  signal_detector.py      # LLM 매매 신호 감지
  chart_screener.py       # 주봉 차트 스크리너 (Ichimoku + MA, KOSPI/KOSDAQ 전종목)
  screener_filters.py     # 스크리너 필터 프리셋
  stage_classifier.py     # 일봉 3단계 분류기 — Stage 1/2/3 분류 + 피크아웃 신호
  backtest_engine.py      # 통합 백테스트 엔진 — ichimoku / stage / cross 3모드
  volume_pattern.py       # 거래량 패턴 분석
  macro_tracker.py        # OLS 팩터 모델 — 6개 매크로 팩터 추적

telegram/                 # 텔레그램 연동
  telegram_bot.py         # 봇 명령어 처리
  telegram_notify.py      # 신호 알림 전송 + Ichimoku/Stage 비교 메시지
  telegram_trade.py       # 매매 기록 명령어 처리

reports/                  # 리포트·요약
  summarizer.py           # 로컬 LLM 한글 요약
  generate_html_report.py # 주봉 스크리닝 HTML 리포트 생성

dashboard/                # 웹 대시보드 (FastAPI + React)
  backend/                # FastAPI 서버 (포트 8000)
  frontend/               # React + Vite (dist/ 정적 빌드)

tests/                    # pytest 테스트 (601개)
docs/                     # 문서
scripts/                  # 운영 스크립트
  start_dashboard.ps1     # 대시보드 서버 시작/재시작 (단일 진입점)
  start_crawler.bat       # 크롤러 직접 실행
  run_aftermarket_sync.bat # 장후 동기화 실행
  duckdns_update.bat      # DuckDNS IP 업데이트
  restart_scheduler.bat   # Windows 작업 스케줄러 NewsCrawler 재시작
  run_sweep.py            # 백테스트 파라미터 그리드서치
  register_tasks.ps1      # Windows 작업 스케줄러 통합 등록 (-Task all|crawler|aftermarket|dashboard)
  start_dashboard_service.bat  # 대시보드 서비스 래퍼 (Task Scheduler용)
sql/                      # DB 스키마 마이그레이션
  rls_policies.sql        # RLS 정책 마이그레이션 (14 테이블 backend_all, pgAdmin/Supabase SQL 에디터 실행)
logs/                     # 로그 파일
```

## 환경변수

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_db
DB_USER=news_user
DB_PASSWORD=your_password
# 또는 단일 DSN
DATABASE_URL=postgresql://news_user:password@localhost:5432/news_db

# Telegram
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# (선택) 여러 Chat ID 허용
ALLOWED_CHAT_IDS=123456789,987654321

# (선택) KRX OpenAPI — OHLCV 캐시·종목마스터 (openapi.krx.co.kr 가입 후 발급)
KRX_OPENAPI_KEY=your_krx_openapi_key

# (선택) KRX 포털 계정 — 수급 데이터 수집 (krx_flow_sync.py)
# data.krx.co.kr 브라우저 로그인 후 DevTools > Cookies에서 복사
KRX_SESSION=your_jsessionid_cookie   # JSESSIONID (필수)
KRX_VISITOR=your_smvisitorid_cookie  # __smVisitorID (선택)
KRX_ID=your_krx_id
KRX_PW=your_krx_password
```

## 대시보드 실행

```bash
scripts\start_dashboard.ps1   # 백엔드 서버 시작/재시작 (http://localhost:8000)
```

탭 구성: 히트맵 · 종목 분석 · Top · 모의투자 · 매크로

## 테스트 실행

```bash
pytest -v
pytest tests/test_backtest_engine.py -v
pytest tests/test_watchlist_brief.py -v
```

## 문서

**시작하기**
- [docs/USER_MANUAL.md](docs/USER_MANUAL.md) — 설치부터 첫 텔레그램 알림까지 전체 가이드 (처음 설치하는 분은 여기서 시작)

**기능별 가이드 (How-to)**
- [docs/howto-screener.md](docs/howto-screener.md) — 주봉 Ichimoku 스크리너 설정·Calibration
- [docs/howto-stage-classifier.md](docs/howto-stage-classifier.md) — 일봉 3단계 분류기 설정
- [docs/howto-watchlist.md](docs/howto-watchlist.md) — 거래대금 워치리스트 온디맨드 조회
- [docs/HowToBacktest.md](docs/HowToBacktest.md) — 통합 백테스트 엔진 사용 가이드
- [docs/Dashboard.md](docs/Dashboard.md) — 웹 대시보드 개발·배포 가이드
- [docs/HTTPS-Setup.md](docs/HTTPS-Setup.md) — Caddy HTTPS 설정 (Let's Encrypt + DuckDNS)

**레퍼런스**
- [docs/reference-env-vars.md](docs/reference-env-vars.md) — 환경변수 전체 목록
- [docs/reference-telegram-commands.md](docs/reference-telegram-commands.md) — Telegram 명령어 전체 목록

**설계 해설 (Explanation)**
- [docs/explanation-signal-pipeline.md](docs/explanation-signal-pipeline.md) — 신호 파이프라인·게이팅·HIGH CONFIDENCE 설계 이유
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — 시스템 아키텍처, 모듈 상세, 데이터 흐름

**변경 이력**
- [CHANGELOG.md](CHANGELOG.md) — 버전별 변경 이력

## 버전

현재 버전: `0.9.9.0` — [CHANGELOG](CHANGELOG.md) 참고
