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
| `/backtest` | 판정별·유형별·종목별 적중률 백테스팅 리포트 |
| `/screener` | 최신 주봉 차트 스크리닝 결과 (DM + 채널) |
| `/help` | 명령어 목록 |

## 데이터 흐름

```
RSS 피드 수집 → 기사 본문 크롤링 → LLM 한글 요약
→ LLM 매매 신호 감지 (BUY/SELL/WATCH)
→ yfinance 시세 교차분석 + Naver Finance 펀더멘털 스코어링 (PER/PBR/EPS)
→ PostgreSQL 저장 → 텔레그램 알림
```

교차분석 결과는 백테스팅 시스템이 주기적으로 추적합니다. 매주 일요일 20:00 KST에 자동 리포트가 발송됩니다.

## 프로젝트 구조

```
run_scheduler.py      # 메인 실행 — RSS 루프 + 봇 병렬 실행
article_fetcher.py    # 기사 본문 크롤링
summarizer.py         # 로컬 LLM 한글 요약
signal_detector.py    # LLM 매매 신호 감지
market_data.py        # yfinance 시세 조회 + 교차분석
backtest.py           # 판정 정확도 추적 + 백테스팅 리포트
chart_screener.py     # 주봉 차트 스크리너 (Ichimoku + MA, KOSPI/KOSDAQ 전종목)
generate_report.py    # 차트 스크리닝 결과를 UTF-8 파일로 저장 (CLI 스크립트)
generate_html_report.py # 차트 스크리닝 결과를 HTML 파일로 저장 — 정배열/일반 섹션, 인라인 CSS
db.py                 # PostgreSQL 연동 (asyncpg)
telegram_bot.py       # 봇 명령어 처리
telegram_notify.py    # 신호 알림 전송
volume_pattern.py     # 거래량 패턴 분석
krx_sync.py           # KRX 전체 종목 DB 동기화 (KOSPI+KOSDAQ ~2500종목)
ticker_cache.py       # 종목명→yfinance 심볼 인메모리 캐시 (startup 로드, 20:00 KST 갱신)
batch_run.py          # 배치 OHLCV 내보내기 + 분석 스크립트
test_backtest.py                   # pytest — 백테스팅 로직 (23개)
test_telegram_routing.py           # pytest — 텔레그램 신호 라우팅 회귀 (4개)
test_summarizer_regression_1.py    # pytest — LLM 헬스체크·Qwen3 thinking 회귀
test_krx_sync.py                   # pytest — KRX 동기화 + 티커 캐시 (31개)
test_ticker_cache_integration.py   # pytest — market_data·volume_pattern 캐시 통합 (7개)
test_article_type.py               # pytest — 기사 유형 분류 (17개)
test_db_dsn.py                     # pytest — DB DSN 설정 회귀 (7개)
test_macro_signal.py               # pytest — MACD/BB/MA 교차분석 스코어링 (29개)
test_signal_prompt.py              # pytest — 신호 감지 프롬프트 · WATCH 임계값 회귀 (10개)
test_chart_screener.py             # pytest — 주봉 스크리너 조건 + KIND 섹터 (26개)
test_screener_telegram_regression_1.py  # pytest — 스크리너 텔레그램 포맷 회귀 (14개)
test_fundamental.py                # pytest — PER/PBR/EPS 펀더멘털 레이어 (41개)
test_generate_html_report.py       # pytest — HTML 리포트 생성 (10개)
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
```

## 테스트 실행

```bash
# 전체 테스트
pytest -v

# 개별 파일
pytest test_backtest.py -v                    # 백테스팅 로직 (23개)
pytest test_telegram_routing.py -v            # 텔레그램 라우팅 신호 게이팅 (4개)
pytest test_summarizer_regression_1.py -v     # LLM 헬스체크·Qwen3 thinking (회귀)
pytest test_krx_sync.py -v                    # KRX 동기화 + 티커 캐시 (31개)
pytest test_ticker_cache_integration.py -v    # market_data·volume_pattern 캐시 통합 (7개)
pytest test_article_type.py -v               # 기사 유형 분류 (17개)
pytest test_db_dsn.py -v                     # DB DSN 설정 (7개)
pytest test_macro_signal.py -v                # MACD/BB/MA 교차분석 스코어링 (29개)
pytest test_signal_prompt.py -v               # 신호 프롬프트 · WATCH 임계값 회귀 (10개)
pytest test_chart_screener.py -v              # 주봉 스크리너 + KIND 섹터 (26개)
pytest test_screener_telegram_regression_1.py -v  # 스크리너 텔레그램 포맷 회귀 (14개)
pytest test_fundamental.py -v                     # PER/PBR/EPS 펀더멘털 레이어 (41개)
pytest test_generate_html_report.py -v            # HTML 리포트 생성 (10개)
```

## 문서

- [USER_MANUAL.md](USER_MANUAL.md) — 설치부터 첫 텔레그램 알림까지 전체 가이드 (처음 설치하는 분은 여기서 시작)
- [ARCHITECTURE.md](ARCHITECTURE.md) — 시스템 아키텍처, 모듈 상세, 데이터 흐름
- [CHANGELOG.md](CHANGELOG.md) — 버전별 변경 이력

## 버전

현재 버전: `0.4.1.0` — [CHANGELOG](CHANGELOG.md) 참고
