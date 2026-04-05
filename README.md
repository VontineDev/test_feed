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
cp env.example .env
# .env 편집 — DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

# 로컬 LLM 실행 (Ollama 기준)
ollama serve
ollama pull qwen2.5:7b

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
| `/backtest` | 판정별·종목별 적중률 백테스팅 리포트 |
| `/help` | 명령어 목록 |

## 데이터 흐름

```
RSS 피드 수집 → 기사 본문 크롤링 → LLM 한글 요약
→ LLM 매매 신호 감지 (BUY/SELL/WATCH)
→ yfinance 시세 교차분석 (CONFIRM/CAUTION/FILTER/NEUTRAL)
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
db.py                 # PostgreSQL 연동 (asyncpg)
telegram_bot.py       # 봇 명령어 처리
telegram_notify.py    # 신호 알림 전송
volume_pattern.py     # 거래량 패턴 분석
batch_run.py          # 배치 OHLCV 내보내기 + 분석 스크립트
test_backtest.py      # pytest 단위 테스트
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
pytest test_backtest.py -v
```

## 버전

현재 버전: `0.2.0.0` — [CHANGELOG](CHANGELOG.md) 참고
