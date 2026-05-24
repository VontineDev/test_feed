# 한국 주식 뉴스 기반 매매 신호 알림 시스템
## 아키텍처 문서 v3.1

> **v2.x → v3.0 전면 재작성 안내**
> 기존 설계서(v2.1)는 기술적 지표(MA·RSI·볼린저) 기반 시스템을 기술했으나,
> 실제 구현된 코드는 **영문 금융 뉴스 크롤링 + 로컬 LLM 신호 감지** 방식입니다.
> v3.0은 현재 코드베이스를 기준으로 전면 재작성한 문서입니다.
> v0.2.0.0부터 연합뉴스·한국경제·매일경제 한국어 피드가 추가되었습니다.
> v0.2.1.0부터 텔레그램 라우팅이 한·외신 모두 `signal.is_actionable` 기반으로 통일되었습니다.
> v0.3.0.0부터 KRX 전체 종목 DB(`krx_listings`)와 인메모리 티커 캐시(`TickerCache`)가 추가되어 ~2500개 종목 자동 해석을 지원합니다.
> v0.2.5.0부터 `article_type` 기사 유형 분류(8종)가 추가되었습니다. 신호 알림에 유형 배지, `/backtest`에 유형별 적중률이 표시됩니다.
> v0.2.6.0부터 주봉 차트 스크리너(`chart_screener.py`)가 추가되었습니다. KOSPI/KOSDAQ 전 종목을 Ichimoku + MA 6-조건으로 스크리닝하고 매주 일요일 20:30 KST에 텔레그램으로 발송합니다. `/screener` 명령어로 온디맨드 조회 가능.
> v0.2.8.0부터 교차분석에 MACD·볼린저밴드·MA20/50 레이어가 추가되었습니다. `PriceContext`에 5개 Optional 필드, 텔레그램 시세 라인에 기술적 지표 토큰 표시.
> v0.2.7.0부터 스크리너 v2: 120주선 조건 G 추가, KIND 섹터 데이터 연동, 섹터별 그룹 포맷 Telegram 출력.
> v0.2.9.0부터 펀더멘털 레이어가 추가되었습니다. Naver Finance 모바일 API에서 PER/PBR/EPS를 조회(인증 불필요)하여 `cross_analyze()` 스코어에 −3~+2 델타를 적용. 텔레그램 시세 라인에 PER/PBR 토큰 표시(주목할 만한 경우에만). 시작 시 `prewarm_fundamentals()`로 캐시 사전 적재. pykrx 불필요.
> v0.4.0.0부터 기사 유형 분류(8종), 주봉 차트 스크리너 v2(120주선 + KIND 섹터 그룹), 교차분석 v2(MACD·볼린저밴드·MA20/50), 펀더멘털 레이어를 통합하여 출시. 이전 v0.2.5.0~v0.2.9.0 브랜치 기능 전체가 이 버전으로 병합됨.
> v0.5.0.0부터 스크리너 우선 아키텍처가 적용되었습니다. 뉴스 신호는 해당 주 주봉 스크리너 통과 종목에만 텔레그램 알림이 발송됩니다. Stage 2 필터 프리셋(`screener_filters.py`), `daily_flow` DB 테이블이 추가되었습니다.
> v0.6.0.0부터 일봉 3단계 분류기(`stage_classifier.py`)가 추가되었습니다. 매일 16:30 KST에 KOSPI + KOSDAQ 전 종목을 Stage 1(랠리 초입) / Stage 2(중간 조정) / Stage 3(과열 재가속)으로 분류하고 `stage_classifications` 테이블에 저장합니다. 주봉 Ichimoku 결과와 교차 비교한 결과를 텔레그램으로 자동 발송합니다.
> v0.7.0.0부터 통합 백테스트 엔진(`backtest_engine.py`)이 추가되었습니다. 이치모쿠(주봉) / 3단계 Stage 1(일봉) / 교차(두 신호 동일 주 발동) 3개 모드로 과거 구간 백테스트를 실행합니다. 지표: 승률(7d/28d/91d), 평균·중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD. KRX 왕복 거래비용(기본 0.21%) 반영. `/backtest ichimoku 2025-01-01 2026-01-01` 텔레그램 명령어로 온디맨드 실행 가능.
> v0.7.1.0부터 데이터 인프라 레이어가 추가되었습니다. **KRX OpenAPI 클라이언트**(`krx_openapi.py`): `data-dbg.krx.co.kr` 공식 REST API로 종목 마스터·OHLCV·지수 시세 수집(Bearer 토큰). **OHLCV DB 캐시**(`ohlcv_cache.py`): psycopg2 기반 캐시 레이어, yfinance 반복 다운로드 감소. **수급 데이터 파이프라인**(`krx_flow_sync.py`): `data.krx.co.kr`에서 외국인·기관 순매수 이력을 `daily_flow` 테이블에 적재, 백테스트 조건 5(외국인·기관 순매수 > 0) 연결. 샤프비율 7d·91d 추가(`sharpe_7d`·`sharpe_91d`). 단위 테스트 총 65개.
> v0.8.0.0부터 **통합 백테스트 엔진 v2**가 추가되었습니다. `/backtest`에 `--tp1 / --tp1-ratio / --trail / --stop` 파라미터가 추가되었고, `OPTIMAL_EXIT_PARAMS` (KOSPI/KOSDAQ/Cross) 3개 상수가 그리드서치로 검증되어 코드에 고정됩니다. 1차 익절(TP1) + 트레일링 스탑 분할 청산 로직, blended_return(가중 수익률) 지표가 추가되었습니다.
> v0.9.0.0부터 **키움 모의투자 자동주문 시스템**(`kiwoom_paper_trader.py`)이 추가되었습니다. Stage/KOSDAQ/Cross/Ichimoku 4개 모델로 T+1 시가 매수주문을 키움 모의투자 서버(mockapi.kiwoom.com)에 제출하고 슬리피지를 측정합니다. `paper_positions` DB 테이블, 3개 스케줄 잡(09:05/16:10/16:40 KST), `/paper·/paper_perf·/paper_exit` 텔레그램 명령어 추가. `OPTIMAL_EXIT_PARAMS_ICHIMOKU` 그리드서치 검증 완료(val_sharpe 7.50, val_win_rate 55.8%). `/volume` 명령어가 `/top`(당일 거래금액 상위 10, KOSPI+KOSDAQ)으로 교체되었습니다. Supabase PgBouncer 호환(`statement_cache_size=0`), RLS 개별 적용(`_RLS_ALWAYS` / `_RLS_IF_EXISTS` DO 블록) 수정.
> v0.7.3.0부터 **거래대금 워치리스트 일보**(Layer 6)가 추가되었습니다. Stage 1 진입 종목을 14캘린더일 동안 매일 17:00 KST에 추적합니다. 거래대금 건강도(vol_ratio = 오늘거래량/진입거래량), 외국인·기관 스트릭, Ichimoku 주봉 통과 여부를 통합하여 Telegram 일보를 발송합니다. Stage 1→2 전환 시 즉시 알림, vol_ratio < 0.6 3거래일 연속 시 랠리 소멸 경고. `watchlist_vol_log` 테이블로 일별 vol_ratio 이력 유지. `python run_scheduler.py --once watchlist|stage`로 즉시 실행 가능.
> v0.9.3.0부터 **거래대금 기반 스테이지 분류**·**뉴스 게이팅 강화**·**Fuzzy 티커 해석**이 추가되었습니다. Stage 1/2/3 거래량 조건이 거래대금(`Volume × Close`)으로 전면 교체되어 소형주 과잉 선정을 방지합니다. 뉴스 게이팅은 Ichimoku 스크리너 OR 최근 7일 이내 Stage 활성 종목(이중 레이어)으로 확장되고, Ichimoku 교차 시 🔥 HIGH CONFIDENCE 배지를 부여합니다. `resolve_fuzzy(threshold=0.82)` 추가(`ticker_cache.py`). `/watchlist` 온디맨드 봇 명령어, vol_ratio 전일 대비 델타 표시, D+10 마지막 추적일 배지. Enhanced Ichimoku(전환선 > 기준선·둘 다 우상향)·조건 G NaN 보정 토글(`SCREENER_G_NAN_STRICT`)·일봉 분류기 티커 캡(`DAILY_CLASSIFIER_TICKERS`)도 포함.
> v0.9.4.0부터 **웹 대시보드 이력 트래킹**이 추가되었습니다. Report 탭에 날짜 범위 선택 바(오늘/-3일/-1주/-2주/-1달)가 추가되어 기간별 Stage·스크리너 등장 종목을 횟수 순으로 조회할 수 있습니다. 종목 클릭 시 Stage 일별 이력(분류일/스테이지/진입고가/피크아웃)·스크리너 주차 이력 팝업 표시. 이력 API 3개(`/api/history/stage`, `/api/history/screener`, `/api/history/ticker/{ticker}`) 추가. 모의투자 모델 카드 클릭으로 포지션 테이블 필터링.
> v0.9.7.0부터 **모의투자 성과분석**이 추가되었습니다. `GET /api/paper/curve`(모델별 누적 P&L 시계열·집계 통계·미실현 현재가), `GET /api/paper/export`(paper_positions CSV, utf-8-sig BOM) 2개 엔드포인트 추가. `PaperAnalytics.tsx` 컴포넌트(누적 P&L 커브 Recharts, 미실현 포지션 리더보드, CSV 다운로드)가 `PaperPortfolio` 하단에 임베드됩니다.
> v0.9.5.0부터 **웹 대시보드 히트맵 재설계**가 적용되었습니다. 데이터 소스를 Stage 분류 종목(10~50개)에서 Kiwoom 당일 거래대금 상위 50종목으로 교체. 셀 크기=당일 실제 거래대금, 등락률=Kiwoom 일중 change_pct. Stage 분류된 종목은 컬러 테두리(S1 파랑·S2 보라·S3 주황)로 오버레이. Stage 분류 잡 미실행 시에도 항상 50종목이 표시됨. Kiwoom 응답 실패 시 Stage 분류 데이터로 폴백.

---

## 1. 시스템 개요

### 1-1. 프로그램 목적

본 시스템은 국내외 금융 뉴스를 실시간으로 수집하고, 로컬 LLM으로 한글 요약 및 매매 신호를 추출하여 텔레그램으로 알려주는 **뉴스 기반 알림 도구**입니다.

영문 소스(Reuters·Bloomberg 등)와 한국어 소스(연합뉴스·한국경제·매일경제)를 모두 지원합니다.

실제 주문은 사용자가 직접 판단하여 실행합니다.

### 1-2. v2.x 설계서와의 차이

| 항목 | v2.x 설계서 (계획) | v3.0 실제 구현 |
|------|------------------|----------------|
| 신호 생성 방식 | 기술적 지표 (MA·RSI·볼린저) | 로컬 LLM 뉴스 분석 |
| 데이터 소스 | pykrx · Kiwoom · KIS | 국내외 금융 뉴스 RSS/크롤링 (영문 + 한국어) |
| 시세 조회 | pykrx (한국 주식) | yfinance (전 세계) |
| 전략 구조 | BaseStrategy 플러그인 | 없음 (LLM이 직접 판단) |
| 백테스트 | BacktestResult·신뢰도 역산 | 없음 |
| DB ORM | SQLAlchemy (SQLite→PG) | asyncpg 직접 (PostgreSQL) |
| 텔레그램 명령어 | /strategies·/backtest·/add_ticker | /status·/signals·/today·/help |
| 장 시간 필터 | MarketContext·스캔 차단 | 없음 (뉴스는 24시간 발생) |

### 1-3. 핵심 설계 원칙

| 원칙 | 설명 |
|------|------|
| 뉴스→신호 파이프라인 | 크롤링 → 한글 요약 → LLM 신호 감지 → 시세 교차분석 → 알림 |
| 로컬 LLM 우선 | Ollama → LM Studio 순서로 폴백. 외부 API 키 불필요 |
| 중복 방지 | url_hash(SHA-1 앞 16자)로 DB 중복 삽입 차단 |
| 단일 알림 채널 | 텔레그램이 유일한 출력 창구 |
| 신호는 참고용 | 자동 주문 없음. 투자 결정은 사용자 몫 |
| 신호 게이팅 | 한·외신 구분 없이 `signal.is_actionable`(strength ≥ 2)일 때만 전송 |

---

## 2. 데이터 흐름

```
📡  RSS 피드 수집 (run_scheduler.py)
    Reuters · CNBC · Yahoo Finance · Bloomberg (영문)
    연합뉴스 · 한국경제 · 매일경제 (한국어, v0.2.0.0~)
    → 새 기사 URL 감지 (url_hash로 중복 필터)
▼
📄  기사 본문 크롤링 (core/article_fetcher.py)
    JSON-LD → 소스별 파서 → fallback 파서 (최대 3,000자)
▼
🤖  로컬 LLM 한글 요약 (reports/summarizer.py)
    Ollama(Qwen3.5-9B) → LM Studio(Qwen3-8B) 폴백
    → 2~3문장 한글 요약 (핵심 수치·종목명 포함)
▼
🎯  LLM 매매 신호 감지 (analysis/signal_detector.py)
    → direction: BUY / SELL / WATCH / NONE
    → strength: 1~5
    → tickers: [{name, yfinance_symbol}]
▼
📊  시세 교차분석 (data/market_data.py)
    yfinance로 관련 종목 시세 조회 + Naver Finance 펀더멘털 조회 (PER/PBR/EPS)
    → CrossAnalysis: CONFIRM / CAUTION / FILTER / NEUTRAL
▼
💾  DB 저장 (core/db.py)
    news_articles + trade_signals (PostgreSQL · asyncpg)
▼
📲  텔레그램 알림 전송 (telegram/telegram_notify.py)
    유효 신호(is_actionable: direction ∈ {BUY,SELL,WATCH} AND strength ≥ 2)만 즉시 발송
    한·외신 동일 기준 적용 (v0.2.1.0~)
▼
👤  사용자 판단
    알림 확인 후 직접 증권사 앱에서 주문
```

---

## 3. 모듈 상세

### 3-1. `core/article_fetcher.py` — 기사 본문 크롤러

**역할**: 뉴스 URL → 본문 텍스트 추출

**파싱 우선순위**:
1. JSON-LD `articleBody` (SEO 스키마, 가장 신뢰도 높음)
2. 소스별 전용 파서 (CNBC · Investing.com · Reuters · Yahoo · MarketWatch · Bloomberg · 연합뉴스 · 한국경제 · 매일경제)
3. 범용 fallback (`<article>` → `<main>` → 전체 `<p>`)

**주요 동작**:
- 최대 본문 길이: 3,000자 (LLM 컨텍스트 고려)
- HTTP 429 대응: 최대 2회 재시도, 지수 백오프 (3s → 6s)
- 실패 시 빈 문자열 반환 (예외 미전파)

```python
body = await fetch_article_body(url, source="cnbc", http=http)
# → str (최대 3,000자, 실패 시 "")
```

---

### 3-2. `reports/summarizer.py` — 로컬 LLM 한글 요약

**역할**: 국내외 금융 기사 → 한글 2~3문장 요약

**LLM 폴백 체인**:
```
Ollama (localhost:11434, Qwen3.5-9B)
  → 실패 시 LM Studio (localhost:1234, Qwen3-8B)
    → 실패 시 요약 없이 저장 (Backend.FAILED)
```

**핵심 설계**:
- `_strip_thinking()`: Qwen3 `<think>...</think>` 추론 블록 제거
- `_call_ollama_native()`: Ollama 네이티브 `/api/chat` 사용, `enable_thinking=False`일 때 `/no_think\n\n` prefix 삽입 (Qwen3 사고 억제)
- `_lmstudio_is_alive()`: `/v1/models` 대신 경량 추론 프로브로 실제 응답 가능 여부 확인 (v0.2.1.0~)
- `summarize_batch()`: Semaphore(3) 병렬 처리 (로컬 LLM 부하 제한)

**반환값**:
```python
@dataclass
class SummaryResult:
    text: str        # 한글 요약 (실패 시 "")
    backend: Backend # OLLAMA | LM_STUDIO | FAILED
    success: bool
```

**시스템 프롬프트 (요약)**:
> 핵심 수치(%, 금액, EPS)와 종목명을 반드시 포함해서 2~3문장으로 간결하게 한글 요약. 순수 한글만 사용, 한자 혼용 금지.

---

### 3-3. `analysis/signal_detector.py` — LLM 매매 신호 감지

**역할**: 뉴스 제목 + 한글 요약 → 매매 신호 JSON 추출

**신호 구조**:

```python
@dataclass
class TradeSignal:
    direction: str          # BUY | SELL | WATCH | NONE
    strength: int           # 1~5 (1=약, 5=강)
    reason: str             # 판단 근거 (한글)
    tickers: list[str]      # 종목 표시명 (알림·로깅용)
    ticker_symbols: dict    # name → yfinance 심볼 (예: {"삼성전자": "005930.KS"})
    backend: Backend
    success: bool
    article_type: str = "other"  # earnings|ma|management|analyst|regulatory|product|macro|other

    @property
    def is_actionable(self) -> bool:
        return self.direction in ("BUY", "SELL", "WATCH") and self.strength >= 2
```

**신호 기준**:

| direction | 조건 |
|-----------|------|
| BUY | 금리 인하, 어닝 서프라이즈, 정책 지원, 지수 급등 |
| SELL | 금리 인상, 어닝 쇼크, 제재, 지수 급락 |
| WATCH | 지정학 리스크, FOMC 의사록, 불확실한 거시 지표 |
| NONE | 매매와 무관 (스포츠, 연예, 정치 등) |

**유효 신호 조건**: `is_actionable` — `direction in (BUY, SELL, WATCH)` AND `strength >= 2`

**LLM 프롬프트 출력 형식**:
```json
{
  "direction": "BUY",
  "strength": 4,
  "reason": "연준 금리 인하로 유동성 확대 기대",
  "tickers": [
    {"name": "S&P500", "symbol": "^GSPC"},
    {"name": "삼성전자", "symbol": "005930.KS"}
  ],
  "article_type": "macro"
}
```

**yfinance 심볼 규칙** (LLM에게 명시):
- 한국 주식: `종목코드.KS` (예: `005930.KS`)
- 한국 지수: `^KS11` (KOSPI), `^KQ11` (KOSDAQ)
- 미국 주식: 표준 티커 (NVDA, AAPL 등)
- 미국 지수: `^GSPC`, `^IXIC`, `^DJI`
- 원자재: `GC=F` (금), `CL=F` (유가), `SI=F` (은), `HG=F` (구리)

---

### 3-4. `data/market_data.py` — 시세 조회 + 교차분석

**역할**: 뉴스 신호 + 실시간 시세 → 교차분석 판정

**시세 조회 우선순위** (`get_price_context()` 5단계 캐스케이드):
1. LLM이 제공한 yfinance 심볼 직접 사용
2. pykrx 지수 매핑 (`PYKRX_INDEX_MAP`) — pykrx 사용 가능 시
3. `ticker_cache.resolve()` — KRX DB 전체 종목 캐시 (KOSPI+KOSDAQ ~2500종목, v0.3.0.0~)
   - 3가지 이름 변형 시도: raw → 소문자 strip → 공백 제거본
4. 내장 매핑 테이블 (`YFINANCE_MAP`) — 한국·미국 주식/지수/원자재 약 80개 매핑
5. 대문자 5자 이하 문자열이면 yfinance 직접 시도

**교차분석 판정**:

| 판정 | 조건 | 점수 보정 |
|------|------|-----------|
| CONFIRM | 뉴스 방향과 시세 방향 일치 | +최대 2점 |
| CAUTION | 일부 역방향 시세 | -conflict수 |
| FILTER | conflict 2개 이상 + 비율 75% 이상 | -conflict×2 |
| NEUTRAL | 보합 또는 시세 데이터 없음 | 변동 없음 |

**BUY 신호 교차분석 기준**:
- `change_pct >= 0.5%` → confirm +1
- `change_pct <= -2%` → conflict +1
- `RSI <= 30` (과매도) → confirm +1 (반등 기대)
- `macd_cross == "bullish_cross"` → confirm +2
- `macd_hist > 0` → confirm +1 / `macd_hist < 0` → conflict +1
- `bb_pct < 20` (과매도) → confirm +1 / `bb_pct > 80` (과매수) → conflict +1
- 두 MA 모두 위 → confirm +1 / 두 MA 모두 아래 → conflict +2

**펀더멘털 레이어 (v0.2.9.0~)**:

`cross_analyze()`는 각 종목에 대해 Naver Finance 모바일 API(`https://m.stock.naver.com/api/stock/{code}/integration`)로 PER/PBR/EPS를 조회합니다. 인증 불필요. `PriceContext`에 `per`, `pbr`, `eps` 필드 추가.

펀더멘털 스코어 델타 (종목별):

| 조건 | 델타 |
|------|------|
| EPS < 0 (적자) | −2 |
| PER > 50 (고평가) | −1 |
| PBR > 5 (고평가) | −1 |
| PER < 15 (저평가) | +1 |
| PBR < 1 (저평가) | +1 |

멀티 티커 신호는 개별 델타의 평균을 최종 스코어에 합산 (합산이 아닌 평균 — 한 종목이 전체를 지배하지 않도록).

캐시: `_fund_cache` (날짜 키) — 프로세스 재시작 시 초기화. 캐시 히트 ~0ms, 미스 ~150ms. `dict.setdefault()`로 스레드 안전.

시작 시 `prewarm_fundamentals()`가 `YFINANCE_MAP`의 한국 티커 전체를 5-worker 스레드 풀로 사전 적재.

텔레그램 표시: 주목할 만한 PER/PBR만 시세 라인에 토큰으로 표시 (`PER:12↓`, `PER:80↑`, `적자`, `PBR:0.6↓`, `PBR:7.0↑`). 평범한 종목은 토큰 없음.

**반환값**:
```python
@dataclass
class CrossAnalysis:
    verdict: str           # CONFIRM | CAUTION | FILTER | NEUTRAL
    score: int             # 0~10
    summary: str           # 한 줄 요약
    price_contexts: list[PriceContext]
```

---

### 3-5. `core/db.py` — PostgreSQL 연동

**DB 엔진**: asyncpg (비동기 커넥션 풀, min=2, max=8)

**테이블 구조**:

```sql
-- 뉴스 기사
CREATE TABLE news_articles (
    id           BIGSERIAL PRIMARY KEY,
    url_hash     CHAR(16) NOT NULL UNIQUE,  -- 중복 방지 키
    url          TEXT NOT NULL,
    source       VARCHAR(32) NOT NULL,       -- cnbc | reuters | investing | ...
    category     VARCHAR(32) NOT NULL,       -- markets | macro | korea
    title_en     TEXT NOT NULL,
    summary_en   TEXT,
    summary_ko   TEXT,
    llm_backend  VARCHAR(16),               -- ollama | lm_studio | failed | disabled
    published_at TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 매매 신호
CREATE TABLE trade_signals (
    id              BIGSERIAL PRIMARY KEY,
    article_id      BIGINT REFERENCES news_articles(id) ON DELETE CASCADE,
    direction       VARCHAR(8) NOT NULL,        -- BUY | SELL | WATCH
    strength        SMALLINT NOT NULL,          -- 1~5
    reason          TEXT,
    tickers         TEXT[],
    llm_backend     VARCHAR(16),
    macro_usd_krw   FLOAT,                      -- 신호 시점 USD/KRW (백테스팅용)
    macro_base_rate FLOAT,                      -- 신호 시점 한국 기준금리 (백테스팅용)
    article_type    VARCHAR(20) DEFAULT 'other',-- earnings|ma|management|analyst|regulatory|product|macro|other
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 주봉 차트 스크리닝 결과
CREATE TABLE chart_signals (
    id          BIGSERIAL PRIMARY KEY,
    ticker      VARCHAR(20) NOT NULL,
    name        TEXT,
    close       FLOAT NOT NULL,
    ma_20w      FLOAT NOT NULL,
    ma_60w      FLOAT NOT NULL,
    cloud_top   FLOAT NOT NULL,
    is_enhanced BOOLEAN DEFAULT FALSE,
    has_gapjum  BOOLEAN DEFAULT FALSE,
    week_of     VARCHAR(10) NOT NULL,        -- ISO 주차 (예: "2026-W16")
    screened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sector      VARCHAR(80) DEFAULT '',      -- KIND 업종명 (v0.2.7.0~)
    ma_120w     FLOAT,                       -- 120주선 값, NULL = 데이터 부족 (v0.2.7.0~)
    UNIQUE(ticker, week_of)
);

-- 일별 외국인·기관 순매수 + 스트릭 (v0.5.0.0~)
CREATE TABLE daily_flow (
    ticker          TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    foreign_net     BIGINT,                  -- 외국인 순매수(주), 양수=순매수
    inst_net        BIGINT,                  -- 기관 순매수(주)
    foreign_streak  SMALLINT,               -- 연속 순매수일(음수=순매도)
    inst_streak     SMALLINT,
    PRIMARY KEY (ticker, trade_date)
);

-- 일봉 3단계 분류 결과 (v0.6.0.0~)
CREATE TABLE stage_classifications (
    ticker          TEXT NOT NULL,
    classified_date DATE NOT NULL,
    stage           SMALLINT NOT NULL,       -- 1(랠리초입) | 2(중간조정) | 3(과열재가속)
    s1_entry_date   DATE,
    s1_high         NUMERIC,                 -- Stage 1 당일 고가
    s1_volume       BIGINT,                  -- Stage 1 당일 거래량 (vol_ratio 기준)
    peakout_flag    BOOLEAN DEFAULT false,
    PRIMARY KEY (ticker, classified_date)
);

-- 워치리스트 일별 거래대금 비율 이력 (v0.7.3.0~)
CREATE TABLE watchlist_vol_log (
    ticker      TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    vol_ratio   FLOAT,                       -- today_vol / s1_vol
    s1_vol      BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

-- KRX 전체 종목 디렉토리 (v0.3.0.0~)
CREATE TABLE krx_listings (
    isin_code       TEXT PRIMARY KEY,
    short_code      TEXT NOT NULL,           -- 6자리 종목코드
    name_ko         TEXT NOT NULL,           -- 정식 종목명 (예: 삼성전자)
    name_ko_abbr    TEXT,                    -- 단축명 (예: 삼성전자)
    name_en         TEXT,
    listed_at       DATE,
    market          TEXT,                    -- KOSPI | KOSDAQ
    security_type   TEXT,
    sector          TEXT,
    stock_type      TEXT,
    par_value       TEXT,
    listed_shares   BIGINT,
    yfinance_symbol TEXT NOT NULL,           -- 예: 005930.KS, 086520.KQ
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
-- 인덱스: name_ko, name_ko_abbr, short_code, updated_at

-- 시간외 단일가 스냅샷 (v0.7.x~, kiwoom/krx_aftermarket_sync.py)
CREATE TABLE aftermarket_snap (
    trade_date    DATE           NOT NULL,
    ticker        VARCHAR(12)    NOT NULL,   -- yfinance 심볼 (005930.KS 등)
    reg_close     NUMERIC(12,0),             -- 정규장 종가
    after_close   NUMERIC(12,0),             -- 시간외 체결가
    after_volume  BIGINT,
    after_value   BIGINT,
    after_chg_pct NUMERIC(6,2),
    fetched_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ticker)
);

-- 키움 모의투자 포지션 (v0.9.0.0~, kiwoom_paper_trader.py)
CREATE TABLE paper_positions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model           VARCHAR(20)  NOT NULL,   -- stage|kosdaq|cross|ichimoku
    ticker          VARCHAR(20)  NOT NULL,
    signal_date     DATE         NOT NULL,
    entry_theory    FLOAT        NOT NULL,   -- T+0 종가 (백테스트 가정 진입가)
    entry_actual    FLOAT,                   -- T+1 시가 (Kiwoom 실제 체결가)
    slippage_pct    FLOAT,                   -- (entry_actual - entry_theory) / entry_theory
    qty             INTEGER,
    kiwoom_buy_no   VARCHAR(20),
    kiwoom_sell_no  VARCHAR(20),
    tp1_pct         FLOAT        NOT NULL DEFAULT 0.15,
    tp1_ratio       FLOAT        NOT NULL DEFAULT 0.50,
    trail_pct       FLOAT        NOT NULL DEFAULT 0.10,
    hard_stop_pct   FLOAT        NOT NULL DEFAULT 0.10,
    tp1_date        DATE,
    tp1_price       FLOAT,
    watermark       FLOAT,                   -- 고점 추적 (트레일링 스탑용)
    exit_date       DATE,
    exit_price      FLOAT,
    exit_type       VARCHAR(20),             -- tp1|trailing|hard_stop|max_hold|manual
    blended_return  FLOAT,                   -- tp1_ratio×tp1_ret + (1-tp1_ratio)×final_ret
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending|open|closed
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `create_pool()` | asyncpg 커넥션 풀 생성 |
| `init_db()` | 테이블·인덱스 자동 생성 |
| `is_duplicate()` | url_hash 중복 체크 |
| `save_article()` | 기사 저장 (ON CONFLICT DO NOTHING) |
| `save_signal()` | 신호 저장 |
| `fetch_latest()` | 최신 기사 조회 (category·source 필터) |
| `fetch_latest_signals()` | 최신 신호 조회 (direction·strength 필터) |
| `load_seen_hashes()` | 재시작 시 중복 해시 복원 |
| `save_chart_signals()` | 스크리닝 결과 저장 (`chart_signals`, sector/ma_120w 포함, ON CONFLICT DO UPDATE) |
| `load_chart_signals_latest()` | 가장 최근 주차 스크리닝 결과 전체 조회 (sector/ma_120w 포함) |
| `save_daily_flow()` | daily_flow upsert (ticker, trade_date, foreign_net, inst_net, streak) |
| `get_prev_streak()` | 전일 foreign_streak·inst_streak 조회 (streak 누적 계산용) |
| `save_stage_classifications()` | stage_classifications 일괄 upsert |
| `get_stage1_history()` | Stage 1 이력 배치 조회 (stage_classifier Stage 2 판단용) |
| `get_stage1_watchlist()` | 최근 N일 이내 Stage 1 종목 1건씩 반환 (워치리스트 일보용, v0.7.3.0~) |
| `upsert_watchlist_vol_log()` | 일별 vol_ratio 이력 upsert (랠리 소멸 3거래일 판정용, v0.7.3.0~) |
| `get_watchlist_vol_log()` | 최근 N거래일 vol_ratio 조회 (LIMIT N ORDER BY trade_date DESC, v0.7.3.0~) |

**DSN 설정 우선순위**:
```
DATABASE_URL 환경변수 → DB_HOST/PORT/NAME/USER/PASSWORD 개별 변수
```

---

### 3-6. `telegram_bot.py` — 봇 명령어 처리

**방식**: Long polling (`getUpdates` 루프)

**지원 명령어**:

| 명령어 | 설명 |
|--------|------|
| `/status` | 업타임, 누적 수집 건수, 최근 24h 수집·신호 건수 |
| `/signals` | 최근 매매 신호 10건 (direction, strength, reason, 시각) |
| `/signals buy\|sell\|watch` | 방향별 신호 필터링 조회 |
| `/today` | 오늘 카테고리별 수집 건수 + 최신 기사 5건 한글 요약 |
| `/backtest <mode> <start> <end> [--tp1 F] [--trail F] [--stop F]` | 통합 백테스트 — ichimoku/stage/cross 모드, 분할 청산 파라미터 지원 |
| `/scan` | 주봉 스크리닝 즉시 실행 |
| `/screener` | 최신 주봉 차트 스크리닝 결과 — DM + 채널 동시 발송 |
| `/top` | 당일 거래금액 상위 10 종목 (KOSPI+KOSDAQ 합산, fdr.StockListing) (v0.9.0.0~) |
| `/paper` | 모의투자 현재 포지션 현황 (모델별 open·pending, 미실현 수익률) (v0.9.0.0~) |
| `/paper_perf` | 모의투자 누적 성과 — 실전 승률·평균 수익·슬리피지 (v0.9.0.0~) |
| `/paper_exit <종목코드>` | 모의투자 특정 종목 수동 강제 청산 (v0.9.0.0~) |
| `/help` | 명령어 목록 |

**알림 예시 (`/signals`)**:
```
🎯 최근 매매 신호 10건

🟢 BUY ⬛⬛⬛⬜⬜
   Federal Reserve cuts rates by 25bp
   💬 금리 인하로 증시 호재 예상
   🕐 03-29 10:32

🔴 SELL ⬛⬛⬛⬛⬜
   Samsung Q1 earnings miss estimates
   💬 실적 부진으로 단기 하락 압력
   🕐 03-29 09:15
```

---

### 3-7. `backtest.py` — 판정 정확도 추적 + 백테스팅

**역할**: `cross_analysis_results` 판정(CONFIRM/CAUTION/FILTER/NEUTRAL)이 실제 시세 움직임을 얼마나 맞혔는지 추적하고 리포트를 생성합니다.

**핵심 흐름**:
```
trade_signals (DB)
→ cross_analysis_results (DB)
→ yfinance 가격 조회 (1h / 4h / 1d / 3d 체크포인트)
→ price_outcomes 저장
→ calculate_metrics() — 판정별·체크포인트별 적중률 집계
→ backtest_report_telegram() — MarkdownV2 포맷 리포트
```

**적중률 정의**:

| direction | 적중 조건 |
|-----------|-----------|
| BUY | `return_pct > 0` |
| SELL | `return_pct < 0` |
| FILTER | `return_pct <= 0` (손실 방어 성공) |
| WATCH | `hit_rate = None` (방향성 없는 모니터링 신호) |

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `track_outcomes()` | 미결 신호의 가격 체크포인트를 채워넣음 (매 스케줄 실행) |
| `backfill_historical()` | 과거 신호에 대한 yfinance 가격 소급 채움 |
| `calculate_metrics()` | 판정별·checkpoint별 적중률 딕셔너리 반환 |
| `backtest_report_telegram()` | `/backtest` 명령어·주간 자동 발송용 MarkdownV2 텍스트 |
| `_fetch_type_breakdown()` | 기사 유형별(article_type) 1d 적중률 집계 (BUY/SELL만, WATCH 제외) |
| `_esc(s)` | MarkdownV2 특수문자 이스케이프 (`&` 포함) |

**자동 스케줄**: 매주 일요일 20:00 KST (`CronTrigger(day_of_week="sun", hour=20, minute=0)`)

---

### 3-8. `chart_screener.py` — 주봉 차트 스크리너

**역할**: KOSPI/KOSDAQ 전 종목(~2,770개)을 Ichimoku + MA 조건으로 스크리닝하여 기술적 돌파 후보를 필터링

**스크리닝 조건 (7가지, A~G 모두 충족)**:

| 조건 | 설명 |
|------|------|
| A | 이번 주 종가 > max(선행스팬A, 선행스팬B) — 구름 상향 돌파 |
| B | 직전 주 종가 ≤ 직전 주 구름 상단 — 이전 주 구름 내/하부 |
| C | 종가 > 20주 이동평균 |
| D | 종가 > 60주 이동평균 |
| E | 20주 이동평균 > 직전 주 20주 이동평균 (우상향) |
| F | 60주 이동평균 > 직전 주 60주 이동평균 (우상향) |
| G | 종가 > 120주 이동평균 (데이터 < 100봉 시 NaN-pass) |

**OHLCV 수집**: `period="3y"` (주봉 ~156봉, 진정한 120주선 계산을 위해 3년 필요)

**섹터 데이터**: `fetch_kind_sector_map()`이 KIND(한국거래소 기업공시시스템)에서 KOSPI/KOSDAQ 전 종목 업종(업종명)을 수집. EUC-KR HTML 파싱. 조회 실패 시 빈 dict 반환 — 스크리너는 계속 실행.

**추가 플래그**:
- `has_gapjum`: 20주MA > 60주MA (정배열) — 결과 상위 정렬
- `is_enhanced`: 향후 H/I 조건 확장 예약 필드

**주요 타입**:
```python
@dataclass
class ScreenResult:
    ticker: str
    name: str
    close: float
    ma_20w: float
    ma_60w: float
    cloud_top: float        # max(Span A, Span B)
    is_enhanced: bool
    has_gapjum: bool        # 20주MA > 60주MA
    screened_at: str
    week_of: str            # ISO 주차 (예: "2026-W16")
    sector: str = ""                  # KIND 업종명 (조회 실패 시 빈 문자열)
    ma_120w: Optional[float] = None  # 120주선 (데이터 부족 시 None)
```

**성능 특이사항**: yfinance 병렬 다운로드 시 데이터 오염 발생 — `SCREENER_WORKERS=1`(직렬)이 기본값. 전 종목 스캔 소요 시간 약 14분.

**자동 스케줄**: 매주 일요일 20:30 KST (`CronTrigger(day_of_week="sun", hour=20, minute=30)`)

---

### 3-9. `kiwoom_paper_trader.py` — 키움 모의투자 자동주문 (v0.9.0.0~)

**역할**: 키움 모의투자 서버(mockapi.kiwoom.com)에 실제 주문을 제출하고, 백테스트 진입가(T+0 종가)와 실전 진입가(T+1 시가 체결가) 사이의 슬리피지를 측정합니다.

**4개 모델 파라미터** (그리드서치 검증값):

| 모델 | 신호 소스 | 슬롯 | tp1 | tp1_ratio | trail | stop | val_sharpe |
|------|----------|------|-----|-----------|-------|------|-----------|
| `stage` | Stage 1 (KOSPI) | 10 | 25% | 50% | 10% | 10% | 4.70 |
| `kosdaq` | Stage 1 (KOSDAQ) | 10 | 25% | 50% | 15% | 10% | 5.48 |
| `cross` | Stage 1 ∩ Ichimoku | 5 | 15% | 50% | 10% | 10% | 5.11 |
| `ichimoku` | 주봉 Ichimoku 7조건 | 10 | 25% | **70%** | 10% | 10% | **7.50** |

**스케줄 잡 3개** (`run_scheduler.py`):
- `09:05 KST` — `open_entry`: pending → T+1 시가로 키움 모의투자 매수주문
- `16:10 KST` — `exit_checker`: open 포지션 EOD 가격 → 손절/익절/트레일 매도주문
- `16:40 KST` — `eod_sampler`: Stage1·Ichimoku·Cross 신호 샘플링 → pending 삽입

**exit_type 분류**: `hard_stop` → `tp1` 기록 → `trailing` → `max_hold`(91일) → `manual`

**슬리피지 측정 목표**: `-0.5% ~ +0.5%` 범위 내 유지 시 백테스트 엣지 유효 판정

---

### 3-10. `telegram_notify.py` — 신호 알림 전송

**역할**: 유효 신호 감지 시 즉시 텔레그램 전송. 주봉 스크리닝 결과는 DM + 채널 동시 발송 (v0.2.7.0~: 섹터별 그룹 포맷 — 상위 5개 섹터 × 섹터당 상위 3종목).

**v0.7.3.0 추가 함수 `send_watchlist_brief(entries, http)`**: 거래대금 워치리스트 일보 전송. plain text 모드(`parse_mode=None`). 확신도 순 정렬된 종목별 4줄 포맷 — 거래대금 비율(✅⚠️❌❓), 외국인/기관 스트릭(🔵🔴❓), Ichimoku 상태(☁️✅/❌/N/A). 워치리스트 없음 시 "워치리스트 없음" 발송.

**알림 메시지 형식**:
```
📈 [BUY] 연준 금리 인하 발표
─────────────────
방향: BUY  강도: ⬛⬛⬛⬛⬜ (4/5)
근거: 연준이 기준금리를 0.25%p 인하. 증시 강한 호재.
교차분석: ✅ CONFIRM (점수: 9/10)
  S&P500 +1.2% (상승)  NVDA +2.8% (급등)
출처: Reuters
시각: 2026-03-29 10:32 KST
⚠️ 참고용 알림입니다. 투자 결정은 직접 하세요.
```

---

## 4. 프로젝트 구조

v0.9.6.0부터 루트 평면 구조 → 기능별 패키지로 재편. v0.9.8.1부터 `jobs/` 패키지 추가.

```
test_feed/
│
├── run_scheduler.py               # 메인 진입점 — RSS 루프 + 봇 병렬 실행, --once 즉시 실행
│                                  # 892줄 (v0.9.8.1~), 잡 로직은 jobs/ 에 위임
│
├── jobs/                          # 스케줄러 잡 패키지 (v0.9.8.1~)
│   ├── stage_job.py               # daily_stage_job() — OHLCV 수집 + stage_classifications 저장
│   ├── screener_job.py            # weekly_screener_job() — 전종목 Ichimoku 스캔 + HTML 리포트
│   ├── infra_jobs.py              # daily_krx_refresh_job() + daily_flow_sync_job()
│   ├── watchlist_job.py           # watchlist_brief_job() + build_watchlist_entries() 헬퍼
│   └── paper_jobs.py              # paper_exit_checker_job() + paper_eod_sampler_job()
│                                  # + paper_open_entry_job()
│
├── core/                          # 공유 유틸리티 (v0.9.6.0~)
│   ├── db.py                      # asyncpg 커넥션 풀, 테이블 init, 모든 DB 헬퍼 함수
│   ├── ticker_cache.py            # 종목명→yfinance 심볼 인메모리 캐시 + resolve_fuzzy()
│   ├── ohlcv_cache.py             # OHLCV DB 캐시 레이어 (psycopg2, daily_ohlcv 테이블)
│   └── article_fetcher.py         # 뉴스 기사 본문 크롤링 (소스별 파서 + fallback)
│
├── data/                          # 데이터 수집·동기화 (v0.9.6.0~)
│   ├── market_data.py             # yfinance 시세 조회 + 교차분석 + Naver Finance 펀더멘털
│   ├── krx_sync.py                # KRX 전체 종목 DB 동기화 (KOSPI+KOSDAQ ~2500종목)
│   ├── krx_openapi.py             # KRX Open API REST 클라이언트 — OHLCV·종목마스터·지수
│   ├── krx_flow_sync.py           # 외국인·기관 순매수 파이프라인 → daily_flow 테이블
│   ├── krx_aftermarket_sync.py    # KRX 시간외 단일가 → aftermarket_snap 적재
│   ├── kiwoom_aftermarket_sync.py # Kiwoom REST API 시간외 단일가 → aftermarket_snap 적재
│   └── kiwoom_paper_trader.py     # 키움 모의투자 자동주문 — 4모델, paper_positions 테이블
│
├── analysis/                      # 분석·전략 (v0.9.6.0~)
│   ├── signal_detector.py         # LLM 매매 신호 감지 (JSON 구조화 출력)
│   ├── chart_screener.py          # 주봉 Ichimoku+MA 스크리너 (KOSPI/KOSDAQ 전종목, 7조건)
│   ├── screener_filters.py        # Stage 2 필터 프리셋
│   ├── stage_classifier.py        # 일봉 3단계 분류기 — Stage 1/2/3 + 피크아웃 신호
│   ├── backtest_engine.py         # 통합 백테스트 엔진 — ichimoku/stage/cross/stage2 4모드
│   ├── volume_pattern.py          # 거래량 패턴 분석
│   └── macro_tracker.py           # OLS 팩터 모델 — 6개 매크로 팩터 추적
│
├── telegram/                      # 텔레그램 연동 (v0.9.6.0~)
│   ├── telegram_bot.py            # 봇 명령어 처리 (long polling, /status /signals 등)
│   ├── telegram_notify.py         # 신호 알림 전송 + 워치리스트 일보 + 스크리너 결과 포맷
│   └── telegram_trade.py          # 매매 기록 명령어 처리 (/buy /sell /port /pnl)
│
├── reports/                       # 리포트·요약 (v0.9.6.0~)
│   ├── summarizer.py              # 로컬 LLM 한글 요약 (Ollama → LM Studio 폴백)
│   └── generate_html_report.py    # 주봉 스크리닝 HTML 리포트 생성 (섹터별·정배열별)
│
├── dashboard/                     # 웹 대시보드 (FastAPI + React)
│   ├── backend/
│   │   ├── main.py                # FastAPI 앱 — BasicAuth 미들웨어, 전체 API 엔드포인트
│   │   └── database.py            # 대시보드 전용 DB 헬퍼
│   └── frontend/
│       ├── src/                   # React + TypeScript 소스
│       │   └── components/        # Heatmap, Report, Top, PaperPortfolio, PaperAnalytics, Macro 등
│       └── dist/                  # Vite 빌드 산출물 (백엔드가 정적 서빙)
│
├── tests/                         # pytest 테스트 (596개, v0.9.8.1 기준)
│   ├── conftest.py
│   ├── test_article_type.py       # 기사 유형 분류
│   ├── test_backtest_engine.py    # 통합 백테스트 엔진 (ichimoku/stage/cross/stage2)
│   ├── test_chart_screener.py     # 스크리너 조건 A~G + KIND 섹터
│   ├── test_dashboard_top.py      # /top API 거래대금 상위 10
│   ├── test_db_dsn.py             # DB DSN 설정
│   ├── test_exit_model.py         # TP1·트레일링·하드스탑 분할 청산 로직
│   ├── test_generate_html_report.py # HTML 리포트 생성
│   ├── test_high_confidence.py    # HIGH CONFIDENCE 배지 + 게이팅
│   ├── test_krx_flow_sync.py      # 외국인·기관 수급 파이프라인
│   ├── test_krx_sync.py           # KRX 동기화 + 티커 캐시
│   ├── test_macro_signal.py       # MACD/BB/MA 교차분석 스코어링
│   ├── test_news_gating.py        # 뉴스 게이팅 — 스크리너 OR Stage 7d 이중 레이어
│   ├── test_p3_remaining.py       # P3 백로그 항목 통합
│   ├── test_paper_analytics.py    # 모의투자 성과분석 API + 프론트 pivotSeries
│   ├── test_replay_stage2.py      # Stage 2 walk-forward 백테스트 재현 (25 tests)
│   ├── test_resolution_diagnostics.py # 티커 해석 미스 카운터
│   ├── test_resolve_fuzzy.py      # fuzzy 티커 매칭 (threshold=0.82)
│   ├── test_scan_cmd.py           # /scan 명령어 즉시 스캔
│   ├── test_screener_cmd.py       # /screener 명령어 회귀
│   ├── test_screener_cmd_regression_1.py # asyncio.run __main__ 가드 회귀
│   ├── test_screener_filters.py   # Stage 2 필터 프리셋
│   ├── test_screener_ohlcv_regression.py # OHLCV 캐시 회귀
│   ├── test_screener_telegram_regression_1.py # 스크리너 텔레그램 포맷 회귀
│   ├── test_screener_telegram_regression_2.py
│   ├── test_signal_prompt.py      # 신호 감지 프롬프트 · WATCH 임계값 회귀
│   ├── test_stage_classifier.py   # 일봉 3단계 분류기
│   ├── test_summarizer_regression_1.py # LLM 헬스체크·Qwen3 thinking 회귀
│   ├── test_telegram_routing.py   # 텔레그램 신호 라우팅 회귀
│   ├── test_trade_integration.py  # trade_log GENERATED COLUMN DB 검증
│   ├── test_trade_journal.py      # /buy /sell /port /pnl 거래 저널
│   ├── test_volume_integration.py # _send_plain, fetch_data 시간대 회귀
│   ├── test_watchlist_brief.py    # 워치리스트 일보 포맷 + DB 헬퍼
│   └── test_watchlist_features.py # vol_ratio_delta, retiring, Stage2 알림
│
├── scripts/                       # 운영 스크립트
│   ├── register_tasks.ps1         # Windows 작업 스케줄러 통합 등록 (-Task all|crawler|aftermarket|dashboard)
│   ├── restart_scheduler.bat      # NewsCrawler 재시작
│   ├── restart_dashboard.bat      # 대시보드 서버 재시작
│   ├── start_dashboard_hidden.vbs # 창 없는 백그라운드 대시보드 시작
│   ├── start_dashboard_service.bat # 대시보드 서비스 래퍼
│   ├── start_crawler.bat          # NewsCrawler 배치 실행
│   ├── run_aftermarket_sync.bat   # 장후 동기화 실행
│   ├── duckdns_update.bat         # DuckDNS IP 업데이트
│   └── run_sweep.py               # 백테스트 파라미터 그리드서치 (288 조합)
│
├── docs/                          # 문서
│   ├── ARCHITECTURE.md            # 본 아키텍처 문서
│   ├── TODOS.md                   # 미결 작업 목록
│   ├── USER_MANUAL.md             # 설치부터 첫 알림까지 전체 가이드
│   ├── Dashboard.md               # 웹 대시보드 개발·배포 가이드
│   ├── DESIGN.md                  # 디자인 토큰 시스템 가이드
│   ├── HTTPS-Setup.md             # Caddy HTTPS 설정 (Let's Encrypt + DuckDNS)
│   ├── HowToBacktest.md           # 백테스트 엔진 사용 가이드
│   ├── howto-screener.md          # 주봉 Ichimoku 스크리너 설정·Calibration
│   ├── howto-stage-classifier.md  # 일봉 3단계 분류기 설정
│   ├── howto-watchlist.md         # 거래대금 워치리스트 온디맨드 조회
│   ├── explanation-signal-pipeline.md # 신호 파이프라인·게이팅·HIGH CONFIDENCE 설계
│   ├── reference-env-vars.md      # 환경변수 전체 목록
│   ├── reference-telegram-commands.md # Telegram 명령어 전체 목록
│   └── krx openapi specs/         # KRX OpenAPI 스펙 문서 (본드·파생·주식·ETF 등)
│
├── sql/
│   ├── pgadmin_queries.sql        # DB 관리 쿼리
│   └── rls_policies.sql           # RLS 정책 마이그레이션 (14 테이블 backend_all)
│
├── logs/                          # 로그 파일 (로테이션 포함)
│
├── VERSION                        # 현재 버전 (SemVer 4자리, 예: 0.9.8.1)
├── CHANGELOG.md                   # 버전별 변경 이력
├── README.md                      # 프로젝트 개요 + 빠른 시작
├── CLAUDE.md                      # Claude Code 프로젝트 지침
├── .env.example                   # 환경변수 템플릿
├── requirements.txt               # Python 의존성
└── pytest.ini                     # testpaths = tests
```

---

## 5. 기술 스택

| 레이어 | 기술 | 역할 |
|--------|------|------|
| 언어 | Python 3.11+ | 전체 시스템 |
| 뉴스 수집 | httpx (비동기) | RSS 피드 + 기사 본문 크롤링 |
| HTML 파싱 | BeautifulSoup4 | 소스별 파서 + fallback |
| LLM 추론 | Ollama (로컬) | Qwen3.5-9B — 요약·신호 감지 |
| LLM fallback | LM Studio (로컬) | Qwen3-8B — Ollama 미실행 시 |
| 시세 조회 | yfinance | 한국·미국·지수·원자재 전 세계 |
| KRX 데이터 | KRX Open API (REST) | 종목마스터·OHLCV·지수 시세 (Bearer 토큰) |
| DB (비동기) | asyncpg + PostgreSQL | 기사·신호 저장, 비동기 커넥션 풀 |
| DB (동기) | psycopg2 + PostgreSQL | OHLCV 캐시·수급 데이터 (백테스트 동기 경로) |
| 알림 | httpx (Telegram Bot API) | MarkdownV2 포맷 메시지 |
| 환경변수 | python-dotenv | API 키·DB 정보 관리 |
| 로깅 | Python logging | 수집·요약·신호·에러 로그 |

---

## 6. 환경 설정

### 6-1. 필수 환경변수 (`.env`)

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

# KRX OpenAPI — OHLCV·종목마스터·지수 (openapi.krx.co.kr 가입 후 발급)
KRX_OPENAPI_KEY=your_krx_openapi_key

# KRX 포털 계정 — 수급 데이터 (data.krx.co.kr, krx_flow_sync.py)
KRX_ID=your_krx_id
KRX_PW=your_krx_password

# Kiwoom REST API — 실전 (시간외 단일가 수집)
KIWOOM_APP_KEY=your_kiwoom_app_key
KIWOOM_APP_SECRET=your_kiwoom_app_secret

# Kiwoom 모의투자 — paper trading (KIWOOM_MOCK_APPKEY 설정 시 자동 활성화)
KIWOOM_MOCK_APPKEY=your_mock_appkey
KIWOOM_MOCK_APPSECRET=your_mock_appsecret
```

### 6-2. 로컬 LLM 실행

```bash
# Ollama (1순위)
ollama serve
ollama pull qwen2.5:7b   # 또는 Qwen3.5-9B

# LM Studio (2순위, fallback)
# → GUI에서 Qwen3-8B 모델 로드 후 Local Server 탭 → Start Server
```

---

## 7. 에러 처리 전략

| 레벨 | 상황 | 처리 |
|------|------|------|
| DEBUG | 티커 매핑 실패, yfinance 조회 실패 | 조용히 건너뜀 |
| WARNING | 기사 본문 크롤링 실패, LLM 폴백 | 로그 기록 + 해당 기사 스킵 |
| WARNING | 429 Rate Limit | 지수 백오프 후 재시도 (최대 2회) |
| INFO | LLM 폴백 성공 | 백엔드 전환 기록 |
| ERROR | DB 저장 실패 | 로그 기록 + 해당 기사 스킵 |
| WARNING | 텔레그램 전송 실패 | 로그 기록 (재시도 없음) |

---

## 8. 뉴스 소스 및 카테고리

### 8-1. 지원 소스

| 소스 | 파서 | 비고 |
|------|------|------|
| CNBC | `_parse_cnbc()` | `div.ArticleBody-articleBody` |
| Investing.com | `_parse_investing()` | `div.WYSIWYG.articlePage` |
| Reuters | `_parse_reuters()` | Google News 우회 URL은 실패 多 |
| Yahoo Finance | `_parse_yahoo()` | `div.caas-body` |
| MarketWatch | `_parse_marketwatch()` | `div.article__body` |
| Bloomberg | `_parse_bloomberg()` | `div.body-content` (페이월 제한) |
| 연합뉴스 | `_parse_yonhap()` | 한국어 본문 직접 수집 |
| 한국경제 | `_parse_hankyung()` | 한국어 본문 직접 수집 |
| 매일경제 | `_parse_mk()` | 한국어 본문 직접 수집 |

### 8-2. 카테고리

| 카테고리 | 내용 |
|----------|------|
| `markets` | 주식·지수·시황 뉴스 |
| `macro` | 금리·경제지표·중앙은행 |
| `korea` | 한국 주식·기업 뉴스 (연합뉴스·한국경제·매일경제 포함) |

---

## 9. 보안 체크리스트

| 항목 | 상태 |
|------|------|
| `.env`를 `.gitignore`에 추가 | ✅ |
| API 키 환경변수로만 관리 | ✅ |
| DB 비밀번호 로그에 미노출 (host/db만 출력) | ✅ |
| Telegram Token 환경변수로만 관리 | ✅ |
| 신호는 참고용임을 메시지에 항상 명시 | ✅ |
| 실거래 자동 주문 기능 없음 | ✅ |
| Supabase RLS 활성화 — 전 테이블 PostgREST anon 노출 차단 | ✅ (v0.9.0.0~) |
| RLS `backend_all` 정책 — 14개 테이블 명시적 allow-all 정책, Security Advisor 경고 해소 | ✅ (v0.9.7.0~) |
| asyncpg `statement_cache_size=0` — Supabase PgBouncer 호환 | ✅ (v0.9.0.0~) |
| 모의투자는 키움 가상 계좌 전용 (실자산 영향 없음) | ✅ |
| 대시보드 localhost 인증 우회 차단 — `client.host` 기반 면제 로직 제거 (Nginx 뒤 모든 요청이 127.0.0.1로 보여 사실상 인증 전면 무력화됐던 취약점 수정, v0.9.8.x) | ✅ |

---

## 10. 향후 개선 방향

| 항목 | 설명 |
|------|------|
| ~~뉴스 소스 확장~~ | ✅ v0.2.9.0 — Naver Finance 모바일 API로 PER/PBR/EPS 펀더멘털 조회 구현 |
| ~~화이트리스트 인증~~ | ✅ v0.1.0.0 — `ALLOWED_CHAT_IDS` 환경변수로 구현 완료 |
| ~~신호 이력 조회~~ | ✅ v0.1.0.0 — `/signals buy\|sell\|watch` 방향 필터 구현 완료 |
| 교차분석 강화 | 거래량 급증, 52주 고/저가 근접 조건 추가 |
| 백테스트 기준선 | 판정 없는 방향별 시장 기준 적중률 추가 (TODOS.md P3 참고) |
| ~~APScheduler 영속성~~ | ✅ v0.2.2.0 — `SQLAlchemyJobStore` Postgres 기반 영속성 구현 완료 |
| ~~백테스트 기준선~~ | ✅ v0.2.2.0 — `backtest_report_telegram()`에 랜덤 기준선 추가 완료 |
| 알림 재시도 | 텔레그램 전송 실패 시 지수 백오프 재시도 |
| Docker 배포 | `docker-compose.yml` 기반 컨테이너화 |
| 모델 교체 용이성 | `OLLAMA_MODEL`·`LM_STUDIO_MODEL` 환경변수화 |

---

*현재 코드베이스 v0.9.8.1 (2026-05-24) 기준*

---

## v0.9.3.0 신규 기능 (2026-05-20)

### 1. Fuzzy 티커 해석 (resolve_fuzzy)

`ticker_cache.py`에 `resolve_fuzzy(name, threshold=0.82)` 메서드 추가.  
LLM이 "셀트리온헬스케어"로 추출하고 KRX DB에는 "셀트리온헬스케어(주)"로 등록된 경우를 자동 매칭합니다.

해석 우선순위: 정확 매칭 → fuzzy 매칭 → `_resolution_misses` 카운터 기록.  
임계값 0.82: "현대차" vs "현대차증권" (ratio ≈ 0.75) false positive를 차단합니다.

### 2. HIGH CONFIDENCE 통합

`TradeSignal`에 `confidence: str = "NORMAL"` 필드 추가.  
해당 주 Ichimoku 스크리너 통과 종목과 교차하는 뉴스 신호는 `"HIGH"`로 상향되어 🔥 배지와 함께 발송됩니다.

### 3. 워치리스트 3가지 개선

- `/watchlist` 봇 커맨드: `_build_watchlist_entries(pool)` 헬퍼로 데이터 로직 분리, 온디맨드 조회 가능
- 전일 대비 거래대금 비율 변화 (vol_ratio_delta): `+5%▲` / `-8%▼` 표시
- D+10 마지막 추적일 표시: `[마지막 추적일]` 배지

### 4. Ichimoku Enhanced 조건 실제 적용

`calc_ichimoku()`에 `tenkan_sen`(전환선, 9주)·`kijun_sen`(기준선, 26주) 컬럼 추가.  
`screen_ticker()`에서 H(전환선 > 기준선)·I(둘 다 우상향) 판정 → `is_enhanced` 실제 설정.  
기존에는 항상 `False`였으나 이제 실제 Enhanced 종목이 배지를 받습니다.

### 5. 조건 G NaN 보정 (SCREENER_G_NAN_STRICT)

`SCREENER_G_NAN_STRICT=1` 환경변수로 120주선 데이터 부족 종목의 통과 여부를 제어할 수 있습니다.  
기본(미설정): NaN → 통과. Strict 모드: NaN → 실패.  
DB에서 `null_pct > 20%` 확인 후 활성화 권장.

### 6. 일봉 분류기 티커 캡 (DAILY_CLASSIFIER_TICKERS)

`DAILY_CLASSIFIER_TICKERS=150` (기본값) 환경변수로 일봉 분류기의 최대 처리 종목 수를 제어합니다.  
Ichimoku 통과 종목은 캡 초과 여부와 관계없이 항상 포함됩니다.

### 7. 뉴스 게이팅 강화 (이중 레이어)

기존 단일 스크리너 게이팅에 `_active_stage_tickers` (최근 7일 이내 Stage 활성 종목) 레이어 추가.  
`get_active_stage_tickers(pool, days=7)` DB 함수, `_daily_stage_job()` 완료 후 자동 캐시 갱신.

| 종목 상태 | 게이팅 결과 |
|----------|------------|
| 스크리너 교차 | HIGH CONFIDENCE + 전달 |
| Stage 7d 활성 (스크리너 미통과) | NORMAL + 전달 |
| 둘 다 해당 없음 | 억제 |
| 게이팅 캐시 비어있음 | 전달 (초기 실행 방어) |
