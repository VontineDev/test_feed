# 한국 주식 뉴스 기반 매매 신호 알림 시스템
## 아키텍처 문서 v4.0

> **v2.x → v3.0 전면 재작성 안내**
> 기존 설계서(v2.1)는 기술적 지표(MA·RSI·볼린저) 기반 시스템을 기술했으나,
> 실제 구현된 코드는 **영문 금융 뉴스 크롤링 + 로컬 LLM 신호 감지** 방식입니다.
> v3.0은 현재 코드베이스를 기준으로 전면 재작성한 문서입니다.

> **2026-06-21 버전 체계 통합 안내**: 아래 블록쿼트 changelog는 v0.9.9.6 항목에서 동결되었습니다. `v1.0.0.0` 이후의 변경 이력과 향후 모든 변경 사항은 프로젝트 루트의 `CHANGELOG.md`를 단일 소스로 참고하세요. 이 문서(`ARCHITECTURE.md`)는 이후 정적인 아키텍처 설명 용도로만 유지됩니다.

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
> v0.9.9.x부터 **대시보드 10명 동시 접속 안정화**가 적용되었습니다. `_bg_refresh()` stale-while-revalidate 패턴으로 캐시 만료 시 Thundering Herd 해소(RISK-04). `_EXT_EXECUTOR(max_workers=4)` + `_ext_thread()` 래퍼로 yfinance·Kiwoom·KRX 외부 API를 전용 풀에서 타임아웃과 함께 실행(RISK-05). `_warmup_caches()` lifespan 사전 로딩으로 cold start 지연 해소(RISK-07). `_HISTORY_MAX_DAYS=365` 이력 쿼리 범위 제한으로 단일 사용자 DB 독점 차단(RISK-08). `GET /health` 엔드포인트 신설 — 업타임·DB 풀·캐시 TTL·SSE 연결 수 반환(RISK-09).
> v0.9.9.6부터 **3단계 역할 기반 접근 제어 + 포트폴리오 탭**이 추가되었습니다. `SPECIAL_USER`/`SPECIAL_PASSWORD` 환경변수로 admin·special·user 3단계 인증 체계 구성. `GET /api/auth/me` — 현재 역할 반환. `GET /api/portfolio` — admin·special 전용, 키움 REST API(`kt00018`·`kt00001`)로 실계좌 총자산·종목별 보유현황 반환(5분 stale-while-revalidate). React `Portfolio.tsx` 컴포넌트 신설. `useRole()` 훅 + `getVisibleTabs(role)` 함수로 탭 목록을 역할에 따라 동적 렌더링.
> v0.9.9.5부터 **OpenDART 전자공시 통합**이 추가되었습니다. `data/dart_sync.py` — Top 20 기업 공시 이벤트 수집·XBRL 재무수치·사업보고서 세그먼트 Ollama 파싱. DB 테이블 4종(`dart_companies`·`dart_disclosures`·`dart_xbrl`·`dart_segments`). 스케줄러 잡 3종(일별 공시·월별 XBRL·연간 세그먼트). `data/dart_download.py` — 보고서 원문 로컬 다운로더. 공시 갭 자동 백필(최대 90일).
> v0.10.0.0부터 **YouTube 내러티브 스크리닝 파이프라인**이 추가되었습니다. `data/youtube_narrative_sync.py` — 삼프로TV(채널 ID: `UChlv4GSd7OQl3js-jkLOnFA`) 자막을 YouTube Data API v3로 수집하고 Gemini 2.5 Flash로 종목 언급을 JSON 구조화 추출. DB 테이블 3종(`youtube_mention_raw`·`youtube_attention_scores`·`youtube_mention_forward_returns`). 스케줄러 잡 3종(09:05 수집·09:35 attention_score 집계·15:40 forward return 채우기 KST 기준). `data/youtube_ticker_aliases.json` — 100개 한국어 약칭 → KRX 코드 수동 매핑. `scripts/youtube_backtest.py` — Spearman IC·t-stat·종목 히트율 블라인드 백테스트. 블라인드 백테스트 프로토콜: `git tag backtest-v1-blind` 후 `--backfill` 실행으로 방법론 동결 보장. attention_score는 soft feature(LEFT JOIN)로 기존 시스템에 영향 없음.
> v0.10.0.0부터 **DART XML Ollama 추출기**(`data/dart_extractor.py`)가 추가되었습니다. `reports/dart/`의 로컬 XML 파일을 keyword grep + 헤더 앵커 2-트랙으로 최대 20,000자 컨텍스트 추출 후 Ollama(`qwen3.5:9b`)로 투자 판단 내러티브 생성. 결과를 `dart_extractions` 테이블에 저장. `scripts/export_dart_md.py` — `dart_extractions` DB → `dart/{날짜}_{기업명}_{기간}_{보고서유형}.md` 내보내기.
> v0.10.0.1부터 **모의투자 exit checker 가격 소스 수정** — `paper_exit_checker_job`의 현재가 조회를 `KiwoomPaperTrader.get_current_price()`(mockapi.kiwoom.com ka10001 — 시장 데이터 미지원)에서 **yfinance 배치 조회**(`_fetch_prices_yf()`)로 교체. 대시보드와 동일 소스 통일. 수정 전: 매일 전 포지션이 "현재가 조회 실패"로 스킵 → 손절·익절 무발동.
> **(v1.0.0.0 ~ v1.0.4.1 항목은 2026-06-21부로 `CHANGELOG.md`(`[0.10.1.12]`~`[0.10.1.17]`)로 이전되었습니다 — 이 블록쿼트 changelog는 v0.9.9.6에서 동결되며, 이후 변경 이력은 `CHANGELOG.md`를 단일 소스로 참고하세요.)**

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
    article_type: str = "other"      # earnings|ma|management|analyst|regulatory|product|macro|other
    confidence: str = "NORMAL"       # "NORMAL" | "HIGH" — Ichimoku 교차 시 HIGH, 🔥 배지 (v0.9.3.0~)

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

**HIGH CONFIDENCE 배지 (v0.9.3.0~)**: 해당 주 Ichimoku 스크리너 통과 종목(`_screener_tickers`)과 교차하는 뉴스 신호는 `confidence="HIGH"`로 상향. 텔레그램 메시지에 🔥 배지 부여.

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
3. `ticker_cache.resolve()` — KRX DB 전체 종목 캐시 (KOSPI+KOSDAQ ~2500종목)
   - 정확 매칭 → `resolve_fuzzy(threshold=0.82)` fuzzy 매칭 → miss 카운터
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

멀티 티커 신호는 개별 델타의 평균을 최종 스코어에 합산 (한 종목이 전체를 지배하지 않도록).

캐시: `_fund_cache` (날짜 키) — 프로세스 재시작 시 초기화. 캐시 히트 ~0ms, 미스 ~150ms.

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
    url_hash     CHAR(16) NOT NULL UNIQUE,
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
    macro_usd_krw   FLOAT,
    macro_base_rate FLOAT,
    article_type    VARCHAR(20) DEFAULT 'other',
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
    is_enhanced BOOLEAN DEFAULT FALSE,      -- 전환선>기준선 AND 둘 다 우상향 (v0.9.3.0~)
    has_gapjum  BOOLEAN DEFAULT FALSE,      -- 20주MA > 60주MA 정배열
    week_of     VARCHAR(10) NOT NULL,       -- ISO 주차 (예: "2026-W16")
    screened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sector      VARCHAR(80) DEFAULT '',
    ma_120w     FLOAT,                      -- NULL = 데이터 부족 (100봉 미만)
    UNIQUE(ticker, week_of)
);

-- 일별 외국인·기관 순매수 + 스트릭
CREATE TABLE daily_flow (
    ticker          TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    foreign_net     BIGINT,
    inst_net        BIGINT,
    foreign_streak  SMALLINT,               -- 연속 순매수일 (음수=순매도)
    inst_streak     SMALLINT,
    PRIMARY KEY (ticker, trade_date)
);

-- 일봉 3단계 분류 결과
CREATE TABLE stage_classifications (
    ticker          TEXT NOT NULL,
    classified_date DATE NOT NULL,
    stage           SMALLINT NOT NULL,      -- 1(랠리초입) | 2(중간조정) | 3(과열재가속)
    s1_entry_date   DATE,
    s1_high         NUMERIC,
    s1_volume       BIGINT,
    peakout_flag    BOOLEAN DEFAULT false,
    PRIMARY KEY (ticker, classified_date)
);

-- 워치리스트 일별 거래대금 비율 이력
CREATE TABLE watchlist_vol_log (
    ticker      TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    vol_ratio   FLOAT,                      -- today_txamt / s1_txamt
    s1_txamt    BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

-- KRX 전체 종목 디렉토리
CREATE TABLE krx_listings (
    isin_code       TEXT PRIMARY KEY,
    short_code      TEXT NOT NULL,
    name_ko         TEXT NOT NULL,
    name_ko_abbr    TEXT,
    name_en         TEXT,
    listed_at       DATE,
    market          TEXT,                   -- KOSPI | KOSDAQ
    security_type   TEXT,
    sector          TEXT,
    stock_type      TEXT,
    par_value       TEXT,
    listed_shares   BIGINT,
    yfinance_symbol TEXT NOT NULL,          -- 예: 005930.KS, 086520.KQ
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 시간외 단일가 스냅샷
CREATE TABLE aftermarket_snap (
    trade_date    DATE           NOT NULL,
    ticker        VARCHAR(12)    NOT NULL,
    reg_close     NUMERIC(12,0),
    after_close   NUMERIC(12,0),
    after_volume  BIGINT,
    after_value   BIGINT,
    after_chg_pct NUMERIC(6,2),
    fetched_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ticker)
);

-- YouTube 종목 언급 원시 데이터
CREATE TABLE youtube_mention_raw (
    id                BIGSERIAL PRIMARY KEY,
    video_id          TEXT         NOT NULL,
    video_date        DATE         NOT NULL,
    speaker           TEXT,
    stock_name_raw    TEXT         NOT NULL,
    ticker            VARCHAR(12),
    direction         TEXT,                      -- buy | neutral | sell
    horizon           TEXT,
    rationale_summary TEXT,
    source_quote      TEXT         NOT NULL,
    created_at        TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (video_id, stock_name_raw, source_quote)
);

-- YouTube attention_score (5영업일 rolling)
CREATE TABLE youtube_attention_scores (
    ticker            VARCHAR(12)  NOT NULL,
    window_end        DATE         NOT NULL,
    mention_count     INT,
    sentiment_weighted NUMERIC(10,3),
    attention_score   NUMERIC(10,4),
    distinct_videos   INT,
    PRIMARY KEY (ticker, window_end)
);

-- YouTube forward return (백테스트용)
CREATE TABLE youtube_mention_forward_returns (
    mention_id  BIGINT  PRIMARY KEY REFERENCES youtube_mention_raw(id) ON DELETE CASCADE,
    ret_1d      NUMERIC(9,4),
    ret_5d      NUMERIC(9,4),
    ret_20d     NUMERIC(9,4),
    filled_at   DATE
);

-- DART XML Ollama 추출 결과
CREATE TABLE dart_extractions (
    id              BIGSERIAL PRIMARY KEY,
    corp_name       TEXT         NOT NULL,
    rcept_no        VARCHAR(20)  NOT NULL,
    report_type     TEXT,
    period          TEXT,
    extraction_text TEXT,
    model           TEXT,
    xml_chars       INT,
    extracted_at    TIMESTAMPTZ,
    UNIQUE (corp_name, rcept_no)
);

-- 키움 모의투자 포지션
CREATE TABLE paper_positions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model           VARCHAR(20)  NOT NULL,  -- stage|kosdaq|cross|ichimoku
    ticker          VARCHAR(20)  NOT NULL,
    signal_date     DATE         NOT NULL,
    entry_theory    FLOAT        NOT NULL,  -- T+0 종가 (백테스트 가정 진입가)
    entry_actual    FLOAT,                  -- T+1 시가 (Kiwoom 실제 체결가)
    slippage_pct    FLOAT,
    qty             INTEGER,
    kiwoom_buy_no   VARCHAR(20),
    kiwoom_sell_no  VARCHAR(20),
    tp1_pct         FLOAT        NOT NULL DEFAULT 0.15,
    tp1_ratio       FLOAT        NOT NULL DEFAULT 0.50,
    trail_pct       FLOAT        NOT NULL DEFAULT 0.10,
    hard_stop_pct   FLOAT        NOT NULL DEFAULT 0.10,
    tp1_date        DATE,
    tp1_price       FLOAT,
    watermark       FLOAT,                  -- 고점 추적 (트레일링 스탑용)
    exit_date       DATE,
    exit_price      FLOAT,
    exit_type       VARCHAR(20),            -- hard_stop|tp1|trailing|max_hold|manual
    blended_return  FLOAT,                  -- tp1_ratio×tp1_ret + (1-tp1_ratio)×final_ret
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending|open|closed
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `create_pool()` | asyncpg 커넥션 풀 생성 |
| `init_db()` | 테이블·인덱스 자동 생성 |
| `save_article()` | 기사 저장 (ON CONFLICT DO NOTHING) |
| `save_signal()` | 신호 저장 |
| `load_seen_hashes()` | 재시작 시 중복 해시 복원 |
| `save_chart_signals()` | 스크리닝 결과 저장 (ON CONFLICT DO UPDATE) |
| `load_chart_signals_latest()` | 가장 최근 주차 스크리닝 결과 전체 조회 |
| `save_daily_flow()` | daily_flow upsert |
| `get_prev_streak()` | 전일 streak 조회 (누적 계산용) |
| `save_stage_classifications()` | stage_classifications 일괄 upsert |
| `get_stage1_history()` | Stage 1 이력 배치 조회 (Stage 2 판단용) |
| `get_active_stage_tickers()` | 최근 N일 이내 Stage 활성 종목 집합 (게이팅용, v0.9.3.0~) |
| `get_stage1_watchlist()` | 최근 N일 이내 Stage 1 종목 반환 (워치리스트 일보용) |
| `upsert_watchlist_vol_log()` | 일별 vol_ratio 이력 upsert |
| `get_watchlist_vol_log()` | 최근 N거래일 vol_ratio 조회 |

**DSN 설정 우선순위**:
```
DATABASE_URL 환경변수 → DB_HOST/PORT/NAME/USER/PASSWORD 개별 변수
```

---

### 3-6. `telegram/telegram_bot.py` — 봇 명령어 처리

**방식**: Long polling (`getUpdates` 루프)

**지원 명령어**:

| 명령어 | 설명 |
|--------|------|
| `/status` | 업타임, 누적 수집 건수, 최근 24h 수집·신호 건수 |
| `/signals` | 최근 매매 신호 10건 (direction, strength, reason, 시각) |
| `/signals buy\|sell\|watch` | 방향별 신호 필터링 조회 |
| `/today` | 오늘 카테고리별 수집 건수 + 최신 기사 5건 한글 요약 |
| `/backtest <mode> <start> <end> [--tp1 F] [--trail F] [--stop F]` | 통합 백테스트 — ichimoku/stage/cross/stage2 모드 |
| `/scan` | 주봉 스크리닝 즉시 실행 (전 종목 실시간, ~14분) |
| `/screener` | 최신 주봉 차트 스크리닝 결과 — DM + 채널 동시 발송 |
| `/watchlist` | 거래대금 워치리스트 즉시 조회 (온디맨드, v0.9.3.0~) |
| `/top` | 당일 거래금액 상위 10 종목 (KOSPI+KOSDAQ 합산) |
| `/paper` | 모의투자 현재 포지션 현황 (모델별 open·pending, 미실현 수익률) |
| `/paper_perf` | 모의투자 누적 성과 — 실전 승률·평균 수익·슬리피지 |
| `/paper_exit <종목코드>` | 모의투자 특정 종목 수동 강제 청산 |
| `/buy <코드> <가격> <수량>` | 진입 기록 (trade_log) |
| `/sell <코드> <가격>` | 청산 기록 FIFO |
| `/port` | 보유 현황 + 미실현 P&L |
| `/pnl [week\|month\|all]` | 실현 P&L 요약 |
| `/help` | 명령어 목록 |

---

### 3-7. `analysis/backtest_engine.py` — 통합 백테스트 엔진 (v0.7.0.0~)

**역할**: Ichimoku·Stage·Cross·Stage2 4개 모드로 과거 구간 walk-forward 백테스트를 실행합니다.

**모드별 신호 소스**:

| 모드 | 신호 조건 | 주요 사용처 |
|------|----------|-----------|
| `ichimoku` | 주봉 Ichimoku 7조건 (A~G) | 주봉 스크리너 성과 검증 |
| `stage` | 일봉 Stage 1 5조건 (KOSPI/KOSDAQ 분리) | 일봉 분류기 성과 검증 |
| `cross` | Ichimoku × Stage 1 동일 ISO 주 교차 | 교차 신호 성과 검증 |
| `stage2` | Stage 1 후 14일 이내 Stage 2 재진입 | Stage 2 신호 walk-forward |

**출력 지표**: 승률(7d/28d/91d), 평균/중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD

**비용**: 왕복 0.210% (매수 0.014% + 매도 0.014% + 증권거래세 0.180% + 농특세 0.002%)

**그리드서치 검증 최적 청산 파라미터**:

| 상수 | 대상 모델 | val_sharpe |
|------|---------|-----------|
| `OPTIMAL_EXIT_PARAMS` | stage (KOSPI) | 4.70 |
| `OPTIMAL_EXIT_PARAMS_KOSDAQ` | stage (KOSDAQ) | 5.48 |
| `OPTIMAL_EXIT_PARAMS_CROSS` | cross | 5.11 |
| `OPTIMAL_EXIT_PARAMS_ICHIMOKU` | ichimoku | 7.50 |

**`BacktestConfig` 주요 필드**:
```python
@dataclass
class BacktestConfig:
    mode: str           # ichimoku | stage | cross | stage2
    start_date: date
    end_date: date
    market: str = "ALL" # KOSPI | KOSDAQ | ALL
    tp1_pct: float = 0.25
    tp1_ratio: float = 0.50
    trail_pct: float = 0.10
    hard_stop_pct: float = 0.10
    hold_weeks: int = 13         # 최대 보유 기간 (주)
    cost: float = TX_COST_DEFAULT
```

**자동 스케줄**: 매주 일요일 20:00 KST — `jobs/screener_job.py`에서 호출 후 결과를 텔레그램 발송.

---

### 3-8. `analysis/chart_screener.py` — 주봉 차트 스크리너

**역할**: KOSPI/KOSDAQ 전 종목(~2,770개)을 Ichimoku + MA 조건으로 스크리닝하여 기술적 돌파 후보를 필터링

**스크리닝 조건 (7가지 A~G, 모두 충족)**:

| 조건 | 설명 |
|------|------|
| A | 이번 주 종가 > max(선행스팬A, 선행스팬B) — 구름 상향 돌파 |
| B | 직전 주 종가 ≤ 직전 주 구름 상단 — 이전 주 구름 내/하부 |
| C | 종가 > 20주 이동평균 |
| D | 종가 > 60주 이동평균 |
| E | 20주 이동평균 > 직전 주 20주 이동평균 (우상향) |
| F | 60주 이동평균 > 직전 주 60주 이동평균 (우상향) |
| G | 종가 > 120주 이동평균 (데이터 < 100봉 시 NaN-pass, `SCREENER_G_NAN_STRICT=1`로 강화) |

**Enhanced 조건 (v0.9.3.0~, `is_enhanced` 실제 설정)**:
- H: 전환선(9주) > 기준선(26주)
- I: 전환선·기준선 둘 다 우상향

**OHLCV 수집**: `period="3y"` (주봉 ~156봉, 120주선 계산을 위해 3년 필요)

**섹터 데이터**: `fetch_kind_sector_map()`이 KIND(한국거래소 기업공시시스템)에서 업종명을 수집. EUC-KR HTML 파싱.

**주요 타입**:
```python
@dataclass
class ScreenResult:
    ticker: str
    name: str
    close: float
    ma_20w: float
    ma_60w: float
    cloud_top: float        # max(선행스팬A, 선행스팬B)
    is_enhanced: bool       # H+I 조건 충족 여부 (v0.9.3.0~부터 실제 설정)
    has_gapjum: bool        # 20주MA > 60주MA 정배열
    screened_at: str
    week_of: str            # ISO 주차 (예: "2026-W16")
    sector: str = ""
    ma_120w: Optional[float] = None
```

**성능**: yfinance 병렬 다운로드 시 데이터 오염 발생 — `SCREENER_WORKERS=1`(직렬) 기본값. 전 종목 스캔 소요 시간 약 14분.

**자동 스케줄**: 매주 일요일 20:30 KST — `jobs/screener_job.py`에서 실행.

---

### 3-9. `analysis/stage_classifier.py` — 일봉 3단계 분류기 (v0.6.0.0~)

**역할**: KOSPI+KOSDAQ 전 종목(~2,770개)을 매일 16:30 KST에 Stage 1/2/3으로 분류합니다. Ichimoku 주봉 스크리너와 완전히 독립된 시스템.

**3단계 정의**:

| Stage | 의미 | 핵심 조건 |
|-------|------|----------|
| 1 | 랠리 초입 | 당일 상승률 ≥ 5%(KOSPI) / 7%(KOSDAQ) + 거래대금 급증 + 외국인/기관 순매수 |
| 2 | 중간 조정·재매집 | Stage 1 후 14일 이내, 고가 −5%~−20% 눌림, 거래대금 30~60% 감소 |
| 3 | 과열 재가속 | Stage 1 후 고점 갱신, RSI ≥ 70, 거래대금 급증 재확인 |

**우선순위**: Stage 3 > Stage 2 > Stage 1 (하나의 종목에 복수 조건 충족 시 높은 Stage 반환)

**거래대금 기준 (v0.9.3.0~)**: 거래량(`Volume`) 대신 `Volume × Close`(거래대금) 기준으로 전환. 소형주 과잉 선정 방지. `compare_tx_amt.py` 검증: MAE 1.38%, Max 3.55%.

**고점 이탈 탐지 (`check_peakout()`)**: Stage 3 진입 후 외국인·기관 연속 순매도 streak ≤ −2 AND 윗꼬리 캔들 패턴으로 고점 이탈 조기 경보.

**핵심 설계**:
- ThreadPoolExecutor 내부에서 asyncpg 직접 호출 금지 — price_df, flow_df, s1_history 모두 진입 전에 배치 로드하여 전달 (learnings: asyncpg-threadpool-no-db)
- `DAILY_CLASSIFIER_TICKERS` 환경변수로 최대 처리 종목 수 제어 (기본 150), Ichimoku 통과 종목은 캡 초과 시에도 항상 포함

**주요 함수**:

| 함수 | 반환 | 설명 |
|------|------|------|
| `classify_stage(ticker, price_df, flow_df, s1_history, market)` | `int \| None` | 3단계 분류 (1/2/3/None) |
| `check_peakout(ticker, flow_df, price_df)` | `bool` | Stage 3 고점 이탈 신호 |

**자동 스케줄**: 매일 16:30 KST — `jobs/stage_job.py`에서 실행, 결과를 `stage_classifications` 테이블에 저장.

---

### 3-10. `data/kiwoom_paper_trader.py` — 키움 모의투자 자동주문 (v0.9.0.0~)

**역할**: 키움 모의투자 서버(mockapi.kiwoom.com)에 실제 주문을 제출하고, 백테스트 진입가(T+0 종가)와 실전 진입가(T+1 시가 체결가) 사이의 슬리피지를 측정합니다.

**7개 모델 파라미터** (그리드서치/backtest 검증값, `MODEL_CONFIG`):

| 모델 | 신호 소스 | 슬롯 | 포지션 | tp1 | tp1_ratio | trail | stop | val_sharpe |
|------|----------|------|--------|-----|-----------|-------|------|-----------|
| `stage` | Stage 1 (KOSPI) | 10 | 1,000만원 | 25% | 50% | 10% | 10% | 4.70 |
| `kosdaq` | Stage 1 (KOSDAQ) | 10 | 1,000만원 | 25% | 50% | 15% | 10% | 5.48 |
| `cross` | Stage 1 ∩ Ichimoku | 5 | 2,000만원 | 15% | 50% | 10% | 10% | 5.11 |
| `ichimoku` | 주봉 Ichimoku 7조건 | 10 | 1,000만원 | 25% | **70%** | 10% | 10% | **7.50** |
| `compose-funnel1` | 주간 FUNNEL-1 | 10 | 1,000만원 | 15% | 50% | 10% | 10% | 0.74* |
| `compose-and1` | 주간 AND-1 | 5 | 2,000만원 | 15% | 50% | 10% | 10% | 1.75* |
| `compose-score1` | 주간 SCORE-1 | 5 | 2,000만원 | 15% | 50% | 10% | 10% | 1.17* |

`*` compose 3개 모델은 train/val 분리 없는 backtest sharpe (val_sharpe 아님).

**스케줄 잡 4개** (`jobs/paper_jobs.py` + `jobs/compose_paper_job.py`):
- `09:05 KST` (평일) — `paper_open_entry_job`: pending → T+1 시가로 키움 모의투자 매수주문
- `15:20 KST` (평일, 정규장 마감 직전) — `paper_exit_checker_job`: 오픈 포지션 Kiwoom 현재가 체크 → 손절/익절/트레일 매도주문
- `16:40 KST` (평일) — `paper_eod_sampler_job`: Stage1·Ichimoku·Cross 신호 샘플링 → pending 삽입
- `21:15 KST` (일요일) — `compose_paper_entry_job`: FUNNEL-1/AND-1/SCORE-1 주간 신호 → pending 삽입 (DB만 필요, Kiwoom 계정 불필요)

**exit_type 분류**: `hard_stop` → `tp1` 기록 → `trail` → `period_end`(91일) → `manual`

**가격 소스 (v1.0.4.1~)**: exit checker가 정규장 중(15:20 KST)으로 옮겨지면서 1분봉 지연이 있는 yfinance 대신 Kiwoom mock API(`get_current_price()`, 종목당 0.5초 딜레이)로 되돌림 — 주문 실행(place_sell)과 동일 서버. 대시보드 `/api/positions`·텔레그램 `/paper`는 표시 전용이라 yfinance를 그대로 사용 (자세한 내력은 `explanation-paper-trading.md` 참고).

**슬리피지 측정 목표**: `-0.5% ~ +0.5%` 범위 내 유지 시 백테스트 엣지 유효 판정

---

### 3-11. `analysis/macro_tracker.py` — OLS 팩터 모델 (v0.9.6.0~)

**역할**: 6개 매크로 팩터와 개별 종목 수익률의 선형 관계를 OLS 회귀로 추정하여 현재 매크로 환경이 해당 종목에 유리한지 불리한지 점수로 출력합니다.

**6개 팩터** (모두 yfinance 일별 무료 수집):

| 팩터 | 심볼 | 해석 |
|------|------|------|
| rate | `^TNX` | 미국 10년 국채금리 — 글로벌 유동성·밸류에이션 기준 |
| fx | `KRW=X` | USD/KRW 환율 (상승 = 원화 약세) |
| oil | `BZ=F` | 브렌트유 선물 |
| vix | `^VIX` | 공포지수 |
| dxy | `DX-Y.NYB` | 달러인덱스 |
| export | `EWY` | 미장 한국 투자심리 ETF (iShares MSCI Korea) — 외국인 수급 대리변수 |

**모델 수식**:
```
r_주식[t] = α + β_rate·Δrate + β_fx·Δfx + β_oil·Δoil + β_vix·Δvix + β_dxy·Δdxy + β_export·Δexport + ε
```
- `rate`: 절대 변화(%p). 나머지: 로그 수익률(×100).

**매크로 영향 점수 (Macro Score, −100~+100)**: 최근 5일 팩터 변화 × 베타 합산 → tanh 정규화.

**대시보드 연동**: `GET /api/macro` 엔드포인트가 `MacroTracker.run_analysis()`를 비동기로 호출, 결과를 React Macro 탭에 표시.

**CLI 사용법**:
```bash
python analysis/macro_tracker.py                             # 기본 KOSPI 대형주 전체 분석
python analysis/macro_tracker.py --tickers 005930.KS        # 삼성전자만 상세 분석
python analysis/macro_tracker.py --scenario rate+0.5        # 금리 +0.5%p 시나리오
python analysis/macro_tracker.py --snapshot-only            # 매크로 현황 스냅샷만 출력
```

---

### 3-12. `telegram/telegram_notify.py` — 신호 알림 전송

**역할**: 유효 신호 감지 시 즉시 텔레그램 전송. 주봉 스크리닝 결과는 DM + 채널 동시 발송.

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `send_signal_message(signal, cross, http)` | 뉴스 신호 알림 (HIGH CONFIDENCE 시 🔥 배지) |
| `send_weekly_screener(results, http)` | 주봉 스크리닝 결과 — 섹터별 그룹 포맷 (상위 5섹터 × 섹터당 상위 3종목) |
| `send_watchlist_brief(entries, http)` | 거래대금 워치리스트 일보 — plain text, 확신도 순 정렬 |
| `_post_message(http, token, chat_id, text, label, parse_mode)` | Telegram Bot API 래퍼 |

**`send_watchlist_brief` 포맷**: 종목별 4줄 — 거래대금 비율(✅⚠️❌❓), 전일 대비 delta(`+5%▲`/`-8%▼`), 외국인/기관 스트릭, Ichimoku 상태. D+10 마지막 추적일 표시. Stage 1→2 전환 시·랠리 소멸(3거래일 연속 vol_ratio < 0.6) 시 별도 단독 알림 발송.

**알림 메시지 형식**:
```
[BUY] 연준 금리 인하 발표
방향: BUY  강도: 4/5
근거: 연준이 기준금리를 0.25%p 인하. 증시 강한 호재.
교차분석: CONFIRM (점수: 9/10)
  S&P500 +1.2%  NVDA +2.8%
출처: Reuters | 2026-03-29 10:32 KST
참고용 알림입니다. 투자 결정은 직접 하세요.
```

---

### 3-13. `jobs/` — 스케줄러 잡 패키지 (v0.9.8.1~)

**역할**: `run_scheduler.py`에서 위임받은 잡 로직을 기능별 모듈로 분리합니다. `run_scheduler.py`는 얇은 래퍼로 외부 import 호환성을 유지합니다.

| 모듈 | 잡 함수 | 스케줄 |
|------|---------|--------|
| `stage_job.py` | `daily_stage_job(db_pool)` | 평일 16:30 KST |
| `screener_job.py` | `weekly_screener_job(db_pool)` | 일요일 20:30 KST |
| `infra_jobs.py` | `daily_krx_refresh_job(db_pool)` | 평일 20:00 KST |
| `infra_jobs.py` | `daily_aftermarket_sync_job()` | 평일 16:05 KST |
| `infra_jobs.py` | `daily_flow_sync_job()` | 평일 18:00 KST |
| `infra_jobs.py` | `daily_market_snap_job()` | 평일 16:10 KST |
| `infra_jobs.py` | `daily_dart_disclosure_job(db_pool)` | 평일 09:00 KST |
| `infra_jobs.py` | `monthly_dart_xbrl_job(db_pool)` | 매월 1일 09:10 KST |
| `infra_jobs.py` | `youtube_narrative_sync_job()` | 평일 09:05 KST |
| `infra_jobs.py` | `youtube_attention_score_job()` | 평일 09:35 KST |
| `infra_jobs.py` | `youtube_forward_return_job()` | 평일 15:40 KST |
| `watchlist_job.py` | `watchlist_brief_job(db_pool)` | 평일 17:00 KST |
| `watchlist_job.py` | `build_watchlist_entries(pool)` | on-demand (`/watchlist` 봇 커맨드) |
| `paper_jobs.py` | `paper_open_entry_job(db_pool, trader)` | 평일 09:05 KST |
| `paper_jobs.py` | `paper_exit_checker_job(db_pool, trader)` | 평일 15:20 KST |
| `paper_jobs.py` | `paper_eod_sampler_job(db_pool, trader)` | 평일 16:40 KST |
| `compose_paper_job.py` | `compose_paper_entry_job(dsn, pool)` | 일요일 21:15 KST |

**설계 원칙**: 각 잡 함수는 `db_pool`/`trader` 등 의존성을 인자로 받아 전역 상태 없이 동작. 전역 캐시(`_screener_tickers`, `_active_stage_tickers`)를 갱신하는 잡은 새 값을 반환 — 호출자(`run_scheduler.py`)가 전역에 대입.

---

### 3-14. `data/dart_sync.py` — OpenDART 전자공시 통합 (v0.9.9.5~)

**역할**: 금융감독원 전자공시시스템(DART) API를 통해 Top 20 기업의 공시 이벤트·XBRL 재무수치·사업보고서 세그먼트를 수집합니다.

**DartClient** (`httpx.AsyncClient` 래퍼):

| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `fetch_corp_codes()` | `/api/corpCode.xml` | 전체 기업고유번호 ZIP 다운로드 → 파싱 |
| `fetch_disclosures()` | `/api/list.json` | 기간·기업·공시유형 필터 공시 목록 조회 |
| `fetch_xbrl()` | `/api/fnlttSinglAcntAll.json` | 단일기업 XBRL 재무수치 (연결/별도) |
| `fetch_document_zip()` | `/api/document.xml` | 사업보고서 원문 ZIP 바이너리 |

**핵심 함수**:
- `sync_disclosures(pool, corp_codes, bgn_de, end_de)` — 공시 목록 → `dart_disclosures` 테이블 upsert. `dart_companies` 자동 시드(FK 레이스컨디션 방지).
- `sync_xbrl(pool, corp_codes)` — 전년도 XBRL 재무수치 → `dart_xbrl` 테이블 upsert.
- `sync_segments(pool, corp_codes, bsns_year)` — 사업보고서 원문 ZIP 다운로드 → `_extract_section_text()`로 II-2/II-4 절 추출 → Ollama 파싱 → `dart_segments` 저장.
- `get_top20_corp_codes(pool)` — `dart_companies.stock_code`로 Top 20 corp_code 조회.

**세그먼트 추출 (`_extract_section_text`)**:
- DART4 XML에서 정규식으로 섹션 헤딩 위치 탐색
- 정지 키워드(다음 섹션 헤딩) 탐색 오프셋 30자 — TOC 내 헤딩도 감지
- 헤딩이 행 시작(`\n` 직전)에 있는지 검사 → 본문 내 인용 구분
- 결과 < 400자이면 TOC 항목으로 판정하여 스킵
- 최대 6,000자 반환 (Ollama 컨텍스트 한도 고려)

**DB 테이블**:

| 테이블 | 주키 | 설명 |
|--------|------|------|
| `dart_companies` | `corp_code (VARCHAR 8)` | DART 기업 마스터 + KRX 종목코드 매핑 |
| `dart_disclosures` | `rcept_no (VARCHAR 14)` | 공시 이벤트 (실적발표·유상증자 등) |
| `dart_xbrl` | `(corp_code, bsns_year, reprt_code, account_nm, fs_div)` | XBRL 재무수치 |
| `dart_segments` | `(corp_code, bsns_year, section)` | Ollama 파싱 세그먼트 결과 |

**환경변수**: `DART_API_KEY` (dart.fss.or.kr 개발자센터에서 발급)

---

### 3-15. `data/dart_download.py` — DART 보고서 로컬 다운로더 (v0.9.9.5~)

**역할**: Top 20 기업의 사업보고서·반기보고서·분기보고서 원문 ZIP을 로컬 디렉터리에 구조화하여 저장합니다. `dart_sync.py` 파이프라인과 독립으로 실행됩니다.

**저장 구조**:
```
reports/dart/
  {기업명}/
    {rcept_no}_{보고서명}/      ← 압축 해제된 XML 파일들
      BIZ_YYYYMMDD_XXXXXX.xml
      ...
    {rcept_no}_meta.json        ← 보고서 메타데이터 (corp_code, rcept_dt, files, size_bytes)
```

**CLI 사용법**:
```bash
python data/dart_download.py                              # 2026년 Top 20 전체
python data/dart_download.py --year 2025                  # 연도 지정
python data/dart_download.py --year 2026 --corp 005930    # 단일 종목코드
python data/dart_download.py --year 2026 --type 사업보고서 # 보고서 종류 필터
python data/dart_download.py --year 2026 --dry-run        # 목록만 확인
python data/dart_download.py --year 2026 --zip            # ZIP 파일 그대로 저장
```

**주요 동작**:
- EUC-KR 파일명 자동 변환 (`cp437` → `cp949` 재인코딩)
- 이미 존재하는 보고서 자동 스킵 (target_dir + meta_path 모두 존재 시)
- API 부하 방지: 보고서 간 1.5초 대기
- Windows cp949 터미널 대응: `sys.stdout.reconfigure(encoding="utf-8")`

---

### 3-16. `data/youtube_narrative_sync.py` — YouTube 내러티브 수집 파이프라인 (v0.10.0.0~)

**역할**: 삼프로TV 유튜브 영상 자막에서 종목 언급을 수집·추출·집계합니다.

**파이프라인**:
```
YouTube Data API v3 (삼프로TV 채널 영상 목록)
    ↓ youtube-transcript-api (한국어 자막)
    ↓ Ollama 로컬 LLM, 기본 qwen3.5:9b (종목 언급 JSON 구조화)
    ↓ youtube_mention_raw  ← 원시 언급
    ↓ youtube_attention_scores  ← 5영업일 rolling attention_score
    ↓ youtube_mention_forward_returns  ← 1d/5d/20d 수익률
```

**attention_score 계산**:
```
attention_score = SUM(sentiment_weight) / distinct_videos
  buy=1.0, neutral=0.5, sell=0.0
```

**CLI**:
```bash
python data/youtube_narrative_sync.py                             # 전일 수집
python data/youtube_narrative_sync.py --backfill --from 2026-01-01  # 소급 수집
python data/youtube_narrative_sync.py --fill-returns              # forward return만
```

**한국어 약칭 매핑**: `youtube_ticker_aliases.json` — "삼전" → "005930.KS" 등 100개 항목.

**블라인드 백테스트 보장**: `git tag backtest-v1-blind` → `--backfill` 순서로 방법론 확정 후 데이터 채우기. 방법론 역산 방지.

**환경변수**: `YOUTUBE_API_KEY` (필수), `OLLAMA_BASE`/`OLLAMA_MODEL` (선택, 기본 `http://localhost:11434` / `qwen3.5:9b`). `GEMINI_API_KEY`는 더 이상 사용하지 않음 (v0.10.0.0 출시 당시엔 Gemini 2.5 Flash 사용, 2026-06-16 Ollama로 마이그레이션됨 — `CHANGELOG.md [0.10.1.12]` 참고).

---

### 3-17. `data/dart_extractor.py` — DART XML Ollama 추출기 (v0.10.0.0~)

**역할**: `reports/dart/` 하위 로컬 DART XML 파일을 Ollama로 처리하여 투자 판단 내러티브를 `dart_extractions` 테이블에 저장합니다.

**2-트랙 컨텍스트 추출**:

| 트랙 | 방식 | 예산 |
|------|------|------|
| anchor_xml | 재무표 헤더 앵커 탐색 → 이하 50행 수집 | 8,000자 |
| grep_xml | KEYWORDS 포함 행 추출 | 나머지 (합산 20,000자 상한) |

**KEYWORDS**: `AI`, `인공지능`, `데이터센터`, `냉각`, `칠러`, `로봇`, `매출`, `영업이익` 등 14개

**ANCHORS**: `부문별 매출실적`, `사업부문별 매출`, `세그먼트별` 등 12개 재무표 헤더

**Ollama 호출**: `qwen3.5:9b` (기본), `OLLAMA_MODEL` 환경변수로 변경 가능. 결과 최대 20,000자 컨텍스트 전달.

**CLI**:
```bash
python data/dart_extractor.py --company LG전자   # 단일 기업 (콘솔 출력)
python data/dart_extractor.py --all              # 전체 처리 + DB 저장
python data/dart_extractor.py --all --force      # 기존 결과 덮어쓰기
```

**내보내기**: `scripts/export_dart_md.py` — `dart_extractions` → `dart/YYYYMMDD_{기업명}_{기간}_{종류}.md`

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
│   ├── paper_jobs.py               # paper_exit_checker_job() + paper_eod_sampler_job()
│   │                                # + paper_open_entry_job()
│   └── compose_paper_job.py        # compose_paper_entry_job() — FUNNEL-1/AND-1/SCORE-1 주간 적재
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
│   ├── kiwoom_paper_trader.py     # 키움 모의투자 자동주문 — 7모델, paper_positions 테이블
│   ├── dart_sync.py               # DART 전자공시 수집·XBRL 파싱·세그먼트 Ollama 추출
│   ├── dart_download.py           # DART 보고서 원문 로컬 다운로더 (CLI 독립 실행)
│   ├── dart_extractor.py          # DART XML → Ollama 내러티브 추출 → dart_extractions
│   ├── youtube_narrative_sync.py  # 삼프로TV 자막 수집 → Ollama 종목 언급 추출 → attention_score
│   └── youtube_ticker_aliases.json # 한국어 약칭 → yfinance 심볼 수동 매핑 (100개)
│
├── analysis/                      # 분석·전략 (v0.9.6.0~)
│   ├── signal_detector.py         # LLM 매매 신호 감지 (JSON 구조화 출력)
│   ├── chart_screener.py          # 주봉 Ichimoku+MA 스크리너 (KOSPI/KOSDAQ 전종목, 7조건)
│   ├── screener_filters.py        # Stage 2 필터 프리셋
│   ├── stage_classifier.py        # 일봉 3단계 분류기 — Stage 1/2/3 + 고점 이탈 신호
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
│   │   │                          # _bg_refresh(): stale-while-revalidate 캐시 갱신 헬퍼
│   │   │                          # _EXT_EXECUTOR: 외부 API 전용 ThreadPoolExecutor(max_workers=4)
│   │   │                          # _ext_thread(): yfinance/Kiwoom/KRX 호출 + 명시적 타임아웃
│   │   │                          # _warmup_caches(): lifespan 시작 시 heatmap/market_index/macro 사전 로딩
│   │   │                          # GET /health: 업타임·DB 풀·캐시 TTL·SSE 연결 수 반환
│   │   │                          # GET /api/auth/me: 현재 요청 역할 반환 (admin|special|user)
│   │   │                          # GET /api/portfolio: admin·special 전용 — kt00018+kt00001 실계좌 조회
│   │   └── database.py            # 대시보드 전용 DB 헬퍼
│   └── frontend/
│       ├── src/                   # React + TypeScript 소스
│       │   ├── hooks/useRole.ts   # useRole() — /api/auth/me 1회 호출, 모듈 캐시
│       │   ├── tabs.ts            # TabConfig.roles 게이팅 + getVisibleTabs(role)
│       │   └── components/        # Heatmap, Report, Top, PaperPortfolio, PaperAnalytics, Macro,
│       │                          # Portfolio (admin·special 전용 — 실계좌 포트폴리오) 등
│       └── dist/                  # Vite 빌드 산출물 (백엔드가 정적 서빙)
│
├── tests/                         # pytest 테스트 (622개, v0.10.0.0 기준)
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
│   ├── test_watchlist_features.py # vol_ratio_delta, retiring, Stage2 알림
│   ├── test_dart_extract.py       # DART XML 추출기 (anchor/grep, Ollama 연동)
│   └── test_youtube_narrative_pure.py # YouTube 내러티브 파싱 순수 단위 테스트
│
├── scripts/                       # 운영 스크립트
│   ├── register_tasks.ps1         # Windows 작업 스케줄러 통합 등록 (-Task all|crawler|aftermarket|dashboard)
│   ├── restart_scheduler.bat      # NewsCrawler 재시작
│   ├── start_dashboard.ps1        # 대시보드 서버 시작/재시작 (단일 진입점)
│   ├── start_dashboard_service.bat # 대시보드 서비스 래퍼 (Task Scheduler용)
│   ├── start_crawler.bat          # NewsCrawler 배치 실행
│   ├── run_aftermarket_sync.bat   # 장후 동기화 실행
│   ├── duckdns_update.bat         # DuckDNS IP 업데이트
│   ├── run_sweep.py               # 백테스트 파라미터 그리드서치 (288 조합)
│   ├── export_dart_md.py          # dart_extractions DB → dart/{날짜}_{기업}.md 내보내기
│   └── youtube_backtest.py        # YouTube attention_score 블라인드 백테스트 (Spearman IC)
│
├── docs/                          # 문서 (Diataxis 프레임워크 기반)
│   ├── ARCHITECTURE.md            # 본 아키텍처 문서 (v4.0)
│   ├── TODOS.md                   # 미결 작업 목록
│   ├── USER_MANUAL.md             # 설치부터 첫 알림까지 전체 가이드
│   ├── Dashboard.md               # 웹 대시보드 API 레퍼런스 + 개발 가이드
│   ├── DESIGN.md                  # UI 디자인 토큰 시스템 (색·컴포넌트 패턴)
│   ├── HTTPS-Setup.md             # Caddy HTTPS 설정 (Let's Encrypt + DuckDNS)
│   ├── HowToBacktest.md           # 백테스트 엔진 사용 가이드
│   │
│   ├── howto-screener.md          # 주봉 Ichimoku 스크리너 설정·Calibration
│   ├── howto-stage-classifier.md  # 일봉 3단계 분류기 설정
│   ├── howto-watchlist.md         # 거래대금 워치리스트 온디맨드 조회
│   ├── howto-dart-setup.md        # DART 파이프라인 설정 + Claude 직접 추출 프롬프트
│   ├── howto-youtube-backfill.md  # YouTube 내러티브 소급 수집 + forward return 채우기
│   ├── howto-krx-flow-import.md   # KRX 외국인·기관 수급 데이터 초기 적재
│   ├── howto-kiwoom-paper-trade.md # 키움 모의투자 설정 (API 키 → 포지션 확인)
│   │
│   ├── reference-env-vars.md      # 환경변수 전체 목록
│   ├── reference-telegram-commands.md # Telegram 명령어 전체 목록
│   ├── reference-dart-pipeline.md # DART 파이프라인 완전 레퍼런스
│   ├── reference-youtube-narrative.md # YouTube 내러티브 수집 레퍼런스
│   ├── reference-krx-pipeline.md  # KRX 데이터 파이프라인 레퍼런스
│   ├── reference-kiwoom.md        # 키움 REST API 연동 레퍼런스
│   ├── reference-scheduler.md     # 스케줄러 잡 전체 목록 및 운영 방법
│   │
│   ├── explanation-signal-pipeline.md # 신호 파이프라인·게이팅·HIGH CONFIDENCE 설계 이유
│   ├── explanation-dart-design.md # DART 3계층 설계 이유 (download/sync/extract 분리)
│   ├── explanation-paper-trading.md # 모의투자 3-잡 파이프라인 설계 (가격 소스 분리, exit 상태 머신)
│   ├── explanation-youtube-narrative-design.md # YouTube 내러티브 설계 개념·블라인드 백테스트 프로토콜
│   │
│   └── krx openapi specs/         # KRX OpenAPI 스펙 문서 (본드·파생·주식·ETF 등)
│
├── sql/
│   ├── pgadmin_queries.sql        # DB 관리 쿼리
│   └── rls_policies.sql           # RLS 정책 마이그레이션 (26 테이블 backend_all, service_role 전용)
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

# 대시보드 인증 (3단계 역할 기반, v0.9.9.6~)
ADMIN_USER=admin          # 관리자 — 스케줄러 트리거 + 포트폴리오 조회
ADMIN_PASSWORD=your_admin_pw
SPECIAL_USER=special      # 특수 사용자 — 포트폴리오 조회, 스케줄러 트리거 불가
SPECIAL_PASSWORD=your_special_pw
DASHBOARD_USER=user       # 일반 사용자 — 읽기 전용
DASHBOARD_PASSWORD=your_user_pw
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
| RLS `backend_all` 정책 — 26개 테이블 `TO service_role` 스코프, anon/authenticated 접근 차단 + Security Advisor(`rls_policy_always_true`) 경고 해소 | ✅ (v0.9.7.0~, 2026-07-23 service_role 스코프 개정) |
| asyncpg `statement_cache_size=0` — Supabase PgBouncer 호환 | ✅ (v0.9.0.0~) |
| 모의투자는 키움 가상 계좌 전용 (실자산 영향 없음) | ✅ |
| 대시보드 localhost 인증 우회 차단 — `client.host` 기반 면제 로직 제거 (Nginx 뒤 모든 요청이 127.0.0.1로 보여 사실상 인증 전면 무력화됐던 취약점 수정, v0.9.8.x) | ✅ |
| 포트폴리오 API 서버사이드 전용 — 키움 Bearer 토큰 브라우저 미노출, admin·special 역할만 접근 (v0.9.9.6~) | ✅ |

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

*현재 코드베이스 v0.10.0.0 (2026-06-01) 기준*

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

캡을 채우는 나머지 종목은 KOSPI/KOSDAQ을 번갈아 선택하고, 날짜 기반 오프셋으로
매일 다른 구간을 스캔합니다(각 시장 유니버스를 ~2~4주 주기로 전체 커버).
2026-07까지는 KOSPI가 리스트 앞부분을 전부 차지해 KOSDAQ이 캡에 절대 들지 못하는
버그가 있었고(`stage_classifications`에 KOSDAQ 행이 0건), `market_map`도 종목코드
접미사 대신 sector로 판정해 모든 종목이 `"KOSPI"`로 분류되는 버그가 겹쳐 있었습니다
— `jobs/stage_job.py`에서 수정(`CHANGELOG.md` `[0.10.1.18]` 참고).

### 7. 뉴스 게이팅 강화 (이중 레이어)

기존 단일 스크리너 게이팅에 `_active_stage_tickers` (최근 7일 이내 Stage 활성 종목) 레이어 추가.  
`get_active_stage_tickers(pool, days=7)` DB 함수, `_daily_stage_job()` 완료 후 자동 캐시 갱신.

| 종목 상태 | 게이팅 결과 |
|----------|------------|
| 스크리너 교차 | HIGH CONFIDENCE + 전달 |
| Stage 7d 활성 (스크리너 미통과) | NORMAL + 전달 |
| 둘 다 해당 없음 | 억제 |
| 게이팅 캐시 비어있음 | 전달 (초기 실행 방어) |
