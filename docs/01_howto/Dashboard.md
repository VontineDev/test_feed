# 웹 대시보드

Trading Dashboard는 FastAPI + React로 구성된 웹 인터페이스입니다.  
거래대금 히트맵, 실시간 매매 신호 피드, 모의투자 포지션, 차트 스크리닝 결과를 제공합니다.

외부 접속: `https://vtrading.duckdns.org` (Caddy HTTPS 경유 → [HTTPS 설정 가이드](HTTPS-Setup.md))

---

## 아키텍처

```
브라우저
  │ HTTPS (Caddy → localhost:8000)
  ▼
FastAPI (dashboard/backend/main.py)
  │ StaticFiles: frontend/dist/
  │ API: /api/*
  ▼
PostgreSQL (Supabase)
```

- **백엔드:** `dashboard/backend/main.py` — FastAPI, asyncpg, uvicorn
- **프론트엔드:** `dashboard/frontend/src/` — React + Vite, TypeScript, nivo TreeMap
- **빌드 산출물:** `dashboard/frontend/dist/` — FastAPI가 정적 파일로 서빙

---

## 탭 구성

| 탭 | 컴포넌트 | 기능 |
|----|----------|------|
| 히트맵 | `Heatmap.tsx` | 당일 거래대금 상위 20종목(Kiwoom ka10032). Stage 분류 결과 오버레이. 장 마감 시 `daily_market_snap`(ka10032 top100, KRX+NXT 합산) 기준으로 전환 — `aftermarket_snap` 미매칭 시 폴백. 헤더에 "MM/DD 합산" 배지 표시. 5분 갱신(장 마감 30분). 상단에 `MarketSummaryBanner` — KOSPI/KOSDAQ 지수 + 시장 심리 한마디 표시. Kiwoom 실패 시 Stage 분류 데이터로 폴백. |
| 종목 분석 | `Report.tsx` | 추세 단계(Stage 분류) + 강세 후보 발굴(차트 스크리닝) 결과. 날짜 범위 선택(오늘/-3일/-1주/-2주/-1달/직접입력)으로 이력 조회 가능. 직접입력 시 `yymmdd ~ yymmdd` 형식 두 필드 입력 → 확인. 종목 클릭 시 우측 패널 분할로 Stage·스크리너 이력 표시; 같은 종목 재클릭 또는 날짜 변경 시 패널 닫힘. 모바일에서는 세로 스택 전환, 숫자패드 자동 활성화. 헤더에 "데이터 기준: YYYY-MM-DD" 표시 — Stage/스크리너/내러티브 각 데이터 신선도(`as_of`) 확인 가능(? 호버 시 세부 날짜). |
| Top | `Top.tsx` | 당일 거래대금 상위 50종목(Kiwoom ka10032). EPS·PER·Forward PER(Naver Finance) 표시. 장 마감 시 `daily_market_snap`(ka10032 top100, KRX+NXT 합산) 기준으로 전환 — `aftermarket_snap` 미수집 시 폴백. "MM/DD 합산" 배지 표시. 5분 캐시(장 마감 30분). |
| 모의투자 | `PaperPortfolio.tsx` | 모델별 요약 + 실시간 포지션(60s 갱신) + 청산 이력 + CSV 다운로드(포지션 헤더 버튼) + 스케줄러 컨트롤. 모델 카드 클릭으로 포지션 필터링 |
| 매크로 | `Macro.tsx` | OLS 팩터 모델 — 6개 팩터(USD/KRW·미국10년금리·브렌트유·VIX·달러인덱스·아이셰어즈 대한민국 ETF(EWY)) 추적. 종목별 분석 대상은 오늘 거래대금 상위 20종목(히트맵 기준), 없으면 `daily_market_snap` 최신 영업일 TOP 20. 결과는 거래대금 순 정렬 |
| 시그널 (우측 패널/모바일) | `SignalFeed.tsx` | 실시간 매매 신호 SSE 스트림. 15초 폴링 |

모바일(≤768px)에서는 하단 탭바(`MobileNav.tsx`)로 전환됩니다.

---

## API 엔드포인트 레퍼런스

### GET /api/heatmap

당일 거래대금 상위 50종목의 히트맵 데이터를 반환합니다.

**데이터 소스 우선순위:**

| 상태 | 데이터 소스 |
|------|------------|
| 장 중 (09:00~15:30 KST, 평일·영업일) | Kiwoom ka10032 실시간 (KRX+NXT 합산) |
| 장 마감·주말·공휴일 — 1순위 | `daily_market_snap` 최신 영업일 (ka10032 top100) |
| 장 마감·주말·공휴일 — 2순위 폴백 | `aftermarket_snap` (`COALESCE(reg_value, after_value)`) |
| Kiwoom 실패 (장 중) | Stage 분류 데이터 (`stage_results`) |

**공휴일 감지:** 네이버 Finance `siseJson.naver` (005930 영업일 역산). 고정 법정공휴일 9종 + Naver API 실패 시 fallback.

**응답:**
```json
{
  "data": [
    {
      "ticker": "005930.KS",
      "name": "삼성전자",
      "stage": 1,
      "amount": 1234567890.0,
      "change_pct": 2.35,
      "market": "KOSPI"
    }
  ],
  "fetched_at": "2026-05-30",
  "cached": false,
  "is_aftermarket": false
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `amount` | float | 거래대금(원) |
| `change_pct` | float | 등락률(%) |
| `stage` | int\|null | Stage 1/2/3 또는 `null`(미분류) |
| `market` | string | `"KOSPI"` / `"KOSDAQ"` / `""` |
| `fetched_at` | string\|null | 장 마감 시 `YYYY-MM-DD` (스냅샷 기준 영업일), 장 중 `null` |
| `cached` | bool | 캐시 히트 여부 |
| `is_aftermarket` | bool | `true` = 장 마감 스냅샷 데이터 사용 중 ("MM/DD 합산" 배지) |

**캐시:** 장 중 5분(`_PRICE_TTL`), 장 마감 30분(`_AFTERMARKET_TTL`). 장중↔장마감 전환 시 `market_open` 태그 변경으로 즉시 무효화.

---

### GET /api/positions

모의투자 오픈·대기 포지션을 반환합니다.

**응답:**
```json
{
  "data": [
    {
      "id": 42,
      "ticker": "005930.KS",
      "name": "삼성전자",
      "model": "stage",
      "signal_date": "2026-05-17",
      "entry_actual": 75000.0,
      "qty": 10,
      "status": "open",
      "tp1_pct": 0.25,
      "trail_pct": 0.10,
      "current_price": 76200.0,
      "unrealized_pct": 1.6
    }
  ]
}
```

`status`는 `open` 또는 `pending`. `tp1_pct`/`trail_pct`는 DB에 저장된 그대로(분수, 0.25 = 25%) 반환됩니다. `current_price`는 yfinance 1분봉(5분 캐시)에서 조회 — `daily_ohlcv` 테이블이 아닙니다.

---

### GET /api/signals/stream

매매 신호 SSE 스트림. 초기 20건 즉시 전송 후 15초마다 새 신호 확인.

```
Content-Type: text/event-stream

data: [{"id": 123, "direction": "BUY", "strength": 4, ...}]
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `direction` | string | `BUY` / `SELL` / `WATCH` |
| `strength` | int | 1–5 (신호 강도) |
| `tickers` | object | `{"종목명": "티커"}` 매핑 |
| `article_type` | string | `macro` / `earnings` / `regulatory` 등 |

---

### POST /api/scheduler/trigger

스케줄러 잡을 수동으로 트리거합니다. **관리자 권한 필요** (`request.state.role == "admin"`, 아니면 403).

**요청:**
```json
{"job": "stage"}
```

`job` 허용값(`_VALID_JOBS`): `stage`, `screener`, `paper_sample`, `dart_screened`, `youtube`, `flow`

**응답:**
```json
{"status": "queued", "job": "stage", "id": 7}
```
이미 대기/실행 중인 동일 잡이 있으면 `{"status": "already_queued", "job": "stage"}`.

---

### GET /api/scheduler/stream

스케줄러 상태 SSE 스트림. 10초 폴링으로 `scheduler_triggers` 테이블 변경 시에만 push.

---

### GET /api/scheduler/status

`scheduler_triggers` 최근 10건 이력을 1회성으로 반환합니다 (SSE 미지원 클라이언트·초기 로드용, 폴링 로직은 `/api/scheduler/stream`과 동일 쿼리).

**응답:**
```json
{
  "data": [
    {"id": 7, "job_name": "stage", "requested_at": "2026-06-20T10:15:00", "executed_at": "2026-06-20T10:15:03", "status": "done"}
  ]
}
```

---

### GET /api/report/stage

오늘 Stage 분류 결과 전체를 반환합니다.

**응답:**
```json
{
  "data": {
    "date": "2026-05-19",
    "summary": [{"stage": 1, "count": 9, "peakout": 1}],
    "stage1": [...],
    "stage2": [...],
    "stage3": [...]
  }
}
```

---

### GET /api/report/screener

최신 주봉 차트 스크리닝 결과를 반환합니다.

**응답:**
```json
{
  "data": {
    "week": "2026-W21",
    "total": 34,
    "enhanced": 4,
    "gapjum": 0,
    "items": [...]
  }
}
```

---

### GET /api/youtube/screener

최신 윈도우의 `attention_score > 0` 종목 전체를 점수 내림차순으로 반환합니다 (Stage·차트 스크리너 데이터를 곁들임).

**응답:**
```json
{
  "data": {
    "total": 42,
    "stage2_plus": 9,
    "in_screener": 18,
    "narrative_q": 25,
    "triple_combo": 3,
    "items": [
      {
        "ticker": "005930.KS",
        "name": "삼성전자",
        "attention_score": 0.62,
        "attention_q": 4,
        "stage": 2,
        "is_enhanced": true,
        "has_gapjum": false,
        "sector": "반도체",
        "close": 87200
      }
    ]
  }
}
```

| 필드 | 설명 |
|------|------|
| `stage2_plus` | 항목 중 `stage >= 2`인 종목 수 |
| `in_screener` | `is_enhanced` 또는 `has_gapjum`인 종목 수 |
| `narrative_q` | `attention_q`가 2/3/4(중간 분위)인 종목 수 |
| `triple_combo` | Stage2+ ∩ 스크리너 통과 ∩ narrative_q 모두 만족하는 종목 수 |

`/api/report/unified`과 달리 Stage/스크리너 데이터가 없는 종목은 제외하지 않고 `youtube_attention_scores`를 기준 집합으로 사용합니다 (LEFT JOIN).

---

### GET /api/report/pipeline-status

수급(`daily_flow`)·Stage·스크리너·유튜브 4개 파이프라인의 최신 적재일과 신선도 상태를 반환합니다. 대시보드 헤더의 파이프라인 상태 표시에 사용됩니다.

**응답:**
```json
{
  "flow":     {"date": "2026-06-20", "tickers": 2680, "status": "ok"},
  "stage":    {"date": "2026-06-20", "status": "ok"},
  "screener": {"date": "2026-06-14", "status": "ok"},
  "youtube":  {"date": "2026-06-20", "status": "ok"}
}
```

**status 판정 기준** — `flow`/`youtube`는 매일(daily) 갱신, `stage`/`screener`는 주간(weekly) 허용치 적용:

| 소스 | `ok` | `warn` | `error` |
|------|------|--------|---------|
| flow, youtube (daily) | gap ≤ 1일 | gap ≤ 3일 | gap > 3일 또는 데이터 없음 |
| stage, screener (weekly) | gap ≤ 7일 | gap ≤ 14일 | gap > 14일 또는 데이터 없음 |

---

### GET /api/report/unified

Stage 분류 + 차트 스크리너 + 유튜브 관심도를 단일 응답으로 통합합니다. `종목 분석` 탭의 Narrative 뷰가 사용합니다.

**쿼리 파라미터:**

| 파라미터 | 타입 | 설명 |
|---------|------|------|
| `start` | `YYYY-MM-DD` | 조회 시작일 (생략 시 오늘 스냅샷) |
| `end`   | `YYYY-MM-DD` | 조회 종료일 (생략 시 오늘 스냅샷) |

파라미터 없으면 각 소스별 최신값을 반환하고, 있으면 해당 기간 내 최신값(DISTINCT ON ticker)을 반환합니다.

**응답:**
```json
{
  "data": {
    "total": 75,
    "stage1": 3,
    "stage2": 1,
    "in_screener": 61,
    "narrative_q": 6,
    "triple_combo": 0,
    "as_of": {
      "stage":     "2026-06-12",
      "screener":  "2026-06-14",
      "narrative": "2026-06-12"
    },
    "items": [
      {
        "ticker": "005930",
        "name": "삼성전자",
        "stage": 1,
        "s1_high": 89000,
        "peakout_flag": false,
        "is_enhanced": true,
        "has_gapjum": false,
        "close": 87200,
        "ma_20w": 82000,
        "cloud_top": 79000,
        "sector": "반도체",
        "attention_score": 0.5,
        "attention_q": 4
      }
    ]
  }
}
```

| 필드 | 설명 |
|------|------|
| `as_of.stage` | `stage_classifications.classified_date` 최신값 (파라미터 있으면 범위 내 최신) |
| `as_of.screener` | `chart_signals.screened_at::date` 최신값 (파라미터 있으면 범위 내 최신) |
| `as_of.narrative` | `youtube_attention_scores.window_end` 최신값 |
| `close` | `chart_signals.close` 우선, 없으면 `daily_ohlcv` 최신 영업일 종가 폴백 |
| `sector` | `chart_signals.sector`(현재 배치 KIND 업종) → 히스토리 최근 `chart_signals.sector` → `krx_listings.sector` 순 폴백 |
| `attention_q` | NTILE(5) 분위 (1=하위 20%, 5=상위 20%) |

**소스 우선순위 (이름 해석):** `ticker_names` → `krx_listings` → `chart_signals.name` → `youtube_mention_raw.stock_name_raw` → ticker 코드

---

### GET /api/report/paper

모의투자 모델별 요약, 오픈 포지션, 최근 청산 이력을 반환합니다.

**응답:**
```json
{
  "data": {
    "model_summary": {
      "stage": {
        "open":    {"count": 10, "avg_return": null},
        "pending": {"count": 0,  "avg_return": null},
        "closed":  {"count": 5,  "avg_return": 3.2},
        "win_rate": 0.4,
        "total_realized": 16.2
      }
    },
    "open": [...],
    "closed": [...]
  }
}
```

---

### GET /api/top

거래대금 상위 N종목. EPS·PER·Forward PER는 Naver Finance에서 병렬 조회.

**데이터 소스 우선순위:**

| 상태 | 데이터 소스 |
|------|------------|
| 장 중 (09:00~15:30 KST) | Kiwoom ka10032 실시간 (KRX+NXT 합산) |
| 장 마감·주말·공휴일 — 1순위 | `daily_market_snap` 최신 영업일 (ka10032 top100) |
| 장 마감·주말·공휴일 — 2순위 폴백 | `aftermarket_snap` (`COALESCE(reg_value, after_value)`) |

**쿼리 파라미터:** `?n=50` (기본값 50, 최대 100) · `?refresh=true` (캐시 강제 갱신)

```json
{
  "items": [
    {
      "rank": 1,
      "ticker": "000660.KS",
      "name": "SK하이닉스",
      "price": 185000,
      "change_pct": 1.5,
      "amount": 430000000000,
      "market": "KOSPI",
      "eps": 12345,
      "per": 15.0,
      "forward_per": 12.3
    }
  ],
  "fetched_at": "2026-05-30",
  "is_aftermarket": true
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `price` | int | 현재가 |
| `change_pct` | float | 등락률(%) |
| `amount` | int | 거래대금(원) |
| `eps` | int\|null | 주당순이익(Naver Finance) |
| `per` | float\|null | PER(Naver Finance, Trailing) |
| `forward_per` | float\|null | 예상 PER(Naver Finance) |
| `fetched_at` | string | 장 중: `HH:MM:SS`, 장 마감: `YYYY-MM-DD` (스냅샷 기준 영업일) |
| `is_aftermarket` | bool | `true` = 장 마감 스냅샷 데이터 ("MM/DD 합산" 배지) |

**캐시:** 장 중 5분, 장 마감 30분.

---

#### daily_market_snap 테이블 (장 마감 1순위 데이터 소스)

`run_scheduler.py` → `daily_market_snap_job` (16:10 KST, 평일)으로 수집.  
NXT 시간외 단일가(15:40~16:00) 종료 후 10분 뒤 실행해 당일 최종 합산 거래대금을 캡처합니다.

```sql
CREATE TABLE daily_market_snap (
    trade_date  DATE         NOT NULL,
    ticker      VARCHAR(12)  NOT NULL,   -- yfinance 형식: 005930.KS / 035720.KQ
    name        VARCHAR(100),
    price       INT,
    change_pct  NUMERIC(7,2),
    amount      BIGINT,                  -- KRX+NXT 합산 거래대금 (원)
    market      VARCHAR(10),             -- KOSPI / KOSDAQ
    PRIMARY KEY (trade_date, ticker)
);
```

- `ticker`: Kiwoom 원본(`000660_AL` → `.KS`, `035720_AQ` → `.KQ`) 정규화
- `amount`: `ka10032`(`stex_tp=3`, KRX+NXT 합산) 기준 — 정규장 + NXT 시간외 포함
- 저장 범위: 거래대금 top100 (삼성전자·SK하이닉스 등 주요 종목 전체 커버)
- `aftermarket_snap`과의 차이: NXT 거래 여부와 무관하게 ka10032 상위 종목 전체 저장

---

#### aftermarket_snap 테이블 (장 마감 2순위 폴백)

`kiwoom_aftermarket_sync.py --incremental` (16:05 KST)으로 수집.  
NXT 시간외 단일가에 참여한 종목만 저장 (~800여 종목). `daily_market_snap` 미수집 시 폴백으로 사용.

| 컬럼 | 설명 |
|------|------|
| `reg_close` | 정규장 종가 |
| `after_close` | NXT 시간외 체결가 |
| `after_value` | NXT 시간외 누적 거래대금 (원) |
| `reg_value` | ka10032 기준 KRX+NXT 합산 거래대금 (원). NULL = ka10032 top500 미매칭 |
| `after_chg_pct` | NXT 시간외 등락률 (%) |

장 마감 조회 시 `COALESCE(reg_value, after_value)` 순으로 거래대금 사용.

---

### GET /api/history/stage

기간 내 Stage 분류 집계를 반환합니다. 등장 횟수 순 정렬, 스테이지별 최대 50개.

**쿼리 파라미터:** `?start=2026-05-01&end=2026-05-20&stage=1` (모두 선택 사항, 기본 14일)

```json
{
  "data": {
    "start": "2026-05-06",
    "end": "2026-05-20",
    "stage_filter": null,
    "items": [
      {
        "ticker": "005930.KS",
        "name": "삼성전자",
        "appearance_count": 5,
        "first_seen": "2026-05-06",
        "last_seen": "2026-05-19",
        "any_peakout": false,
        "stage_queried": 1,
        "latest_stage": 2
      }
    ]
  }
}
```

---

### GET /api/history/screener

기간 내 차트 스크리너 통과 이력을 주차 단위로 집계합니다. 등장 횟수 순 정렬, 최대 100개.

**쿼리 파라미터:** `?start=2026-04-01&end=2026-05-20` (기본 14일)

```json
{
  "data": {
    "start": "2026-05-06",
    "end": "2026-05-20",
    "start_week": "2026-W19",
    "end_week": "2026-W21",
    "items": [
      {
        "ticker": "005930.KS",
        "name": "삼성전자",
        "week_count": 3,
        "first_week": "2026-W19",
        "last_week": "2026-W21",
        "any_enhanced": true,
        "any_gapjum": false
      }
    ]
  }
}
```

---

### GET /api/market_index

KOSPI/KOSDAQ 지수 현재가 + 등락률을 반환합니다. KRX OpenAPI 성공 시 확정 종가, 실패 시 yfinance fallback. 5분 TTL 캐시.

**데이터 소스 우선순위:**

| 단계 | 소스 | 설명 |
|------|------|------|
| 1 | KRX OpenAPI `/idx/kospi_dd_trd`, `/idx/ksq_dd_trd` | 확정 종가 + prev_close |
| 2 (KRX 실패 시) | yfinance `period='10d', interval='1d'` | 일별 데이터로 close + prev_close 보완. 오늘 부분 데이터를 제외하고 직전 완결 영업일을 prev_close로 사용 |
| 3 (장중 + 데이터 있을 때) | yfinance `period='1d', interval='1m'` | 1분봉 최신값으로 close 덮어쓰기 (`is_realtime: true`) |

```json
{
  "market_status": "closed",
  "is_realtime": false,
  "kospi":  { "close": 2650.12, "change_pct": 0.45, "prev_close": 2638.30 },
  "kosdaq": { "close":  870.54, "change_pct": -0.12, "prev_close": 871.59 },
  "sentiment": "보합",
  "sentiment_detail": "큰 방향성 없이 혼조",
  "as_of": "2026-05-24T17:05:00+09:00"
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `market_status` | string | `open` (장중) / `closed` (장마감·주말) |
| `is_realtime` | bool | `true` = yfinance 1분봉 장중 실시간, `false` = 일별 데이터(KRX 또는 yfinance daily) |
| `kospi` / `kosdaq` | object\|null | 지수 없는 날 `null` |
| `sentiment` | string | `강세` / `상승` / `보합` / `하락` / `급락` |
| `sentiment_detail` | string | 한국어 한 줄 설명 |
| `as_of` | string | ISO 8601 KST 타임스탬프 |

**sentiment 판정:** KOSPI/KOSDAQ 등락률 단순 평균 기준. ±0.5% 이내 = 보합, ±2.0% 초과 = 강세/급락.

`MarketSummaryBanner` 컴포넌트가 이 엔드포인트를 5분 주기로 폴링합니다.

---

### GET /api/dart/summary/{ticker}

DART 최신 보고서 기준 재무 요약(매출·영업이익·사업부문)을 반환합니다.

**경로 파라미터:** `ticker` — yfinance 심볼 (예: `005930.KS`). 앞 6자리 stock_code로 `dart_companies`와 조인.

```json
{
  "data": {
    "corp_name":    "삼성전자",
    "period":       "2024/12",
    "report_type":  "사업보고서",
    "extracted_at": "2026-05-20 03:12:00",
    "revenue": {
      "periods":      ["2023/12", "2024/12"],
      "unit":         "억원",
      "consolidated": { "revenue": [2589355, 3006947], "op_profit": [-14884, 326454] },
      "segments": [
        { "name": "DS", "revenues": [null, 1234000], "yoy_growth": [null, 0.12] }
      ]
    },
    "segments": [
      { "segment_name": "DS", "revenue_share_pct": 41.0 }
    ]
  }
}
```

| 필드 | 설명 |
|------|------|
| `data` | `null` = DART 데이터 없음 (미수집 종목 or ETF) |
| `revenue.unit` | `억원` / `백만원` / `조원` 등 |
| `revenue.consolidated` | 연결 기준 매출·영업이익 배열 (periods 순서와 대응) |
| `revenue.segments` | 사업부문별 매출·YoY 성장률 배열 |
| `segments` | 사업부문 구성 요약 (매출 비중, 주요 제품) |

`StageHistoryPopup` 내 `DartFinancials` 컴포넌트가 종목 클릭 시 이 엔드포인트를 호출합니다.

---

### GET /api/history/ticker/{ticker}

특정 종목의 추세 단계 이력과 강세 후보 등장 이력을 함께 반환합니다.

**경로 파라미터:** `ticker` — yfinance 심볼 (예: `005930.KS`)  
**쿼리 파라미터:** `?start=2026-04-01&end=2026-05-20` (기본 14일)

```json
{
  "data": {
    "ticker": "005930.KS",
    "start": "2026-05-06",
    "end": "2026-05-20",
    "stage_history": [
      {"classified_date": "2026-05-19", "stage": 2, "peakout_flag": false, "s1_high": 87000, "s1_txamt": 1234567890}
    ],
    "screener_history": [
      {"week_of": "2026-W21", "is_enhanced": true, "has_gapjum": false, "close": 87500}
    ]
  }
}
```

---

### GET /api/paper/history

특정 종목의 모의투자 전체 이력(과거 모든 포지션, 모델 무관)을 신호일 역순으로 반환합니다. `종목 분석` 탭에서 종목 클릭 시 모의투자 이력 표시에 사용됩니다.

**쿼리 파라미터:** `?ticker=005930.KS` (필수)

**응답:**
```json
{
  "data": [
    {
      "id": 42, "model": "stage", "ticker": "005930.KS", "name": "삼성전자",
      "signal_date": "2026-05-17", "entry_theory": 74800.0, "entry_actual": 75000.0,
      "slippage_pct": 0.0027, "qty": 10, "status": "open",
      "tp1_pct": 0.25, "tp1_ratio": 0.5, "tp1_date": null, "tp1_price": null,
      "trail_pct": 0.10, "hard_stop_pct": 0.10, "watermark": 75000.0,
      "exit_date": null, "exit_price": null, "exit_type": null,
      "blended_return": null, "created_at": "2026-05-17T09:05:02",
      "current_price": 76200.0, "unrealized_pct": 1.6
    }
  ],
  "ticker": "005930.KS",
  "name": "삼성전자"
}
```

`tp1_pct`/`trail_pct`/`hard_stop_pct` 등은 DB에 저장된 그대로(분수, 예: `0.25` = 25%) 반환됩니다 — `/api/positions`와 동일한 단위. `current_price`는 `open`/`pending` 행에만 채워집니다(yfinance 단일 종목 조회, 공유 캐시 미사용).

---

### GET /api/paper/curve

모델별 누적 P&L 시계열, 집계 통계, ticker_name_map, 미실현 포지션 현재가를 단일 응답으로 반환합니다.

**응답:**
```json
{
  "data": {
    "series": {
      "stage": [{"date": "2026-04-15", "cumulative": 0.05}, ...]
    },
    "model_stats": {
      "stage": {
        "n_trades": 12,
        "n_wins": 8,
        "win_rate": 0.667,
        "avg_win": 0.082,
        "avg_loss": -0.045,
        "total_realized": 0.31,
        "total_unrealized": 0.06
      }
    },
    "ticker_name_map": {"005930.KS": "삼성전자"},
    "open_positions": [
      {
        "ticker": "000660.KS",
        "name": "SK하이닉스",
        "model": "stage",
        "unrealized_pct": 0.067
      }
    ]
  }
}
```

`series`는 closed 포지션의 `blended_return` Window Function 누계. `total_unrealized`는 open 포지션의 `(current_price / entry_actual) - 1` 합산. 현재가는 yfinance 5분 캐시에서 조회합니다.

---

### GET /api/paper/export

`paper_positions` 테이블 전체를 CSV로 다운로드합니다. Excel 한글 호환을 위해 utf-8-sig BOM 인코딩을 사용합니다.

**응답:** `text/csv; charset=utf-8`, `Content-Disposition: attachment; filename="paper_positions_YYYYMMDD.csv"`

컬럼: `model, ticker, name, signal_date, entry_theory, entry_actual, slippage_pct, qty, status, tp1_pct, tp1_ratio, tp1_date, tp1_price, trail_pct, hard_stop_pct, watermark, exit_date, exit_price, exit_type, blended_return, created_at`

---

### GET /api/macro

`MacroTracker`(OLS 팩터 모델) 분석 결과를 반환합니다 (`Macro.tsx` 탭). 10분 캐시 — 최초 호출은 yfinance 다운로드로 30~60초 소요, 이후 캐시 즉시 반환.

**쿼리 파라미터:** `?refresh=true` (캐시 무시하고 강제 재분석)

**분석 대상 종목 선정:** 1순위 — 오늘 히트맵 캐시에서 거래대금 상위 20종목으로 변환 가능한 것. 히트맵이 비어 있으면(5종목 미만) 2순위 — 전일 `aftermarket_snap` TOP 20. 둘 다 없으면 `DEFAULT_TICKERS`(코드 하드코딩) 사용.

**6개 팩터:** `rate`(미국10년금리) · `fx`(USD/KRW) · `oil`(브렌트유) · `vix`(VIX) · `dxy`(달러인덱스) · `export`(EWY, 외국인 수급 대리지표)

**응답:**
```json
{
  "snapshot": {
    "rate": {"current": 4.25, "change_1d": 0.02, "change_5d": -0.05, "change_20d": 0.10, "change_60d": 0.30, "z_score_60d": 0.8}
  },
  "stocks": [
    {
      "ticker": "005930.KS", "name": "삼성전자",
      "n_obs": 504, "r_squared": 0.42, "adj_r_squared": 0.40, "residual_std": 1.2,
      "macro_score": 0.18, "macro_score_5d": 0.03, "macro_score_20d": 0.09,
      "significant_factors": ["fx", "export"],
      "betas": {"rate": -0.1, "fx": 0.6, "oil": 0.05, "vix": -0.2, "dxy": -0.3, "export": 0.4},
      "alpha": 0.0002,
      "t_stats": {"fx": 2.1, "export": 1.9},
      "p_values": {"fx": 0.03, "export": 0.05},
      "factor_contribs_5d": {"rate": -0.001, "fx": 0.012, "oil": 0.0, "vix": 0.004, "dxy": -0.002, "export": 0.006}
    }
  ],
  "fetched_at": "14:32:10",
  "cached": false
}
```

| 필드 | 설명 |
|------|------|
| `snapshot` | 팩터별 현재값·1d/5d/20d/60d 변화율·60일 z-score (팩터 6개 키) |
| `stocks` | 종목별 OLS 회귀 결과. 히트맵 경로면 거래대금 순(상위 20), 아니면 `macro_score` 내림차순 |
| `betas`/`t_stats`/`p_values` | 팩터별 회귀계수·t값·p값 (`alpha`는 별도 필드로 분리) |
| `factor_contribs_5d` | `beta × change_5d` — 팩터별 5일 기여도 |
| `cached` / `stale` | `true` = 캐시 히트. `stale: true`면 백그라운드 갱신 중인 오래된 데이터 |

캐시 갱신 실패 시 이전 데이터를 `stale: true, error: "분석 오류 — 이전 데이터 표시 중"`와 함께 반환합니다 (500 대신 graceful degradation).

---

### POST /api/feedback

대시보드 피드백을 텔레그램으로 전송합니다 (`TELEGRAM_TOKEN`/`TELEGRAM_CHAT_ID` 미설정 시 503).

**요청:**
```json
{"text": "히트맵 갱신이 느려요", "screenshot": null}
```

`screenshot`은 base64 JPEG (선택). 첨부 시 `sendPhoto`, 없으면 `sendMessage`로 전송. 메시지에 요청자 역할(`role`)과 본문 900자까지 포함.

**응답:** `{"status": "sent"}`

---

### GET /api/auth/me

현재 요청의 인증 역할을 반환합니다. 프론트엔드가 admin/special/user 역할별 UI 분기(스케줄러 트리거 버튼, 포트폴리오 탭 노출 등)에 사용합니다.

**응답:** `{"role": "admin"}`

---

### GET /api/portfolio

수동 입력 포트폴리오(`manual_portfolio` 테이블) 조회. **admin/special 역할만 접근 가능** (그 외 403).

**응답:**
```json
{
  "summary": {"tot_pur_amt": 10000000, "tot_evlt_amt": 10500000, "tot_evlt_pl": 500000, "tot_prft_rt": 5.0},
  "holdings": [
    {
      "id": 1, "stk_cd": "005930.KS", "stk_nm": "삼성전자", "market": "KR",
      "avg_price": 75000, "qty": 10, "cur_prc": 78000,
      "pur_amt": 750000, "evlt_amt": 780000, "evltv_prft": 30000,
      "pur_amt_krw": 750000, "evlt_amt_krw": 780000, "evltv_prft_krw": 30000,
      "prft_rt": 4.0, "poss_rt": 7.4
    }
  ],
  "usd_krw": 1380.5
}
```

미국 주식(티커에 숫자 없음)은 `avg_price`/`pur_amt`/`evlt_amt`/`evltv_prft`를 USD 원화(native) 기준으로, `*_krw` 필드는 `usd_krw` 환율로 환산해 별도 제공합니다. 한국 주식은 둘이 동일합니다. `tot_*`/`poss_rt`(포트폴리오 내 비중)는 항상 원화 환산 기준.

현재가: 한국 주식은 `aftermarket_snap.reg_close` 우선 → yfinance 폴백, 미국 주식은 yfinance 직접 조회.

---

### POST /api/portfolio/holdings

종목 추가 (**admin 전용**, 그 외 403). 동일 `ticker` 존재 시 UPDATE(upsert).

**요청:** `{"ticker": "005930.KS", "name": "삼성전자", "avg_price": 75000, "qty": 10}`

`qty`/`avg_price` ≤ 0이면 422. 응답: `201` + 생성/갱신된 행 (`id`, `ticker`, `name`, `avg_price`, `qty`).

---

### PUT /api/portfolio/holdings/{holding_id} · DELETE /api/portfolio/holdings/{holding_id}

각각 종목 수정·삭제 (**admin 전용**). PUT 응답 `{"ok": true}`, 대상 없으면 404. DELETE는 `204 No Content`.

---

### GET /api/ticker/lookup

종목코드/티커로 종목명을 조회합니다 (포트폴리오 수동 추가 폼의 자동완성용).

**쿼리 파라미터:** `?q=005930` (한국: 6자리 숫자 → KR, 그 외 → US로 판정)

**조회 순서 (한국):** `ticker_names` → `krx_listings` → (둘 다 없으면) Yahoo Finance 검색(`.KS`/`.KQ` 순)
**조회 순서 (미국):** Yahoo Finance 검색만

**응답:** `{"ticker": "005930", "name": "삼성전자", "market": "KR"}` — 못 찾으면 `404`.

---

## 데이터 수집 스케줄

`run_scheduler.py`에 평일(mon-fri)로 등록된 주요 잡.

| 시각 (KST) | 잡 | 대상 테이블 | 설명 |
|-----------|-----|------------|------|
| 09:05 | `youtube_narrative_sync_job` | `youtube_mention_raw` | 삼프로TV 전일 영상 → LLM 종목 언급 추출 |
| 09:35 | `youtube_attention_score_job` | `youtube_attention_scores` | 5영업일 롤링 attention_score 집계 |
| 15:40 | `youtube_forward_return_job` | `youtube_mention_forward_returns` | 언급 종목 +1d/+5d/+20d 수익률 채우기 |
| 16:05 | `daily_aftermarket_sync_job` | `aftermarket_snap` | NXT 시간외 단일가 종목 수집 (`--incremental`), `reg_value` 동시 갱신 |
| 16:10 | `daily_market_snap_job` (id: `daily_market_snap`) | `daily_market_snap` | NXT 종료(16:00) 후 10분 — 히트맵/Top 장마감 즉시 반영용 1차 스냅샷 |
| 20:10 | `daily_market_snap_job` (id: `daily_market_snap_final`) | `daily_market_snap` | 완전한 당일 최종값으로 upsert (동일 함수, 잡 ID만 다름) |

---

## YouTube 내러티브 파이프라인

삼프로TV(채널 ID: `UChlv4GSd7OQl3js-jkLOnFA`) 자막을 LLM으로 분석해 종목 언급·방향성을 추출하고, 5영업일 롤링 `attention_score`를 산출합니다.

### 파이프라인 흐름

```
YouTube Data API v3 → 영상 목록
  → youtube-transcript-api → 한국어 자막
  → Ollama 로컬 LLM(기본 qwen3.5:9b) → 종목 언급 JSON
  → youtube_mention_raw 테이블
  → 5영업일 롤링 → youtube_attention_scores
  → 15:40 KST yfinance → youtube_mention_forward_returns (백테스트 재료)
```

### 테이블 구조

| 테이블 | 설명 |
|--------|------|
| `youtube_mention_raw` | 수집 영상별 종목 언급 (video_id, video_date, ticker, direction: buy/sell/neutral, horizon, rationale_summary, source_quote) |
| `youtube_attention_scores` | 5영업일 롤링 attention_score, 영상 수 가중 평균 |
| `youtube_mention_forward_returns` | 언급 종목 +1d/+5d/+20d 수익률 — 블라인드 백테스트 재료 |

### attention_score 계산

```
attention_score = SUM(sentiment_weight) / distinct_video_count
  sentiment_weight: buy=1.0, neutral=0.5, sell=0.0
  rolling window: 5 영업일
```

### 블라인드 백테스트

방법론을 `git tag backtest-v1-blind`(2026-05-31)로 동결 후 과거 데이터에 적용해 IC(Information Coefficient)를 측정합니다.

합격 기준: **IC(ret_5d) > 0.05 AND t-stat > 1.65 AND 샘플 ≥ 100**

```bash
python scripts/youtube_backtest.py --ret ret_5d
```

**진행 상태**: 분산 백필 완료 후 실행됨 — `[조건부]` 판정(IC +0.0136, t-stat +0.69, n=2,587). 합격 기준 미달로 `attention_score`는 아직 `effective_confidence`에 편입하지 않음. 결과 상세: [백필 계획](../02_reference/plan-youtube-backfill.md), 판정 기준: [백테스트 실행 방법](howto-youtube-run-backtest.md).

상세 설계: [유튜브 내러티브 스크리닝 설계 문서](explanation-youtube-narrative-design.md)

---

## 개발 서버 실행

```bash
# 백엔드 (터미널 1)
cd dashboard/backend
uvicorn main:app --reload --port 8000

# 프론트엔드 개발 서버 (터미널 2)
cd dashboard/frontend
npm run dev
# → http://localhost:5173
```

개발 중에는 Vite(5173)와 FastAPI(8000)가 분리됩니다. CORS 설정(`main.py:388`)이 5173을 허용합니다.

---

## 프로덕션 빌드 및 배포

```bash
# 1. 프론트엔드 빌드
cd dashboard/frontend
npm run build
# → dashboard/frontend/dist/ 생성

# 2. FastAPI 서버 시작
#    (Caddy가 이미 실행 중이면 재시작 불필요 — StaticFiles가 dist를 바로 서빙)
cd dashboard/backend
uvicorn main:app --port 8000
```

빌드된 파일이 `dist/`에 위치하면 FastAPI가 `StaticFiles`로 서빙합니다(`main.py:3213`).  
새 빌드 후 브라우저에서 `Ctrl+Shift+R` (강제 새로고침) 필요.

---

## 인증

**외부 접속 (Caddy경유):** Caddy basicauth가 브라우저 인증 다이얼로그를 처리합니다. 한 번 인증하면 세션 동안 모든 API 호출에 자동 포함됩니다.

**FastAPI `_BasicAuthMiddleware` (`dashboard/backend/main.py:394`):** 3단계 역할 기반 Basic Auth — localhost 우회 로직은 없으며, 환경변수가 하나라도 설정되면 `127.0.0.1` 포함 모든 요청에 인증을 요구합니다.

| 환경변수 | 역할 | 권한 |
|---------|------|------|
| `ADMIN_USER` / `ADMIN_PASSWORD` | `admin` | 스케줄러 트리거 + 포트폴리오 전체 |
| `SPECIAL_USER` / `SPECIAL_PASSWORD` | `special` | 포트폴리오 조회 + 읽기 |
| `DASHBOARD_USER` / `DASHBOARD_PASSWORD` | `user` (단, `ADMIN_USER` 미설정 시 `admin`으로 간주 — 하위 호환) | 읽기 전용 |

세 변수 모두 미설정이면 인증 자체가 비활성화되고 모든 요청이 `role=admin`으로 통과합니다(로컬 개발 환경 기본값).

---

## 모바일 레이아웃

768px 이하에서 다음과 같이 작동합니다:

- 데스크탑 레이아웃 숨김
- 하단 탭바(`MobileNav`) 표시 — 히트맵·종목 분석·Top·모의투자·시그널 5탭
- 탭별 컴포넌트가 전체 화면을 차지
- 히트맵은 `ResponsiveTreeMap`의 높이 요구 충족을 위해 명시적 높이 지정(스타일 규칙은 `DESIGN.md` 참조)

---

## 관련 문서

- [HTTPS 설정 가이드](HTTPS-Setup.md) — Caddy로 외부 HTTPS 접속 설정
- [ARCHITECTURE.md](ARCHITECTURE.md) — 전체 시스템 아키텍처
- [../CHANGELOG.md](../CHANGELOG.md) — 버전별 변경 이력
- [유튜브 내러티브 스크리닝 설계](explanation-youtube-narrative-design.md) — 삼프로TV LLM 파이프라인 설계 문서
