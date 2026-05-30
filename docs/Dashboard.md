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
| 히트맵 | `Heatmap.tsx` | 당일 거래대금 상위 50종목(Kiwoom ka10032). Stage 분류 결과 오버레이. 장 마감 시 `aftermarket_snap` 합산(KRX+NXT) 거래대금 기준으로 전환, 헤더에 "전일 합산" 배지 표시. 5분 갱신(장 마감 30분). 상단에 `MarketSummaryBanner` — KOSPI/KOSDAQ 지수 + 시장 심리 한마디 표시. Kiwoom 실패 시 Stage 분류 데이터로 폴백. |
| 종목 분석 | `Report.tsx` | 추세 단계(Stage 분류) + 강세 후보 발굴(차트 스크리닝) 결과. 날짜 범위 선택(오늘/-3일/-1주/-2주/-1달)으로 이력 조회 가능. 종목 클릭 시 우측 패널 분할로 Stage·스크리너 이력 표시; 같은 종목 재클릭 또는 날짜 변경 시 패널 닫힘. 모바일에서는 세로 스택 전환. 섹션 헤더 ⓘ 호버 시 기능 설명 팝업. |
| Top | `Top.tsx` | 당일 거래대금 상위 50종목(Kiwoom ka10032). EPS·PER·Forward PER(Naver Finance) 표시. 장 마감 시 `aftermarket_snap` 합산(KRX+NXT) 거래대금 기준으로 전환, "전일 합산" 배지 표시. 5분 캐시(장 마감 30분). |
| 모의투자 | `PaperPortfolio.tsx` | 모델별 요약 + 실시간 포지션(60s 갱신) + 청산 이력 + CSV 다운로드(포지션 헤더 버튼) + 스케줄러 컨트롤. 모델 카드 클릭으로 포지션 필터링 |
| 매크로 | `Macro.tsx` | OLS 팩터 모델 — 6개 팩터(USD/KRW·미국10년금리·브렌트유·VIX·달러인덱스·아이셰어즈 대한민국 ETF(EWY)) 추적. 종목별 분석 대상은 오늘 거래대금 상위 20종목(히트맵 기준), 없으면 전날 aftermarket_snap TOP 20. 결과는 거래대금 순 정렬 |
| 시그널 (우측 패널/모바일) | `SignalFeed.tsx` | 실시간 매매 신호 SSE 스트림. 15초 폴링 |

모바일(≤768px)에서는 하단 탭바(`MobileNav.tsx`)로 전환됩니다.

---

## API 엔드포인트 레퍼런스

### GET /api/heatmap

당일 거래대금 상위 50종목의 히트맵 데이터를 반환합니다. 장 중에는 Kiwoom ka10032 실시간 데이터(KRX+NXT 합산), 장 마감 시에는 `aftermarket_snap`의 `reg_value`(KRX) + `after_value`(NXT) 합산 기준으로 전환합니다. Kiwoom 응답 실패 시 Stage 분류 데이터로 폴백합니다.

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
  "cached": false,
  "is_aftermarket": false
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `amount` | float | 거래대금(장 중: ka10032 KRX+NXT 합산, 장 마감: `reg_value`+`after_value` 합산) |
| `change_pct` | float | 등락률(장 중: 당일 실시간, 장 마감: NXT 시간외 등락률) |
| `stage` | int\|null | Stage 1/2/3 또는 `null`(미분류) |
| `market` | string | `"KOSPI"` / `"KOSDAQ"` / `""` |
| `cached` | bool | 캐시 히트 여부 |
| `is_aftermarket` | bool | `true` = 장 마감 합산 데이터 사용 중 ("전일 합산" 배지) |

**캐시:** 장 중 5분(`_PRICE_TTL`), 장 마감 30분(`_AFTERMARKET_TTL`).

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
      "tp1_pct": 5.0,
      "trail_pct": 3.0,
      "current_price": 76200.0,
      "unrealized_pct": 1.6
    }
  ]
}
```

`status`는 `open` 또는 `pending`. `current_price`는 `daily_ohlcv` 테이블 최신 종가.

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

스케줄러 잡을 수동으로 트리거합니다.

**요청:**
```json
{"job_name": "stage"}
```

`job_name` 허용값: `stage`, `screener`, `paper_sample`

**응답:**
```json
{"ok": true, "job_name": "stage", "trigger_id": 7}
```

---

### GET /api/scheduler/stream

스케줄러 상태 SSE 스트림. 1초 폴링으로 `scheduler_triggers` 테이블 변경 감지.

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
        "closed":  {"count": 5,  "avg_return": 3.2}
      }
    },
    "open": [...],
    "closed": [...]
  }
}
```

---

### GET /api/top

거래대금 상위 N종목. 장 중: Kiwoom ka10032 실시간(KRX+NXT 합산), 장 마감: `aftermarket_snap` 합산(`reg_value`+`after_value`) 기준. EPS·PER·Forward PER는 Naver Finance에서 병렬 조회.

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
  "fetched_at": "14:32:11",
  "is_aftermarket": false
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `price` | int | 현재가(장 중: 실시간, 장 마감: NXT 시간외 체결가) |
| `change_pct` | float | 등락률(%) |
| `amount` | int | 거래대금(원)(장 중: KRX+NXT 합산, 장 마감: `reg_value`+`after_value`) |
| `eps` | int\|null | 주당순이익(Naver Finance) |
| `per` | float\|null | PER(Naver Finance, Trailing) |
| `forward_per` | float\|null | 예상 PER(Naver Finance) |
| `fetched_at` | string | 조회 시각(장 중: HH:MM:SS, 장 마감: YYYY-MM-DD) |
| `is_aftermarket` | bool | `true` = 장 마감 합산 데이터 ("전일 합산" 배지) |

**캐시:** 장 중 5분, 장 마감 30분.

#### aftermarket_snap 테이블 (장 마감 데이터 소스)

`kiwoom_aftermarket_sync.py --incremental` (16:05 KST 실행)으로 수집.

| 컬럼 | 설명 |
|------|------|
| `reg_close` | 정규장 종가 |
| `after_close` | NXT 시간외 체결가 |
| `after_value` | NXT 시간외 누적 거래대금 (원) |
| `reg_value` | KRX+NXT 합산 거래대금 (ka10032 기준, 원). NULL이면 after_value만 사용 |
| `after_chg_pct` | NXT 시간외 등락률 (%) |

`reg_value`는 `kiwoom_aftermarket_sync.py` 실행 시 `ka10032`(거래대금상위, `stex_tp=3`)를 호출해 당일 최종 합산 거래대금을 매칭 저장합니다. ka10032 top500 이외 종목은 NULL.

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

KOSPI/KOSDAQ 지수 현재가 + 등락률을 반환합니다. KRX OpenAPI + yfinance 혼합 조회, 5분 TTL 캐시.

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
| `is_realtime` | bool | `true` = yfinance 장중 실시간, `false` = KRX 확정 종가 |
| `kospi` / `kosdaq` | object\|null | 지수 없는 날 `null` |
| `sentiment` | string | `강세` / `상승` / `보합` / `하락` / `급락` |
| `sentiment_detail` | string | 한국어 한 줄 설명 |
| `as_of` | string | ISO 8601 KST 타임스탬프 |

**sentiment 판정:** KOSPI/KOSDAQ 등락률 단순 평균 기준. ±0.5% 이내 = 보합, ±2.0% 초과 = 강세/급락.

`MarketSummaryBanner` 컴포넌트가 이 엔드포인트를 5분 주기로 폴링합니다.

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

개발 중에는 Vite(5173)와 FastAPI(8000)가 분리됩니다. CORS 설정(`main.py:138`)이 5173을 허용합니다.

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

빌드된 파일이 `dist/`에 위치하면 FastAPI가 `StaticFiles`로 서빙합니다(`main.py:1151-1153`).  
새 빌드 후 브라우저에서 `Ctrl+Shift+R` (강제 새로고침) 필요.

---

## 인증

**외부 접속 (Caddy경유):** Caddy basicauth가 브라우저 인증 다이얼로그를 처리합니다. 한 번 인증하면 세션 동안 모든 API 호출에 자동 포함됩니다.

**내부 접속 (localhost:8000 직접):** FastAPI `_BasicAuthMiddleware`가 `127.0.0.1` / `::1` 접속을 인증 없이 통과시킵니다(`main.py:149`). 개발 환경에서는 인증 없이 바로 접근할 수 있습니다.

`DASHBOARD_USER` / `DASHBOARD_PASSWORD` 환경변수가 없으면 FastAPI 인증 자체가 비활성화됩니다.

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
