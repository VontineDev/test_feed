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
| 히트맵 | `Heatmap.tsx` | 당일 거래대금 상위 50종목(Kiwoom ka10032). 셀 크기=실제 거래대금, 색상=등락률. Stage 분류 종목은 컬러 테두리 오버레이. 5분 자동갱신 |
| 레포트 | `Report.tsx` | Stage 분류 결과 + 차트 스크리닝 결과. 날짜 범위 선택(오늘/-3일/-1주/-2주/-1달)으로 이력 조회 가능. 종목 클릭 시 Stage·스크리너 이력 팝업 |
| Top | `Top.tsx` | 당일 거래대금 상위 20종목. Kiwoom ka10032 API, 5분 캐시 |
| 모의투자 | `PaperPortfolio.tsx` | 모델별 요약 + 실시간 포지션(60s 갱신) + 청산 이력 + 스케줄러 컨트롤 + 성과분석(누적 P&L 커브·미실현 리더보드·CSV 다운로드). 모델 카드 클릭으로 포지션 필터링 |
| 매크로 | `Macro.tsx` | OLS 팩터 모델 6개 매크로 팩터(USD/KRW·기준금리·KOSPI 52주 고저비·VIX·구리/금 비율·10Y-2Y 스프레드) 추적. `--scenario` CLI로 시나리오별 시뮬레이션 가능 |
| 시그널 (우측 패널/모바일) | `SignalFeed.tsx` | 실시간 매매 신호 SSE 스트림. 15초 폴링 |

모바일(≤768px)에서는 하단 탭바(`MobileNav.tsx`)로 전환됩니다.

---

## API 엔드포인트 레퍼런스

### GET /api/heatmap

당일 거래대금 상위 50종목(Kiwoom ka10032)의 히트맵 데이터를 반환합니다. Stage 분류 결과와 무관하게 항상 채워집니다. Kiwoom 응답 실패 시 Stage 분류 데이터로 폴백합니다.

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
  "cached": false
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `amount` | float | 당일 실제 거래대금(Kiwoom). 셀 크기 결정 |
| `change_pct` | float | 당일 등락률(%). Kiwoom 실시간 기준 |
| `stage` | int\|null | Stage 1/2/3 또는 `null`(미분류) |
| `cached` | bool | 5분 캐시 히트 여부 |

**캐시:** 5분(`_HEATMAP_TTL = 300`).

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

당일 거래대금 상위 N종목. Kiwoom ka10032 API, 5분 캐시.

**쿼리 파라미터:** `?n=20` (기본값 20, 최대 50)

```json
{
  "data": [
    {"rank": 1, "ticker": "000660.KS", "name": "SK하이닉스",
     "close": 185000, "change_pct": 1.5, "volume": 2345678,
     "amount": 4.3e11, "market": "KOSPI"}
  ]
}
```

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

### GET /api/history/ticker/{ticker}

특정 종목의 Stage 분류 이력과 스크리너 등장 이력을 함께 반환합니다.

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

모델별 누적 P&L 시계열, 집계 통계, ticker_name_map, 미실현 포지션 현재가를 단일 응답으로 반환합니다. `PaperAnalytics` 컴포넌트가 호출합니다.

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

- 데스크탑 레이아웃(`app-desktop-layout`) 숨김
- 하단 탭바(`MobileNav`) 표시 — 히트맵·레포트·Top·모의투자·시그널 5탭
- 탭별 컴포넌트가 전체 화면을 차지
- 히트맵은 `.heatmap-root` CSS 클래스로 명시적 높이 부여(`100svh - 42px - 56px`) — `ResponsiveTreeMap`이 높이를 필요로 하기 때문

---

## 관련 문서

- [HTTPS 설정 가이드](HTTPS-Setup.md) — Caddy로 외부 HTTPS 접속 설정
- [ARCHITECTURE.md](ARCHITECTURE.md) — 전체 시스템 아키텍처
- [../CHANGELOG.md](../CHANGELOG.md) — 버전별 변경 이력
