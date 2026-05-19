# 웹 대시보드

Trading Dashboard는 FastAPI + React로 구성된 웹 인터페이스입니다.  
Stage 분류 히트맵, 실시간 매매 신호 피드, 모의투자 포지션, 차트 스크리닝 결과를 제공합니다.

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
| 히트맵 | `Heatmap.tsx` | Stage 1/2/3 종목을 거래대금 크기·등락률 색상으로 표시. 5분 자동갱신 |
| 레포트 | `Report.tsx` | Stage 분류 결과 + 차트 스크리닝 결과. 섹션 접기/펼치기 지원 |
| Top | `Top.tsx` | 당일 거래대금 상위 20종목. Kiwoom ka10032 API, 5분 캐시 |
| 모의투자 | `PaperPortfolio.tsx` | 모델별 요약 + 실시간 포지션(60s 갱신) + 청산 이력 + 스케줄러 컨트롤 |
| 시그널 (우측 패널/모바일) | `SignalFeed.tsx` | 실시간 매매 신호 SSE 스트림. 15초 폴링 |

모바일(≤768px)에서는 하단 탭바(`MobileNav.tsx`)로 전환됩니다.

---

## API 엔드포인트 레퍼런스

### GET /api/heatmap

Stage 분류 종목의 히트맵 데이터를 반환합니다.

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
| `amount` | float | `s1_high × s1_volume` — 히트맵 셀 크기 결정 |
| `change_pct` | float | 당일 등락률(%). yfinance 2일 종가 비교 |
| `cached` | bool | 5분 캐시 히트 여부 |

**캐시:** 5분(`_PRICE_TTL = 300`). 가격 데이터와 Stage 구조 동일 TTL.

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

개발 중에는 Vite(5173)와 FastAPI(8000)가 분리됩니다. CORS 설정(`main.py:131`)이 5173을 허용합니다.

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

빌드된 파일이 `dist/`에 위치하면 FastAPI가 `StaticFiles`로 서빙합니다(`main.py:797-799`).  
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
