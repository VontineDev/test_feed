# KRX 데이터 파이프라인 레퍼런스

KRX(한국거래소) 데이터를 수집하는 4개 모듈의 완전한 기술 레퍼런스.

## 모듈 역할

| 모듈 | 역할 | 데이터 소스 |
|------|------|-------------|
| `krx_openapi.py` | OHLCV, 종목기본정보, 지수 수집 | KRX OpenAPI (openapi.krx.co.kr) |
| `krx_sync.py` | 전 종목 리스트 → `krx_listings` 동기화 | KRX OpenAPI |
| `krx_flow_sync.py` | 외국인·기관·개인 순매수 → `daily_flow` | data.krx.co.kr 직접 크롤(`--backend krx-direct`, **스케줄러 기본값**) — 또는 pykrx/csv/키움 ka10045(`personal_net` 없음, 쿠키 만료 시 수동 폴백) |
| `data/kiwoom_aftermarket_sync.py` | 시간외 단일가 스냅샷 → `aftermarket_snap` (**스케줄러가 실제로 호출하는 모듈**) | 키움 REST API ka10032 |
| `data/krx_aftermarket_sync.py` | 위와 동일 테이블, 과거 날짜 backfill 전용 (키움 REST는 당일 데이터만 제공) | KRX BLD API (data.krx.co.kr) |

---

## 환경 변수

| 변수 | 모듈 | 설명 |
|------|------|------|
| `KRX_OPENAPI_KEY` | krx_openapi, krx_sync | KRX OpenAPI 인증키. openapi.krx.co.kr 가입 후 발급 |
| `KRX_ID` | krx_flow_sync (krx-direct/pykrx 백엔드, **기본값**) | data.krx.co.kr 로그인 아이디 |
| `KRX_PW` | krx_flow_sync (krx-direct/pykrx 백엔드, **기본값**) | data.krx.co.kr 로그인 비밀번호 |
| `KRX_SESSION`/`KRX_VISITOR` | krx_flow_sync (krx-direct 백엔드) | data.krx.co.kr 브라우저 JSESSIONID 쿠키 — KRX_ID/PW 대신 사용 가능한 대안이지만, KRX_ID/PW와 달리 만료 시 수동 갱신 필요 |
| `KIWOOM_APPKEY`/`KIWOOM_SECRETKEY` | krx_flow_sync (kiwoom 백엔드, 수동 폴백) | [키움 연동 레퍼런스](reference-kiwoom.md) 참고 — `kiwoom_aftermarket_sync.py`와 동일 키 재사용 |

---

## krx_openapi.py

### `KRXOpenAPIClient`

```python
from data.krx_openapi import KRXOpenAPIClient, get_client

client = KRXOpenAPIClient()            # KRX_OPENAPI_KEY 환경변수 사용
client = KRXOpenAPIClient(appkey="X")  # 직접 전달
client = get_client()                  # 모듈 수준 싱글턴 (권장)
```

Base URL: `https://data-dbg.krx.co.kr/svc/apis`  
인증: `AUTH_KEY` 헤더. Rate limit: 10 req/s.

#### 종목 기본정보

| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `get_kospi_tickers(bas_dd)` | `/sto/stk_isu_base_info` | KOSPI 전 종목 기본정보 |
| `get_kosdaq_tickers(bas_dd)` | `/sto/ksq_isu_base_info` | KOSDAQ 전 종목 기본정보 |
| `get_all_tickers(bas_dd)` | 위 두 API 조합 | KOSPI + KOSDAQ → `[(yfinance_symbol, name, market), ...]` |

```python
tickers = client.get_all_tickers("20260529")
# [("005930.KS", "삼성전자", "KOSPI"), ("035720.KQ", "카카오", "KOSDAQ"), ...]
```

#### 일별 OHLCV

| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `get_kospi_ohlcv(bas_dd)` | `/sto/stk_bydd_trd` | KOSPI 전 종목 일별 시세 (raw) |
| `get_kosdaq_ohlcv(bas_dd)` | `/sto/ksq_bydd_trd` | KOSDAQ 전 종목 일별 시세 (raw) |
| `get_daily_ohlcv_all(bas_dd)` | 위 두 API 조합 | KOSPI + KOSDAQ 정규화 결과 |

`get_daily_ohlcv_all` 반환 형식:
```python
[{"symbol": "005930.KS", "market": "KOSPI", "date": date(...),
  "open": 71000.0, "high": 72000.0, "low": 70500.0,
  "close": 71500.0, "volume": 12345678}]
```

#### 지수 시세

| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `get_kospi_index(bas_dd)` | `/idx/kospi_dd_trd` | KOSPI 지수 시리즈 전체 (raw) |
| `get_kospi_index_ohlcv(bas_dd)` | 위 API 필터 | KOSPI 종합지수 단일 dict (`symbol="^KS11"`) |
| `get_kosdaq_index(bas_dd)` | `/idx/ksq_dd_trd` | KOSDAQ 지수 시리즈 전체 (raw) |
| `get_kosdaq_index_ohlcv(bas_dd)` | 위 API 필터 | KOSDAQ 종합지수 단일 dict (`symbol="^KQ11"`) |

`get_kospi_index_ohlcv` 반환 형식:
```python
{"symbol": "^KS11", "market": "IDX", "date": date(...),
 "open": 2750.0, "high": 2780.0, "low": 2740.0,
 "close": 2760.0, "volume": 987654321, "prev_close": 2730.0}
```

**미제공**: 외국인·기관 투자자별 순매수. → `krx_flow_sync.py` 사용.

---

## krx_openapi.py 호출처

| 파일 | 함수/컨텍스트 | 사용 메서드 | 목적 |
|------|--------------|------------|------|
| `data/krx_sync.py:103` | `sync_krx_listings()` | `get_kospi_tickers`, `get_kosdaq_tickers` | `krx_listings` 테이블 upsert |
| `core/ohlcv_cache.py:239` | `fill_daily_from_krx()` | `get_daily_ohlcv_all`, `get_kospi_index_ohlcv` | `daily_ohlcv` 캐시 채우기 (백테스트용) |
| `analysis/chart_screener.py:118` | `get_all_tickers()` | `get_all_tickers` | 스크리너 종목 목록 조회 (1순위, FDR fallback) |
| `dashboard/backend/main.py:2546` | `_fetch_krx()` 내부 | `get_kospi_index_ohlcv`, `get_kosdaq_index_ohlcv` | 대시보드 시장 현황 패널 |
| `telegram/telegram_bot.py:165` | `/status` 명령 처리 | `get_kospi_index_ohlcv`, `get_kosdaq_index_ohlcv` | 텔레그램 상태 메시지 코스피·코스닥 등락률 |

(라인 번호는 코드가 바뀌면 드리프트되는 참고값입니다 — 정확한 위치는 함수명으로 검색하세요.)

---

## krx_sync.py

KRX 전 종목 리스트를 `krx_listings` 테이블에 동기화.

### `sync_krx_listings(pool) -> int`

스케줄러에서 호출. `KRX_OPENAPI_KEY` 미설정 시 경고 로그 후 0 반환.

```python
from data.krx_sync import sync_krx_listings
n = await sync_krx_listings(pool)
print(f"동기화: {n}건")
```

KOSPI + KOSDAQ 전 종목 upsert. `isin_code` PK 기준. `yfinance_symbol` 자동 생성 (`005930` → `005930.KS`, KOSDAQ는 `.KQ`).

### DB 테이블: `krx_listings`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `isin_code` | TEXT PK | ISIN (KR7005930003 형식) |
| `short_code` | TEXT | 6자리 종목코드 |
| `name_ko` | TEXT | 한국어 종목명 |
| `name_ko_abbr` | TEXT | 한국어 약칭 |
| `name_en` | TEXT | 영문명 |
| `listed_at` | DATE | 상장일 |
| `market` | TEXT | KOSPI / KOSDAQ |
| `security_type` | TEXT | 증권 구분 |
| `sector` | TEXT | 업종 |
| `stock_type` | TEXT | 보통주/우선주 등 |
| `par_value` | TEXT | 액면가 |
| `listed_shares` | BIGINT | 상장주식수 (`ohlcv_cache.load_listed_shares()`가 사용 — 유통주식수 근사치) |
| `yfinance_symbol` | TEXT | yfinance용 심볼 (005930.KS) |
| `updated_at` | TIMESTAMPTZ | |

---

## krx_flow_sync.py

외국인·기관·개인 순매수 이력을 `daily_flow` 테이블에 적재.

### 백엔드 선택

| 백엔드 | 조건 | 특징 |
|--------|------|------|
| `krx-direct` (**CLI/스케줄러 공통 기본값**) | KRX_ID/KRX_PW 또는 KRX_SESSION | data.krx.co.kr 직접 HTTP 요청. `personal_net` 채움. `KRX_ID`/`KRX_PW` 설정 시 자동 로그인·세션 만료 시 자동 재로그인(2026-07-11부터 정상 동작) — 수동 갱신 불필요. `KRX_SESSION` 브라우저 쿠키만 쓰는 경우엔 만료 시 수동 갱신 필요 |
| `pykrx` | KRX_ID/KRX_PW | pykrx 라이브러리 경유. Python 3.12 이하 권장 |
| `csv` | --csv 파일경로 | 수동 CSV 임포트 |
| `kiwoom` (수동 폴백, 비권장) | KIWOOM_APPKEY/SECRETKEY | ka10045, Bearer 토큰. 브라우저 쿠키 불필요하지만 **`personal_net`이 항상 NULL** — 키움은 단일 증권사라 시장 전체 개인 순매수를 구조적으로 가질 수 없음. `KRX_SESSION` 만료로 krx-direct가 당장 안 될 때만 임시로 사용 |

**주의**: data.krx.co.kr은 한국 ISP 또는 VPN 환경에서만 접근 가능 (kiwoom 백엔드는 해당 없음).

### CLI

```bash
# 증분 모드 (스케줄러 매일 18:00 KST 실행 — krx-direct 기본값)
python data/krx_flow_sync.py --incremental

# krx-direct 벌크 적재 (초기 1회)
python data/krx_flow_sync.py --start 2023-01-01 --end 2026-05-03

# 특정 백엔드
python data/krx_flow_sync.py --start 2025-01-01 --backend pykrx

# CSV 임포트 (수동)
python data/krx_flow_sync.py --csv /path/to/data.csv --backend csv

# kiwoom 수동 폴백 (KRX_SESSION 만료 시, personal_net은 NULL로 남음)
python data/krx_flow_sync.py --start 2026-06-01 --end 2026-06-19 --backend kiwoom

# 응답 구조 확인 (krx-direct 첫 실행 권장)
python data/krx_flow_sync.py --probe 005930

# 로그인 응답 원문 확인 (KRX_ID/KRX_PW 진단, DB 불필요)
python data/krx_flow_sync.py --probe-login

# 이미 저장된 날짜도 덮어쓰기 (kiwoom 폴백으로 비어버린 personal_net 재백필용)
python data/krx_flow_sync.py --start 2025-01-01 --end 2026-05-03 --force

# 시장/티커 수 제한 (테스트)
python data/krx_flow_sync.py --start 2025-01-01 --end 2026-05-03 --market KOSPI --max 50
```

### CSV 형식

컬럼: `date, ticker, foreign_net, inst_net` (`personal_net`은 CSV 백엔드에서 지원하지 않음 — krx-direct/pykrx 백엔드만 채움)

```csv
date,ticker,foreign_net,inst_net
2025-01-02,005930.KS,123456,-78900
2025-01-02,035720.KQ,-5000,12000
```

KRX 다운로드 CSV(한국어 헤더) 자동 감지:
```
날짜,종목코드,종목명,외국인순매수,기관합계
20250102,005930,삼성전자,123456,-78900
```

### DB 테이블: `daily_flow`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (ticker, trade_date) | |
| `foreign_net` | BIGINT | 외국인 순매수 (주) |
| `inst_net` | BIGINT | 기관 순매수 (주) |
| `foreign_streak` | SMALLINT | 외국인 연속 순매수일 (음수 = 순매도) |
| `inst_streak` | SMALLINT | 기관 연속 순매수일 (음수 = 순매도) |
| `personal_net` | BIGINT | 개인 순매수 (주). 음수 = 순매도 |
| `personal_streak` | SMALLINT | 개인 연속 순매수일 |
| `created_at` | TIMESTAMPTZ | |

---

## data/kiwoom_aftermarket_sync.py (스케줄러가 실제로 호출하는 모듈)

시간외 단일가 스냅샷 수집 (키움 REST API ka10032). `jobs/infra_jobs.py:daily_aftermarket_sync_job()`이 서브프로세스로 `--incremental` 실행 (평일 16:05 KST).

**주의**: 키움 REST API는 당일 실시간 데이터만 제공. 과거 날짜 backfill은 아래 `krx_aftermarket_sync.py`를 사용.

### CLI

```bash
# 당일 수집 (15:40~16:00 사이 실행 권장)
python data/kiwoom_aftermarket_sync.py --today

# 날짜 override (데이터는 당일, 적재 날짜만 변경)
python data/kiwoom_aftermarket_sync.py --today --trade-date 2026-05-09

# 시장 필터
python data/kiwoom_aftermarket_sync.py --today --market KOSPI

# 응답 구조 확인 (DB 저장 없음)
python data/kiwoom_aftermarket_sync.py --probe

# mockapi.kiwoom.com 사용 (장외 테스트)
python data/kiwoom_aftermarket_sync.py --today --mock

# 증분 (어제 날짜로 당일 데이터 적재 — 스케줄러 16:05 KST 실행)
python data/kiwoom_aftermarket_sync.py --incremental

# 이미 수집된 날짜도 재수집
python data/kiwoom_aftermarket_sync.py --today --force
```

## data/krx_aftermarket_sync.py (과거 날짜 backfill 전용)

동일 `aftermarket_snap` 테이블을 채우지만 KRX BLD API(data.krx.co.kr)를 사용 — 날짜 범위 조회가 가능해 과거 backfill에만 쓴다. 스케줄러에는 연결돼 있지 않음 (수동 실행).

### CLI

```bash
# 응답 구조 확인 (BLD/컬럼명 검증)
python data/krx_aftermarket_sync.py --probe 2026-05-09

# 단일 날짜 수집
python data/krx_aftermarket_sync.py --date 2026-05-09

# 날짜 범위 backfill
python data/krx_aftermarket_sync.py --start 2026-01-01 --end 2026-05-09

# 전일 데이터만 수집 (스케줄러 미연결 — 수동 실행 시에만 의미 있음)
python data/krx_aftermarket_sync.py --incremental

# 시장 필터 + 종목 수 제한 + 요청 간 딜레이
python data/krx_aftermarket_sync.py --start 2026-01-01 --end 2026-05-09 --market KOSPI --max 50 --delay 2.0
```

### DB 테이블: `aftermarket_snap` (`data/kiwoom_aftermarket_sync.py:104-121` 기준 — 실제 운영 스키마)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (trade_date, ticker) | |
| `reg_close` | NUMERIC(12,0) | 정규장 종가 |
| `after_close` | NUMERIC(12,0) | 시간외 체결가 |
| `after_volume` | BIGINT | 시간외 누적 거래량 |
| `after_value` | BIGINT | 시간외 누적 거래대금 (원) |
| `reg_value` | BIGINT | 정규장 거래대금 (`close × volume`으로 계산, API 미제공) |
| `after_chg_pct` | NUMERIC(6,2) | 시간외 등락률 (%) |
| `fetched_at` | TIMESTAMPTZ | |

`after_value`는 `acc_trde_prica × _VALUE_UNIT` 환산. ka10032 응답의 `acc_trde_prica` 단위는 백만원으로 추정되어 `_VALUE_UNIT = 1_000_000`(`data/kiwoom_aftermarket_sync.py:92`) 적용 — 실제 값이 다르면 이 상수를 조정해야 한다고 코드 주석에 명시되어 있음.

---

## 스케줄러 잡

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `krx_daily_refresh` | 매일 20:00 (요일 제한 없음) | krx_listings 갱신 (종목 상장/폐지 반영) |
| `daily_aftermarket_sync` | 평일 16:05 | 시간외 단일가 스냅샷 |
| `daily_flow_sync` | 평일 18:00 | 외국인·기관·개인 순매수 증분 sync (`--backend krx-direct`) |

---

## 관련 문서

- [KRX 외국인·기관 수급 데이터 임포트 방법](howto-krx-flow-import.md)
- [스케줄러 레퍼런스](reference-scheduler.md)
