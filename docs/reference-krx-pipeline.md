# KRX 데이터 파이프라인 레퍼런스

KRX(한국거래소) 데이터를 수집하는 4개 모듈의 완전한 기술 레퍼런스.

## 모듈 역할

| 모듈 | 역할 | 데이터 소스 |
|------|------|-------------|
| `krx_openapi.py` | OHLCV, 종목기본정보, 지수 수집 | KRX OpenAPI (openapi.krx.co.kr) |
| `krx_sync.py` | 전 종목 리스트 → `krx_listings` 동기화 | KRX OpenAPI |
| `krx_flow_sync.py` | 외국인·기관 순매수 → `daily_flow` | data.krx.co.kr (직접 크롤) 또는 pykrx |
| `krx_aftermarket_sync.py` | 시간외 단일가 스냅샷 → `aftermarket_snap` | KRX BLD API (data.krx.co.kr) |

---

## 환경 변수

| 변수 | 모듈 | 설명 |
|------|------|------|
| `KRX_OPENAPI_KEY` | krx_openapi, krx_sync | KRX OpenAPI 인증키. openapi.krx.co.kr 가입 후 발급 |
| `KRX_ID` | krx_flow_sync | data.krx.co.kr 로그인 아이디 |
| `KRX_PW` | krx_flow_sync | data.krx.co.kr 로그인 비밀번호 |

---

## krx_openapi.py

### `KRXOpenAPIClient`

```python
from data.krx_openapi import KRXOpenAPIClient

client = KRXOpenAPIClient()  # KRX_OPENAPI_KEY 환경변수 사용
# 또는
client = KRXOpenAPIClient(appkey="YOUR_KEY")
```

Base URL: `https://data-dbg.krx.co.kr/svc/apis`  
인증: `AUTH_KEY` 헤더. Rate limit: 10 req/s.

#### `get_stock_listing(bas_dd) -> list[dict]`

KOSPI/KOSDAQ 전 종목 기본정보 조회.

```python
items = client.get_stock_listing("20260529")
# [{"isin": "KR7005930003", "short_code": "005930", "name_ko": "삼성전자", ...}]
```

#### `get_stock_ohlcv(bas_dd, market="KOSPI") -> list[dict]`

일별 OHLCV (시가/고가/저가/종가/거래량). `market`: `"KOSPI"` 또는 `"KOSDAQ"`.

#### `get_index_ohlcv(bas_dd, index_code="1") -> list[dict]`

지수 시세. `index_code="1"` → KOSPI 종합지수.

**미제공**: 외국인·기관 투자자별 순매수. → `krx_flow_sync.py` 사용.

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
| `sector` | TEXT | 업종 |
| `yfinance_symbol` | TEXT | yfinance용 심볼 (005930.KS) |
| `updated_at` | TIMESTAMPTZ | |

---

## krx_flow_sync.py

외국인·기관 순매수 이력을 `daily_flow` 테이블에 적재.

### 백엔드 선택

| 백엔드 | 조건 | 특징 |
|--------|------|------|
| `krx-direct` (기본, 권장) | KRX_ID/KRX_PW | data.krx.co.kr 직접 HTTP 요청. Python 3.14 호환 |
| `pykrx` | KRX_ID/KRX_PW | pykrx 라이브러리 경유. Python 3.12 이하 권장 |
| `csv` | --csv 파일경로 | 수동 CSV 임포트 |

**주의**: data.krx.co.kr은 한국 ISP 또는 VPN 환경에서만 접근 가능. 해외 IP 차단.

### CLI

```bash
# 벌크 적재 (초기 1회)
python data/krx_flow_sync.py --start 2023-01-01 --end 2026-05-03

# 특정 백엔드
python data/krx_flow_sync.py --start 2025-01-01 --backend pykrx

# CSV 임포트 (수동)
python data/krx_flow_sync.py --csv /path/to/data.csv --backend csv

# 응답 구조 확인 (첫 실행 권장)
python data/krx_flow_sync.py --probe 005930

# 증분 모드 (스케줄러 매일 18:00 KST)
python data/krx_flow_sync.py --incremental

# 시장/티커 수 제한 (테스트)
python data/krx_flow_sync.py --start 2025-01-01 --end 2026-05-03 --market KOSPI --max 50
```

### CSV 형식

컬럼: `date, ticker, foreign_net, inst_net`

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
| PK | (trade_date, ticker) | |
| `foreign_net` | BIGINT | 외국인 순매수 (주) |
| `inst_net` | BIGINT | 기관 순매수 (주) |
| `updated_at` | TIMESTAMPTZ | |

---

## krx_aftermarket_sync.py

시간외 단일가 스냅샷 수집 (data.krx.co.kr BLD API).

**주의**: KRX BLD API는 당일 실시간 데이터만 제공. 과거 날짜 backfill은 이 모듈로 불가능.

### CLI

```bash
# 당일 수집 (15:40~16:00 사이 실행 권장)
python data/krx_aftermarket_sync.py --today

# 날짜 override (데이터는 당일, 적재 날짜만 변경)
python data/krx_aftermarket_sync.py --today --trade-date 2026-05-09

# 시장 필터
python data/krx_aftermarket_sync.py --today --market KOSPI

# 응답 구조 확인
python data/krx_aftermarket_sync.py --probe

# 증분 (어제 날짜로 당일 데이터 적재 — 스케줄러 16:05 KST 실행)
python data/krx_aftermarket_sync.py --incremental
```

### DB 테이블: `aftermarket_snap`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (trade_date, ticker) | |
| `reg_close` | NUMERIC | 정규장 종가 |
| `after_close` | NUMERIC | 시간외 체결가 |
| `after_volume` | BIGINT | 시간외 누적 거래량 |
| `after_value` | BIGINT | 시간외 누적 거래대금 (원) |
| `after_chg_pct` | NUMERIC | 시간외 등락률 (%) |

`after_value`는 `acc_trde_prica × 10,000`원 환산. API 응답 단위가 만원이므로 `_VALUE_UNIT = 10_000` 상수 적용.

---

## 스케줄러 잡

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `krx_daily_refresh` | 평일 20:00 | krx_listings 갱신 (종목 상장/폐지 반영) |
| `daily_aftermarket_sync` | 평일 16:05 | 시간외 단일가 스냅샷 |
| `daily_flow_sync` | 평일 18:00 | 외국인·기관 순매수 증분 sync |

---

## 관련 문서

- [KRX 외국인·기관 수급 데이터 임포트 방법](howto-krx-flow-import.md)
- [스케줄러 레퍼런스](reference-scheduler.md)
