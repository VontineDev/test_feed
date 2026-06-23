# 키움 연동 레퍼런스

키움 REST API와 키움 모의투자 서버를 통해 시간외 스냅샷 수집과 모의투자 주문을 처리하는 2개 모듈.

## 모듈 역할

| 모듈 | 역할 |
|------|------|
| `data/kiwoom_aftermarket_sync.py` | 실 계좌 API로 시간외 단일가 스냅샷 수집 |
| `data/kiwoom_paper_trader.py` | 모의투자 서버에 주문 제출 및 포지션 추적 |

---

## 환경 변수

| 변수 | 모듈 | 설명 |
|------|------|------|
| `KIWOOM_APPKEY` | kiwoom_aftermarket_sync | 키움 실 API 앱키 |
| `KIWOOM_SECRETKEY` | kiwoom_aftermarket_sync | 키움 실 API 시크릿키 |
| `KIWOOM_TOKEN` | kiwoom_aftermarket_sync | 미리 발급된 토큰 직접 주입 (선택) |
| `KIWOOM_MOCK_APPKEY` | kiwoom_paper_trader | 모의투자 앱키 |
| `KIWOOM_MOCK_APPSECRET` | kiwoom_paper_trader | 모의투자 시크릿키 |
| `KIWOOM_MOCK_ACCOUNT` | kiwoom_paper_trader | 모의투자 계좌번호 |

`KIWOOM_MOCK_APPKEY` 미설정 시 스케줄러에서 모의투자 잡 전체 비활성화.

---

## kiwoom_aftermarket_sync.py

키움 REST API로 당일 시간외 단일가 데이터를 수집합니다.

### 도메인 및 API

- 실 API: `https://openapi.kiwoom.com`
- 인증: `au10001 POST /oauth2/token` (Bearer Token)
- 토큰 폐기: `au10002 POST /oauth2/revoke`

### 수집 API

| API 코드 | 엔드포인트 | 설명 |
|----------|-----------|------|
| `ka10098` | `POST /api/dostk/rkinfo` | 시간외단일가등락률조회 (bulk, KOSPI/KOSDAQ 전체) |
| `ka10087` | `POST /api/dostk/mrkcond` | 시간외단일호가조회 (per-stock) |

`ka10098` 2회 호출(KOSPI + KOSDAQ)로 전 종목 커버.

### `KiwoomClient` (`data/kiwoom_aftermarket_sync.py`에 정의 — `kiwoom_paper_trader.py`가 import해서 재사용)

모든 메서드는 동기(`def`)이며 내부적으로 `_post()`로 REST 호출한다 (`async def` 아님).

```python
class KiwoomClient:
    def issue_token(appkey, secretkey) -> str        # au10001, 토큰 발급 및 캐싱
    def revoke_token(appkey, secretkey) -> None        # au10002
    def fetch_aftermarket_bulk(market, ...) -> list[dict]  # ka10098, KOSPI or KOSDAQ
    def fetch_aftermarket_single(stk_cd) -> dict        # ka10087, 단일 종목
    def fetch_portfolio_balance(qry_tp="1", ...) -> dict  # kt00018, 계좌평가잔고
    def fetch_cash_detail(qry_tp="3") -> dict            # kt00001, 예수금상세현황 (실 계좌용)
    def fetch_top_volume(n=20, ...) -> list[dict]       # 대시보드 /top 탭용
```

### CLI

```bash
# 당일 수집 (15:40~16:00 사이 실행 권장)
python data/kiwoom_aftermarket_sync.py --today

# 증분 (스케줄러 16:05 KST)
python data/kiwoom_aftermarket_sync.py --incremental

# 응답 구조 확인 (DB 저장 없음)
python data/kiwoom_aftermarket_sync.py --probe

# 날짜 override / 시장 필터 / mock 서버 / 재수집
python data/kiwoom_aftermarket_sync.py --today --trade-date 2026-05-09
python data/kiwoom_aftermarket_sync.py --today --market KOSPI
python data/kiwoom_aftermarket_sync.py --today --mock
python data/kiwoom_aftermarket_sync.py --today --force
```

**주의**: 키움 REST API는 당일 실시간 데이터만 제공. 과거 날짜 backfill 불가능 → `krx_aftermarket_sync.py` 사용.

---

## kiwoom_paper_trader.py

키움 모의투자 서버(`https://mockapi.kiwoom.com`)에 주문을 제출합니다. 실 계좌와 독립적으로 동작. KRX만 지원.

### `KiwoomPaperTrader`

모든 메서드는 동기(`def`)이다 (`async def` 아님). `jobs/paper_jobs.py`에서 `loop.run_in_executor()`로 비동기 컨텍스트에 올린다.

```python
class KiwoomPaperTrader:
    def place_buy(ticker, qty, trde_tp="3", price="") -> str   # kt10000, 주문번호 반환
    def place_sell(ticker, qty, trde_tp="3", price="") -> str   # kt10001, 주문번호 반환
    def get_positions() -> list[dict]                            # kt00018(qry_tp=2), 보유종목 리스트
    def get_balance() -> dict                                    # kt00018(qry_tp=1), 예수금/총손익 요약
    def get_current_price(ticker) -> Optional[int]               # ka10001, 현재가
    def get_open_price(ticker) -> Optional[int]                  # ka10001, 당일 시가
```

`get_balance()`는 `kt00005`(체결잔고요청)가 모의투자에서 미지원이라 `kt00018`을 합산 조회(`qry_tp="1"`)해 대체한다.

### API 매핑

| API 코드 | 엔드포인트 | 설명 |
|----------|-----------|------|
| `kt10000` | `POST /api/dostk/ordr` | 주식 매수주문 |
| `kt10001` | `POST /api/dostk/ordr` | 주식 매도주문 |
| `kt00018` | `POST /api/dostk/acnt` | 계좌평가잔고내역 (`qry_tp=2`: 보유종목, `qry_tp=1`: 합산 요약) |
| `kt00001` | `POST /api/dostk/acnt` | 예수금상세현황요청 (실 계좌 전용 — `fetch_cash_detail()`) |

`kt00005`는 모의투자 미지원이라 코드에서 호출하지 않는다 (`get_balance()`가 `kt00018`로 대체).

### `MODEL_CONFIG`

모의투자 슬롯과 포지션당 금액을 모델별로 설정합니다.

```python
MODEL_CONFIG = {
    "stage":           {"max_slots": 10, "position_krw": 10_000_000},
    "kosdaq":          {"max_slots": 10, "position_krw": 10_000_000},
    "cross":           {"max_slots":  5, "position_krw": 20_000_000},
    "ichimoku":        {"max_slots": 10, "position_krw": 10_000_000},
    "compose-funnel1": {"max_slots": 10, "position_krw": 10_000_000},
    "compose-and1":    {"max_slots":  5, "position_krw": 20_000_000},
    "compose-score1":  {"max_slots":  5, "position_krw": 20_000_000},
}
```

각 모델은 최대 `max_slots`개 포지션을 보유하고, 신규 진입 시 `position_krw`만큼 주문합니다. 주문 수량: `floor(position_krw / current_price)`, 최소 1주.

### 티커 변환

yfinance 심볼(`005930.KS`) → 6자리 코드(`005930`) 자동 변환. `_to_6digit()` 유틸 함수.

---

## 스케줄러 잡 (모의투자)

`paper_open_entry`/`paper_exit_checker`/`paper_eod_sampler`는 `KIWOOM_MOCK_APPKEY` 설정 시에만 등록됩니다. `compose_paper_entry`는 예외로 DB 풀만 있으면 등록됩니다 (Kiwoom 계정 불필요).

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `paper_open_entry` | 평일 09:05 | T+1 진입 주문 실행 |
| `paper_exit_checker` | 평일 15:20 (정규장 마감 직전) | 익절/손절 조건 확인 → 시장가 매도 |
| `paper_eod_sampler` | 평일 16:40 | 일별 포지션 스냅샷 저장 |
| `compose_paper_entry` | 일요일 21:15 | FUNNEL-1/AND-1/SCORE-1 주간 신호 → pending 적재 (DB만 필요, Kiwoom 불필요) |

---

## 관련 문서

- [키움 모의투자 설정 방법](howto-kiwoom-paper-trade.md)
- [스케줄러 레퍼런스](reference-scheduler.md)
