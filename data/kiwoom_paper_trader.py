"""
kiwoom_paper_trader.py — 키움 모의투자 주문 실행 및 포지션 추적

API 매핑:
  kt10000  POST /api/dostk/ordr    주식 매수주문 (모의투자 서버)
  kt10001  POST /api/dostk/ordr    주식 매도주문 (모의투자 서버)
  kt00018  POST /api/dostk/acnt    계좌평가잔고내역요청 — 보유종목 + 손익 (모의투자 서버)
  kt00005  POST /api/dostk/acnt    체결잔고요청 — 예수금 (모의투자 서버)
  ka10001  POST /api/dostk/stkinfo 주식기본정보요청 — 현재가/시가 (실 API 서버)

모의투자 서버(mockapi.kiwoom.com)는 주문 체결/계좌 조회만 지원하고
시장 데이터(ka10001)는 지원하지 않아 항상 실패한다 — 그래서 현재가/시가
조회는 실 API 서버(api.kiwoom.com)의 읽기 전용 시세 클라이언트로 분리했다.
주문/계좌 조회는 계속 모의투자 서버를 사용(실제 매매 발생 방지).

환경변수:
  KIWOOM_MOCK_APPKEY, KIWOOM_MOCK_APPSECRET, KIWOOM_MOCK_ACCOUNT  — 모의투자(주문/계좌)
  KIWOOM_APPKEY, KIWOOM_SECRETKEY                                 — 실 API(시세 조회 전용)
"""
from __future__ import annotations

import logging
import math
import os
import time
from datetime import date
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from data.kiwoom_aftermarket_sync import KiwoomClient

logger = logging.getLogger(__name__)

_PAPER_APPKEY    = os.environ.get("KIWOOM_MOCK_APPKEY", "")
_PAPER_SECRETKEY = os.environ.get("KIWOOM_MOCK_APPSECRET", "")
_PAPER_ACCOUNT   = os.environ.get("KIWOOM_MOCK_ACCOUNT", "")
_QUOTE_APPKEY    = os.environ.get("KIWOOM_APPKEY", "")
_QUOTE_SECRETKEY = os.environ.get("KIWOOM_SECRETKEY", "")
_EXCHANGE        = "KRX"   # mockapi는 KRX만 지원

# 모델별 슬롯 수 (분산 종목 수) — 포지션당 금액은 계좌 자산 기준으로 동적 계산
# (compute_slot_krw 참고, 2026-07-31 재설계: 고정 원화 금액이 계좌 실제 규모를
# 무시해 신규 진입이 "매수증거금 부족"으로 상시 실패하는 문제 발견)
MODEL_CONFIG: dict[str, dict] = {
    "stage":           {"max_slots": 10},
    "kosdaq":          {"max_slots": 10},
    "cross":           {"max_slots":  5},
    "ichimoku":        {"max_slots": 10},
    "compose-funnel1": {"max_slots": 10},
    "compose-and1":    {"max_slots":  5},
    "compose-score1":  {"max_slots":  5},
}

# kosdaq: stage_job의 KOSDAQ 분류 버그로 신호가 전혀 생성되지 않음(역대 0건,
# 2026-07-29 investigate 세션 확인). 고쳐지기 전까지 자본 배분에서 제외 —
# 안 그러면 죽은 모델에 1/N 자본이 영구히 묶여 나머지 모델 몫이 줄어든다.
ACTIVE_MODELS: list[str] = [m for m in MODEL_CONFIG if m != "kosdaq"]

# 계좌 총자산(추정예탁자산) 중 신규 진입에 쓰지 않고 현금으로 남겨둘 비중.
# 2026-07-31: 고정 슬롯 금액(모델당 1000만~2000만원 × 슬롯수, 합계 약 7억원)이
# 실제 계좌 규모(약 3.75억, 이미 -18.6% 손실)를 크게 초과해 신규 매수가
# "RC4025 모의투자 매수증거금이 부족합니다"로 상시 실패하는 걸 확인 → 계좌
# 규모에 연동한 배분으로 교체.
CASH_RESERVE_RATIO = 0.20


def compute_slot_krw(balance: dict) -> dict[str, int]:
    """계좌 총자산 기준 모델별 슬롯(1포지션)당 매수 금액 계산.

    배포 가능 자본(총자산 × (1-현금비중))을 활성 모델 수로 균등 분배해 모델별
    "동일 금액"으로 시작하게 하고, 그 금액을 각 모델의 max_slots로 나눠 슬롯당
    금액을 정한다 — 슬롯이 많은 모델(분산 10종목)은 슬롯당 금액이 작고, 슬롯이
    적은 모델(5종목)은 슬롯당 금액이 크지만 모델 총액은 동일하다.
    """
    total_capital = balance["prsm_dpst_aset_amt"]
    deployable = total_capital * (1 - CASH_RESERVE_RATIO)
    per_model_capital = deployable / len(ACTIVE_MODELS)
    return {
        model: int(per_model_capital / MODEL_CONFIG[model]["max_slots"])
        for model in ACTIVE_MODELS
    }


def deployable_capital(balance: dict) -> float:
    """현금 비중을 제외하고 신규 진입에 쓸 수 있는 자본 총액."""
    return balance["prsm_dpst_aset_amt"] * (1 - CASH_RESERVE_RATIO)


# ── 유틸 ─────────────────────────────────────────────────────────────────────

def _to_6digit(ticker: str) -> str:
    """005930.KS → 005930"""
    return ticker.split(".")[0]


def _qty_from_price(position_krw: int, price: float) -> int:
    """포지션 금액 / 현재가 → 주문 수량 (최소 1주)"""
    if price <= 0:
        return 0
    return max(1, math.floor(position_krw / price))


# ── KiwoomPaperTrader ────────────────────────────────────────────────────────

class KiwoomPaperTrader:
    """키움 모의투자 서버(mockapi.kiwoom.com)에 주문을 제출하고 잔고를 조회."""

    def __init__(self) -> None:
        if not _PAPER_APPKEY or not _PAPER_SECRETKEY:
            raise RuntimeError(
                "KIWOOM_MOCK_APPKEY / KIWOOM_MOCK_APPSECRET 환경변수 미설정"
            )
        self._client = KiwoomClient(use_mock=True)
        self._client.issue_token(_PAPER_APPKEY, _PAPER_SECRETKEY)
        logger.info("[paper] 키움 모의투자 클라이언트 초기화 완료")

        # 시세 조회 전용 실 API 클라이언트 — 모의투자 서버는 ka10001(시장데이터)을
        # 지원하지 않아 항상 실패했다. 주문/계좌는 계속 self._client(모의투자) 사용.
        self._quote_client: Optional[KiwoomClient] = None
        if _QUOTE_APPKEY and _QUOTE_SECRETKEY:
            try:
                self._quote_client = KiwoomClient(use_mock=False)
                self._quote_client.issue_token(_QUOTE_APPKEY, _QUOTE_SECRETKEY)
                logger.info("[paper] 실 API 시세 클라이언트 초기화 완료 (ka10001)")
            except Exception as e:
                logger.warning("[paper] 실 API 시세 클라이언트 초기화 실패 — 시세 조회 불가: %s", e)
                self._quote_client = None
        else:
            logger.warning(
                "[paper] KIWOOM_APPKEY/KIWOOM_SECRETKEY 미설정 — 현재가/시가 조회 불가 "
                "(모의투자 서버는 ka10001 미지원)"
            )

    # ── 주문 ─────────────────────────────────────────────────────────────────

    def place_buy(
        self,
        ticker: str,
        qty: int,
        trde_tp: str = "3",   # 3=시장가, 0=보통(지정가)
        price: str = "",
    ) -> str:
        """kt10000 매수주문. 주문번호(ord_no) 반환."""
        stk_cd = _to_6digit(ticker)
        data, _ = self._client._post(
            "/api/dostk/ordr", "kt10000",
            {
                "acnt_no":      _PAPER_ACCOUNT,
                "dmst_stex_tp": _EXCHANGE,
                "stk_cd":       stk_cd,
                "ord_qty":      str(qty),
                "ord_uv":       price,        # 시장가면 빈 문자열
                "trde_tp":      trde_tp,
                "cond_uv":      "",
            },
        )
        ord_no = data.get("ord_no", "")
        logger.info("[paper] 매수주문 %s %d주 → 주문번호=%s", stk_cd, qty, ord_no)
        return ord_no

    def place_sell(
        self,
        ticker: str,
        qty: int,
        trde_tp: str = "3",
        price: str = "",
    ) -> str:
        """kt10001 매도주문. 주문번호(ord_no) 반환."""
        stk_cd = _to_6digit(ticker)
        data, _ = self._client._post(
            "/api/dostk/ordr", "kt10001",
            {
                "acnt_no":      _PAPER_ACCOUNT,
                "dmst_stex_tp": _EXCHANGE,
                "stk_cd":       stk_cd,
                "ord_qty":      str(qty),
                "ord_uv":       price,
                "trde_tp":      trde_tp,
                "cond_uv":      "",
            },
        )
        ord_no = data.get("ord_no", "")
        logger.info("[paper] 매도주문 %s %d주 → 주문번호=%s", stk_cd, qty, ord_no)
        return ord_no

    def cancel_order(self, ticker: str, orig_ord_no: str, cncl_qty: int = 0) -> str:
        """kt10003 주식 취소주문. 원주문번호로 미체결분을 취소하고 새 주문번호 반환.

        cncl_qty=0(기본값)이면 잔량 전부 취소(API 스펙: "'0' 입력시 잔량 전부
        취소" — docs/02_reference/키움 REST API 문서.pdf p.429). 부분체결된
        주문도 잔량만 취소되고 이미 체결된 수량은 그대로 유지된다.
        """
        stk_cd = _to_6digit(ticker)
        data, _ = self._client._post(
            "/api/dostk/ordr", "kt10003",
            {
                "dmst_stex_tp": _EXCHANGE,
                "orig_ord_no":  orig_ord_no,
                "stk_cd":       stk_cd,
                "cncl_qty":     str(cncl_qty),
            },
        )
        ord_no = data.get("ord_no", "")
        logger.info("[paper] 취소주문 %s 원주문=%s 취소수량=%s → 신규주문번호=%s",
                    stk_cd, orig_ord_no, cncl_qty, ord_no)
        return ord_no

    def check_execution(self, ticker: str, ord_no: str, is_buy: bool) -> int:
        """ka10076 체결요청으로 특정 주문의 실제 체결 수량을 조회.

        place_buy/place_sell과 같은 계좌 패밀리(/api/dostk/acnt)라 모의투자
        서버(self._client)를 그대로 쓴다. 조회 실패 시 예외를 전파하지 않고
        0을 반환한다 — 호출부(confirm_fill)가 재시도 루프를 돌리기 때문.
        """
        stk_cd = _to_6digit(ticker)
        try:
            data, _ = self._client._post(
                "/api/dostk/acnt", "ka10076",
                {
                    "stk_cd":  stk_cd,
                    "qry_tp":  "1",                      # 1:종목
                    "sell_tp": "2" if is_buy else "1",    # 2:매수, 1:매도
                    "ord_no":  ord_no,
                    "stex_tp": "0",                       # 0:전체
                },
            )
        except Exception as e:
            logger.warning("[paper] %s 체결 확인 실패(ord_no=%s): %s", stk_cd, ord_no, e)
            return 0

        filled = 0
        for row in data.get("cntr") or []:
            try:
                filled += int(str(row.get("cntr_qty", "0")).strip() or "0")
            except ValueError:
                pass
        return filled

    def get_position_qty(self, ticker: str) -> int:
        """get_positions()에서 특정 티커의 계좌 합산 보유수량 조회 (없으면 0).

        다른 모델이 동시에 같은 티커를 보유 중이어도 이 값은 계좌 전체 합산이므로,
        체결 확인은 항상 주문 전/후 스냅샷의 델타로 판단해야 한다(confirm_fill 참고).
        """
        stk_cd = _to_6digit(ticker)
        for p in self.get_positions():
            if p["stk_cd"] == stk_cd:
                return p["rmnd_qty"]
        return 0

    def confirm_fill(
        self, ticker: str, ord_no: str, qty: int, is_buy: bool, qty_before: int,
        attempts: int = 3, delay_s: float = 1.5,
    ) -> int:
        """주문 전/후 계좌 보유수량 델타로 체결 수량을 추정, 최대 attempts회 폴링.

        누적 체결량이 qty 이상이면 즉시 종료. 동기 함수 — place_buy/place_sell과
        마찬가지로 호출부에서 run_in_executor로 감싼다.

        원래는 ka10076(체결요청, check_execution())으로 확인했으나, 이 모의투자
        계좌에서는 신규 주문 직후든 몇 시간 뒤든 항상 빈 체결내역(cntr=[])만
        반환하는 것으로 확인됐다(2026-08-05 라이브 검증 — 실제로는 100% 체결된
        21건 전부가 미확인 처리되며 DB status가 broker 실보유와 어긋남). 그래서
        get_positions()(kt00018) 스냅샷 비교로 대체한다.

        qty_before는 호출부가 place_buy/place_sell 제출 **직전**에
        get_position_qty()로 조회해 전달해야 한다 — 델타 계산이라 같은 티커를
        동시에 보유한 다른 모델의 몫과 무관하게 이 주문이 실제로 바꾼 수량만 잡아낸다.
        """
        filled = 0
        qty_now = qty_before
        for i in range(attempts):
            if i > 0:
                time.sleep(delay_s)
            qty_now = self.get_position_qty(ticker)
            filled = max(0, (qty_now - qty_before) if is_buy else (qty_before - qty_now))
            if filled >= qty:
                break
        side = "매수" if is_buy else "매도"
        logger.info("[paper] %s %s 체결 확인: %d/%d주 (주문번호=%s, 보유수량 %d→%d)",
                    ticker, side, filled, qty, ord_no, qty_before, qty_now)
        return filled

    # ── 계좌 조회 ────────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict]:
        """kt00018 계좌평가잔고내역요청. 보유종목 리스트 반환.

        Returns:
            list of {stk_cd, stk_nm, cur_prc, rmnd_qty, evltv_prft, prft_rt, pur_pric}
        """
        data, _ = self._client._post(
            "/api/dostk/acnt", "kt00018",
            {"qry_tp": "2", "dmst_stex_tp": _EXCHANGE},
        )
        items = data.get("acnt_evlt_remn_indv_tot", [])
        result = []
        for item in items:
            # kt00018은 종목코드를 "A005930"처럼 거래소 접두사를 붙여 반환한다.
            # 반면 주문 제출(kt10000/kt10001)과 _to_6digit()은 접두사 없는 6자리를
            # 쓰므로, 여기서 정규화해두지 않으면 get_position_qty()의 stk_cd 비교가
            # 항상 실패해 보유수량이 0으로 잡힌다 (confirm_fill() 전체를 무력화시킨
            # 버그 — 2026-08-05 도입 이후 매수/매도 체결 확인이 전부 오탐 처리됨,
            # 2026-08-10 investigate 세션에서 발견).
            _stk_cd = item.get("stk_cd", "")
            if _stk_cd.startswith("A") and _stk_cd[1:].isdigit():
                _stk_cd = _stk_cd[1:]
            result.append({
                "stk_cd":     _stk_cd,
                "stk_nm":     item.get("stk_nm", ""),
                "cur_prc":    int(item.get("cur_prc", "0") or 0),
                "rmnd_qty":   int(item.get("rmnd_qty", "0") or 0),
                "evltv_prft": int(item.get("evltv_prft", "0") or 0),
                "prft_rt":    float(item.get("prft_rt", "0") or 0),
                "pur_pric":   int(item.get("pur_pric", "0") or 0),
            })
        logger.info("[paper] 보유종목 %d개 조회", len(result))
        return result

    def get_balance(self) -> dict:
        """kt00018 계좌평가잔고 요약 (예수금/총손익).
        kt00005는 모의투자 미지원이므로 kt00018 합산 조회로 대체.
        """
        data, _ = self._client._post(
            "/api/dostk/acnt", "kt00018",
            {"qry_tp": "1", "dmst_stex_tp": _EXCHANGE},
        )
        return {
            "tot_pur_amt":        int(data.get("tot_pur_amt", "0") or 0),        # 총매입금액
            "tot_evlt_amt":       int(data.get("tot_evlt_amt", "0") or 0),       # 총평가금액
            "tot_evlt_pl":        int(data.get("tot_evlt_pl", "0") or 0),        # 총평가손익
            "tot_prft_rt":        float(data.get("tot_prft_rt", "0") or 0),      # 총수익률(%)
            "prsm_dpst_aset_amt": int(data.get("prsm_dpst_aset_amt", "0") or 0),# 추정예탁자산
        }

    def get_current_price(self, ticker: str) -> Optional[int]:
        """ka10001 주식기본정보요청으로 현재가 반환 (실 API 서버 — 모의투자 서버는 시장데이터 미지원).

        KRX API는 하락일 종목에 cur_prc를 음수로 반환하므로 abs() 처리.
        Returns None on failure (실 API 클라이언트 미설정 포함).
        """
        stk_cd = _to_6digit(ticker)
        if self._quote_client is None:
            logger.warning("[paper] %s 현재가 조회 불가 — 실 API 시세 클라이언트 미설정", stk_cd)
            return None
        try:
            data, _ = self._quote_client._post(
                "/api/dostk/stkinfo", "ka10001",
                {"stk_cd": stk_cd},
            )
            raw = data.get("cur_prc", "0") or "0"
            return abs(int(raw))
        except Exception as e:
            logger.warning("[paper] %s 현재가 조회 실패: %s", stk_cd, e)
            return None

    def get_open_price(self, ticker: str) -> Optional[int]:
        """ka10001의 open_pric 필드로 당일 시가 반환 (실 API 서버)."""
        stk_cd = _to_6digit(ticker)
        if self._quote_client is None:
            logger.warning("[paper] %s 시가 조회 불가 — 실 API 시세 클라이언트 미설정", stk_cd)
            return None
        try:
            data, _ = self._quote_client._post(
                "/api/dostk/stkinfo", "ka10001",
                {"stk_cd": stk_cd},
            )
            raw = data.get("open_pric", "0") or "0"
            return abs(int(raw))
        except Exception as e:
            logger.warning("[paper] %s 시가 조회 실패: %s", stk_cd, e)
            return None


# ── DB 스키마 ─────────────────────────────────────────────────────────────────

_CREATE_PAPER_POSITIONS = """
CREATE TABLE IF NOT EXISTS paper_positions (
    id              BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model           VARCHAR(20)     NOT NULL,
    ticker          VARCHAR(20)     NOT NULL,
    signal_date     DATE            NOT NULL,
    entry_theory    FLOAT           NOT NULL,
    entry_actual    FLOAT,
    slippage_pct    FLOAT,
    qty             INTEGER,
    kiwoom_buy_no   VARCHAR(20),
    kiwoom_sell_no  VARCHAR(20),
    tp1_pct         FLOAT           NOT NULL DEFAULT 0.15,
    tp1_ratio       FLOAT           NOT NULL DEFAULT 0.50,
    trail_pct       FLOAT           NOT NULL DEFAULT 0.10,
    hard_stop_pct   FLOAT           NOT NULL DEFAULT 0.10,
    tp1_date        DATE,
    tp1_price       FLOAT,
    watermark       FLOAT,
    exit_date       DATE,
    exit_price      FLOAT,
    exit_type       VARCHAR(20),
    blended_return  FLOAT,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pp_model  ON paper_positions (model);
CREATE INDEX IF NOT EXISTS idx_pp_status ON paper_positions (status);
CREATE INDEX IF NOT EXISTS idx_pp_ticker ON paper_positions (ticker, signal_date);
"""

# 기존 테이블(구 버전)에 새로 추가된 컬럼을 멱등 적용.
# SERIAL → IDENTITY 변경은 새 테이블에만 적용; 기존 SERIAL PK는 그대로 유지.
_MIGRATE_PAPER_POSITIONS = """
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS kiwoom_buy_no  VARCHAR(20);
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS kiwoom_sell_no VARCHAR(20);
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS tp1_pct        FLOAT NOT NULL DEFAULT 0.15;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS tp1_ratio      FLOAT NOT NULL DEFAULT 0.50;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS trail_pct      FLOAT NOT NULL DEFAULT 0.10;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS hard_stop_pct  FLOAT NOT NULL DEFAULT 0.10;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS tp1_date       DATE;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS tp1_price      FLOAT;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS watermark      FLOAT;
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS blended_return FLOAT;
"""


async def init_paper_positions(pool) -> None:
    """paper_positions 테이블 생성 + 컬럼 마이그레이션 + RLS 활성화 (멱등)."""
    async with pool.acquire() as conn:
        for stmt in _CREATE_PAPER_POSITIONS.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        for stmt in _MIGRATE_PAPER_POSITIONS.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        await conn.execute(
            "ALTER TABLE paper_positions ENABLE ROW LEVEL SECURITY;"
        )
        await conn.execute("""
            DO $$ BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_policies
                WHERE schemaname='public' AND tablename='paper_positions' AND policyname='backend_all'
              ) THEN
                CREATE POLICY backend_all ON paper_positions FOR ALL TO service_role USING (true) WITH CHECK (true);
              ELSE
                ALTER POLICY backend_all ON paper_positions TO service_role;
              END IF;
            END $$;
        """)
    logger.info("[paper] paper_positions 테이블 준비 완료")


async def insert_pending(
    pool,
    model: str,
    ticker: str,
    signal_date: date,
    entry_theory: float,
    tp1_pct: float = 0.15,
    tp1_ratio: float = 0.50,
    trail_pct: float = 0.10,
    hard_stop_pct: float = 0.10,
) -> int:
    """pending 포지션 삽입. 삽입된 id 반환."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO paper_positions
                (model, ticker, signal_date, entry_theory,
                 tp1_pct, tp1_ratio, trail_pct, hard_stop_pct, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
            RETURNING id
            """,
            model, ticker, signal_date, entry_theory,
            tp1_pct, tp1_ratio, trail_pct, hard_stop_pct,
        )
        return row["id"]


async def get_pending_positions(pool) -> list[dict]:
    """미체결 pending 포지션 전체 조회 (signal_date 무관).

    날짜별로 끊어서 조회하면 특정 회차의 진입 실패(API 오류 등)가
    이후 실행에서 더 최근 날짜의 pending에 가려져 영구히 재시도되지
    않는 문제가 있었다 — 그래서 status만으로 전체를 가져와 매 실행마다
    밀린 건까지 재시도한다.

    오래 방치되어 신호 유효성이 의심되는 건은 status='held'로 전환해
    이 조회에서 제외한다 (재검토 후 수동으로 'pending' 복귀 필요).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM paper_positions WHERE status='pending' ORDER BY signal_date ASC",
        )
    return [dict(r) for r in rows]


async def update_to_open(
    pool,
    pos_id: int,
    entry_actual: float,
    qty: int,
    order_no: str,
) -> None:
    """pending → open: 실제 체결가, 수량, 주문번호 기록."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE paper_positions
            SET status='open', entry_actual=$1, qty=$2,
                kiwoom_buy_no=$3,
                slippage_pct=($1 - entry_theory) / NULLIF(entry_theory, 0),
                watermark=$1
            WHERE id=$4
            """,
            entry_actual, qty, order_no, pos_id,
        )


async def get_open_positions(pool) -> list[dict]:
    """현재 open 상태 포지션 전체 조회."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM paper_positions WHERE status='open' ORDER BY signal_date"
        )
    return [dict(r) for r in rows]


async def update_to_closed(
    pool,
    pos_id: int,
    exit_price: float,
    exit_type: str,
    order_no: str,
    blended_return: Optional[float] = None,
) -> None:
    """open → closed: 청산가, exit_type, 주문번호 기록."""
    from datetime import date as _date
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE paper_positions
            SET status='closed', exit_date=$1, exit_price=$2,
                exit_type=$3, kiwoom_sell_no=$4, blended_return=$5
            WHERE id=$6
            """,
            _date.today(), exit_price, exit_type, order_no, blended_return, pos_id,
        )


async def get_open_slot_count(pool, model: str) -> int:
    """현재 model의 open+pending 포지션 수."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) FROM paper_positions WHERE model=$1 AND status IN ('open','pending')",
            model,
        )
    return row["count"]


# ── 빠른 동작 테스트 ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    trader = KiwoomPaperTrader()

    # 계좌 요약
    bal = trader.get_balance()
    print("\n[계좌 요약]")
    for k, v in bal.items():
        print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")

    # 보유 포지션
    positions = trader.get_positions()
    print(f"\n[보유종목] {len(positions)}개")
    for p in positions[:5]:
        print(f"  {p['stk_nm']}({p['stk_cd']}) {p['rmnd_qty']}주 현재가={p['cur_prc']:,} 손익률={p['prft_rt']}%")

    # 삼성전자 현재가 조회
    price = trader.get_current_price("005930")
    print(f"\n[삼성전자 현재가] {price:,}원" if price else "\n[현재가 조회 실패]")

    # 테스트 매수주문 (장 중에만 동작, 주석 해제 시 실제 모의주문 발생)
    # ord_no = trader.place_buy("005930", qty=1)
    # print(f"\n[테스트 매수] 주문번호={ord_no}")
