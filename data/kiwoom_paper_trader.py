"""
kiwoom_paper_trader.py — 키움 모의투자 주문 실행 및 포지션 추적

API 매핑:
  kt10000  POST /api/dostk/ordr  주식 매수주문
  kt10001  POST /api/dostk/ordr  주식 매도주문
  kt00018  POST /api/dostk/acnt  계좌평가잔고내역요청 (보유종목 + 손익)
  kt00005  POST /api/dostk/acnt  체결잔고요청 (예수금)

모의투자 도메인: https://mockapi.kiwoom.com (KRX만 지원)
환경변수: KIWOOM_MOCK_APPKEY, KIWOOM_MOCK_APPSECRET
"""
from __future__ import annotations

import logging
import math
import os
from datetime import date
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from data.kiwoom_aftermarket_sync import KiwoomClient

logger = logging.getLogger(__name__)

_PAPER_APPKEY    = os.environ.get("KIWOOM_MOCK_APPKEY", "")
_PAPER_SECRETKEY = os.environ.get("KIWOOM_MOCK_APPSECRET", "")
_PAPER_ACCOUNT   = os.environ.get("KIWOOM_MOCK_ACCOUNT", "")
_EXCHANGE        = "KRX"   # mockapi는 KRX만 지원

# 모델별 슬롯 수 / 포지션당 금액(원)
MODEL_CONFIG: dict[str, dict] = {
    "stage":           {"max_slots": 10, "position_krw": 10_000_000},
    "kosdaq":          {"max_slots": 10, "position_krw": 10_000_000},
    "cross":           {"max_slots":  5, "position_krw": 20_000_000},
    "ichimoku":        {"max_slots": 10, "position_krw": 10_000_000},
    "compose-funnel1": {"max_slots": 10, "position_krw": 10_000_000},
    "compose-and1":    {"max_slots":  5, "position_krw": 20_000_000},
    "compose-score1":  {"max_slots":  5, "position_krw": 20_000_000},
}

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
            result.append({
                "stk_cd":     item.get("stk_cd", ""),
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
        """ka10001 주식기본정보요청으로 현재가 반환.

        KRX API는 하락일 종목에 cur_prc를 음수로 반환하므로 abs() 처리.
        Returns None on failure.
        """
        stk_cd = _to_6digit(ticker)
        try:
            data, _ = self._client._post(
                "/api/dostk/stkinfo", "ka10001",
                {"stk_cd": stk_cd},
            )
            raw = data.get("cur_prc", "0") or "0"
            return abs(int(raw))
        except Exception as e:
            logger.warning("[paper] %s 현재가 조회 실패: %s", stk_cd, e)
            return None

    def get_open_price(self, ticker: str) -> Optional[int]:
        """ka10001의 open_pric 필드로 당일 시가 반환."""
        stk_cd = _to_6digit(ticker)
        try:
            data, _ = self._client._post(
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
                CREATE POLICY backend_all ON paper_positions FOR ALL USING (true) WITH CHECK (true);
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


async def get_pending_positions(pool, signal_date: date) -> list[dict]:
    """signal_date 기준 pending 포지션 조회."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM paper_positions WHERE status='pending' AND signal_date=$1",
            signal_date,
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
