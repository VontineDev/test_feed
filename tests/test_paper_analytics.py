"""
test_paper_analytics.py — /api/paper/curve + /api/paper/export 단위 테스트

커버리지:
  /api/paper/curve  — 정상(closed+open), closed 없음, open 없음(yfinance 미호출)
  /api/paper/export — 정상(CSV 헤더+데이터, BOM), 빈 테이블(헤더만)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# dashboard/backend/를 path에 추가해야 `from database import ...`가 동작함
_BACKEND = Path(__file__).parent.parent / "dashboard" / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))


# ── 공통 픽스처 ──────────────────────────────────────────────

def _make_pool(*fetch_returns):
    """asyncpg Pool mock.

    fetch_returns: conn.fetch() 호출 순서에 맞게 side_effect로 전달.
    """
    conn = AsyncMock()
    if len(fetch_returns) == 1:
        conn.fetch = AsyncMock(return_value=fetch_returns[0])
    else:
        conn.fetch = AsyncMock(side_effect=list(fetch_returns))
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=None)

    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


def _row(**kwargs):
    """asyncpg Record처럼 동작하는 mock row.

    asyncpg Record는 for v in row 시 값(values)을 반환함.
    dict() 변환, r["key"] 접근 모두 지원.
    """
    class FakeRow(dict):
        def __iter__(self):
            # asyncpg Record: 이터레이션 → values
            return iter(self.values())

        def __getattr__(self, k):
            try:
                return self[k]
            except KeyError:
                raise AttributeError(k)
    return FakeRow(kwargs)


# ── /api/paper/curve ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_curve_happy_path():
    """closed 포지션 + open 포지션 → series/model_stats/open_positions 반환."""
    import routers_paper as paper_mod  # 라우터 분리(2026-07) — patch는 소유 모듈에

    # Query 1: 누적 시계열 (stage 모델, 2건)
    series_rows = [
        _row(model='stage', date='2026-04-15', cumulative=Decimal('0.05')),
        _row(model='stage', date='2026-04-20', cumulative=Decimal('0.12')),
    ]
    # Query 2: 집계 통계
    stats_rows = [
        _row(model='stage', n_trades=2, n_wins=2,
             avg_win=Decimal('0.085'), avg_loss=None,
             total_realized=Decimal('0.12')),
    ]
    # Query 3: ticker_name_map
    name_rows = [
        _row(ticker='005930.KS', name='삼성전자'),
    ]
    # Query 4: open 포지션
    open_rows = [
        _row(ticker='000660.KS', model='stage', entry_actual=Decimal('150000'),
             qty=10, name='SK하이닉스'),
    ]

    pool, conn = _make_pool(series_rows, stats_rows, name_rows, open_rows)

    async def _get_pool():
        return pool

    with patch.object(paper_mod, 'get_pool', side_effect=_get_pool):
        async def fake_prices(tickers, *, update_cache=True):
            return {'000660.KS': 160000.0}

        with patch.object(paper_mod, '_fetch_current_prices', side_effect=fake_prices):
            response = await paper_mod.get_paper_curve()

    d = response['data']
    assert 'stage' in d['series']
    assert len(d['series']['stage']) == 2
    assert d['series']['stage'][1]['cumulative'] == 0.12

    st = d['model_stats']['stage']
    assert st['n_trades'] == 2
    assert st['n_wins'] == 2
    assert abs(st['win_rate'] - 1.0) < 0.001

    assert d['ticker_name_map']['005930.KS'] == '삼성전자'

    op = d['open_positions'][0]
    assert op['ticker'] == '000660.KS'
    # unrealized_pct = (160000 / 150000) - 1 ≈ 0.0667
    assert op['unrealized_pct'] is not None
    assert abs(op['unrealized_pct'] - round(160000 / 150000 - 1, 4)) < 1e-4


@pytest.mark.asyncio
async def test_curve_no_closed_positions():
    """closed 포지션 없음 → series={}, model_stats={}, open_positions=[]."""
    import routers_paper as paper_mod  # 라우터 분리(2026-07) — patch는 소유 모듈에

    pool, conn = _make_pool([], [], [], [])  # 4개 쿼리 모두 빈 결과

    async def _get_pool():
        return pool

    with patch.object(paper_mod, 'get_pool', side_effect=_get_pool):
        async def fake_prices(tickers, *, update_cache=True):
            return {}
        with patch.object(paper_mod, '_fetch_current_prices', side_effect=fake_prices):
            response = await paper_mod.get_paper_curve()

    d = response['data']
    assert d['series'] == {}
    assert d['model_stats'] == {}
    assert d['open_positions'] == []


@pytest.mark.asyncio
async def test_curve_no_open_positions_skips_yfinance():
    """open 포지션 없음 → _fetch_current_prices 미호출."""
    import routers_paper as paper_mod  # 라우터 분리(2026-07) — patch는 소유 모듈에

    series_rows = [
        _row(model='kosdaq', date='2026-05-01', cumulative=Decimal('0.03')),
    ]
    stats_rows = [
        _row(model='kosdaq', n_trades=1, n_wins=1,
             avg_win=Decimal('0.03'), avg_loss=None,
             total_realized=Decimal('0.03')),
    ]
    name_rows = [_row(ticker='035720.KS', name='카카오')]
    open_rows = []  # open 없음

    pool, _ = _make_pool(series_rows, stats_rows, name_rows, open_rows)

    called = []

    async def spy_prices(tickers, *, update_cache=True):
        called.append(tickers)
        return {}

    async def _get_pool():
        return pool

    with patch.object(paper_mod, 'get_pool', side_effect=_get_pool):
        with patch.object(paper_mod, '_fetch_current_prices', side_effect=spy_prices):
            response = await paper_mod.get_paper_curve()

    # open_positions가 없으니 yfinance 호출 없어야 함
    assert called == [], f"_fetch_current_prices called unexpectedly: {called}"
    assert response['data']['open_positions'] == []


# ── /api/paper/export ────────────────────────────────────────

@pytest.mark.asyncio
async def test_export_happy_path():
    """정상 데이터 → CSV 헤더 + 데이터 행, utf-8-sig BOM 포함."""
    import routers_paper as paper_mod  # 라우터 분리(2026-07) — patch는 소유 모듈에

    rows = [
        _row(
            model='stage', ticker='005930.KS', name='삼성전자',
            signal_date=date(2026, 4, 1), entry_theory=70000.0,
            entry_actual=71000.0, slippage_pct=0.014, qty=140,
            status='closed', tp1_pct=0.15, tp1_ratio=0.5,
            tp1_date=date(2026, 4, 10), tp1_price=81650.0,
            trail_pct=0.10, hard_stop_pct=0.10, watermark=81650.0,
            exit_date=date(2026, 4, 20), exit_price=77000.0,
            exit_type='trail', blended_return=0.065,
            created_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        ),
    ]
    pool, _ = _make_pool(rows)

    async def _get_pool():
        return pool

    with patch.object(paper_mod, 'get_pool', side_effect=_get_pool):
        response = await paper_mod.get_paper_export()

    # utf-8-sig BOM 확인
    assert response.body[:3] == b'\xef\xbb\xbf', "BOM 없음"

    text = response.body.decode('utf-8-sig')
    lines = text.strip().split('\r\n') if '\r\n' in text else text.strip().split('\n')

    # 헤더 확인
    assert lines[0].startswith('model,ticker,name'), f"헤더 이상: {lines[0]}"
    # 데이터 행
    assert '삼성전자' in lines[1], f"종목명 없음: {lines[1]}"
    assert 'stage' in lines[1]

    # Content-Disposition
    cd = response.headers.get('content-disposition', '')
    assert 'paper_positions_' in cd
    assert '.csv' in cd


@pytest.mark.asyncio
async def test_export_empty_table():
    """빈 테이블 → 헤더만 있는 CSV."""
    import routers_paper as paper_mod  # 라우터 분리(2026-07) — patch는 소유 모듈에

    pool, _ = _make_pool([])  # 빈 결과

    async def _get_pool():
        return pool

    with patch.object(paper_mod, 'get_pool', side_effect=_get_pool):
        response = await paper_mod.get_paper_export()

    text = response.body.decode('utf-8-sig')
    lines = [l for l in text.strip().split('\n') if l]
    assert len(lines) == 1, f"헤더 외 데이터 있음: {lines}"
    assert 'model' in lines[0]
