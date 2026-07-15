"""거래일 날짜 헬퍼 — 주말 보정 로직의 단일 구현.

기존에 data/krx_flow_sync.py, analysis/chart_screener.py,
data/kiwoom_aftermarket_sync.py, data/krx_aftermarket_sync.py에
각각 복사돼 있던 동일 로직을 통합 (2026-07 리팩토링 Phase A).
"""

from __future__ import annotations

from datetime import date, timedelta


def last_trading_day(today: date) -> date:
    """달력상 어제로부터 가장 가까운 평일(월~금) 반환.

    단순 "어제"를 그대로 쓰면 토/일/월 실행 시 비거래일이 되어
    금요일 거래일 데이터를 어떤 실행도 대상으로 삼지 못하고 영구히
    누락시킨다 (평일 전용 cron에는 주말 실행 회차가 없음).

    한국 공휴일은 보정하지 않는다 — 공휴일 다음 실행일에 조회하면
    데이터 소스가 빈 응답을 주고, 증분 모드가 다음 회차에 채운다.
    """
    d = today - timedelta(days=1)
    while d.weekday() >= 5:  # 5=토, 6=일
        d -= timedelta(days=1)
    return d
