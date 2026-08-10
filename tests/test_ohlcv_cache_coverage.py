"""_classify_coverage() 단위 테스트 — DB/네트워크 없는 순수 함수.

2026-08-10: daily_ohlcv를 KRX OpenAPI로 2022년치까지 백필한 직후에도
batch_fetch_cached()가 계속 "캐시 히트 0"으로 잡히는 걸 발견 — 원인은 캘린더
"어제"(effective_end)와 "마지막 거래일"(cov[1], DB의 실제 최신 데이터)을
혼동한 것. 주말/공휴일에는 거래가 없어 월요일마다(또는 연휴 다음날마다)
데이터가 완전히 최신이어도 항상 미스로 잡혔다 — 결과적으로 매 백테스트가
yfinance로 재수집했고, 이게 누적돼 2026-08-10 세션에서 결국 rate-limit을
유발함(TechnicalQuant.md 교차조합 백테스트 결과 오염 참고).
"""
from datetime import date, timedelta

from core.ohlcv_cache import _classify_coverage


def test_hit_when_last_data_exactly_matches_effective_end():
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)  # 일요일
    coverage = {"005930.KS": (date(2022, 1, 3), date(2026, 8, 9), 1000)}
    hits, misses = _classify_coverage(["005930.KS"], coverage, fetch_start, effective_end)
    assert hits == ["005930.KS"]
    assert misses == []


def test_hit_when_last_trading_day_is_friday_before_weekend_effective_end():
    """회귀 테스트: effective_end가 주말(예: 일요일)이고 DB의 마지막 데이터는
    직전 금요일까지만 있어도(거래가 없었으므로 정상) 히트로 잡혀야 한다."""
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)   # 일요일("어제")
    last_trading_day = date(2026, 8, 7)  # 금요일 — 실제 마지막 거래일, DB엔 이게 최신
    coverage = {"005930.KS": (date(2022, 1, 3), last_trading_day, 1124)}
    hits, misses = _classify_coverage(["005930.KS"], coverage, fetch_start, effective_end)
    assert hits == ["005930.KS"], "주말 때문에 생기는 2일 갭은 슬랙 범위 안이라 히트여야 함"
    assert misses == []


def test_miss_when_gap_exceeds_slack():
    """4일 슬랙을 초과하는 진짜 오래된 데이터는 여전히 미스여야 한다."""
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)
    stale_last_date = date(2026, 8, 1)  # 8일 갭 — 슬랙(4일) 초과
    coverage = {"005930.KS": (date(2022, 1, 3), stale_last_date, 1100)}
    hits, misses = _classify_coverage(["005930.KS"], coverage, fetch_start, effective_end)
    assert hits == []
    assert misses == ["005930.KS"]


def test_miss_when_no_coverage_at_all():
    hits, misses = _classify_coverage(["999999.KS"], {}, date(2023, 1, 8), date(2026, 8, 9))
    assert hits == []
    assert misses == ["999999.KS"]


def test_miss_when_history_does_not_reach_fetch_start():
    """DB에 최근 데이터는 있지만 fetch_start 이전 이력이 없으면(짧은 히스토리) 미스."""
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)
    coverage = {"483650.KS": (date(2025, 7, 24), date(2026, 8, 7), 200)}  # 최근 상장
    hits, misses = _classify_coverage(["483650.KS"], coverage, fetch_start, effective_end)
    assert hits == []
    assert misses == ["483650.KS"]


def test_miss_when_row_count_below_threshold():
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)
    coverage = {"005930.KS": (date(2022, 1, 3), date(2026, 8, 7), 10)}  # 30행 미만
    hits, misses = _classify_coverage(["005930.KS"], coverage, fetch_start, effective_end)
    assert hits == []
    assert misses == ["005930.KS"]


def test_mixed_symbols_partitioned_correctly():
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)
    coverage = {
        "005930.KS": (date(2022, 1, 3), date(2026, 8, 7), 1124),   # 히트 (슬랙 내)
        "483650.KS": (date(2025, 7, 24), date(2026, 8, 7), 200),   # 미스 (이력 부족)
    }
    hits, misses = _classify_coverage(
        ["005930.KS", "483650.KS", "000000.KS"], coverage, fetch_start, effective_end
    )
    assert hits == ["005930.KS"]
    assert set(misses) == {"483650.KS", "000000.KS"}


def test_custom_slack_is_respected():
    fetch_start = date(2023, 1, 8)
    effective_end = date(2026, 8, 9)
    coverage = {"005930.KS": (date(2022, 1, 3), date(2026, 8, 5), 1120)}  # 4일 갭
    hits, misses = _classify_coverage(
        ["005930.KS"], coverage, fetch_start, effective_end, coverage_slack=timedelta(days=0)
    )
    assert hits == [], "슬랙 0이면 4일 갭도 미스여야 함"
    assert misses == ["005930.KS"]
