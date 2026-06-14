# TODOS


## daily_ohlcv 캐시 워밍 잡 (2026-06-14, plan-eng-review)
- **What:** 전종목 일봉 OHLCV를 daily_ohlcv에 선충전하는 워밍 잡
- **Why:** 캐시 희소 → compose 백테스트의 vol_ratio(SCORE-1 가중치) 계산이 daily_ohlcv에 의존
- **Pros:** vol_ratio 기반 전략 신호 품질 개선, 백테스트 반복 가속
- **Cons:** 첫 워밍 런 수시간, 스토리지 증가
- **Context:** screener_backfill.py는 fetch-once 최적화로 yfinance 부하 해결됨(2026-06-14). 이 TODO는 vol_ratio 데이터 품질 개선용으로만 남음.
- **Depends on:** 없음

## Tier-1 조합전략 백필 완료 (2026-06-14)
- stage_classifications: 2025-W01 ~ 2026-W24 백필 완료 (jobs/stage_backfill.py)
- chart_signals: 2025-W01 ~ 2026-W24 백필 완료, 4,144건 저장 (jobs/screener_backfill.py, fetch-once)
- Tier-1 전략 4종 백테스트 검증: AND-1(샤프 1.75) / FUNNEL-1(샤프 0.74, 주력) / SCORE-1(샤프 0.62) / AND-2(신호 희소)
- CLI: python scripts/run_compose.py --strategy ALL --start 2025-01-01 --end 2026-06-14
