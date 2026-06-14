# TODOS


## daily_ohlcv 캐시 워밍 잡 (완료 2026-06-14)
- `jobs/ohlcv_warm.py` — 백필 + 일배치 함수 구현
- 백필: `python jobs/ohlcv_warm.py --start 2025-01-02` (KRX OpenAPI, 평일 순회)
- 일배치: 평일 18:30 KST run_scheduler 자동 실행 (KRX_OPENAPI_KEY 필요)
- 이미 채워진 날짜 스킵 — 재실행 안전

## Tier-1 조합전략 백필 완료 (2026-06-14)
- stage_classifications: 2025-W01 ~ 2026-W24 백필 완료 (jobs/stage_backfill.py)
- chart_signals: 2025-W01 ~ 2026-W24 백필 완료, 4,144건 저장 (jobs/screener_backfill.py, fetch-once)
- Tier-1 전략 4종 백테스트 검증: AND-1(샤프 1.75) / FUNNEL-1(샤프 0.74, 주력) / SCORE-1(샤프 0.62) / AND-2(신호 희소)
- CLI: python scripts/run_compose.py --strategy ALL --start 2025-01-01 --end 2026-06-14
