"""analysis.backtest — 백테스트 엔진 패키지.

구 단일 3,360줄 모듈 analysis/backtest_engine.py를 관심사별로 분해
(2026-07 리팩토링 Phase C). 소비자는 각 서브모듈(config/models/helpers/
fetch/replay/exit_models/engine)에서 직접 import한다 — re-export 심은
전체 소비자 마이그레이션 완료 후 삭제됨(Phase D).
"""
