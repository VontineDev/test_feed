"""analysis.backtest — 백테스트 엔진 패키지.

analysis/backtest_engine.py(구 단일 3,360줄 모듈)를 관심사별로 분해
(2026-07 리팩토링 Phase C). 외부 코드는 기존처럼
`from analysis.backtest_engine import ...`을 계속 사용할 수 있다
(backtest_engine.py가 re-export 심 역할).
"""
