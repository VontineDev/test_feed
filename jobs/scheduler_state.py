"""jobs/scheduler_state.py — 스케줄러 프로세스 공유 상태 (단일 소유자).

run_scheduler.main()이 기동 시 채우고, 잡·워커들이 읽는 프로세스 전역
싱글턴. 반드시 `from jobs import scheduler_state as state` 후
`state.db_pool` 형태의 **속성 접근**으로 사용할 것 — 속성 접근은 매번
그 시점의 최신 바인딩을 읽으므로 main()의 재할당이 전 모듈에 반영된다.
`from jobs.scheduler_state import db_pool` 같은 이름 import는 값을
import 시점에 스냅샷해 이후 재할당을 못 보므로 금지.

여기 두는 것은 여러 모듈이 공유하는 재할당 상태만이다. 단일 모듈만 쓰는
상태(_summary_queue/_seen_hashes/매크로 캐시)는 소유 모듈에 남긴다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncpg import Pool
    from data.kiwoom_paper_trader import KiwoomPaperTrader

# asyncpg 커넥션 풀 — main()이 기동 시 생성, DB 없으면 None 유지
db_pool: Pool | None = None

# KiwoomPaperTrader (모의투자, KIWOOM_MOCK_APPKEY 설정 시에만)
paper_trader: KiwoomPaperTrader | None = None

# ── 스크리닝·Stage 캐시 (뉴스 게이팅용) ─────────────────────
# screener_tickers: 주봉 스크리닝 통과 종목 (일요일 갱신)
# active_stage_tickers: 최근 7일 이내 Stage 1/2/3 분류 종목 (일봉 분류기 갱신)
# 둘 다 비어 있으면 게이팅 비활성 (초기 실행 방어).
screener_tickers: set[str] = set()
active_stage_tickers: set[str] = set()
