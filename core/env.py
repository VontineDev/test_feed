""".env 로딩 공용 헬퍼.

기존에 모듈마다 import 시점에 각자 load_dotenv()를 호출하던 패턴을
정리 (2026-07 리팩토링 Phase B — 접점이 생기는 모듈부터 점진 적용).
"""

from __future__ import annotations

_loaded = False


def load_env_once() -> None:
    """.env를 프로세스당 1회만 로드 (이미 설정된 환경변수는 덮지 않음).

    dotenv 미설치 환경에서는 조용히 no-op.
    """
    global _loaded
    if _loaded:
        return
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    _loaded = True


def refresh_env() -> None:
    """.env를 다시 읽어 기존 환경변수를 덮어씀 (override=True).

    의도된 용도: krx_flow_sync의 세션 만료 수동 복구 대기 루프 —
    사람이 .env의 KRX_SESSION을 갱신하면 실행 중 프로세스가 30초
    주기로 이를 다시 읽는다. 일반 코드에서는 load_env_once()를 쓸 것.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(override=True)
    except ImportError:
        pass
