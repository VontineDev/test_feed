"""
ticker_cache.py — KRX 종목 인메모리 캐시 싱글턴.
애플리케이션 시작 시 한 번 로드, 매일 20:00 KST 갱신.
"""
from __future__ import annotations

import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

LOAD_SQL = """
SELECT name_ko, name_ko_abbr, name_en, short_code, yfinance_symbol
FROM krx_listings
"""


class TickerCache:
    """
    KRX 종목 조회 캐시. load() 호출 전에도 resolve()는 None을 반환하므로 안전.

    스레드 안전성:
    _by_name/_by_short를 단일 튜플 언패킹으로 동시 할당하여
    스레드 풀 코루틴이 두 dict 중 하나만 업데이트된 불일치 상태를 볼 수 없게 함.
    """

    def __init__(self) -> None:
        # load() 전에도 resolve() 호출이 AttributeError 없이 동작하도록 초기화
        self._by_name: dict[str, str] = {}   # 한글명/영문명 → yfinance 심볼
        self._by_short: dict[str, str] = {}  # 단축코드 → yfinance 심볼
        self._loaded: bool = False

    async def load(self, pool: asyncpg.Pool) -> None:
        """krx_listings에서 전체 로드. 여러 번 호출 가능 (갱신용)."""
        rows = await pool.fetch(LOAD_SQL)
        by_name: dict[str, str] = {}
        by_short: dict[str, str] = {}

        for row in rows:
            sym = row["yfinance_symbol"]
            short = row["short_code"]
            by_short[short] = sym

            # name_ko_abbr (ISU_ABBRV) 우선 — ISU_NM이 잘린 경우 커버
            # name_ko, name_ko_abbr, name_en 모두 조회 키로 등록
            for field in ("name_ko_abbr", "name_ko", "name_en"):
                val = row[field]
                if val:
                    by_name[val] = sym

        # 원자적 할당: 스레드 풀 코루틴이 중간 상태를 보지 못하게 함
        self._by_name, self._by_short = by_name, by_short
        self._loaded = True
        logger.info(
            "[ticker_cache] %d개 이름 항목, %d개 단축코드 로드 완료",
            len(by_name), len(by_short),
        )

    def resolve(self, name: str) -> Optional[str]:
        """이름 또는 단축코드로 yfinance 심볼 조회. 찾지 못하면 None."""
        return self._by_name.get(name) or self._by_short.get(name)

    @property
    def loaded(self) -> bool:
        return self._loaded


# 모듈 수준 싱글턴 — import 즉시 사용 가능. load() 전에는 resolve()가 None 반환.
ticker_cache = TickerCache()
