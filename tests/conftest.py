import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def env_dsn(monkeypatch):
    """DB 접속 환경변수를 고정하고 DATABASE_URL을 제거.

    core.db.get_dsn()이 DB_* 폴백 경로를 타도록 만든다.
    반환값: 기대 DSN 문자열.
    """
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "news_db")
    monkeypatch.setenv("DB_USER", "news_user")
    monkeypatch.setenv("DB_PASSWORD", "testpass")
    return "postgresql://news_user:testpass@localhost:5432/news_db"


# 주말/평일 케이스용 고정 날짜 (2026-07 기준 실제 요일)
FROZEN_DATES = {
    "monday": date(2026, 7, 13),
    "friday": date(2026, 7, 17),
    "saturday": date(2026, 7, 18),
    "sunday": date(2026, 7, 19),
}


@pytest.fixture
def frozen_date():
    """요일 이름 → date 매핑. 날짜 의존 로직(주말 보정 등) 테스트용."""
    return dict(FROZEN_DATES)


class FakePoolConnection:
    """asyncpg Connection 대역: 실행된 쿼리를 기록하고 canned row를 반환."""

    def __init__(self, pool: "FakePool"):
        self._pool = pool

    async def fetch(self, query, *args):
        self._pool.queries.append((query, args))
        return self._pool.rows.get("fetch", [])

    async def fetchrow(self, query, *args):
        self._pool.queries.append((query, args))
        return self._pool.rows.get("fetchrow")

    async def fetchval(self, query, *args):
        self._pool.queries.append((query, args))
        return self._pool.rows.get("fetchval")

    async def execute(self, query, *args):
        self._pool.queries.append((query, args))
        return self._pool.rows.get("execute", "OK")

    async def executemany(self, query, args_iter):
        self._pool.queries.append((query, tuple(args_iter)))
        return self._pool.rows.get("executemany", "OK")


class FakePool:
    """asyncpg Pool 대역.

    사용법:
        pool = FakePool()
        pool.rows["fetch"] = [{"ticker": "005930.KS"}]
        ... 코드 실행 ...
        assert "SELECT" in pool.queries[0][0]
    """

    def __init__(self):
        self.queries: list[tuple] = []
        self.rows: dict = {}
        self._conn = FakePoolConnection(self)

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool._conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()

    async def fetch(self, query, *args):
        return await self._conn.fetch(query, *args)

    async def fetchrow(self, query, *args):
        return await self._conn.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        return await self._conn.fetchval(query, *args)

    async def execute(self, query, *args):
        return await self._conn.execute(query, *args)

    async def close(self):
        pass


@pytest.fixture
def fake_pool():
    """쿼리를 기록하고 canned row를 돌려주는 asyncpg Pool 스텁."""
    return FakePool()
