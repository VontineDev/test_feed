"""
test_db_dsn.py  —  db.get_dsn() 특수문자 URL 인코딩 회귀 테스트
────────────────────────────────────────────────────────────
ISSUE: DB_PASSWORD에 &, #, / 등 특수문자가 포함된 경우
       DSN URL 파싱이 깨져 DB 연결 실패 (pool=None → "DB 미연결 상태").
FIX:  get_dsn()에서 urllib.parse.quote()로 user/password URL 인코딩.

실행:
    pytest test_db_dsn.py -v
"""

from __future__ import annotations

import os
from unittest.mock import patch
from urllib.parse import urlparse, unquote

import pytest


def _get_dsn_with_password(password: str, user: str = "news_user") -> str:
    """DB_PASSWORD 환경변수를 mock하여 get_dsn() 호출."""
    env = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "news_db",
        "DB_USER": user,
        "DB_PASSWORD": password,
    }
    with patch.dict(os.environ, env, clear=False):
        # DATABASE_URL이 있으면 안 됨
        os.environ.pop("DATABASE_URL", None)
        from importlib import reload
        import db
        reload(db)
        return db.get_dsn()


class TestGetDsnUrlEncoding:
    def test_plain_password_no_encoding_needed(self):
        """특수문자 없는 비밀번호는 그대로 통과."""
        dsn = _get_dsn_with_password("simplepass")
        parsed = urlparse(dsn)
        assert parsed.hostname == "localhost"
        assert unquote(parsed.password) == "simplepass"

    def test_hash_in_password_is_encoded(self):
        """# 문자가 URL fragment로 해석되지 않아야 함."""
        dsn = _get_dsn_with_password("pass#word")
        parsed = urlparse(dsn)
        # hostname이 올바르게 파싱돼야 함 — #이 fragment로 끊기면 None
        assert parsed.hostname == "localhost", (
            f"hostname={parsed.hostname!r} — '#' in password broke URL parsing. DSN: {dsn}"
        )
        assert unquote(parsed.password) == "pass#word"

    def test_ampersand_in_password_is_encoded(self):
        """& 문자가 쿼리 파라미터 구분자로 해석되지 않아야 함."""
        dsn = _get_dsn_with_password("UCi65&z#52Rj4p/")
        parsed = urlparse(dsn)
        assert parsed.hostname == "localhost", (
            f"hostname={parsed.hostname!r} — special chars broke URL parsing. DSN: {dsn}"
        )
        assert unquote(parsed.password) == "UCi65&z#52Rj4p/"

    def test_slash_in_password_is_encoded(self):
        """/ 문자가 경로 구분자로 해석되지 않아야 함."""
        dsn = _get_dsn_with_password("pass/word")
        parsed = urlparse(dsn)
        assert parsed.hostname == "localhost"
        assert unquote(parsed.password) == "pass/word"

    def test_at_sign_in_password_is_encoded(self):
        """@ 문자가 userinfo 구분자로 해석되지 않아야 함."""
        dsn = _get_dsn_with_password("p@ssword")
        parsed = urlparse(dsn)
        assert parsed.hostname == "localhost"
        assert unquote(parsed.password) == "p@ssword"

    def test_user_password_roundtrip(self):
        """실제 .env 예시 비밀번호 전체 라운드트립 검증."""
        pw = "UCi65&z#52Rj4p/"
        dsn = _get_dsn_with_password(pw)
        parsed = urlparse(dsn)
        assert parsed.hostname == "localhost"
        assert parsed.port == 5432
        assert parsed.path == "/news_db"
        assert unquote(parsed.username) == "news_user"
        assert unquote(parsed.password) == pw

    def test_missing_db_password_raises(self):
        """DB_PASSWORD 미설정 시 RuntimeError."""
        env = {"DB_HOST": "localhost", "DB_PORT": "5432",
               "DB_NAME": "news_db", "DB_USER": "news_user"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("DB_PASSWORD", None)
            os.environ.pop("DATABASE_URL", None)
            from importlib import reload
            import db
            reload(db)
            with pytest.raises(RuntimeError, match="DB_PASSWORD"):
                db.get_dsn()
