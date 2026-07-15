"""core/db_sync.py — 동기 psycopg2 연결 헬퍼 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from core.db_sync import connect


class TestConnect:
    def test_explicit_dsn_passed_through(self):
        with patch("core.db_sync.psycopg2") as mock_pg:
            mock_pg.connect.return_value = MagicMock()
            connect("postgresql://u:p@h:5432/d")
        mock_pg.connect.assert_called_once_with("postgresql://u:p@h:5432/d")

    def test_default_dsn_from_get_dsn(self, env_dsn):
        with patch("core.db_sync.psycopg2") as mock_pg:
            mock_pg.connect.return_value = MagicMock()
            connect()
        mock_pg.connect.assert_called_once_with(env_dsn)


class TestAliasesStillResolve:
    """이관 후에도 기존 _connect 별칭이 canonical과 동일 객체인지."""

    def test_ohlcv_cache_alias(self):
        from core.ohlcv_cache import _connect
        assert _connect is connect

    def test_youtube_alias(self):
        from data.youtube_narrative_sync import _connect
        assert _connect is connect

    def test_krx_aftermarket_alias(self):
        from data.krx_aftermarket_sync import _connect
        assert _connect is connect

    def test_kiwoom_aftermarket_alias(self):
        from data.kiwoom_aftermarket_sync import _connect
        assert _connect is connect
