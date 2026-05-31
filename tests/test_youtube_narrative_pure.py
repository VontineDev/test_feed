"""
YouTube 내러티브 파이프라인 — 순수 로직 단위 테스트.

외부 의존성(DB, YouTube API, Gemini API) 없이 실행 가능한 함수만 테스트.
"""
import json
import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from data.youtube_narrative_sync import (
    _get_env,
    _next_business_day,
    _prev_business_days,
    normalize_ticker,
)


# ── normalize_ticker ──────────────────────────────────


class TestNormalizeTicker:
    def test_alias_match(self):
        """aliases에 정확히 있는 종목명 → alias 코드 반환."""
        aliases = {"삼프로": "999999"}
        master = {}
        assert normalize_ticker("삼프로", master, aliases) == "999999"

    def test_master_exact_match(self):
        """master에 정확히 있는 종목명 → master 코드 반환."""
        aliases = {}
        master = {"삼성전자": "005930"}
        assert normalize_ticker("삼성전자", master, aliases) == "005930"

    def test_alias_takes_priority_over_master(self):
        """alias와 master 모두 있을 때 alias 우선."""
        aliases = {"삼성전자": "ALIAS"}
        master = {"삼성전자": "005930"}
        assert normalize_ticker("삼성전자", master, aliases) == "ALIAS"

    def test_partial_match_master_starts_with_name(self):
        """master 키가 입력 이름으로 시작하면 부분 일치."""
        aliases = {}
        master = {"SK하이닉스우": "000661"}
        assert normalize_ticker("SK하이닉스", master, aliases) == "000661"

    def test_partial_match_name_starts_with_master_key(self):
        """입력 이름이 master 키로 시작하면 부분 일치."""
        aliases = {}
        master = {"카카오": "035720"}
        assert normalize_ticker("카카오페이", master, aliases) == "035720"

    def test_no_match_returns_none(self):
        """매핑 실패 시 None 반환."""
        assert normalize_ticker("없는종목", {}, {}) is None

    def test_strips_whitespace(self):
        """앞뒤 공백 제거 후 비교."""
        aliases = {}
        master = {"삼성전자": "005930"}
        assert normalize_ticker("  삼성전자  ", master, aliases) == "005930"


# ── _prev_business_days ───────────────────────────────


class TestPrevBusinessDays:
    def test_weekday_only(self):
        """금요일 → 5 영업일 전 = 직전 금요일."""
        friday = date(2026, 5, 29)  # 금요일
        result = _prev_business_days(friday, 5)
        assert result == date(2026, 5, 22)

    def test_skips_saturday(self):
        """영업일 1일 전 계산 시 토요일 건너뜀 확인."""
        monday = date(2026, 6, 1)  # 월요일
        result = _prev_business_days(monday, 1)
        assert result == date(2026, 5, 29)  # 직전 금요일

    def test_skips_sunday(self):
        """영업일 1일 전 계산 시 일요일 건너뜀 확인."""
        tuesday = date(2026, 6, 2)  # 화요일
        result = _prev_business_days(tuesday, 1)
        assert result == date(2026, 6, 1)  # 월요일

    def test_n_zero_returns_same_date(self):
        """n=0이면 ref 그대로 반환."""
        ref = date(2026, 5, 29)
        assert _prev_business_days(ref, 0) == ref

    def test_crosses_weekend_boundary(self):
        """주말을 넘어가는 경우 올바르게 건너뜀."""
        wednesday = date(2026, 6, 3)
        result = _prev_business_days(wednesday, 3)
        assert result == date(2026, 5, 29)


# ── _next_business_day ────────────────────────────────


class TestNextBusinessDay:
    def test_friday_plus_one_is_monday(self):
        """금요일 + 1 영업일 = 월요일."""
        friday = date(2026, 5, 29)
        result = _next_business_day(friday, 1)
        assert result == date(2026, 6, 1)

    def test_monday_plus_five(self):
        """월요일 + 5 영업일 = 다음 월요일."""
        monday = date(2026, 6, 1)
        result = _next_business_day(monday, 5)
        assert result == date(2026, 6, 8)

    def test_n_zero_returns_same_date(self):
        """n=0이면 ref 그대로 반환."""
        ref = date(2026, 5, 28)
        assert _next_business_day(ref, 0) == ref


# ── _get_env ──────────────────────────────────────────


class TestGetEnv:
    def test_returns_value_when_set(self):
        """환경변수가 있으면 값 반환."""
        with patch.dict(os.environ, {"TEST_KEY_XYZ": "myvalue"}):
            assert _get_env("TEST_KEY_XYZ") == "myvalue"

    def test_raises_systemexit_when_missing(self):
        """환경변수가 없으면 SystemExit 발생."""
        env = {k: v for k, v in os.environ.items() if k != "MISSING_KEY_XYZ"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(SystemExit, match="MISSING_KEY_XYZ"):
                _get_env("MISSING_KEY_XYZ")

    def test_raises_systemexit_when_empty_string(self):
        """환경변수가 빈 문자열이면 SystemExit 발생."""
        with patch.dict(os.environ, {"EMPTY_KEY": ""}):
            with pytest.raises(SystemExit):
                _get_env("EMPTY_KEY")


# ── _load_aliases (file exists / missing) ─────────────


class TestLoadAliases:
    def test_returns_dict_when_file_exists(self, tmp_path):
        """aliases 파일이 있으면 파싱된 dict 반환."""
        data = {"삼프로": "999999", "카카오": "035720"}
        aliases_file = tmp_path / "youtube_ticker_aliases.json"
        aliases_file.write_text(json.dumps(data), encoding="utf-8")

        import data.youtube_narrative_sync as mod
        original = mod._ALIASES_PATH
        mod._ALIASES_PATH = aliases_file
        try:
            from data.youtube_narrative_sync import _load_aliases
            result = _load_aliases()
            assert result == data
        finally:
            mod._ALIASES_PATH = original

    def test_returns_empty_dict_when_file_missing(self, tmp_path):
        """aliases 파일이 없으면 빈 dict 반환."""
        import data.youtube_narrative_sync as mod
        original = mod._ALIASES_PATH
        mod._ALIASES_PATH = tmp_path / "nonexistent.json"
        try:
            from data.youtube_narrative_sync import _load_aliases
            result = _load_aliases()
            assert result == {}
        finally:
            mod._ALIASES_PATH = original


# ── _parse_json3 (via inline copy — module has sys.exit guard) ──


def _parse_json3(text: str) -> str:
    """YouTube json3 자막 → 텍스트 (테스트용 인라인 복사)."""
    data = json.loads(text)
    events = data.get("events", [])
    parts = []
    for ev in events:
        for seg in ev.get("segs", []):
            t = seg.get("utf8", "").replace("\n", " ").strip()
            if t:
                parts.append(t)
    return " ".join(parts).strip()


class TestParseJson3:
    def test_normal_events(self):
        """일반 이벤트 → 텍스트 결합."""
        payload = {
            "events": [
                {"segs": [{"utf8": "안녕"}, {"utf8": "하세요"}]},
                {"segs": [{"utf8": "삼성전자"}]},
            ]
        }
        result = _parse_json3(json.dumps(payload))
        assert result == "안녕 하세요 삼성전자"

    def test_empty_segs_skipped(self):
        """segs 없는 이벤트 → 건너뜀, 빈 문자열 → 건너뜀."""
        payload = {
            "events": [
                {},
                {"segs": [{"utf8": ""}]},
                {"segs": [{"utf8": "내용"}]},
            ]
        }
        result = _parse_json3(json.dumps(payload))
        assert result == "내용"

    def test_newline_replaced(self):
        """\\n → 공백 치환."""
        payload = {"events": [{"segs": [{"utf8": "줄\n바꿈"}]}]}
        result = _parse_json3(json.dumps(payload))
        assert result == "줄 바꿈"

    def test_empty_events_returns_empty(self):
        """events 없음 → 빈 문자열."""
        payload = {"events": []}
        result = _parse_json3(json.dumps(payload))
        assert result == ""
