"""
fetch_top_volume() 단위 테스트.

⚠️  API ID(ka10052)와 응답 스키마(stk_cd, trde_prica 등)는 플레이스홀더.
    Kiwoom OpenAPI 포털에서 실제 API ID 및 필드명 확인 후 아래 mock 데이터를 업데이트할 것.
    특히 trde_prica 단위(원/만원/백만원)를 확인하고 _TOP_VALUE_UNIT 조정 필요.
"""
from unittest.mock import MagicMock

import pytest


def _make_client(mock_output: list[dict]) -> "KiwoomClient":
    from kiwoom_aftermarket_sync import KiwoomClient
    client = KiwoomClient(use_mock=False)
    client.inject_token("dummy-token-for-tests")
    client._post = MagicMock(return_value=({"output": mock_output}, {}))
    return client


class TestFetchTopVolume:
    def test_success_returns_ranked_list(self):
        """정상 응답 → rank/ticker/name/price/change_pct/amount 포함."""
        row = {
            "stk_cd": "005930",
            "stk_nm": "삼성전자",
            "cur_prc": "80000",
            "flu_rt": "1.5",
            "trde_prica": "5000",  # 단위 확인 후 조정
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert len(result) == 1
        item = result[0]
        assert item["rank"] == 1
        assert item["ticker"] == "005930"
        assert item["name"] == "삼성전자"
        assert item["price"] == 80000
        assert item["change_pct"] == pytest.approx(1.5)
        # amount 단위는 _TOP_VALUE_UNIT 적용 결과 — 실제 API 확인 후 검증
        assert item["amount"] > 0

    def test_empty_output_returns_empty_list(self):
        """빈 output → 빈 리스트 반환 (크래시 없음)."""
        client = _make_client([])
        result = client.fetch_top_volume(n=20)
        assert result == []

    def test_api_error_propagates(self):
        """_post() RuntimeError → fetch_top_volume()이 그대로 전파."""
        from kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        client._post = MagicMock(side_effect=RuntimeError("API 오류 [ka10052]: 잘못된 요청"))
        with pytest.raises(RuntimeError, match="API 오류"):
            client.fetch_top_volume(n=20)

    def test_n_cap_respected(self):
        """n=2 요청 시 최대 2개만 반환."""
        rows = [
            {"stk_cd": f"{i:06d}", "stk_nm": f"종목{i}", "cur_prc": "1000",
             "flu_rt": "0.5", "trde_prica": "100"}
            for i in range(5)
        ]
        client = _make_client(rows)
        result = client.fetch_top_volume(n=2)
        assert len(result) == 2
        assert result[0]["rank"] == 1
        assert result[1]["rank"] == 2

    def test_missing_fields_handled_gracefully(self):
        """필드 누락 시 기본값(0, '') 반환하고 크래시 없음."""
        row = {}  # 모든 필드 없음
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert len(result) == 1
        item = result[0]
        assert item["price"] == 0
        assert item["change_pct"] == pytest.approx(0.0)
        assert item["amount"] == 0

    def test_negative_change_pct(self):
        """하락 종목 → change_pct 음수 반환."""
        row = {
            "stk_cd": "000660",
            "stk_nm": "SK하이닉스",
            "cur_prc": "150000",
            "flu_rt": "-2.3",
            "trde_prica": "3000",
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert result[0]["change_pct"] == pytest.approx(-2.3)

    def test_short_stk_cd_zero_padded(self):
        """4자리 종목코드 → 6자리 zero-padding 적용."""
        row = {
            "stk_cd": "5930",  # 삼성전자 (4자리)
            "stk_nm": "삼성전자",
            "cur_prc": "80000",
            "flu_rt": "0",
            "trde_prica": "1000",
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert result[0]["ticker"] == "005930"

    def test_market_param_passed_to_post(self):
        """market 파라미터가 _post()의 mrkt_tp body에 전달된다."""
        from kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        client._post = MagicMock(return_value=({"output": []}, {}))
        client.fetch_top_volume(n=5, market="001")
        call_body = client._post.call_args[0][2]  # 세 번째 positional arg = body dict
        assert call_body["mrkt_tp"] == "001"
