"""
fetch_top_volume() 단위 테스트.

API: ka10032 거래대금상위요청 (Kiwoom REST API 문서 p.102-103)
  - URL: /api/dostk/rkinfo
  - 응답 배열 키: trde_prica_upper
  - 거래대금 단위: 백만원 (×1_000_000 → 원)
  - 검증: 삼성전자 예시 trde_prica=5308092 × 1_000_000 ≈ 5.31조원 ✓
"""
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from data.kiwoom_aftermarket_sync import KiwoomClient


_RESPONSE_KEY = "trde_prica_upper"  # ka10032 응답 배열 키


def _make_client(mock_rows: list[dict]) -> "KiwoomClient":
    from data.kiwoom_aftermarket_sync import KiwoomClient
    client = KiwoomClient(use_mock=False)
    client.inject_token("dummy-token-for-tests")
    client._post = MagicMock(return_value=({_RESPONSE_KEY: mock_rows}, {}))
    return client


class TestFetchTopVolume:
    def test_success_returns_ranked_list(self):
        """정상 응답 → rank/ticker/name/price/change_pct/amount 포함.

        trde_prica=5308092 (백만원) × 1_000_000 = 5.308조원 (삼성전자 수준).
        """
        row = {
            "now_rank": "1",
            "stk_cd": "005930",
            "stk_nm": "삼성전자",
            "cur_prc": "-152000",
            "flu_rt": "-0.07",
            "trde_prica": "5308092",   # 백만원 단위 (문서 예시 값 그대로)
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert len(result) == 1
        item = result[0]
        assert item["rank"] == 1
        assert item["ticker"] == "005930"
        assert item["name"] == "삼성전자"
        assert item["price"] == 152000               # cur_prc 부호 제거
        assert item["change_pct"] == pytest.approx(-0.07)
        assert item["amount"] == 5_308_092 * 1_000_000  # 백만원 × 1_000_000 = 원

    def test_empty_output_returns_empty_list(self):
        """빈 배열 → 빈 리스트 반환 (크래시 없음)."""
        client = _make_client([])
        result = client.fetch_top_volume(n=20)
        assert result == []

    def test_api_error_propagates(self):
        """_post() RuntimeError → fetch_top_volume()이 그대로 전파."""
        from data.kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        client._post = MagicMock(side_effect=RuntimeError("API 오류 [ka10032]: 잘못된 요청"))
        with pytest.raises(RuntimeError, match="API 오류"):
            client.fetch_top_volume(n=20)

    def test_n_cap_respected(self):
        """n=2 요청 시 최대 2개만 반환."""
        rows = [
            {"now_rank": str(i+1), "stk_cd": f"{i:06d}", "stk_nm": f"종목{i}",
             "cur_prc": "1000", "flu_rt": "0.5", "trde_prica": "100"}
            for i in range(5)
        ]
        client = _make_client(rows)
        result = client.fetch_top_volume(n=2)
        assert len(result) == 2
        assert result[0]["rank"] == 1
        assert result[1]["rank"] == 2

    def test_missing_fields_handled_gracefully(self):
        """필드 누락 시 기본값(0) 반환하고 크래시 없음."""
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
            "now_rank": "1",
            "stk_cd": "000660",
            "stk_nm": "SK하이닉스",
            "cur_prc": "-150000",
            "flu_rt": "-2.3",
            "trde_prica": "3000",
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        assert result[0]["change_pct"] == pytest.approx(-2.3)

    def test_short_stk_cd_zero_padded(self):
        """4자리 종목코드 → 6자리 zero-padding 적용."""
        row = {
            "now_rank": "1",
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
        """market 파라미터가 _post()의 mrkt_tp body에 전달되고, stex_tp/mang_stk_incls 포함."""
        from data.kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        client._post = MagicMock(return_value=({_RESPONSE_KEY: []}, {}))
        client.fetch_top_volume(n=5, market="001")
        call_body = client._post.call_args[0][2]  # 세 번째 positional arg = body dict
        assert call_body["mrkt_tp"] == "001"
        assert call_body["stex_tp"] == "3"
        assert "mang_stk_incls" in call_body

    def test_now_rank_used_over_index(self):
        """now_rank 필드가 있으면 인덱스 대신 해당 값을 rank로 사용."""
        rows = [
            {"now_rank": "5", "stk_cd": "000001", "stk_nm": "A", "cur_prc": "1000",
             "flu_rt": "0", "trde_prica": "100"},
        ]
        client = _make_client(rows)
        result = client.fetch_top_volume(n=1)
        assert result[0]["rank"] == 5

    def test_trde_prica_unit_백만원(self):
        """거래대금 단위: 백만원 × 1_000_000 → 원 (문서 검증값)."""
        row = {
            "now_rank": "1",
            "stk_cd": "005930",
            "stk_nm": "삼성전자",
            "cur_prc": "-152000",
            "flu_rt": "-0.07",
            "trde_prica": "5308092",  # 5,308,092 백만원 = 5.308조원
        }
        client = _make_client([row])
        result = client.fetch_top_volume(n=1)
        expected_amount_won = 5_308_092 * 1_000_000
        assert result[0]["amount"] == expected_amount_won
        assert result[0]["amount"] > 5_000_000_000_000  # 5조원 이상


class TestFetchAllByValue:
    """fetch_all_by_value() 단위 테스트 — KOSPI/KOSDAQ 순차 조회 + cont-yn 페이지네이션.

    ka10032는 mrkt_tp="101"(KOSDAQ)로 명시 요청해도 stk_cd 접미사가 "_AL"로
    고정 반환되는 걸 실측으로 확인했다(실제 시장과 무관). 그래서
    fetch_all_by_value는 KOSPI(001)/KOSDAQ(101)를 따로 호출해 응답 접미사
    대신 "요청한 시장"을 직접 ticker에 태그한다 — 아래 테스트는 이 태깅이
    응답 접미사와 무관하게 항상 요청 시장 기준으로 나오는지 검증한다.
    """

    def _row(self, rank: int, suffix: str = "_AL") -> dict:
        return {
            "now_rank": str(rank), "stk_cd": f"{rank:06d}{suffix}", "stk_nm": f"종목{rank}",
            "cur_prc": "1000", "flu_rt": "0.5", "trde_prica": "100",
        }

    def test_both_markets_queried_and_tagged_by_request_not_suffix(self):
        """KOSPI/KOSDAQ 응답이 전부 "_AL" 접미사여도 요청한 시장 기준으로 태그."""
        from data.kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        mock_post = MagicMock(side_effect=[
            ({_RESPONSE_KEY: [self._row(1)]}, {}),   # mrkt_tp=001 (KOSPI) 호출
            ({_RESPONSE_KEY: [self._row(2)]}, {}),   # mrkt_tp=101 (KOSDAQ) 호출 — 응답도 "_AL"
        ])
        client._post = mock_post
        result = client.fetch_all_by_value()

        assert mock_post.call_count == 2
        first_body = mock_post.call_args_list[0][0][2]
        second_body = mock_post.call_args_list[1][0][2]
        assert first_body["mrkt_tp"] == "001"
        assert second_body["mrkt_tp"] == "101"

        assert len(result) == 2
        assert result[0]["ticker"] == "000001.KS"   # KOSPI 요청분 → .KS
        assert result[1]["ticker"] == "000002.KQ"   # KOSDAQ 요청분 → .KQ (접미사는 "_AL"이었음)

    def test_paginates_within_each_market(self):
        """시장별로 cont-yn="Y" → "N" 페이지네이션 후 다음 시장으로 이동."""
        from data.kiwoom_aftermarket_sync import KiwoomClient
        client = KiwoomClient(use_mock=False)
        client.inject_token("dummy-token-for-tests")
        kospi_p1 = [self._row(i) for i in range(1, 101)]
        kospi_p2 = [self._row(i) for i in range(101, 151)]
        kosdaq_p1 = [self._row(i) for i in range(1, 51)]
        mock_post = MagicMock(side_effect=[
            ({_RESPONSE_KEY: kospi_p1}, {"cont-yn": "Y", "next-key": "00110001"}),
            ({_RESPONSE_KEY: kospi_p2}, {"cont-yn": "N", "next-key": ""}),
            ({_RESPONSE_KEY: kosdaq_p1}, {"cont-yn": "N", "next-key": ""}),
        ])
        client._post = mock_post
        result = client.fetch_all_by_value()
        assert mock_post.call_count == 3
        assert len(result) == 150 + 50
        assert all(t["ticker"].endswith(".KS") for t in result[:150])
        assert all(t["ticker"].endswith(".KQ") for t in result[150:])

    def test_empty_responses_return_empty_list(self):
        client = _make_client([])
        result = client.fetch_all_by_value()
        assert result == []
