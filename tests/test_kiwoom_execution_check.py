"""check_execution() / confirm_fill() 단위 테스트.

API: ka10076 체결요청 (Kiwoom REST API 문서 p.190-192)
  - URL: /api/dostk/acnt
  - 응답 배열 키: cntr, 체결수량 필드: cntr_qty
  - place_buy()/place_sell()이 반환한 ord_no가 실제로 체결됐는지 확인하는 용도.
"""
from unittest.mock import MagicMock

from data.kiwoom_aftermarket_sync import KiwoomClient
from data.kiwoom_paper_trader import KiwoomPaperTrader

_RESPONSE_KEY = "cntr"  # ka10076 응답 배열 키


def _make_trader(mock_post: MagicMock) -> KiwoomPaperTrader:
    """KiwoomPaperTrader.__init__을 건너뛰어 인증/네트워크 없이 인스턴스 생성."""
    trader = object.__new__(KiwoomPaperTrader)
    client = KiwoomClient(use_mock=True)
    client.inject_token("dummy-token-for-tests")
    client._post = mock_post
    trader._client = client
    return trader


def _cntr_row(qty: int) -> dict:
    return {"ord_no": "0000001", "cntr_qty": str(qty), "ord_stt": "체결"}


class TestCheckExecution:
    def test_full_fill_sums_cntr_qty(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: [_cntr_row(10)]}, {}))
        trader = _make_trader(mock_post)
        assert trader.check_execution("005930", "0000001", is_buy=True) == 10

    def test_multiple_rows_summed(self):
        mock_post = MagicMock(
            return_value=({_RESPONSE_KEY: [_cntr_row(3), _cntr_row(4)]}, {})
        )
        trader = _make_trader(mock_post)
        assert trader.check_execution("005930", "0000001", is_buy=True) == 7

    def test_empty_cntr_returns_zero(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: []}, {}))
        trader = _make_trader(mock_post)
        assert trader.check_execution("005930", "0000001", is_buy=False) == 0

    def test_post_exception_returns_zero_not_raised(self):
        mock_post = MagicMock(side_effect=RuntimeError("API 오류 [ka10076]"))
        trader = _make_trader(mock_post)
        assert trader.check_execution("005930", "0000001", is_buy=True) == 0

    def test_malformed_cntr_qty_ignored(self):
        mock_post = MagicMock(
            return_value=({_RESPONSE_KEY: [{"cntr_qty": "abc"}, _cntr_row(5)]}, {})
        )
        trader = _make_trader(mock_post)
        assert trader.check_execution("005930", "0000001", is_buy=True) == 5

    def test_sell_tp_and_body_fields(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: []}, {}))
        trader = _make_trader(mock_post)
        trader.check_execution("005930", "0000123", is_buy=False)
        call_body = mock_post.call_args[0][2]
        assert call_body["stk_cd"] == "005930"
        assert call_body["qry_tp"] == "1"
        assert call_body["sell_tp"] == "1"          # 매도 조회
        assert call_body["ord_no"] == "0000123"

        trader.check_execution("005930", "0000123", is_buy=True)
        call_body2 = mock_post.call_args[0][2]
        assert call_body2["sell_tp"] == "2"          # 매수 조회


class TestConfirmFill:
    def test_full_fill_stops_after_first_attempt(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: [_cntr_row(10)]}, {}))
        trader = _make_trader(mock_post)
        filled = trader.confirm_fill("005930", "0000001", qty=10, is_buy=True, delay_s=0)
        assert filled == 10
        assert mock_post.call_count == 1

    def test_never_filled_polls_all_attempts(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: []}, {}))
        trader = _make_trader(mock_post)
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, attempts=3, delay_s=0
        )
        assert filled == 0
        assert mock_post.call_count == 3

    def test_fills_on_second_attempt(self):
        mock_post = MagicMock(side_effect=[
            ({_RESPONSE_KEY: []}, {}),
            ({_RESPONSE_KEY: [_cntr_row(10)]}, {}),
        ])
        trader = _make_trader(mock_post)
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, attempts=3, delay_s=0
        )
        assert filled == 10
        assert mock_post.call_count == 2

    def test_partial_fill_returns_last_observed_amount(self):
        mock_post = MagicMock(return_value=({_RESPONSE_KEY: [_cntr_row(4)]}, {}))
        trader = _make_trader(mock_post)
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, attempts=2, delay_s=0
        )
        assert filled == 4
        assert mock_post.call_count == 2
