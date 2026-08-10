"""check_execution() / confirm_fill() 단위 테스트.

check_execution(): ka10076 체결요청 (Kiwoom REST API 문서 p.190-192)
  - URL: /api/dostk/acnt
  - 응답 배열 키: cntr, 체결수량 필드: cntr_qty
  - place_buy()/place_sell()이 반환한 ord_no가 실제로 체결됐는지 확인하는 용도.
  - 2026-08-05: 실 계좌 검증 결과 이 모의투자 계좌에서 신규 주문도 항상 빈
    체결내역을 반환하는 것으로 확인돼 confirm_fill()의 내부 구현에서는
    더 이상 쓰지 않음(get_position_qty() 델타로 대체). 메서드 자체는 응답
    파싱 로직이 독립적으로 올바르므로 유지 — TestCheckExecution 테스트 참고.

confirm_fill(): get_position_qty()(get_positions()/kt00018) 전후 스냅샷
  델타로 실제 체결 수량을 추정 — TestConfirmFill 참고.
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
    """2026-08-05: ka10076이 이 모의투자 계좌에서 신규 주문도 항상 빈 체결내역을
    반환하는 것으로 확인돼(실제로는 100% 체결된 주문이 전부 미확인 처리됨),
    confirm_fill()은 check_execution()(ka10076) 대신 get_position_qty()
    (get_positions()/kt00018 기반) 전후 스냅샷 델타로 체결량을 추정하도록 변경됨.
    """

    def test_full_fill_stops_after_first_attempt(self):
        trader = _make_trader(MagicMock())
        trader.get_position_qty = MagicMock(return_value=10)  # qty_before=0 → +10
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, qty_before=0, delay_s=0
        )
        assert filled == 10
        assert trader.get_position_qty.call_count == 1

    def test_never_filled_polls_all_attempts(self):
        trader = _make_trader(MagicMock())
        trader.get_position_qty = MagicMock(return_value=0)  # qty_before=0 → 변화 없음
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, qty_before=0,
            attempts=3, delay_s=0,
        )
        assert filled == 0
        assert trader.get_position_qty.call_count == 3

    def test_fills_on_second_attempt(self):
        trader = _make_trader(MagicMock())
        trader.get_position_qty = MagicMock(side_effect=[0, 10])  # qty_before=0
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, qty_before=0,
            attempts=3, delay_s=0,
        )
        assert filled == 10
        assert trader.get_position_qty.call_count == 2

    def test_partial_fill_returns_last_observed_amount(self):
        trader = _make_trader(MagicMock())
        trader.get_position_qty = MagicMock(return_value=4)  # qty_before=0 → +4
        filled = trader.confirm_fill(
            "005930", "0000001", qty=10, is_buy=True, qty_before=0,
            attempts=2, delay_s=0,
        )
        assert filled == 4
        assert trader.get_position_qty.call_count == 2

    def test_sell_delta_uses_qty_before_minus_qty_now(self):
        trader = _make_trader(MagicMock())
        trader.get_position_qty = MagicMock(return_value=0)  # 100 → 0, 전량 매도
        filled = trader.confirm_fill(
            "005930", "0000001", qty=100, is_buy=False, qty_before=100, delay_s=0
        )
        assert filled == 100

    def test_concurrent_other_model_holding_does_not_break_delta(self):
        """같은 티커를 다른 모델이 동시 보유해도, 델타 계산이라 이 주문분만 잡아낸다."""
        trader = _make_trader(MagicMock())
        # 이 모델 100주 + 다른 모델 50주 = before 150 → 이 모델분만 매도돼 50 남음
        trader.get_position_qty = MagicMock(return_value=50)
        filled = trader.confirm_fill(
            "005930", "0000001", qty=100, is_buy=False, qty_before=150, delay_s=0
        )
        assert filled == 100


class TestGetPositionsStkCdNormalization:
    """kt00018은 종목코드를 "A005930"처럼 거래소 접두사를 붙여 반환한다.

    2026-08-05 ~ 2026-08-10 회귀 버그: get_positions()가 접두사를 그대로 남겨두면
    get_position_qty()의 `stk_cd == _to_6digit(ticker)` 비교가 절대 참이 될 수
    없어 보유수량이 항상 0으로 잡힌다 → confirm_fill()이 모든 매수/매도를
    미체결로 오판 → 실제로는 체결된 매도 주문이 DB에서 청산 확정되지 않고
    청산 필요 수량이 부풀려진 채 open 상태로 남아, 다음 실행에서 "매도가능수량이
    부족합니다"(800033) 브로커 거부로 이어졌다(2026-08-10 investigate 세션에서
    실계좌 대조로 확인).
    """

    def _positions_response(self, rows: list[dict]) -> tuple[dict, dict]:
        return {"acnt_evlt_remn_indv_tot": rows}, {}

    def test_strips_exchange_prefix(self):
        mock_post = MagicMock(
            return_value=self._positions_response(
                [{"stk_cd": "A005930", "stk_nm": "삼성전자", "rmnd_qty": "10"}]
            )
        )
        trader = _make_trader(mock_post)
        positions = trader.get_positions()
        assert positions[0]["stk_cd"] == "005930"

    def test_get_position_qty_matches_prefixed_broker_response(self):
        """kt00018의 실제 응답 형태("A"+6자리)를 그대로 넣어도 매칭돼야 한다."""
        mock_post = MagicMock(
            return_value=self._positions_response(
                [{"stk_cd": "A000500", "stk_nm": "가온전선", "rmnd_qty": "7"}]
            )
        )
        trader = _make_trader(mock_post)
        assert trader.get_position_qty("000500.KS") == 7

    def test_get_position_qty_returns_zero_when_not_held(self):
        mock_post = MagicMock(
            return_value=self._positions_response(
                [{"stk_cd": "A005930", "stk_nm": "삼성전자", "rmnd_qty": "10"}]
            )
        )
        trader = _make_trader(mock_post)
        assert trader.get_position_qty("000660.KS") == 0

    def test_non_prefixed_stk_cd_left_untouched(self):
        """접두사가 없는 응답이 와도 (방어적으로) 그대로 통과시켜야 한다."""
        mock_post = MagicMock(
            return_value=self._positions_response(
                [{"stk_cd": "005930", "stk_nm": "삼성전자", "rmnd_qty": "5"}]
            )
        )
        trader = _make_trader(mock_post)
        assert trader.get_position_qty("005930.KS") == 5
