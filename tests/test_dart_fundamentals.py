"""data/dart_sync.py::extract_fundamentals() 단위 테스트.

fnlttSinglAcntAll 응답은 재무상태표(BS)/손익계산서(IS,CIS)/자본변동표(SCE) 등을
한 응답에 섞어 반환하고, 같은 account_nm이 여러 섹션에 중복 등장할 수 있다
(2026-08-06 삼성전자 라이브 프로브로 확인: "자본총계"가 SCE 여러 열에서 8회
반복, "당기순이익"이 총계/지배기업귀속/비지배지분 3종으로 반복). sj_div 필터링
+ 첫 매칭 채택 로직이 정확한 합계만 뽑아내는지 검증한다.
"""
from data.dart_sync import extract_fundamentals


def _row(sj_div, account_nm, thstrm, frmtrm=None):
    return {
        "sj_div": sj_div, "account_nm": account_nm,
        "thstrm_amount": str(thstrm), "frmtrm_amount": str(frmtrm) if frmtrm is not None else "",
    }


class TestExtractFundamentals:
    def test_extracts_bs_and_is_accounts(self):
        items = [
            _row("BS", "자산총계", 1000, 900),
            _row("BS", "부채총계", 400, 350),
            _row("BS", "자본총계", 600, 550),
            _row("IS", "매출액", 2000, 1800),
            _row("IS", "당기순이익", 150, 120),
        ]
        result = extract_fundamentals(items)
        assert result["assets"] == 1000
        assert result["liabilities"] == 400
        assert result["equity"] == 600
        assert result["revenue"] == 2000
        assert result["revenue_prev"] == 1800
        assert result["net_income"] == 150

    def test_ignores_non_bs_rows_for_balance_sheet_accounts(self):
        """자본변동표(SCE) 등에서 "자본총계"가 반복 등장해도 BS 섹션 값만 채택."""
        items = [
            _row("SCE", "자본총계", 999),   # 노이즈 — 무시돼야 함
            _row("SCE", "자본총계", 888),   # 노이즈
            _row("BS", "자본총계", 600, 550),  # 진짜 값
        ]
        result = extract_fundamentals(items)
        assert result["equity"] == 600

    def test_first_matching_net_income_wins_over_later_duplicates(self):
        """당기순이익이 총계/지배기업귀속/비지배지분 순으로 반복 등장 — 첫 값(총계) 채택."""
        items = [
            _row("IS", "당기순이익", 45206),   # 총계 (원본 표시 순서상 최상단)
            _row("IS", "당기순이익", 44260),   # 지배기업 소유주지분 (중복, 무시돼야 함)
            _row("IS", "당기순이익", 945),     # 비지배지분 (중복, 무시돼야 함)
        ]
        result = extract_fundamentals(items)
        assert result["net_income"] == 45206

    def test_cis_accepted_as_income_statement_when_is_missing(self):
        items = [_row("CIS", "당기순이익", 300)]
        result = extract_fundamentals(items)
        assert result["net_income"] == 300

    def test_net_income_loss_suffix_variant_matches(self):
        """2026-08-06 라이브 확인: 상당수 회사가 "당기순이익" 대신
        "당기순이익(손실)"을 쓴다 — 별칭 매칭 확인."""
        items = [_row("CIS", "당기순이익(손실)", -500, -100)]
        result = extract_fundamentals(items)
        assert result["net_income"] == -500

    def test_revenue_ifrs_wording_variant_matches(self):
        """일부 회사가 "매출액" 대신 "수익(매출액)"을 쓴다."""
        items = [_row("IS", "수익(매출액)", 2000, 1800)]
        result = extract_fundamentals(items)
        assert result["revenue"] == 2000
        assert result["revenue_prev"] == 1800

    def test_missing_accounts_are_none(self):
        result = extract_fundamentals([_row("BS", "자산총계", 1000)])
        assert result.get("revenue") is None
        assert result.get("net_income") is None

    def test_empty_items_returns_empty_dict(self):
        assert extract_fundamentals([]) == {}

    def test_malformed_amount_string_becomes_none(self):
        items = [_row("BS", "자본총계", "N/A")]
        result = extract_fundamentals(items)
        assert result["equity"] is None

    def test_revenue_prev_only_captured_alongside_revenue(self):
        items = [_row("IS", "매출액", 2000, 1800)]
        result = extract_fundamentals(items)
        assert result["revenue"] == 2000
        assert result["revenue_prev"] == 1800
        # net_income 계정이 아예 없으면 관련 prev 키도 안 만들어짐
        assert "net_income_prev" not in result
