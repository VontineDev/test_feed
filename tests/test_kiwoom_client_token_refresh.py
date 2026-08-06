"""KiwoomClient._post() 토큰 자동 재발급 단위 테스트.

2026-08-04 라이브 확인: issue_token()은 프로세스 시작 시 단 한 번만 호출되고
갱신 로직이 없어, 토큰이 만료되면(관측상 발급 후 ~18~21시간) 그 뒤로는 재시작
전까지 모든 API 호출이 8005("Token이 유효하지 않습니다")로 계속 실패했다 —
이 기간 동안 paper-exit이 포지션 전부 "현재가 없음"으로 스킵해 손절 감시가
하루 이상 무력화됨. _post()가 8005를 감지하면 자동 재발급 후 1회 재시도하도록
수정했다.
"""
from unittest.mock import MagicMock

from data.kiwoom_aftermarket_sync import KiwoomClient


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data


def _make_client() -> KiwoomClient:
    """__init__을 건너뛰어 실제 네트워크 세션 없이 인스턴스 생성."""
    client = object.__new__(KiwoomClient)
    client._base = "https://api.kiwoom.com"
    client._session = MagicMock()
    client._token = None
    client._token_expires = None
    client._appkey = None
    client._secretkey = None
    return client


_TOKEN_RESP = _FakeResponse(200, {
    "return_code": 0, "token": "fresh-token-2", "expires_dt": "20260807120000",
})
_AUTH_ERROR_RESP = _FakeResponse(200, {
    "return_code": 1, "return_msg": "인증에 실패했습니다[8005:Token이 유효하지 않습니다]",
})
_OTHER_ERROR_RESP = _FakeResponse(200, {
    "return_code": 1, "return_msg": "그 외 오류",
})
_SUCCESS_RESP = _FakeResponse(200, {"return_code": 0, "data": "ok"})


class TestIssueTokenStoresCredentials:
    def test_issue_token_stores_appkey_and_secretkey(self):
        client = _make_client()
        client._session.post = MagicMock(return_value=_FakeResponse(200, {
            "return_code": 0, "token": "t1", "expires_dt": "20260807120000",
        }))
        client.issue_token("my-appkey", "my-secret")
        assert client._appkey == "my-appkey"
        assert client._secretkey == "my-secret"
        assert client._token == "t1"


class TestPostTokenRefresh:
    def test_success_no_refresh_needed(self):
        client = _make_client()
        client._token = "old-token"
        client._session.post = MagicMock(return_value=_SUCCESS_RESP)
        data, _ = client._post("/api/dostk/stkinfo", "ka10001", {})
        assert data["data"] == "ok"
        assert client._session.post.call_count == 1

    def test_8005_triggers_reissue_and_retry_succeeds(self):
        client = _make_client()
        client._token = "expired-token"
        client._appkey, client._secretkey = "ak", "sk"
        client._session.post = MagicMock(side_effect=[
            _AUTH_ERROR_RESP,   # 1차: 만료된 토큰으로 API 호출 → 8005
            _TOKEN_RESP,        # issue_token() 내부 재발급 호출
            _SUCCESS_RESP,      # 2차: 새 토큰으로 재시도 → 성공
        ])
        data, _ = client._post("/api/dostk/stkinfo", "ka10001", {})
        assert data["data"] == "ok"
        assert client._token == "fresh-token-2"
        assert client._session.post.call_count == 3

    def test_8005_without_stored_credentials_raises_immediately(self):
        """inject_token()만 쓴 경우(appkey/secretkey 없음) — 재발급 시도하지 않고 즉시 예외."""
        client = _make_client()
        client._token = "injected-token"  # appkey/secretkey는 None
        client._session.post = MagicMock(return_value=_AUTH_ERROR_RESP)
        try:
            client._post("/api/dostk/stkinfo", "ka10001", {})
            assert False, "RuntimeError를 기대했으나 발생하지 않음"
        except RuntimeError as e:
            assert "8005" in str(e)
        assert client._session.post.call_count == 1  # 재시도 없음

    def test_still_invalid_after_refresh_raises_without_infinite_loop(self):
        client = _make_client()
        client._token = "expired-token"
        client._appkey, client._secretkey = "ak", "sk"
        client._session.post = MagicMock(side_effect=[
            _AUTH_ERROR_RESP,   # 1차 호출 → 8005
            _TOKEN_RESP,        # 재발급
            _AUTH_ERROR_RESP,   # 2차 호출도 여전히 8005 (재발급해도 안 되는 케이스)
        ])
        try:
            client._post("/api/dostk/stkinfo", "ka10001", {})
            assert False, "RuntimeError를 기대했으나 발생하지 않음"
        except RuntimeError as e:
            assert "8005" in str(e)
        # 1회만 재발급 시도(무한루프 없음) — session.post는 정확히 3번만 호출됨
        assert client._session.post.call_count == 3

    def test_non_token_error_raises_without_refresh_attempt(self):
        client = _make_client()
        client._token = "some-token"
        client._appkey, client._secretkey = "ak", "sk"
        client._session.post = MagicMock(return_value=_OTHER_ERROR_RESP)
        try:
            client._post("/api/dostk/stkinfo", "ka10001", {})
            assert False, "RuntimeError를 기대했으나 발생하지 않음"
        except RuntimeError as e:
            assert "그 외 오류" in str(e)
        assert client._session.post.call_count == 1

    def test_429_retry_still_works_after_refactor(self):
        """헤더 생성을 루프 안으로 옮긴 리팩토링이 기존 429 재시도 동작을 깨지 않는지 확인."""
        client = _make_client()
        client._token = "some-token"
        rate_limited = _FakeResponse(429, {}, headers={"Retry-After": "0"})
        client._session.post = MagicMock(side_effect=[rate_limited, _SUCCESS_RESP])
        data, _ = client._post("/api/dostk/stkinfo", "ka10001", {})
        assert data["data"] == "ok"
        assert client._session.post.call_count == 2
