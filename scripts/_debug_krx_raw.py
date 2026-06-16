"""KRX 세션 원시 응답 진단 (실행: python scripts/_debug_krx_raw.py)"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

import requests

JSESSIONID = os.environ.get("KRX_SESSION", "")
VISITOR_ID  = os.environ.get("KRX_VISITOR", "")

BASE   = "https://data.krx.co.kr"
WARMUP = BASE + "/contents/MDC/MDI/index.cmd"
DATA   = BASE + "/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":          BASE + "/contents/MDC/MDI/outerLoader/index.cmd",
    "X-Requested-With": "XMLHttpRequest",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
}

print(f"[debug] JSESSIONID 길이: {len(JSESSIONID)} / 앞5자: {JSESSIONID[:5]!r}")
print(f"[debug] KRX_VISITOR 길이: {len(VISITOR_ID)}")

s = requests.Session()
if JSESSIONID:
    s.cookies.set("JSESSIONID", JSESSIONID, domain=".krx.co.kr")
    s.cookies.set("mdc.client_session", "true", domain="data.krx.co.kr")
if VISITOR_ID:
    s.cookies.set("__smVisitorID", VISITOR_ID, domain="data.krx.co.kr")

print(f"\n[step 1] warmup 건너뜀 (KRX_SESSION 주입 시 서버가 새 JSESSIONID 덮어쓰는 버그 회피)")

print(f"\n[step 2] data POST {DATA}")
payload = {
    "bld":        "dbms/MDC/STAT/standard/MDCSTAT02302",
    "isuSrtCd":  "005930",
    "isuCd":     "KR7005930003",  # 삼성전자 ISIN
    "strtDd":    "20260601",
    "endDd":     "20260615",
    "inqTpCd":   "1",
    "trdVolVal": "1",
    "askBid":    "3",
    "csvxls_isNo": "false",
}
try:
    r1 = s.post(DATA, data=payload, headers=HEADERS, timeout=30)
    print(f"         status={r1.status_code}")
    print(f"         Content-Type: {r1.headers.get('Content-Type','')}")
    raw = r1.content
    print(f"         bytes: {len(raw)}")
    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("euc-kr", errors="replace")
    print(f"         응답 앞 500자:\n{text[:500]}")
except Exception as e:
    print(f"         FAIL: {e}")
