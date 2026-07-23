"""
scripts/probe_premarket_ka10032.py — 08:00~09:00 프리마켓 시간대 ka10032 데이터 포함 여부 확인.

routers_heatmap.py TODO 엣지1/2/5: 키움 ka10032(거래대금상위)가 장전 단일가
(08:00~09:00) 체결분을 trde_prica에 포함하는지 확인되지 않아 프리마켓 히트맵이
전일 스냅샷으로 폴백 중. 이 스크립트는 그 확인용 1회성 프로브다.

결과 해석:
  amount(trde_prica 파싱값)가 0이 아닌 종목이 있으면 → ka10032가 프리마켓을
  포함 → common.py에 _is_premarket() 추가해 실시간 경로로 연결 가능.
  전부 0이면 → 별도 프리마켓 API가 필요(범위 커짐).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv

load_dotenv()

from data.kiwoom_aftermarket_sync import KiwoomClient  # noqa: E402


def main() -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    appkey = os.environ.get("KIWOOM_APPKEY")
    secretkey = os.environ.get("KIWOOM_SECRETKEY")
    if not appkey or not secretkey:
        print(f"[{now}] KIWOOM_APPKEY / KIWOOM_SECRETKEY 환경변수 미설정 — 중단")
        return

    client = KiwoomClient(use_mock=False)
    client.issue_token(appkey, secretkey)
    items = client.fetch_top_volume(n=10)

    print(f"[{now}] ka10032 fetch_top_volume(n=10) 결과 — {len(items)}건")
    if not items:
        print("  (응답 없음 — 장 미개장 또는 전 종목 amount=0으로 필터링됨)")
        return
    for it in items:
        print(f"  rank={it['rank']:<3} ticker={it['ticker']:<8} "
              f"name={it['name']:<12} price={it['price']:<8} "
              f"change_pct={it['change_pct']:<7} amount={it['amount']}")

    nonzero = sum(1 for it in items if it.get("amount"))
    print(f"\n결론: amount>0 종목 {nonzero}/{len(items)} — "
          f"{'프리마켓 데이터 포함 가능성 높음' if nonzero else '프리마켓 데이터 미포함으로 추정'}")


if __name__ == "__main__":
    main()
