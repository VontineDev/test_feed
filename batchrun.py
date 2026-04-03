"""거래대금 상위 종목 일괄 거래량 패턴 분석

사용법:
    python batchrun.py              # KR_ALIASES 전체
    python batchrun.py 20           # 상위 20개만
    python batchrun.py report       # DB 캐시에서 HTML 리포트 생성
    python batchrun.py report 20    # 상위 20개만 리포트
"""

import sys
import os
import time
from datetime import datetime

import pandas as pd

from volume_pattern import (
    KR_ALIASES, resolve_ticker, fetch_data, build_report, save_report,
    save_to_db, _load_from_db,
)

# 거래대금 순위 (2026-04-02 기준)
TOP100 = [
    "삼성전자", "SK하이닉스", "SK오션플랜트", "넥스틸", "삼천당제약",
    "두산에너빌리티", "NAVER", "SK이터닉스", "삼성전기", "현대차",
    "미래에셋증권", "에코프로", "한국항공우주", "현대로템", "셀트리온",
    "펄어비스", "대명에너지", "OCI홀딩스", "애경케미칼", "현대모비스",
    "LIG넥스원", "삼성SDI", "LG이노텍", "S-Oil", "신한지주",
    "한화시스템", "한화에어로스페이스", "한화비전", "한올바이오파마",
    "포스코인터내셔널", "코웨이", "한화솔루션", "현대건설", "엘앤에프",
    "우리금융지주", "한미반도체", "에코프로비엠", "TCC스틸", "SKC",
    "한텍", "SK이노베이션", "오리온", "삼아알미늄", "LS ELECTRIC",
    "노타", "비츠로셀", "HD건설기계", "지아이이노베이션", "삼성생명",
    "클래시스", "POSCO홀딩스", "SK바이오팜", "HL만도", "메디포스트",
    "로보티즈", "현대무벡스", "이수스페셜티케미컬", "한화오션", "리노공업",
    "동국제약", "에임드바이오", "대원제약", "대주전자재료", "리가켐바이오",
    "셀바스AI", "삼성E&A", "원텍", "LG화학", "삼성화재",
    "하이브", "스피어", "HD현대", "알테오젠", "SK스퀘어",
    "시노펙스", "한전기술", "DL이앤씨", "삼현", "에이비엘바이오",
    "DB손해보험", "크래프톤", "HMM", "LG에너지솔루션", "일진전기",
    "현대오토에버", "씨에스윈드", "HLB제약", "RFHIC", "솔브레인",
    "신대양제지", "한전KPS", "SK가스", "알지노믹스", "CJ제일제당",
    "현대글로비스", "포스코퓨처엠", "에이프릴바이오", "삼성중공업",
    "고려아연", "오름테라퓨틱",
]


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else len(TOP100)
    targets = TOP100[:limit]

    print(f"\n{'='*70}")
    print(f"  거래대금 상위 {len(targets)}개 종목 일괄 분석")
    print(f"{'='*70}\n")

    success, fail, cached = 0, 0, 0
    failed_names = []

    for i, name in enumerate(targets, 1):
        ticker, display_name, market = resolve_ticker(name)
        tag = f"[{i:3d}/{len(targets)}]"
        print(f"{tag} {name} ({ticker}) ...", end=" ", flush=True)

        try:
            df, full_name, data_source = fetch_data(ticker, market)
            if df.empty:
                print("데이터 없음 ✗")
                fail += 1
                failed_names.append(name)
                continue

            report = build_report(df, ticker, display_name, full_name, market, data_source)
            save_report(report, display_name, full_name)

            if data_source == "db":
                print(f"DB 캐시 ✓")
                cached += 1
            else:
                # DB 저장
                import asyncio
                symbol = ticker.replace(".KS", "").replace(".KQ", "")
                try:
                    asyncio.run(save_to_db(df, symbol, market))
                except Exception:
                    pass
                print(f"API 조회 → 저장 ✓")
                # yfinance 속도 제한 방지
                time.sleep(0.5)

            success += 1
        except Exception as e:
            print(f"오류: {e} ✗")
            fail += 1
            failed_names.append(name)
            time.sleep(0.5)

    # 결과 요약
    print(f"\n{'='*70}")
    print(f"  완료: {success}개 성공 (캐시 {cached} / API {success - cached}) | {fail}개 실패")
    if failed_names:
        print(f"  실패 종목: {', '.join(failed_names)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
