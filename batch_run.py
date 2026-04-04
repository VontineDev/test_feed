"""100개 종목 거래량 패턴 배치 분석
- yf.download 배치 호출로 API 요청 최소화 (rate-limit 방지)
- volume_pattern.py의 build_report 함수 재사용
- 결과: reports/ 폴더에 개별 .txt + 통합 HTML 리포트
"""

import sys, os, time
from datetime import datetime
import pandas as pd
import yfinance as yf

# volume_pattern.py와 같은 디렉터리에서 실행
sys.path.insert(0, os.path.dirname(__file__))
from volume_pattern import build_report, save_report

# ── 100개 종목 (한글명, KRX 코드) ──────────────────────────────
STOCKS = [
    ("삼성전자",         "005930"),
    ("SK하이닉스",       "000660"),
    ("SK오션플랜트",     "009070"),
    ("넥스틸",           "009140"),
    ("삼천당제약",       "000250"),
    ("두산에너빌리티",   "034020"),
    ("NAVER",            "035420"),
    ("SK이터닉스",       "403870"),
    ("삼성전기",         "009150"),
    ("현대차",           "005380"),
    ("미래에셋증권",     "006800"),
    ("에코프로",         "086520"),
    ("한국항공우주",     "047810"),
    ("현대로템",         "064350"),
    ("셀트리온",         "068270"),
    ("펄어비스",         "263750"),
    ("대명에너지",       "389260"),
    ("OCI홀딩스",        "010060"),
    ("애경케미칼",       "161000"),
    ("현대모비스",       "012330"),
    ("LIG넥스원",        "079550"),
    ("삼성SDI",          "006400"),
    ("LG이노텍",         "011070"),
    ("S-Oil",            "010950"),
    ("신한지주",         "055550"),
    ("한화시스템",       "272210"),
    ("한화에어로스페이스","012450"),
    ("한화비전",         "213580"),
    ("한올바이오파마",   "009420"),
    ("포스코인터내셔널", "047050"),
    ("코웨이",           "021240"),
    ("한화솔루션",       "009830"),
    ("현대건설",         "000720"),
    ("엘앤에프",         "066970"),
    ("우리금융지주",     "316140"),
    ("한미반도체",       "042700"),
    ("에코프로비엠",     "247540"),
    ("TCC스틸",          "002710"),
    ("SKC",              "011790"),
    ("한텍",             "210540"),
    ("SK이노베이션",     "096770"),
    ("오리온",           "271560"),
    ("삼아알미늄",       "006110"),
    ("LS ELECTRIC",      "010120"),
    ("노타",             "278990"),
    ("비츠로셀",         "082920"),
    ("HD건설기계",       "267270"),
    ("지아이이노베이션", "358570"),
    ("삼성생명",         "032830"),
    ("클래시스",         "214150"),
    ("POSCO홀딩스",      "005490"),
    ("SK바이오팜",       "326030"),
    ("HL만도",           "204320"),
    ("메디포스트",       "078160"),
    ("로보티즈",         "108490"),
    ("현대무벡스",       "228670"),
    ("이수스페셜티케미컬","457390"),
    ("한화오션",         "042660"),
    ("리노공업",         "058470"),
    ("동국제약",         "086450"),
    ("에임드바이오",     "388790"),
    ("대원제약",         "003220"),
    ("대주전자재료",     "078600"),
    ("리가켐바이오",     "141080"),
    ("셀바스AI",         "108860"),
    ("삼성E&A",          "028050"),
    ("원텍",             "336570"),
    ("LG화학",           "051910"),
    ("삼성화재",         "000810"),
    ("하이브",           "352820"),
    ("스피어",           "453340"),
    ("HD현대",           "267250"),
    ("알테오젠",         "196170"),
    ("SK스퀘어",         "402340"),
    ("시노펙스",         "025320"),
    ("한전기술",         "053050"),
    ("DL이앤씨",         "375500"),
    ("삼현",             "369300"),
    ("에이비엘바이오",   "298380"),
    ("DB손해보험",       "005830"),
    ("크래프톤",         "259960"),
    ("HMM",              "011200"),
    ("LG에너지솔루션",   "373220"),
    ("일진전기",         "103590"),
    ("현대오토에버",     "307950"),
    ("씨에스윈드",       "112610"),
    ("HLB제약",          "067630"),
    ("RFHIC",            "218410"),
    ("솔브레인",         "357780"),
    ("신대양제지",       "015350"),
    ("한전KPS",          "051600"),
    ("SK가스",           "018670"),
    ("알지노믹스",       "311690"),
    ("CJ제일제당",       "097950"),
    ("현대글로비스",     "086280"),
    ("포스코퓨처엠",     "003670"),
    ("에이프릴바이오",   "397030"),
    ("삼성중공업",       "010140"),
    ("고려아연",         "010130"),
    ("오름테라퓨틱",     "475270"),
]

# 당일 제공된 등락률 (리포트용 참고 데이터)
CHANGE_PCT = {
    "삼성전자": -7.38, "SK하이닉스": -7.83, "SK오션플랜트": +2.21,
    "넥스틸": +24.31, "삼천당제약": -21.90, "두산에너빌리티": -6.92,
    "NAVER": -7.09, "SK이터닉스": +0.18, "삼성전기": -7.53,
    "현대차": -6.45, "미래에셋증권": -8.40, "에코프로": -5.76,
    "한국항공우주": -2.92, "현대로템": +7.25, "셀트리온": -5.87,
    "펄어비스": -10.83, "대명에너지": +13.22, "OCI홀딩스": +10.85,
    "애경케미칼": +5.09, "현대모비스": -5.86, "LIG넥스원": -1.13,
    "삼성SDI": +0.92, "LG이노텍": -6.55, "S-Oil": -5.57,
    "신한지주": 0.00, "한화시스템": +0.62, "한화에어로스페이스": +5.70,
    "한화비전": -7.01, "한올바이오파마": -24.70, "포스코인터내셔널": +4.49,
    "코웨이": -0.27, "한화솔루션": -7.05, "현대건설": -7.75,
    "엘앤에프": +8.54, "우리금융지주": -2.43, "한미반도체": -8.22,
    "에코프로비엠": -4.00, "TCC스틸": +3.53, "SKC": -4.67,
    "한텍": +3.97, "SK이노베이션": +1.74, "오리온": -1.51,
    "삼아알미늄": -0.87, "LS ELECTRIC": -5.41, "노타": -2.20,
    "비츠로셀": -1.42, "HD건설기계": -4.99, "지아이이노베이션": -11.74,
    "삼성생명": -3.08, "클래시스": -4.02, "POSCO홀딩스": -2.16,
    "SK바이오팜": -6.06, "HL만도": -6.47, "메디포스트": -9.95,
    "로보티즈": -6.36, "현대무벡스": +3.10, "이수스페셜티케미컬": -5.88,
    "한화오션": -6.37, "리노공업": -6.59, "동국제약": -5.34,
    "에임드바이오": -2.72, "대원제약": -2.86, "대주전자재료": -9.08,
    "리가켐바이오": -13.33, "셀바스AI": -10.38, "삼성E&A": -8.44,
    "원텍": -7.55, "LG화학": -3.68, "삼성화재": -2.09,
    "하이브": -8.46, "스피어": -9.14, "HD현대": -3.03,
    "알테오젠": -4.30, "SK스퀘어": -7.68, "시노펙스": -6.22,
    "한전기술": -7.65, "DL이앤씨": -8.68, "삼현": +2.18,
    "에이비엘바이오": -11.38, "DB손해보험": -2.08, "크래프톤": -4.00,
    "HMM": -2.30, "LG에너지솔루션": -1.84, "일진전기": -7.97,
    "현대오토에버": -4.60, "씨에스윈드": +3.61, "HLB제약": -5.35,
    "RFHIC": -9.55, "솔브레인": -6.56, "신대양제지": +0.55,
    "한전KPS": -5.98, "SK가스": -0.57, "알지노믹스": -14.56,
    "CJ제일제당": -0.45, "현대글로비스": -4.33, "포스코퓨처엠": 0.00,
    "에이프릴바이오": -6.73, "삼성중공업": -0.74, "고려아연": -3.23,
    "오름테라퓨틱": -10.75,
}


def batch_download(tickers_ks: list[str]) -> dict:
    """yf.download 배치 호출 → {ticker: DataFrame} 반환"""
    print(f"\n  [배치 다운로드] {len(tickers_ks)}개 티커 5분봉 요청 중...")
    # group_by='ticker'로 티커별 분리
    raw = yf.download(
        tickers_ks,
        period="5d",
        interval="5m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    result = {}
    for t in tickers_ks:
        try:
            if len(tickers_ks) == 1:
                df = raw.copy()
            else:
                df = raw[t].copy()
            df = df.dropna(how="all")
            if not df.empty:
                df.index = df.index.tz_convert("Asia/Seoul")
            result[t] = df
        except Exception as e:
            print(f"  [WARN] {t} 슬라이스 실패: {e}")
            result[t] = pd.DataFrame()
    return result


def make_html_report(summaries: list[dict], date_str: str) -> str:
    """개별 리포트 요약을 하나의 HTML 파일로 합친다."""
    up   = [s for s in summaries if s["pct"] > 0]
    down = [s for s in summaries if s["pct"] < 0]
    flat = [s for s in summaries if s["pct"] == 0]

    def color(pct):
        if pct > 0:   return "#d32f2f"
        if pct < 0:   return "#1565c0"
        return "#555"

    def sign(pct):
        return f"+{pct:.2f}%" if pct > 0 else f"{pct:.2f}%"

    rows_html = ""
    for i, s in enumerate(summaries, 1):
        c = color(s["pct"])
        peak = s.get("peak_hour", "N/A")
        vol  = f"{s['avg_daily_vol']:,}" if s.get("avg_daily_vol") else "N/A"
        data_ok = "✓" if s["has_data"] else "✗"
        rows_html += f"""
        <tr>
          <td style="color:#888;text-align:center">{i}</td>
          <td><strong>{s['name']}</strong><br><span style="color:#aaa;font-size:11px">{s['ticker']}</span></td>
          <td style="color:{c};font-weight:bold;text-align:right">{sign(s['pct'])}</td>
          <td style="text-align:right">{peak}</td>
          <td style="text-align:right">{vol}</td>
          <td style="text-align:center;color:{'green' if s['has_data'] else 'red'}">{data_ok}</td>
        </tr>"""

    up_names   = ", ".join(s["name"] for s in sorted(up,   key=lambda x: -x["pct"])[:5])
    down_names = ", ".join(s["name"] for s in sorted(down, key=lambda x:  x["pct"])[:5])

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>거래대금 상위 100 종목 볼륨 패턴 리포트 {date_str}</title>
<style>
  body {{ font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif; background:#f9f9f9; margin:0; padding:20px; }}
  h1 {{ font-size:22px; border-bottom:2px solid #333; padding-bottom:8px; }}
  h2 {{ font-size:16px; margin-top:30px; color:#333; }}
  .summary-box {{ display:flex; gap:16px; flex-wrap:wrap; margin:16px 0; }}
  .card {{ background:#fff; border-radius:8px; padding:14px 20px; box-shadow:0 1px 4px rgba(0,0,0,.1); min-width:140px; }}
  .card .num {{ font-size:28px; font-weight:bold; }}
  .card .lbl {{ font-size:12px; color:#888; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:8px; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,.08); }}
  th {{ background:#222; color:#fff; padding:8px 12px; font-size:13px; text-align:left; }}
  td {{ padding:7px 12px; font-size:13px; border-bottom:1px solid #f0f0f0; }}
  tr:hover td {{ background:#f5f5f5; }}
  .note {{ font-size:12px; color:#888; margin-top:6px; }}
  pre {{ background:#1e1e1e; color:#d4d4d4; padding:16px; border-radius:6px; font-size:11px;
         overflow-x:auto; white-space:pre-wrap; word-break:break-all; }}
  details {{ margin:6px 0; }}
  summary {{ cursor:pointer; color:#1565c0; font-size:13px; padding:4px 0; }}
</style>
</head>
<body>
<h1>📊 거래대금 상위 100 종목 — 볼륨 패턴 리포트</h1>
<p style="color:#555;font-size:13px">생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;|&nbsp; 데이터: yfinance 5분봉 5거래일</p>

<div class="summary-box">
  <div class="card"><div class="num" style="color:#d32f2f">{len(up)}</div><div class="lbl">상승 종목</div></div>
  <div class="card"><div class="num" style="color:#1565c0">{len(down)}</div><div class="lbl">하락 종목</div></div>
  <div class="card"><div class="num" style="color:#555">{len(flat)}</div><div class="lbl">보합 종목</div></div>
  <div class="card"><div class="num">{len(summaries)}</div><div class="lbl">분석 완료</div></div>
</div>

<h2>📈 당일 등락 상위 5 (상승)</h2>
<p style="font-size:13px">{up_names or "없음"}</p>
<h2>📉 당일 등락 하위 5 (하락)</h2>
<p style="font-size:13px">{down_names or "없음"}</p>

<h2>📋 종목별 요약</h2>
<table>
  <thead>
    <tr>
      <th style="width:40px">#</th>
      <th>종목명</th>
      <th style="text-align:right">당일등락</th>
      <th style="text-align:right">피크시간대</th>
      <th style="text-align:right">일평균거래량</th>
      <th style="text-align:center">데이터</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
<p class="note">* 피크시간대: 5거래일 평균 거래량 기준 최대 시간대 (한국시간)</p>

<h2>📄 종목별 상세 리포트</h2>
"""

    for s in summaries:
        html += f"""
<details>
  <summary>[{s['ticker']}] {s['name']} — {sign(s['pct'])}</summary>
  <pre>{s.get('report_text', '데이터 없음')}</pre>
</details>"""

    html += "\n</body>\n</html>"
    return html


def main():
    os.makedirs("reports", exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    tickers_ks = [f"{code}.KS" for _, code in STOCKS]

    # ── 1) 배치 다운로드 (API 요청 1회) ──────────────────────────
    data_map = batch_download(tickers_ks)

    # ── 2) yfinance가 멀티-레벨 컬럼을 반환할 때 처리 ─────────────
    # 일부 버전에서는 single-ticker라도 이중 컬럼이 올 수 있으므로 fallback
    summaries = []
    failed = []

    for (name, code), ticker in zip(STOCKS, tickers_ks):
        df = data_map.get(ticker, pd.DataFrame())
        pct = CHANGE_PCT.get(name, 0.0)

        summary = {"name": name, "ticker": ticker, "pct": pct, "has_data": False}

        if df is None or df.empty:
            print(f"  [SKIP] {name} ({ticker}) — 데이터 없음")
            summary["report_text"] = "yfinance 데이터를 가져올 수 없었습니다."
            failed.append(name)
        else:
            try:
                report_txt = build_report(df, ticker, name, "", "KR", "yfinance")
                save_report(report_txt, name, "")

                # 피크시간대 추출
                df2 = df.copy()
                df2["hour"] = df2.index.hour
                trading_days = df2.index.normalize().nunique() or 1
                hourly = df2.groupby("hour")["Volume"].sum()
                peak_hour = int(hourly.idxmax()) if not hourly.empty else None
                avg_vol   = int(df2["Volume"].sum() / trading_days)

                summary["has_data"]     = True
                summary["peak_hour"]    = f"{peak_hour:02d}:00" if peak_hour is not None else "N/A"
                summary["avg_daily_vol"]= avg_vol
                summary["report_text"]  = report_txt
                print(f"  [OK]   {name} ({ticker}) — 피크 {summary['peak_hour']}, 일평균 {avg_vol:,}주")
            except Exception as e:
                print(f"  [ERR]  {name} ({ticker}) — {e}")
                summary["report_text"] = f"리포트 생성 오류: {e}"
                failed.append(name)

        summaries.append(summary)

    # ── 3) 통합 HTML 저장 ─────────────────────────────────────────
    html = make_html_report(summaries, date_str)
    out_dir = "/sessions/blissful-inspiring-planck/mnt/test_feed"
    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, f"volume_pattern_report_{date_str}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  ✅ 통합 HTML 리포트 저장: {html_path}")
    print(f"  ✅ 개별 txt 저장: reports/ ({len(summaries)-len(failed)}개 성공)")
    if failed:
        print(f"  ⚠️  데이터 없음: {', '.join(failed)}")


if __name__ == "__main__":
    main()
