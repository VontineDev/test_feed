"""시간대별 거래량 패턴 분석 — 한국/미국 주식 지원

사용법:
    python volume_pattern.py 삼성전자
    python volume_pattern.py 005930
    python volume_pattern.py AAPL
    python volume_pattern.py TSLA
"""

import sys
import os
from datetime import datetime

import pandas as pd
import yfinance as yf

# 한국 주식 별명 → KRX 종목코드
KR_ALIASES = {
    "삼성전자": "005930", "SK하이닉스": "000660", "LG에너지솔루션": "373220",
    "삼성바이오로직스": "207940", "현대차": "005380", "기아": "000270",
    "셀트리온": "068270", "KB금융": "105560", "신한지주": "055550",
    "POSCO홀딩스": "005490", "네이버": "035420", "카카오": "035720",
    "LG화학": "051910", "삼성SDI": "006400", "현대모비스": "012330",
    "삼성물산": "028260", "한국전력": "015760", "SK이노베이션": "096770",
    "LG전자": "066570", "포스코퓨처엠": "003670",
}

# 시장 판별용 타임존
MARKET_TZ = {
    "KR": "Asia/Seoul",
    "US": "America/New_York",
}


def resolve_ticker(raw: str) -> tuple[str, str, str]:
    """입력을 (yfinance 티커, 표시 이름, 시장 코드)로 변환한다."""
    raw = raw.strip()

    # 1) 한글 별명
    if raw in KR_ALIASES:
        code = KR_ALIASES[raw]
        return f"{code}.KS", raw, "KR"

    # 2) 순수 6자리 숫자 → KRX
    if raw.isdigit() and len(raw) == 6:
        return f"{raw}.KS", raw, "KR"

    # 3) 이미 .KS/.KQ 접미사 포함
    if raw.upper().endswith((".KS", ".KQ")):
        return raw.upper(), raw.split(".")[0], "KR"

    # 4) 나머지는 미국 주식으로 간주
    return raw.upper(), raw.upper(), "US"


def fetch_data(ticker: str, market: str):
    """5분봉 데이터를 가져와서 현지 시간으로 변환한다. 미국 주식은 프리/애프터마켓 포함."""
    t = yf.Ticker(ticker)
    prepost = (market == "US")
    df = t.history(period="5d", interval="5m", prepost=prepost)
    if df.empty:
        return df, ""

    # 미국 주식은 한국 시간 기준으로 표시
    tz = "Asia/Seoul" if market == "US" else MARKET_TZ[market]
    df.index = df.index.tz_convert(tz)

    # yfinance info에서 종목 이름 가져오기
    try:
        name = t.info.get("shortName") or t.info.get("longName") or ""
    except Exception:
        name = ""
    return df, name


def _kr_to_us_hour(kr_hour: int) -> int:
    """한국 시간 → 미국 동부 시간 (EDT: -13h, EST: -14h). 대략 -13h 사용."""
    return (kr_hour - 13) % 24


def _format_hour(hour: int, market: str) -> str:
    """시간 레이블 포맷. 미국 주식이면 한국시간(미국시간) 형태."""
    if market == "US":
        us_hour = _kr_to_us_hour(hour)
        return f"{hour:02d}:00({us_hour:02d}:00)"
    return f"{hour:02d}:00"


def _format_half_hour(hour: int, minute: int, market: str) -> str:
    """30분 단위 레이블 포맷."""
    if market == "US":
        us_hour = _kr_to_us_hour(hour)
        return f"{hour:02d}:{minute:02d}({us_hour:02d}:{minute:02d})"
    return f"{hour:02d}:{minute:02d}"


def build_report(df, ticker: str, display_name: str, full_name: str, market: str) -> str:
    """분석 결과를 문자열로 생성한다."""
    lines = []
    title = f"{full_name or display_name} ({ticker})"

    lines.append(f"\n{'='*70}")
    lines.append(f"  {title} 시간대별 거래량 패턴 분석")
    lines.append(f"{'='*70}\n")

    date_min = df.index.min().strftime("%Y-%m-%d")
    date_max = df.index.max().strftime("%Y-%m-%d")
    trading_days = df.index.normalize().nunique()
    tz_label = "한국시간(미국동부)" if market == "US" else "한국시간"
    lines.append(f"  기간: {date_min} ~ {date_max} ({trading_days}거래일)")
    lines.append(f"  데이터: 5분봉 {len(df)}건 | 시간 기준: {tz_label}\n")

    # ── 일별 거래량 ──
    df["date"] = df.index.date
    daily_vol = df.groupby("date")["Volume"].sum()
    total_period_vol = daily_vol.sum()
    lines.append(f"  일별 거래량:")
    for date, vol in daily_vol.items():
        weekday = pd.Timestamp(date).day_name()[:3]
        lines.append(f"     {date} ({weekday})  {int(vol):>14,}주")
    lines.append(f"     {'합계':>18}  {int(total_period_vol):>14,}주")
    lines.append(f"     {'일평균':>17}  {int(total_period_vol / trading_days):>14,}주\n")

    # ── 시간대별 집계 ──
    df["hour"] = df.index.hour
    hourly = df.groupby("hour")["Volume"].agg(["sum", "mean", "count"])
    hourly.columns = ["total_vol", "avg_vol", "bar_count"]
    total_volume = hourly["total_vol"].sum()
    hourly["pct"] = hourly["total_vol"] / total_volume * 100
    hourly["daily_avg"] = hourly["total_vol"] / trading_days

    max_daily_avg = hourly["daily_avg"].max()
    bar_width = 40

    time_col_w = 15 if market == "US" else 5
    time_header = "한국(미국)" if market == "US" else "시간"
    lines.append(f"  {time_header:>{time_col_w}}  {'일평균 거래량':>14}  {'비중':>6}  분포")
    lines.append(f"  {'─'*time_col_w}  {'─'*14}  {'─'*6}  {'─'*bar_width}")

    for hour in range(24):
        time_label = _format_hour(hour, market)
        if hour in hourly.index:
            row = hourly.loc[hour]
            if row["daily_avg"] > 0:
                bar_len = int(row["daily_avg"] / max_daily_avg * bar_width)
                bar = "█" * bar_len
                pct_str = f"{row['pct']:5.1f}%"
                vol_str = f"{int(row['daily_avg']):>14,}"
                marker = " ◀ PEAK" if row["daily_avg"] == max_daily_avg else ""
                lines.append(f"  {time_label:>{time_col_w}}  {vol_str}  {pct_str}  {bar}{marker}")
            else:
                lines.append(f"  {time_label:>{time_col_w}}  {'─':>14}  {'─':>6}  프리/애프터마켓 (거래량 미제공)")
        else:
            lines.append(f"  {time_label:>{time_col_w}}  {'─':>14}  {'─':>6}  거래불가")

    lines.append("")

    # ── TOP 3 ──
    top3 = hourly.nlargest(3, "daily_avg")
    lines.append("  거래량 TOP 3 시간대:")
    for rank, (hour, row) in enumerate(top3.iterrows(), 1):
        time_label = _format_hour(hour, market)
        lines.append(f"     {rank}. {time_label}  "
                     f"(일평균 {int(row['daily_avg']):,}주, 비중 {row['pct']:.1f}%)")

    # ── 30분 단위 ──
    lines.append(f"\n{'─'*70}")
    lines.append("  30분 단위 상세 분석\n")

    df["half_hour"] = df.index.hour * 100 + (df.index.minute // 30) * 30
    half_hourly = df.groupby("half_hour")["Volume"].sum()
    half_hourly_daily = half_hourly / trading_days
    max_hh = half_hourly_daily.max()

    for slot, vol in half_hourly_daily.items():
        h, m = divmod(slot, 100)
        time_label = _format_half_hour(h, m, market)
        bar_len = int(vol / max_hh * bar_width) if max_hh > 0 else 0
        bar = "▓" * bar_len
        marker = " ◀" if vol == max_hh else ""
        lines.append(f"  {time_label:>{time_col_w}}  {int(vol):>12,}  {bar}{marker}")

    lines.append(f"\n{'='*70}\n")
    return "\n".join(lines)


def save_report(report: str, display_name: str, full_name: str):
    """reports/ 폴더에 날짜_티커(주식이름).txt 형식으로 저장한다."""
    os.makedirs("reports", exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")

    # 파일명에 쓸 수 없는 문자 제거
    safe_name = (full_name or display_name).replace("/", "_").replace("\\", "_")
    safe_name = safe_name.replace(":", "").replace("*", "").replace("?", "")
    safe_name = safe_name.replace('"', "").replace("<", "").replace(">", "").replace("|", "")
    filename = f"{date_str}_{display_name}({safe_name}).txt"
    filepath = os.path.join("reports", filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report)
    return filepath


def main():
    if len(sys.argv) < 2:
        print("사용법: python volume_pattern.py <티커 또는 종목명>")
        print("  예) python volume_pattern.py 삼성전자")
        print("      python volume_pattern.py 005930")
        print("      python volume_pattern.py AAPL")
        sys.exit(1)

    raw_input = " ".join(sys.argv[1:])
    ticker, display_name, market = resolve_ticker(raw_input)

    print(f"\n  {display_name} → {ticker} ({market} 시장) 데이터 조회 중...")
    df, full_name = fetch_data(ticker, market)

    if df.empty:
        print(f"  '{raw_input}'에 대한 데이터를 가져올 수 없습니다.")
        print("  티커/종목명을 확인해 주세요.")
        sys.exit(1)

    report = build_report(df, ticker, display_name, full_name, market)
    print(report)

    filepath = save_report(report, display_name, full_name)
    print(f"  레포트 저장: {filepath}\n")


if __name__ == "__main__":
    main()
