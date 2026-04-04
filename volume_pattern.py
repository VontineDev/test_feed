"""시간대별 거래량 패턴 분석 — 한국/미국 주식 지원

사용법:
    python volume_pattern.py 삼성전자
    python volume_pattern.py 005930
    python volume_pattern.py AAPL
    python volume_pattern.py TSLA
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# 한국 주식 별명 → KRX 종목코드
# 코스피(KOSPI) 종목 별명 → KRX 종목코드
KR_KOSPI = {
    # ── 주요 대형주 ──
    "삼성전자": "005930", "SK하이닉스": "000660", "LG에너지솔루션": "373220",
    "삼성바이오로직스": "207940", "현대차": "005380", "기아": "000270",
    "셀트리온": "068270", "KB금융": "105560", "신한지주": "055550",
    "POSCO홀딩스": "005490", "네이버": "035420", "NAVER": "035420",
    "카카오": "035720", "LG화학": "051910", "삼성SDI": "006400",
    "현대모비스": "012330", "삼성물산": "028260", "한국전력": "015760",
    "SK이노베이션": "096770", "LG전자": "066570", "포스코퓨처엠": "003670",
    # ── 거래대금 상위 (코스피) ──
    "SK오션플랜트": "100090", "삼천당제약": "000250",
    "두산에너빌리티": "034020", "삼성전기": "009150",
    "미래에셋증권": "006800", "한국항공우주": "047810",
    "현대로템": "064350", "OCI홀딩스": "010060",
    "LIG넥스원": "079550", "LG이노텍": "011070", "S-Oil": "010950",
    "한화시스템": "272210", "한화에어로스페이스": "012450",
    "한화비전": "489790", "한올바이오파마": "009420",
    "포스코인터내셔널": "047050", "코웨이": "021240", "한화솔루션": "009830",
    "현대건설": "000720", "우리금융지주": "316140",
    "한미반도체": "042700", "TCC스틸": "002710", "SKC": "011790",
    "오리온": "271560", "삼아알미늄": "006110", "LS ELECTRIC": "010120",
    "HD건설기계": "267270", "삼성생명": "032830",
    "SK바이오팜": "326030", "HL만도": "204320",
    "한화오션": "042660", "대원제약": "003220",
    "삼성E&A": "028050", "삼성화재": "000810", "하이브": "352820",
    "HD현대": "267250", "SK스퀘어": "402340",
    "한전기술": "052690", "DL이앤씨": "375500", "DB손해보험": "005830",
    "크래프톤": "259960", "HMM": "011200", "일진전기": "103590",
    "현대오토에버": "307950", "신대양제지": "016590",
    "한전KPS": "051600", "SK가스": "018670",
    "CJ제일제당": "097950", "현대글로비스": "086280",
    "삼성중공업": "010140", "고려아연": "010130",
}

# 코스닥(KOSDAQ) 종목 별명 → KRX 종목코드
KR_KOSDAQ = {
    "SK이터닉스": "475150", "에코프로": "086520", "펄어비스": "263750",
    "대명에너지": "389260", "애경케미칼": "161000", "넥스틸": "092790",
    "엘앤에프": "066970", "에코프로비엠": "247540", "한텍": "098070",
    "노타": "486990", "비츠로셀": "082920", "지아이이노베이션": "358570",
    "클래시스": "214150", "메디포스트": "078160", "로보티즈": "108490",
    "현대무벡스": "319400", "이수스페셜티케미컬": "457190",
    "리노공업": "058470", "동국제약": "086450", "에임드바이오": "0009K0",
    "대주전자재료": "078600", "리가켐바이오": "141080",
    "셀바스AI": "108860", "원텍": "336570", "스피어": "347700",
    "알테오젠": "196170", "시노펙스": "025320",
    "삼현": "437730", "에이비엘바이오": "298380",
    "씨에스윈드": "112610", "HLB제약": "047920", "RFHIC": "218410",
    "솔브레인": "357780", "알지노믹스": "476830",
    "에이프릴바이오": "397030", "오름테라퓨틱": "475830",
}

# 통합 조회용 (resolve_ticker에서 사용)
KR_ALIASES = {**KR_KOSPI, **KR_KOSDAQ}

# 시장 판별용 타임존
MARKET_TZ = {
    "KR": "Asia/Seoul",
    "US": "America/New_York",
}


def resolve_ticker(raw: str) -> tuple[str, str, str]:
    """입력을 (yfinance 티커, 표시 이름, 시장 코드)로 변환한다."""
    raw = raw.strip()

    # 1) 한글/영문 별명 → 코스닥이면 .KQ, 코스피면 .KS
    if raw in KR_KOSDAQ:
        code = KR_KOSDAQ[raw]
        return f"{code}.KQ", raw, "KR"
    if raw in KR_KOSPI:
        code = KR_KOSPI[raw]
        return f"{code}.KS", raw, "KR"

    # 2) 순수 6자리 숫자 → KRX (기본 .KS, 실패 시 fetch_data에서 .KQ 폴백)
    if raw.isdigit() and len(raw) == 6:
        return f"{raw}.KS", raw, "KR"

    # 3) 이미 .KS/.KQ 접미사 포함
    if raw.upper().endswith((".KS", ".KQ")):
        return raw.upper(), raw.split(".")[0], "KR"

    # 4) 나머지는 미국 주식으로 간주
    return raw.upper(), raw.upper(), "US"


def _load_from_db(symbol: str) -> pd.DataFrame:
    """DB에서 캐시된 5분봉 데이터를 로드한다."""
    import db as _db

    async def _query():
        pool = await _db.create_pool()
        await _db.init_db(pool)
        try:
            return await _db.fetch_intraday_volumes(pool, symbol, "5m", 2000)
        finally:
            await pool.close()

    try:
        rows = asyncio.run(_query())
    except Exception as e:
        logger.warning("[DB] 조회 실패: %s", e)
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })
    df.index = pd.DatetimeIndex(df["ts"]).tz_convert("Asia/Seoul")
    df = df.sort_index()
    return df


def _is_db_fresh(df: pd.DataFrame, max_age_hours: int = 12) -> bool:
    """DB 데이터가 충분히 최신인지 확인한다."""
    if df.empty:
        return False
    latest = df.index.max()
    now = pd.Timestamp.now(tz=latest.tz)
    age = (now - latest).total_seconds() / 3600
    return age < max_age_hours


def fetch_data(ticker: str, market: str):
    """데이터 조회. DB 캐시 → yfinance 순서."""
    full_name = ""
    symbol = ticker.replace(".KS", "").replace(".KQ", "")

    # yfinance에서 종목 이름 가져오기
    t = yf.Ticker(ticker)
    try:
        full_name = t.info.get("shortName") or t.info.get("longName") or ""
    except Exception:
        pass

    # 1) DB 캐시 확인
    try:
        db_5m = _load_from_db(symbol)
    except Exception:
        db_5m = pd.DataFrame()

    if _is_db_fresh(db_5m):
        print(f"  DB 캐시 사용 (5분봉 {len(db_5m)}건)")
        return db_5m, full_name, "db"

    # 2) yfinance (.KS 실패 시 .KQ 폴백)
    prepost = (market == "US")
    df = t.history(period="5d", interval="5m", prepost=prepost)

    if df.empty and market == "KR" and ticker.endswith(".KS"):
        ticker_kq = ticker.replace(".KS", ".KQ")
        t = yf.Ticker(ticker_kq)
        try:
            full_name = full_name or t.info.get("shortName") or t.info.get("longName") or ""
        except Exception:
            pass
        df = t.history(period="5d", interval="5m")

    if df.empty:
        return df, "", "yfinance"

    tz = "Asia/Seoul" if market == "US" else MARKET_TZ[market]
    df.index = df.index.tz_convert(tz)
    return df, full_name, "yfinance"


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


def build_report(df, ticker: str, display_name: str, full_name: str, market: str,
                  data_source: str = "yfinance") -> str:
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
    source_labels = {"db": "DB 캐시", "yfinance": "yfinance"}
    source_label = source_labels.get(data_source, data_source)
    lines.append(f"  기간: {date_min} ~ {date_max} ({trading_days}거래일)")
    lines.append(f"  데이터: 5분봉 {len(df)}건 | 시간 기준: {tz_label}")
    lines.append(f"  소스: {source_label}\n")

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


async def save_to_db(df: pd.DataFrame, symbol: str, market: str):
    """5분봉 데이터를 PostgreSQL에 저장한다."""
    import db

    pool = await db.create_pool()
    await db.init_db(pool)

    rows = []
    for ts, r in df.iterrows():
        ts_utc = ts.tz_convert("UTC")
        rows.append({
            "symbol": symbol,
            "market": market,
            "ts": ts_utc.to_pydatetime(),
            "interval": "5m",
            "open": r.get("Open"),
            "high": r.get("High"),
            "low": r.get("Low"),
            "close": r.get("Close"),
            "volume": int(r.get("Volume", 0)),
            "is_extended": False,
            "source": "yfinance",
        })

    if rows:
        inserted = await db.save_intraday_volumes(pool, rows)
        print(f"  DB 저장: {inserted}건 신규 / {len(rows)}건 총")

    await pool.close()


def main():
    if len(sys.argv) < 2:
        print("사용법: python volume_pattern.py <티커 또는 종목명>")
        print("  예) python volume_pattern.py 삼성전자")
        print("      python volume_pattern.py AAPL")
        sys.exit(1)

    raw_input = " ".join(sys.argv[1:])
    ticker, display_name, market = resolve_ticker(raw_input)

    print(f"\n  {display_name} → {ticker} ({market} 시장) 데이터 조회 중...")
    df, full_name, data_source = fetch_data(ticker, market)

    if df.empty:
        print(f"  '{raw_input}'에 대한 데이터를 가져올 수 없습니다.")
        print("  티커/종목명을 확인해 주세요.")
        sys.exit(1)

    report = build_report(df, ticker, display_name, full_name, market, data_source)
    print(report)

    filepath = save_report(report, display_name, full_name)
    print(f"  레포트 저장: {filepath}")

    # DB 자동 저장 (캐시에서 로드한 경우 건너뜀)
    if data_source != "db":
        symbol = ticker.replace(".KS", "").replace(".KQ", "")
        try:
            asyncio.run(save_to_db(df, symbol, market))
        except Exception as e:
            print(f"  DB 저장 건너뜀: {e}")

    print()


if __name__ == "__main__":
    main()
