import sys
sys.stdout.reconfigure(encoding="utf-8")
from pykrx import stock as krx
from datetime import date, timedelta

today = date.today()
# 최근 영업일 찾기
for delta in range(7):
    dt = today - timedelta(days=delta)
    dt_str = dt.strftime("%Y%m%d")
    try:
        df = krx.get_market_ohlcv_by_ticker(dt_str, market="KOSPI")
        if len(df) > 0 and df["거래대금"].sum() > 0:
            print(f"사용 날짜: {dt_str}, 종목수: {len(df)}")
            print(f"컬럼: {list(df.columns)}")
            print(f"\n거래대금 상위 10:")
            top10 = df.sort_values("거래대금", ascending=False).head(10)
            for ticker, row in top10.iterrows():
                name = krx.get_market_ticker_name(ticker)
                val_eok = int(row["거래대금"]) // 100_000_000
                print(f"  {ticker} {name:<20} {val_eok:,}억")
            break
    except Exception as e:
        print(f"{dt_str}: 오류 - {e}")
