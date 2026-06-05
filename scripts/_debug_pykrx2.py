import sys
sys.stdout.reconfigure(encoding="utf-8")
from pykrx import stock as krx
from datetime import date, timedelta

# get_market_cap_by_ticker 시도
today = date.today()
for delta in range(7):
    dt = today - timedelta(days=delta)
    dt_str = dt.strftime("%Y%m%d")
    try:
        df = krx.get_market_cap_by_ticker(dt_str, market="KOSPI")
        if df is not None and len(df) > 0:
            print(f"시가총액 날짜: {dt_str}, 종목수: {len(df)}")
            print(f"컬럼: {list(df.columns)}")
            top10 = df.sort_values(df.columns[0], ascending=False).head(10)
            for ticker, row in top10.iterrows():
                name = krx.get_market_ticker_name(ticker)
                print(f"  {ticker} {name:<20} {list(row)}")
            break
    except Exception as e:
        print(f"{dt_str}: cap 오류 - {e}")

# yfinance 대형주 거래대금
print("\n=== yfinance 접근 ===")
import yfinance as yf
sample = ["005930.KS","000660.KS","005380.KS","051910.KS","000270.KS",
          "373220.KS","012330.KS","066570.KS","034730.KS","086790.KS"]
import pandas as pd
df2 = yf.download(sample, period="5d", auto_adjust=True, progress=False, timeout=30)
if df2 is not None and not df2.empty:
    vol = df2["Volume"].mean()
    close = df2["Close"].mean()
    value = (vol * close).sort_values(ascending=False)
    print("거래대금 근사값 (종가×거래량 평균):")
    for sym, v in value.items():
        print(f"  {sym:<15} {v/1e8:,.0f}억")
