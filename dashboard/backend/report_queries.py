"""
dashboard/backend/report_queries.py
`/api/report/unified` 통합 스크리너 SQL 상수 — routers_report.py에서 분리.
"""
from __future__ import annotations

# ── GET /api/report/unified ───────────────────────────────────
# stage(1-3) + screener + youtube 세 소스 UNION, attention_score 기준 정렬
# start/end 없으면 최신 스냅샷(today), 있으면 기간 내 최신 데이터(history)
_UNIFIED_TAIL = """
    ,
    tn AS (
        SELECT SPLIT_PART(ticker, '.', 1) AS t, name_ko
        FROM   ticker_names
    ),
    kl AS (
        SELECT SPLIT_PART(yfinance_symbol, '.', 1) AS t, name_ko, sector
        FROM   krx_listings
    ),
    mr AS (
        SELECT DISTINCT ON (ticker) ticker AS t, stock_name_raw
        FROM   youtube_mention_raw
        ORDER  BY ticker, created_at DESC
    ),
    kind AS (
        SELECT DISTINCT ON (SPLIT_PART(ticker, '.', 1))
               SPLIT_PART(ticker, '.', 1) AS t, sector
        FROM   chart_signals
        WHERE  sector IS NOT NULL AND sector != ''
        ORDER  BY SPLIT_PART(ticker, '.', 1), screened_at DESC
    ),
    ohlcv AS (
        SELECT SPLIT_PART(symbol, '.', 1) AS t, close AS ohlcv_close
        FROM   daily_ohlcv
        WHERE  market = 'KR'
          AND  date = (SELECT MAX(date) FROM daily_ohlcv WHERE market = 'KR')
    ),
    all_tickers AS (
        SELECT t FROM sc
        UNION SELECT t FROM cs
        UNION SELECT t FROM yt
    )
    SELECT
        at.t                                                                AS ticker,
        COALESCE(tn.name_ko, kl.name_ko, cs.name, mr.stock_name_raw, at.t) AS name,
        yt.attention_score,
        yt.attention_q,
        sc.stage, sc.s1_high, sc.s1_volume, sc.peakout_flag,
        cs.is_enhanced, cs.has_gapjum, COALESCE(cs.sector, kind.sector, kl.sector) AS sector,
        COALESCE(cs.close, ohlcv.ohlcv_close) AS close,
        cs.ma_20w, cs.cloud_top
    FROM   all_tickers at
    LEFT JOIN sc    ON sc.t    = at.t
    LEFT JOIN cs    ON cs.t    = at.t
    LEFT JOIN yt    ON yt.t    = at.t
    LEFT JOIN tn    ON tn.t    = at.t
    LEFT JOIN kl    ON kl.t    = at.t
    LEFT JOIN mr    ON mr.t    = at.t
    LEFT JOIN kind  ON kind.t  = at.t
    LEFT JOIN ohlcv ON ohlcv.t = at.t
    ORDER BY
        yt.attention_score DESC NULLS LAST,
        sc.stage           ASC  NULLS LAST,
        cs.is_enhanced     DESC NULLS LAST
"""

_UNIFIED_TODAY_SQL = """
    WITH
    sc AS (
        SELECT DISTINCT ON (SPLIT_PART(ticker, '.', 1))
               SPLIT_PART(ticker, '.', 1) AS t,
               stage, s1_high, s1_volume, peakout_flag
        FROM   stage_classifications
        WHERE  classified_date = (SELECT MAX(classified_date) FROM stage_classifications)
        ORDER  BY SPLIT_PART(ticker, '.', 1), classified_date DESC
    ),
    cs AS (
        SELECT SPLIT_PART(ticker, '.', 1) AS t,
               is_enhanced, has_gapjum, close, sector, name, ma_20w, cloud_top
        FROM   chart_signals
        WHERE  week_of = (SELECT MAX(week_of) FROM chart_signals)
    ),
    yt AS (
        SELECT ticker AS t,
               attention_score,
               NTILE(5) OVER (ORDER BY attention_score) AS attention_q
        FROM   youtube_attention_scores
        WHERE  window_end = (SELECT MAX(window_end) FROM youtube_attention_scores)
          AND  attention_score > 0
    )
""" + _UNIFIED_TAIL

_UNIFIED_HISTORY_SQL = """
    WITH
    sc AS (
        SELECT DISTINCT ON (SPLIT_PART(ticker, '.', 1))
               SPLIT_PART(ticker, '.', 1) AS t,
               stage, s1_high, s1_volume, peakout_flag
        FROM   stage_classifications
        WHERE  classified_date BETWEEN $1 AND $2
        ORDER  BY SPLIT_PART(ticker, '.', 1), classified_date DESC
    ),
    cs AS (
        SELECT DISTINCT ON (SPLIT_PART(ticker, '.', 1))
               SPLIT_PART(ticker, '.', 1) AS t,
               is_enhanced, has_gapjum, close, sector, name, ma_20w, cloud_top
        FROM   chart_signals
        WHERE  screened_at::date BETWEEN $1 AND $2
        ORDER  BY SPLIT_PART(ticker, '.', 1), screened_at DESC
    ),
    yt AS (
        SELECT ticker AS t,
               attention_score,
               NTILE(5) OVER (ORDER BY attention_score) AS attention_q
        FROM   youtube_attention_scores
        WHERE  window_end = (SELECT MAX(window_end)
                             FROM   youtube_attention_scores
                             WHERE  window_end <= $2)
          AND  attention_score > 0
    )
""" + _UNIFIED_TAIL

_AS_OF_SQL = """
    SELECT
        (SELECT MAX(classified_date) FROM stage_classifications)   AS stage_date,
        (SELECT MAX(screened_at)::date FROM chart_signals)         AS screener_date,
        (SELECT MAX(window_end)      FROM youtube_attention_scores) AS narrative_date
"""

_AS_OF_HISTORY_SQL = """
    SELECT
        (SELECT MAX(classified_date) FROM stage_classifications
         WHERE  classified_date BETWEEN $1 AND $2)                 AS stage_date,
        (SELECT MAX(screened_at)::date FROM chart_signals
         WHERE  screened_at::date BETWEEN $1 AND $2)               AS screener_date,
        (SELECT MAX(window_end) FROM youtube_attention_scores
         WHERE  window_end <= $2)                                   AS narrative_date
"""
