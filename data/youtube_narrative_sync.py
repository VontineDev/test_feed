"""
youtube_narrative_sync.py — 삼프로TV 내러티브 기반 종목 언급 수집·추출·집계

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
설계:
  YouTube Data API v3로 삼프로TV 신규 업로드를 수집,
  youtube-transcript-api로 자막을 가져와,
  Gemini Flash LLM으로 종목 언급을 구조화 추출한다.

  attention_score = SUM(sentiment_weight) / distinct_videos
    buy=1.0, neutral=0.5, sell=0.0 (v1 기본값)

  블라인드 백테스트 보장:
    방법론 확정(git tag) → 과거 데이터 실행 → 가격 대조 순서 고정.
    --backfill 모드로 과거 데이터를 소급 수집.

환경변수:
  YOUTUBE_API_KEY       YouTube Data API v3 키 (필수)
  GEMINI_API_KEY        Google Gemini API 키 (필수)
  DATABASE_URL          PostgreSQL DSN

사용법:
  # 전일 업로드 수집 (운영 스케줄 09:05 KST):
  python youtube_narrative_sync.py

  # 과거 소급 수집 (블라인드 백테스트용):
  python youtube_narrative_sync.py --backfill --from 2026-01-01

  # forward return 채우기만:
  python youtube_narrative_sync.py --fill-returns

  # 테이블 DDL만 실행:
  python youtube_narrative_sync.py --ensure-tables
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────
_CHANNEL_ID = "UChlv4GSd7OQl3js-jkLOnFA"  # 삼프로TV 3PROTV
_ALIASES_PATH = Path(__file__).parent / "youtube_ticker_aliases.json"
_SENTIMENT_WEIGHT = {"buy": 1.0, "neutral": 0.5, "sell": 0.0}
_MIN_TRANSCRIPT_LEN = 200  # 자막이 이보다 짧으면 유효하지 않은 것으로 간주
_ROLLING_DAYS = 5           # attention_score rolling window (영업일)

# ── DDL ───────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS youtube_mention_raw (
    id                BIGSERIAL PRIMARY KEY,
    video_id          TEXT         NOT NULL,
    video_date        DATE         NOT NULL,
    speaker           TEXT,
    stock_name_raw    TEXT         NOT NULL,
    ticker            VARCHAR(12),
    direction         TEXT,
    horizon           TEXT,
    rationale_summary TEXT,
    source_quote      TEXT         NOT NULL,
    created_at        TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (video_id, stock_name_raw, source_quote)
);
CREATE INDEX IF NOT EXISTS idx_yt_mention_date   ON youtube_mention_raw (video_date DESC);
CREATE INDEX IF NOT EXISTS idx_yt_mention_ticker ON youtube_mention_raw (ticker);

CREATE TABLE IF NOT EXISTS youtube_attention_scores (
    ticker            VARCHAR(12)  NOT NULL,
    window_end        DATE         NOT NULL,
    mention_count     INT,
    sentiment_weighted NUMERIC(10,3),
    attention_score   NUMERIC(10,4),
    distinct_videos   INT,
    PRIMARY KEY (ticker, window_end)
);
ALTER TABLE youtube_attention_scores
    ALTER COLUMN sentiment_weighted TYPE NUMERIC(10,3),
    ALTER COLUMN attention_score    TYPE NUMERIC(10,4);
CREATE INDEX IF NOT EXISTS idx_yt_attn_window ON youtube_attention_scores (window_end DESC);

CREATE TABLE IF NOT EXISTS youtube_mention_forward_returns (
    mention_id  BIGINT  PRIMARY KEY REFERENCES youtube_mention_raw(id) ON DELETE CASCADE,
    ret_1d      NUMERIC(9,4),
    ret_5d      NUMERIC(9,4),
    ret_20d     NUMERIC(9,4),
    filled_at   DATE
);
"""


def _connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_tables(dsn: str) -> None:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL.strip().split(";"):
                s = stmt.strip()
                if s:
                    cur.execute(s)
        conn.commit()
        logger.info("[yt-sync] 테이블 확인 완료")
    finally:
        conn.close()


# ── YouTube Data API ───────────────────────────────────
def _yt_service(api_key: str):
    from googleapiclient.discovery import build
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def fetch_video_list(api_key: str, from_date: date, to_date: date) -> list[dict]:
    """채널에서 기간 내 업로드된 영상 목록 반환."""
    svc = _yt_service(api_key)
    published_after  = datetime(from_date.year, from_date.month, from_date.day,
                                tzinfo=timezone.utc).isoformat()
    published_before = datetime(to_date.year, to_date.month, to_date.day, 23, 59, 59,
                                tzinfo=timezone.utc).isoformat()
    videos = []
    page_token = None
    while True:
        req = svc.search().list(
            channelId=_CHANNEL_ID,
            part="snippet",
            type="video",
            publishedAfter=published_after,
            publishedBefore=published_before,
            maxResults=50,
            pageToken=page_token,
            order="date",
        )
        resp = req.execute()
        for item in resp.get("items", []):
            vid = item["id"]["videoId"]
            pub = item["snippet"]["publishedAt"][:10]  # YYYY-MM-DD
            title = item["snippet"]["title"]
            videos.append({"video_id": vid, "video_date": pub, "title": title})
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)
    logger.info("[yt-sync] 영상 목록: %d개 (%s ~ %s)", len(videos), from_date, to_date)
    return videos


# ── Transcript ────────────────────────────────────────
def fetch_transcript(video_id: str) -> str | None:
    """자막 텍스트 반환. 없으면 None. (youtube-transcript-api v1.2+)"""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
        api = YouTubeTranscriptApi()
        # ko 우선, auto-generated 포함 fallback
        try:
            fetched = api.fetch(video_id, languages=["ko", "ko-KR"])
        except NoTranscriptFound:
            try:
                tlist = api.list(video_id)
                # 한국어 자동 생성 자막 시도
                t = tlist.find_generated_transcript(["ko", "ko-KR"])
                fetched = t.fetch()
            except Exception:
                return None
        text = " ".join(s.text for s in fetched)
        if len(text) < _MIN_TRANSCRIPT_LEN:
            return None
        return text
    except Exception as e:
        logger.debug("[yt-sync] 자막 없음 %s: %s", video_id, e)
        return None


# ── LLM 추출 ──────────────────────────────────────────
_EXTRACT_PROMPT = """당신은 한국 주식시장 전문가입니다.
아래 한국 금융 방송 자막에서 언급된 주식 종목을 추출하세요.

규칙:
1. 원문에 실제로 등장한 종목명만 추출합니다 (없으면 빈 배열 반환).
2. 명확한 매수/매도 의견이 없으면 direction은 "neutral"로 설정합니다.
3. source_quote는 해당 종목명이 포함된 원문 문장을 그대로 복사합니다 (필수).
4. rationale_summary는 언급 맥락을 1-2문장으로 요약합니다.
5. horizon: short(1-4주) / mid(1-3개월) / long(3개월 이상) / unknown

반드시 아래 JSON 배열 형식으로만 응답하세요. 설명 없이 JSON만 출력합니다.
[
  {
    "stock_name_raw": "원문 종목명",
    "direction": "buy|sell|neutral",
    "horizon": "short|mid|long|unknown",
    "rationale_summary": "언급 맥락 요약",
    "source_quote": "원문 문장 그대로"
  }
]

자막:
"""


def extract_mentions(transcript: str, gemini_api_key: str) -> list[dict]:
    """Gemini Flash로 종목 언급 추출. 실패 시 []."""
    try:
        from google import genai
        client = genai.Client(api_key=gemini_api_key)
        prompt = _EXTRACT_PROMPT + transcript[:8000]  # 토큰 제한 방어
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        text = response.text.strip()
        # JSON 파싱 — 마크다운 코드블록 제거
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        mentions = json.loads(text)
        if not isinstance(mentions, list):
            return []
        _VALID_DIRECTIONS = {"buy", "sell", "neutral"}
        _VALID_HORIZONS = {"short", "mid", "long", "unknown"}
        result = []
        for m in mentions:
            if not m.get("source_quote", "").strip():
                continue
            d = m.get("direction", "neutral")
            h = m.get("horizon", "unknown")
            if d not in _VALID_DIRECTIONS:
                logger.warning("[yt-sync] LLM 비정상 direction=%r → neutral로 대체", d)
                m["direction"] = "neutral"
            if h not in _VALID_HORIZONS:
                logger.warning("[yt-sync] LLM 비정상 horizon=%r → unknown으로 대체", h)
                m["horizon"] = "unknown"
            result.append(m)
        return result
    except Exception as e:
        logger.warning("[yt-sync] LLM 추출 실패: %s", e)
        return []


# ── 티커 정규화 ───────────────────────────────────────
def _load_aliases() -> dict[str, str]:
    if _ALIASES_PATH.exists():
        return json.loads(_ALIASES_PATH.read_text(encoding="utf-8"))
    return {}


def _load_ticker_master() -> dict[str, str]:
    """pykrx 종목 마스터: 종목명 → 6자리 코드."""
    try:
        from pykrx import stock
        today_str = date.today().strftime("%Y%m%d")
        master = {}
        for mkt in ["KOSPI", "KOSDAQ"]:
            try:
                tickers = stock.get_market_ticker_list(today_str, market=mkt)
                for tk in tickers:
                    name = stock.get_market_ticker_name(tk)
                    if name:
                        master[name] = tk
            except Exception:
                pass
        return master
    except Exception as e:
        logger.warning("[yt-sync] pykrx 마스터 로드 실패: %s", e)
        return {}


def normalize_ticker(name_raw: str, master: dict[str, str], aliases: dict[str, str]) -> str | None:
    """종목명 → 6자리 KRX 코드. 매핑 실패 시 None."""
    name = name_raw.strip()
    if not name:
        return None
    if name in aliases:
        return aliases[name]
    if name in master:
        return master[name]
    # 부분 일치 (앞부분)
    for k, v in master.items():
        if k.startswith(name) or name.startswith(k):
            return v
    return None


# ── DB 저장 ───────────────────────────────────────────
def save_mentions(dsn: str, records: list[dict]) -> int:
    if not records:
        return 0
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO youtube_mention_raw
                    (video_id, video_date, speaker, stock_name_raw, ticker,
                     direction, horizon, rationale_summary, source_quote)
                VALUES %s
                ON CONFLICT (video_id, stock_name_raw, source_quote) DO NOTHING
                """,
                [
                    (
                        r["video_id"], r["video_date"], r.get("speaker"),
                        r["stock_name_raw"], r.get("ticker"),
                        r.get("direction"), r.get("horizon"),
                        r.get("rationale_summary"), r["source_quote"][:500],
                    )
                    for r in records
                ],
            )
            n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


# ── 집계 ─────────────────────────────────────────────
def _prev_business_days(ref: date, n: int) -> date:
    """ref 기준으로 n 영업일 전 날짜 (한국 공휴일 미반영, 단순 주말 제외)."""
    d = ref
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return d


def compute_attention_scores(dsn: str, window_end: date | None = None) -> int:
    """rolling 5영업일 attention_score 집계 → youtube_attention_scores upsert."""
    if window_end is None:
        window_end = date.today()
    window_start = _prev_business_days(window_end, _ROLLING_DAYS)

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    ticker,
                    COUNT(*)                                          AS mention_count,
                    SUM(CASE direction
                            WHEN 'buy'     THEN 1.0
                            WHEN 'sell'    THEN 0.0
                            ELSE                0.5
                        END)                                          AS sentiment_weighted,
                    COUNT(DISTINCT video_id)                          AS distinct_videos
                FROM   youtube_mention_raw
                WHERE  video_date BETWEEN %s AND %s
                  AND  ticker IS NOT NULL
                GROUP  BY ticker
            """, (window_start, window_end))
            rows = cur.fetchall()
            if not rows:
                return 0
            records = []
            for ticker, mention_count, sw, distinct_videos in rows:
                score = float(sw) / max(distinct_videos, 1)
                records.append((ticker, window_end, mention_count, float(sw), score, distinct_videos))
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO youtube_attention_scores
                    (ticker, window_end, mention_count, sentiment_weighted,
                     attention_score, distinct_videos)
                VALUES %s
                ON CONFLICT (ticker, window_end) DO UPDATE SET
                    mention_count      = EXCLUDED.mention_count,
                    sentiment_weighted = EXCLUDED.sentiment_weighted,
                    attention_score    = EXCLUDED.attention_score,
                    distinct_videos    = EXCLUDED.distinct_videos
                """,
                records,
            )
        conn.commit()
        logger.info("[yt-sync] attention_score %s: %d종목", window_end, len(records))
        return len(records)
    finally:
        conn.close()


# ── Forward Return 채우기 ─────────────────────────────
def _prev_business_day_or_self(d: date) -> date:
    """주말이면 직전 금요일 반환 (공휴일 미보정)."""
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def fill_forward_returns(dsn: str) -> int:
    """미채워진 언급 레코드에 yfinance 과거 가격으로 forward return 채우기.

    설계:
      - ticker_names DB 테이블로 6자리 코드 → yfinance 심볼(.KS/.KQ) 매핑
      - 전 레코드 날짜 범위를 yfinance 배치 다운로드 한 번으로 처리
      - video_date가 주말이면 직전 금요일 종가를 기준가로 사용
      - 레코드 단위 예외 격리: 개별 실패가 루프 전체를 중단시키지 않음
    """
    import pandas as pd
    import yfinance as yf

    conn = _connect(dsn)
    filled = 0
    try:
        # ticker_names에서 6자리 코드 → yfinance 심볼 매핑 구성
        with conn.cursor() as cur:
            cur.execute("SELECT LEFT(ticker, 6), ticker FROM ticker_names")
            ticker_to_sym: dict[str, str] = dict(cur.fetchall())

        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.ticker, r.video_date
                FROM   youtube_mention_raw r
                LEFT JOIN youtube_mention_forward_returns fr ON fr.mention_id = r.id
                WHERE  r.ticker IS NOT NULL
                  AND  fr.mention_id IS NULL
                ORDER  BY r.video_date DESC
                LIMIT  500
            """)
            rows = cur.fetchall()

        if not rows:
            logger.info("[yt-sync] forward return 채우기: 미채움 레코드 없음")
            return 0

        today = date.today()

        # 고유 yfinance 심볼 목록
        yf_symbols: list[str] = list({
            ticker_to_sym.get(t, t + ".KS")
            for _, t, _ in rows
        })

        # 날짜 범위: 가장 오래된 video_date 직전 금요일 ~ 오늘
        dates = [
            date.fromisoformat(str(vd)) if isinstance(vd, str) else vd
            for _, _, vd in rows
        ]
        start_date = _prev_business_day_or_self(min(dates))

        logger.info("[yt-sync] yfinance 배치 다운로드: %d종목 (%s ~ %s)",
                    len(yf_symbols), start_date, today)
        raw = yf.download(
            yf_symbols,
            start=start_date.strftime("%Y-%m-%d"),
            end=_next_business_day(today, 1).strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
        )

        # 가격 캐시 구성: {yf_symbol: {date: close}}
        price_cache: dict[str, dict[date, float]] = {}
        if not raw.empty:
            close = raw["Close"] if len(yf_symbols) > 1 else raw[["Close"]].rename(
                columns={"Close": yf_symbols[0]}
            )
            for sym in close.columns:
                price_cache[sym] = {
                    idx.date(): float(v)
                    for idx, v in close[sym].items()
                    if pd.notna(v)
                }

        logger.info("[yt-sync] 가격 캐시: %d종목", len(price_cache))

        # 레코드별 수익률 계산 및 저장
        for mention_id, ticker6, video_date in rows:
            if isinstance(video_date, str):
                video_date = date.fromisoformat(video_date)

            yf_sym = ticker_to_sym.get(ticker6, ticker6 + ".KS")
            sym_cache = price_cache.get(yf_sym, {})

            base_date = _prev_business_day_or_self(video_date)
            close_base = sym_cache.get(base_date)
            if not close_base:
                continue

            results: dict[str, float | None] = {"ret_1d": None, "ret_5d": None, "ret_20d": None}
            for ret_col, bdays in (("ret_1d", 1), ("ret_5d", 5), ("ret_20d", 20)):
                target = _next_business_day(video_date, bdays)
                if target > today:
                    continue
                close_target = sym_cache.get(target)
                if close_target:
                    results[ret_col] = round((close_target / close_base) - 1, 6)

            if not any(v is not None for v in results.values()):
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO youtube_mention_forward_returns
                            (mention_id, ret_1d, ret_5d, ret_20d, filled_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (mention_id) DO UPDATE SET
                            ret_1d    = COALESCE(EXCLUDED.ret_1d,  youtube_mention_forward_returns.ret_1d),
                            ret_5d    = COALESCE(EXCLUDED.ret_5d,  youtube_mention_forward_returns.ret_5d),
                            ret_20d   = COALESCE(EXCLUDED.ret_20d, youtube_mention_forward_returns.ret_20d),
                            filled_at = EXCLUDED.filled_at
                    """, (mention_id, results["ret_1d"], results["ret_5d"],
                          results["ret_20d"], today))
                conn.commit()
                filled += 1
            except Exception as e:
                logger.warning("[yt-sync] forward return 저장 실패 id=%d: %s", mention_id, e)
                conn.rollback()

        logger.info("[yt-sync] forward return 채우기: %d건", filled)
        return filled
    finally:
        conn.close()


def _next_business_day(ref: date, n: int) -> date:
    d = ref
    count = 0
    while count < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return d


# ── 메인 오케스트레이터 ───────────────────────────────
def run_sync(
    dsn: str,
    api_key: str,
    gemini_key: str,
    from_date: date,
    to_date: date,
) -> int:
    """기간 내 영상 수집 → 전사 → 추출 → 저장. 저장 건수 반환."""
    ensure_tables(dsn)
    aliases = _load_aliases()
    master  = _load_ticker_master()

    videos = fetch_video_list(api_key, from_date, to_date)
    total_saved = 0

    for i, v in enumerate(videos, 1):
        vid   = v["video_id"]
        vdate = date.fromisoformat(v["video_date"])
        logger.info("[yt-sync] [%d/%d] %s %s", i, len(videos), vdate, v["title"][:40])

        transcript = fetch_transcript(vid)
        if not transcript:
            logger.debug("[yt-sync] 자막 없음: %s", vid)
            continue

        mentions = extract_mentions(transcript, gemini_key)
        time.sleep(4)  # Gemini free tier: 15 RPM → 4초/건
        if not mentions:
            continue

        records = []
        for m in mentions:
            ticker = normalize_ticker(m["stock_name_raw"], master, aliases)
            records.append({
                "video_id":          vid,
                "video_date":        vdate,
                "speaker":           None,  # v1: YouTube 자막 화자 분리 없음
                "stock_name_raw":    m["stock_name_raw"],
                "ticker":            ticker,
                "direction":         m.get("direction", "neutral"),
                "horizon":           m.get("horizon", "unknown"),
                "rationale_summary": m.get("rationale_summary"),
                "source_quote":      m["source_quote"],
            })

        n = save_mentions(dsn, records)
        total_saved += n
        logger.info("[yt-sync]   -> %d/%d건 저장", n, len(records))

    return total_saved


# ── CLI ───────────────────────────────────────────────
def _get_env(key: str) -> str:
    v = os.environ.get(key, "")
    if not v:
        raise SystemExit(f"환경변수 {key} 미설정")
    return v


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="YouTube 내러티브 종목 언급 수집")
    parser.add_argument("--backfill", action="store_true", help="과거 소급 수집 모드")
    parser.add_argument("--from", dest="from_date", default=None, help="시작일 YYYY-MM-DD")
    parser.add_argument("--to",   dest="to_date",   default=None, help="종료일 YYYY-MM-DD")
    parser.add_argument("--fill-returns", action="store_true", help="forward return만 채우기")
    parser.add_argument("--ensure-tables", action="store_true", help="테이블 DDL만 실행")
    parser.add_argument("--compute-scores", action="store_true", help="attention_score 집계만")
    args = parser.parse_args()

    # .env를 스크립트 위치 기준으로 명시적 로드 (백그라운드 실행 대응)
    _root = Path(__file__).parent.parent
    try:
        from dotenv import load_dotenv as _ldenv
        _ldenv(_root / ".env", override=True)
    except ImportError:
        pass
    try:
        import sys as _sys
        _sys.path.insert(0, str(_root))
        from core.db import get_dsn as _get_dsn
        dsn = _get_dsn()
    except Exception:
        dsn = _get_env("DATABASE_URL")
    api_key     = _get_env("YOUTUBE_API_KEY")
    gemini_key  = _get_env("GEMINI_API_KEY")

    if args.ensure_tables:
        ensure_tables(dsn)

    elif args.fill_returns:
        fill_forward_returns(dsn)

    elif args.compute_scores:
        compute_attention_scores(dsn)

    elif args.backfill:
        from_date = date.fromisoformat(args.from_date) if args.from_date else date(2026, 1, 1)
        to_date   = date.fromisoformat(args.to_date)   if args.to_date   else date.today()
        logger.info("[yt-sync] 백필 시작: %s ~ %s", from_date, to_date)
        n = run_sync(dsn, api_key, gemini_key, from_date, to_date)
        logger.info("[yt-sync] 백필 완료: 총 %d건 저장", n)
        fill_forward_returns(dsn)
        # 백테스트 SQL이 ON window_end = video_date 조인이므로
        # 각 날짜마다 attention_score를 개별 집계해야 함
        current = from_date
        while current <= to_date:
            compute_attention_scores(dsn, window_end=current)
            current += timedelta(days=1)
        logger.info("[yt-sync] attention_score 집계 완료: %s ~ %s", from_date, to_date)

    else:
        # 운영 모드: 전일 업로드 수집
        yesterday = date.today() - timedelta(days=1)
        n = run_sync(dsn, api_key, gemini_key, yesterday, yesterday)
        logger.info("[yt-sync] 운영 수집 완료: %d건", n)
