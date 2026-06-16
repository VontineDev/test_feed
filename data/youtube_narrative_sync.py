"""
youtube_narrative_sync.py — 삼프로TV 내러티브 기반 종목 언급 수집·추출·집계

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
설계:
  YouTube Data API v3로 삼프로TV 신규 업로드를 수집,
  youtube-transcript-api로 자막을 가져와,
  Ollama 로컬 LLM으로 종목 언급을 구조화 추출한다.

  attention_score = SUM(sentiment_weight) / distinct_videos
    buy=1.0, neutral=0.5, sell=0.0 (v1 기본값)

  블라인드 백테스트 보장:
    방법론 확정(git tag) → 과거 데이터 실행 → 가격 대조 순서 고정.
    --backfill 모드로 과거 데이터를 소급 수집.

환경변수:
  YOUTUBE_API_KEY       YouTube Data API v3 키 (필수)
  OLLAMA_BASE           Ollama 서버 주소 (기본: http://localhost:11434)
  OLLAMA_MODEL          추출에 사용할 모델 (기본: qwen3.5:9b)
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
_TICKER_MASTER_CACHE = Path(__file__).parent / "youtube_ticker_master_cache.json"
_COOKIES_PATH = Path(__file__).parent.parent / "docs" / "youtube.com_cookies.txt"
_SENTIMENT_WEIGHT = {"buy": 1.0, "neutral": 0.5, "sell": 0.0}
_MIN_TRANSCRIPT_LEN = 200   # 자막이 이보다 짧으면 유효하지 않은 것으로 간주
_ROLLING_DAYS = 5            # attention_score rolling window (영업일)
_MAX_TRANSCRIPT_CHARS = 8000    # LLM 컨텍스트 상한
_FILL_RETURNS_BATCH = 500       # fill_forward_returns per-call 처리 행 수
_TRANSCRIPT_FETCH_SLEEP = 2.0   # YouTube IP 차단 방지 — 자막 요청 간격
# Tor SOCKS5 프록시: TOR_PROXY 환경변수로 활성화 (예: socks5h://127.0.0.1:9050)
_TOR_PROXY = os.environ.get("TOR_PROXY", "")
# Tor 컨트롤 포트: 회로 자동 교체용 (Tor Browser=9151, Expert Bundle=9051)
_TOR_CONTROL_PORT = int(os.environ.get("TOR_CONTROL_PORT", "9151"))
_TOR_NEWNYM_WAIT = 15  # NEWNYM 후 새 회로 구축 대기 시간(초)

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

CREATE TABLE IF NOT EXISTS youtube_backfill_queue (
    video_id        TEXT         PRIMARY KEY,
    video_date      DATE         NOT NULL,
    title           TEXT,
    status          TEXT         NOT NULL DEFAULT 'pending',
    attempts        INT          NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_yt_backfill_queue_status ON youtube_backfill_queue (status, video_date);
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
            for tbl in ("youtube_mention_raw", "youtube_attention_scores",
                        "youtube_mention_forward_returns", "youtube_backfill_queue"):
                cur.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY")
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


# ── 분산 백필 큐 ───────────────────────────────────────
def enqueue_backfill_videos(dsn: str, api_key: str, from_date: date, to_date: date) -> int:
    """기간 내 영상 목록만 수집해 backfill 큐에 적재 (검색 API만 사용 — 자막 요청 없음, 차단 위험 없음).

    재실행 안전: video_id PRIMARY KEY → 이미 큐에 있는 영상은 건너뜀.
    """
    videos = fetch_video_list(api_key, from_date, to_date)
    if not videos:
        return 0
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO youtube_backfill_queue (video_id, video_date, title)
                VALUES %s
                ON CONFLICT (video_id) DO NOTHING
                """,
                [(v["video_id"], v["video_date"], v["title"][:300]) for v in videos],
            )
            n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def _fetch_transcript_classified(video_id: str) -> tuple[str | None, str]:
    """자막 텍스트와 실패 분류 반환.

    반환 status: "ok" | "blocked" | "no_transcript"
      - blocked: YouTube IP 차단(RequestBlocked) — 일시적, 재시도 대상으로 큐에 남겨야 함
      - no_transcript: 자막 비활성/미존재/길이 부족 — 영구 스킵
    """
    from youtube_transcript_api import NoTranscriptFound, RequestBlocked
    try:
        api = _make_yt_api()
        try:
            fetched = api.fetch(video_id, languages=["ko", "ko-KR"])
        except NoTranscriptFound:
            tlist = api.list(video_id)
            t = tlist.find_generated_transcript(["ko", "ko-KR"])
            fetched = t.fetch()
        text = " ".join(s.text for s in fetched)
        if len(text) < _MIN_TRANSCRIPT_LEN:
            return None, "no_transcript"
        return text, "ok"
    except RequestBlocked as e:
        logger.warning("[yt-backfill] IP 차단 감지 %s: %s", video_id, type(e).__name__)
        return None, "blocked"
    except Exception as e:
        logger.warning("[yt-backfill] 자막 없음 %s: %s", video_id, e)
        return None, "no_transcript"


def process_backfill_queue(dsn: str, limit: int = 8) -> dict:
    """큐에서 pending 영상을 오래된 날짜순으로 최대 limit개 처리.

    설계 (분산 실행):
      - 1회 호출 = 1배치. 일일 운영 잡과 동일한 소량(기본 8개)만 처리하고 종료.
      - IP 차단(blocked) 감지 시 즉시 배치 중단 — 큐 상태를 pending으로 유지해
        다음 실행(스케줄)에서 자동 재시도.
      - 외부 스케줄러(Windows 작업 스케줄러 등)가 하루 여러 번 호출하는 것을 전제.
    """
    aliases = _load_aliases()
    master  = _load_ticker_master()

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT video_id, video_date, title
                FROM   youtube_backfill_queue
                WHERE  status = 'pending'
                ORDER  BY video_date ASC, video_id ASC
                LIMIT  %s
            """, (limit,))
            rows = cur.fetchall()

        if not rows:
            logger.info("[yt-backfill] 처리 대기 영상 없음 — 백필 완료 또는 큐 미적재")
            return {"processed": 0, "saved": 0, "blocked": False}

        saved_total = 0
        for i, (video_id, video_date, title) in enumerate(rows, 1):
            logger.info("[yt-backfill] [%d/%d] %s %s", i, len(rows), video_date, str(title)[:40])
            text, status = _fetch_transcript_classified(video_id)
            time.sleep(_TRANSCRIPT_FETCH_SLEEP)

            if status == "blocked":
                logger.warning("[yt-backfill] IP 차단 감지 — 회로 교체 후 재시도")
                if _tor_rotate_circuit():
                    text, status = _fetch_transcript_classified(video_id)
                    time.sleep(_TRANSCRIPT_FETCH_SLEEP)
                if status == "blocked":
                    logger.warning("[yt-backfill] 회로 교체 후에도 차단 — 배치 중단 (이번 배치 %d/%d개 처리)",
                                   i - 1, len(rows))
                    return {"processed": i - 1, "saved": saved_total, "blocked": True}
                logger.info("[yt-backfill] 회로 교체 성공 — 처리 재개")

            n_saved = 0
            if status == "ok" and text:
                mentions = extract_mentions(text)
                if mentions:
                    records = [
                        {
                            "video_id":          video_id,
                            "video_date":        video_date,
                            "speaker":           None,
                            "stock_name_raw":    m["stock_name_raw"],
                            "ticker":            normalize_ticker(m["stock_name_raw"], master, aliases),
                            "direction":         m.get("direction", "neutral"),
                            "horizon":           m.get("horizon", "unknown"),
                            "rationale_summary": m.get("rationale_summary"),
                            "source_quote":      m["source_quote"],
                        }
                        for m in mentions
                    ]
                    n_saved = save_mentions(dsn, records)
                    saved_total += n_saved

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE youtube_backfill_queue
                    SET    status = %s, attempts = attempts + 1, last_attempt_at = NOW()
                    WHERE  video_id = %s
                """, (status, video_id))
            conn.commit()
            logger.info("[yt-backfill]   -> %s (%d건 저장)", status, n_saved)

        logger.info("[yt-backfill] 배치 완료: %d개 처리, %d건 저장", len(rows), saved_total)
        return {"processed": len(rows), "saved": saved_total, "blocked": False}
    finally:
        conn.close()


# ── Tor 회로 교체 ─────────────────────────────────────
def _tor_rotate_circuit() -> bool:
    """Tor 컨트롤 포트로 NEWNYM 신호를 보내 새 출구 노드로 교체.

    TOR_PROXY가 설정되지 않으면 즉시 False 반환.
    stem 미설치 또는 컨트롤 포트 인증 실패 시 경고 후 False 반환.
    성공 시 _TOR_NEWNYM_WAIT초 대기 후 True 반환.
    """
    if not _TOR_PROXY:
        return False
    try:
        from stem import Signal
        from stem.control import Controller
        with Controller.from_port(port=_TOR_CONTROL_PORT) as ctrl:
            ctrl.authenticate()
            ctrl.signal(Signal.NEWNYM)
        logger.info("[yt-sync] Tor 회로 교체 완료 (NEWNYM) - %d초 대기", _TOR_NEWNYM_WAIT)
        time.sleep(_TOR_NEWNYM_WAIT)
        return True
    except ImportError:
        logger.warning("[yt-sync] stem 미설치 — 자동 회로 교체 불가 (pip install stem)")
        return False
    except Exception as e:
        logger.warning("[yt-sync] Tor 회로 교체 실패: %s", e)
        return False


# ── Transcript ────────────────────────────────────────
def _make_yt_api() -> "YouTubeTranscriptApi":
    """YouTubeTranscriptApi 인스턴스 생성.

    우선순위:
      1. TOR_PROXY 환경변수가 있으면 SOCKS5 프록시 적용 (IP 차단 우회)
      2. docs/youtube.com_cookies.txt 가 있으면 인증 쿠키 적용
      3. 기본값 (프록시/쿠키 없음)
    """
    from youtube_transcript_api import YouTubeTranscriptApi
    import requests

    session = requests.Session()

    # 쿠키 적용
    if _COOKIES_PATH.exists():
        import http.cookiejar
        jar = http.cookiejar.MozillaCookieJar(str(_COOKIES_PATH))
        try:
            jar.load(ignore_discard=True, ignore_expires=True)
            session.cookies = jar
            logger.debug("[yt-sync] 쿠키 적용: %s", _COOKIES_PATH.name)
        except Exception as e:
            logger.warning("[yt-sync] 쿠키 파일 로드 실패: %s", e)

    # Tor 프록시 적용 (TOR_PROXY=socks5h://127.0.0.1:9050)
    if _TOR_PROXY:
        session.proxies = {"http": _TOR_PROXY, "https": _TOR_PROXY}
        logger.info("[yt-sync] Tor 프록시 적용: %s", _TOR_PROXY)

    return YouTubeTranscriptApi(http_client=session)


def fetch_transcript(video_id: str) -> str | None:
    """자막 텍스트 반환. 없으면 None. (youtube-transcript-api v1.2+)"""
    try:
        from youtube_transcript_api import NoTranscriptFound
        api = _make_yt_api()
        # ko 우선, auto-generated 포함 fallback
        try:
            fetched = api.fetch(video_id, languages=["ko", "ko-KR"])
        except NoTranscriptFound:
            try:
                tlist = api.list(video_id)
                t = tlist.find_generated_transcript(["ko", "ko-KR"])
                fetched = t.fetch()
            except Exception:
                return None
        text = " ".join(s.text for s in fetched)
        if len(text) < _MIN_TRANSCRIPT_LEN:
            return None
        return text
    except Exception as e:
        logger.warning("[yt-sync] 자막 없음 %s: %s", video_id, e)
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


def extract_mentions(transcript: str) -> list[dict]:
    """Ollama 로컬 모델로 종목 언급 추출. 실패 시 []."""
    import requests as _req
    import re as _re
    ollama_base  = os.environ.get("OLLAMA_BASE", "http://localhost:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "qwen3.5:9b")
    prompt = "/no_think\n\n" + _EXTRACT_PROMPT + transcript[:_MAX_TRANSCRIPT_CHARS]
    try:
        resp = _req.post(
            f"{ollama_base}/api/chat",
            json={
                "model": ollama_model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "options": {"num_predict": 2000, "temperature": 0.1, "repeat_penalty": 1.0},
                "think": False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        text = resp.json()["message"]["content"].strip()
    except Exception as e:
        logger.warning("[yt-sync] LLM 추출 실패: %s", e)
        return []
    # <think> 블록 제거 (Qwen3 reasoning)
    text = _re.sub(r'<think>.*?</think>', '', text, flags=_re.DOTALL)
    text = _re.sub(r'<think>.*', '', text, flags=_re.DOTALL).strip()
    # 마크다운 코드블록 제거
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()
    try:
        mentions = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("[yt-sync] LLM JSON 파싱 실패: %r", text[:200])
        return []
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


# ── 티커 정규화 ───────────────────────────────────────
def _load_aliases() -> dict[str, str]:
    if _ALIASES_PATH.exists():
        return json.loads(_ALIASES_PATH.read_text(encoding="utf-8"))
    return {}


def _load_ticker_master() -> dict[str, str]:
    """pykrx 종목 마스터: 종목명 → 6자리 코드.

    KRX API는 장 마감 이후 당일 날짜로 조회 시 빈 결과를 반환한다.
    오늘 → 어제 순으로 최대 5 거래일까지 폴백한다.
    모든 폴백이 실패하면 파일 캐시를 사용한다.
    성공 시 파일 캐시를 갱신한다.
    """
    try:
        from pykrx import stock
        ref = date.today()
        for delta in range(5):
            candidate = ref - timedelta(days=delta)
            if candidate.weekday() >= 5:
                continue
            date_str = candidate.strftime("%Y%m%d")
            master: dict[str, str] = {}
            for mkt in ["KOSPI", "KOSDAQ"]:
                try:
                    tickers = stock.get_market_ticker_list(date_str, market=mkt)
                    for tk in tickers:
                        name = stock.get_market_ticker_name(tk)
                        if name:
                            master[name] = tk
                except Exception:
                    pass
            if master:
                if delta > 0:
                    logger.info("[yt-sync] pykrx 마스터: %s 기준 %d건 (%d일 폴백)",
                                date_str, len(master), delta)
                try:
                    _TICKER_MASTER_CACHE.write_text(
                        json.dumps(master, ensure_ascii=False), encoding="utf-8"
                    )
                except Exception:
                    pass
                return master
    except Exception as e:
        logger.warning("[yt-sync] pykrx 마스터 로드 실패: %s", e)

    # pykrx 실패 시 파일 캐시 사용
    if _TICKER_MASTER_CACHE.exists():
        try:
            cached = json.loads(_TICKER_MASTER_CACHE.read_text(encoding="utf-8"))
            logger.warning("[yt-sync] pykrx 실패 — 캐시 파일 사용 (%d건)", len(cached))
            return cached
        except Exception:
            pass
    logger.warning("[yt-sync] pykrx 마스터 및 캐시 모두 실패 — 빈 마스터 사용")
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
                LIMIT  %s
            """, (_FILL_RETURNS_BATCH,))
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
            timeout=60,
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

        # 레코드별 수익률 계산
        upsert_rows: list[tuple] = []
        for mention_id, ticker6, video_date in rows:
            if isinstance(video_date, str):
                video_date = date.fromisoformat(video_date)

            yf_sym = ticker_to_sym.get(ticker6, ticker6 + ".KS")
            sym_cache = price_cache.get(yf_sym, {})

            base_date = _prev_business_day_or_self(video_date)
            close_base = sym_cache.get(base_date)
            if not close_base:
                continue

            ret_1d = ret_5d = ret_20d = None
            for ret_val_ref, bdays in (("ret_1d", 1), ("ret_5d", 5), ("ret_20d", 20)):
                target = _next_business_day(video_date, bdays)
                if target > today:
                    continue
                close_target = sym_cache.get(target)
                if close_target:
                    val = round((close_target / close_base) - 1, 6)
                    if ret_val_ref == "ret_1d":
                        ret_1d = val
                    elif ret_val_ref == "ret_5d":
                        ret_5d = val
                    else:
                        ret_20d = val

            if ret_1d is None and ret_5d is None and ret_20d is None:
                continue

            upsert_rows.append((mention_id, ret_1d, ret_5d, ret_20d, today))

        # 단일 배치 upsert
        if upsert_rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO youtube_mention_forward_returns
                        (mention_id, ret_1d, ret_5d, ret_20d, filled_at)
                    VALUES %s
                    ON CONFLICT (mention_id) DO UPDATE SET
                        ret_1d    = COALESCE(EXCLUDED.ret_1d,  youtube_mention_forward_returns.ret_1d),
                        ret_5d    = COALESCE(EXCLUDED.ret_5d,  youtube_mention_forward_returns.ret_5d),
                        ret_20d   = COALESCE(EXCLUDED.ret_20d, youtube_mention_forward_returns.ret_20d),
                        filled_at = EXCLUDED.filled_at
                    """,
                    upsert_rows,
                )
            conn.commit()
            filled = len(upsert_rows)

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
        time.sleep(_TRANSCRIPT_FETCH_SLEEP)
        if not transcript:
            logger.debug("[yt-sync] 자막 없음: %s", vid)
            continue

        mentions = extract_mentions(transcript)
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
    api_key = _get_env("YOUTUBE_API_KEY")

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
        n = run_sync(dsn, api_key, from_date, to_date)
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
        n = run_sync(dsn, api_key, yesterday, yesterday)
        logger.info("[yt-sync] 운영 수집 완료: %d건", n)
