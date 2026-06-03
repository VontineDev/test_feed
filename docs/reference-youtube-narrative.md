# YouTube 내러티브 수집 레퍼런스

삼프로TV 영상 자막에서 종목 언급을 수집·집계하는 시스템. `data/youtube_narrative_sync.py`

## 아키텍처

```
YouTube Data API v3
    │  (삼프로TV 채널 영상 목록)
    ↓
youtube-transcript-api  [쿠키 선택 적용]
    │  (한국어 자막 텍스트)
    ↓
Gemini Flash LLM
    │  (종목 언급 JSON 추출)
    ↓
youtube_mention_raw         ← 원시 언급 데이터
    ↓
youtube_attention_scores    ← 5영업일 rolling attention_score
    ↓
yfinance (배치 다운로드)    ← 종가 데이터
    ↓
youtube_mention_forward_returns  ← 1d/5d/20d 수익률 (백테스트용)
```

## 환경 변수

| 변수 | 필수 | 설명 |
|------|------|------|
| `YOUTUBE_API_KEY` | 필수 | YouTube Data API v3 키 |
| `GEMINI_API_KEY` | 필수 | Google Gemini API 키 |
| `DATABASE_URL` | 필수 | PostgreSQL DSN |

## 모듈 상수

| 상수 | 값 | 설명 |
|------|-----|------|
| `_MIN_TRANSCRIPT_LEN` | `200` | 이보다 짧은 자막은 유효하지 않은 것으로 간주 |
| `_ROLLING_DAYS` | `5` | attention_score rolling window (영업일) |
| `_MAX_TRANSCRIPT_CHARS` | `8000` | Gemini 프롬프트에 전달하는 자막 최대 길이 |
| `_GEMINI_RPM_SLEEP` | `4.0` | Gemini free tier 15 RPM 대응 대기 시간(초) |
| `_FILL_RETURNS_BATCH` | `500` | `fill_forward_returns` 1회 처리 최대 행 수 |

**쿠키 파일 (선택)**: `docs/youtube.com_cookies.txt` — Netscape 형식 쿠키. 파일이 존재하면 자막 요청에 자동 적용됩니다. IP 차단 우회에 사용. `.gitignore` 등록됨.

## CLI

```bash
# 전일 업로드 수집 (스케줄러 09:05 KST)
python data/youtube_narrative_sync.py

# 과거 소급 수집 (블라인드 백테스트용)
python data/youtube_narrative_sync.py --backfill --from 2026-01-01

# forward return 채우기만
python data/youtube_narrative_sync.py --fill-returns

# 테이블 DDL만 실행 (첫 실행 시)
python data/youtube_narrative_sync.py --ensure-tables
```

## 수집 채널

`_CHANNEL_ID = "UChlv4GSd7OQl3js-jkLOnFA"` — 삼프로TV (3PROTV)

## 주요 함수

### `fetch_video_list(api_key, from_date, to_date) -> list[dict]`

채널에서 기간 내 업로드된 영상 목록 반환. 페이지네이션 자동 처리 (maxResults=50).

반환 형식:
```python
[{"video_id": "abc123", "video_date": "2026-05-29", "title": "제목"}]
```

### `_make_yt_api() -> YouTubeTranscriptApi`

`YouTubeTranscriptApi` 인스턴스를 생성합니다. `docs/youtube.com_cookies.txt`가 존재하면 `http.cookiejar.MozillaCookieJar`로 로드해 `requests.Session`에 적용한 뒤 `YouTubeTranscriptApi(http_client=session)`으로 반환합니다. 파일이 없으면 쿠키 없이 반환합니다. `fetch_transcript` 내부에서 매 호출마다 인스턴스를 새로 생성합니다.

### `fetch_transcript(video_id) -> str | None`

한국어 자막 텍스트 반환. 언어 우선순위: `["ko", "ko-KR"]` → 자동 생성 자막 fallback. 자막이 `_MIN_TRANSCRIPT_LEN = 200`자 미만이면 `None` 반환. YouTube IP 차단 시 `IpBlocked` 예외가 발생하며 `None`을 반환하고 WARNING 로그를 남깁니다.

### `extract_mentions(transcript, gemini_api_key) -> list[dict]`

Gemini Flash로 종목 언급 추출. 응답 형식:

```json
[
  {
    "stock_name_raw": "삼성전자",
    "direction": "buy",
    "horizon": "mid",
    "rationale_summary": "HBM3E 납품 본격화로 실적 턴어라운드 기대",
    "source_quote": "삼성전자가 엔비디아에 HBM3E를..."
  }
]
```

`direction`: `buy` / `sell` / `neutral`. `horizon`: `short` / `mid` / `long` / `unknown`.

LLM이 배열 대신 객체를 반환하면 빈 배열로 처리 (버그 방어).

### `_prev_business_day_or_self(d: date) -> date`

날짜가 토요일·일요일이면 직전 금요일을 반환합니다. 공휴일은 보정하지 않습니다. `fill_forward_returns` 내부에서 `video_date`가 주말일 때 기준 종가를 찾기 위해 사용됩니다.

### `fill_forward_returns(dsn: str) -> int`

미채워진 `youtube_mention_raw` 레코드에 1d/5d/20d forward return을 채웁니다.

**동작 방식**:
1. `ticker_names` 테이블에서 6자리 KRX 코드 → yfinance 심볼(`.KS`/`.KQ`) 매핑 구성
2. `LEFT JOIN`으로 미채움 레코드 최대 `_FILL_RETURNS_BATCH`(500)건 조회
3. 전체 종목·전체 날짜 범위를 yfinance 배치 다운로드 한 번으로 처리 (`auto_adjust=True`, `timeout=60`)
4. `video_date`가 주말이면 `_prev_business_day_or_self`로 기준가 날짜 롤백
5. 수익률 계산 후 전체 결과를 `psycopg2.extras.execute_values`로 **단일 배치 upsert** (트랜잭션 1회)
6. `ON CONFLICT DO UPDATE SET ... COALESCE` — 부분 채움 후 재실행 시 기존 값 보존

반환값: 저장된 forward return 레코드 수.

미채움 레코드가 500건 초과면 반복 실행이 필요합니다. `ticker_names` 테이블에 없는 종목은 `<코드>.KS`로 fallback 처리됩니다 (KOSDAQ 종목은 수동으로 `ticker_names`에 `.KQ` 심볼을 추가해야 정확합니다).

### `compute_attention_scores(dsn, window_end, rolling_days=5)`

`youtube_mention_raw`에서 `window_end` 기준 `rolling_days` 영업일 rolling attention_score 계산.

```
attention_score = SUM(sentiment_weight) / distinct_videos
    buy=1.0, neutral=0.5, sell=0.0
```

결과를 `youtube_attention_scores`에 upsert.

## 티커 별칭 매핑

`data/youtube_ticker_aliases.json` — 종목 원문명 → KRX 종목코드 매핑.

예:
```json
{"삼성전자": "005930", "하닉": "000660", "SK하이닉스": "000660"}
```

방송에서 약칭·별명으로 언급되는 종목을 정규화. 파일에 없는 종목명은 `ticker` 컬럼이 NULL로 저장된다.

---

## DB 테이블

### `youtube_mention_raw`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | BIGSERIAL PK | |
| `video_id` | TEXT | YouTube 영상 ID |
| `video_date` | DATE | 영상 업로드 날짜 |
| `speaker` | TEXT | 발언자 (파싱 가능한 경우) |
| `stock_name_raw` | TEXT | 원문 종목명 |
| `ticker` | VARCHAR(12) | KRX 종목코드 (별칭 매핑 후) |
| `direction` | TEXT | buy / sell / neutral |
| `horizon` | TEXT | short / mid / long / unknown |
| `rationale_summary` | TEXT | 언급 맥락 요약 |
| `source_quote` | TEXT | 원문 인용 (NOT NULL) |
| `created_at` | TIMESTAMPTZ | |

UNIQUE 제약: `(video_id, stock_name_raw, source_quote)` — 동일 영상 내 중복 방지.

### `youtube_attention_scores`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `ticker` | VARCHAR(12) PK | |
| `window_end` | DATE PK | rolling window 기준일 |
| `mention_count` | INT | 해당 기간 언급 건수 |
| `sentiment_weighted` | NUMERIC(10,3) | SUM(sentiment_weight) |
| `attention_score` | NUMERIC(10,4) | sentiment_weighted / distinct_videos |
| `distinct_videos` | INT | 언급된 영상 수 |

`attention_score`는 10.0+도 가능 (5개 영상 × 10 buy 언급 = 점수 10). NUMERIC(10,4)로 오버플로 방지.

### `youtube_mention_forward_returns`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `mention_id` | BIGINT PK FK | → `youtube_mention_raw.id` |
| `ret_1d` | NUMERIC(9,4) | 언급 다음날 수익률 |
| `ret_5d` | NUMERIC(9,4) | 5영업일 수익률 |
| `ret_20d` | NUMERIC(9,4) | 20영업일 수익률 |
| `filled_at` | DATE | 채우기 날짜 |

---

## 스케줄러 잡

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `youtube_narrative_sync` | 평일 09:05 | 전일 업로드 수집 + 언급 추출 |
| `youtube_attention_score` | 평일 09:35 | attention_score 계산 (sync 대비 30분 여유) |
| `youtube_forward_return` | 평일 15:40 | forward return 채우기 |

`YOUTUBE_API_KEY` 또는 `GEMINI_API_KEY` 미설정 시 잡이 자동 스킵.

09:35 시작은 의도된 설계: 삼프로TV 일평균 5~15개 영상, Gemini 호출 포함 sync는 60~120초 소요. 09:05에 sync 시작 후 30분이면 완료 보장.

---

## 관련 문서

- [튜토리얼 — 첫 설정부터 첫 수집까지](tutorial-youtube-narrative-quickstart.md)
- [소급 수집 방법 (how-to)](howto-youtube-backfill.md)
- [백테스트 실행 방법 (how-to)](howto-youtube-run-backtest.md)
- [월별 백필 스크립트 레퍼런스](reference-youtube-backfill-monthly.md)
- [백필 계획](plan-youtube-backfill.md)
- [설계 개념·블라인드 백테스트 프로토콜](explanation-youtube-narrative-design.md)
