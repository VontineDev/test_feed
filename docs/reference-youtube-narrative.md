# YouTube 내러티브 수집 레퍼런스

삼프로TV 영상 자막에서 종목 언급을 수집·집계하는 시스템. `data/youtube_narrative_sync.py`

## 아키텍처

```
YouTube Data API v3
    │  (삼프로TV 채널 영상 목록)
    ↓
youtube-transcript-api
    │  (한국어 자막 텍스트)
    ↓
Gemini Flash LLM
    │  (종목 언급 JSON 추출)
    ↓
youtube_mention_raw         ← 원시 언급 데이터
    ↓
youtube_attention_scores    ← 5영업일 rolling attention_score
    ↓
youtube_mention_forward_returns  ← 1d/5d/20d 수익률 (백테스트용)
```

## 환경 변수

| 변수 | 필수 | 설명 |
|------|------|------|
| `YOUTUBE_API_KEY` | 필수 | YouTube Data API v3 키 |
| `GEMINI_API_KEY` | 필수 | Google Gemini API 키 |
| `DATABASE_URL` | 필수 | PostgreSQL DSN |

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

### `fetch_transcript(video_id) -> str | None`

한국어 자막 텍스트 반환. 언어 우선순위: `["ko", "ko-KR"]` → 자동 생성 자막 fallback. 자막이 `_MIN_TRANSCRIPT_LEN = 200`자 미만이면 `None` 반환.

### `extract_mentions(transcript, video_id, video_date) -> list[dict]`

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

- [YouTube 내러티브 소급 수집 방법](howto-youtube-backfill.md)
- [스케줄러 레퍼런스](reference-scheduler.md)
- [youtube_narrative_screening_concept.md](../youtube_narrative_screening_concept.md) — 설계 개념 문서
