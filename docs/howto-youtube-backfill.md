# YouTube 내러티브 소급 수집 방법

## 목적

과거 날짜의 삼프로TV 영상을 소급 수집하여 백테스트용 `youtube_mention_raw` 데이터를 채웁니다. 블라인드 백테스트 원칙: 방법론 확정(git tag) → 과거 데이터 소급 수집 → 가격 대조 순서를 지켜야 합니다.

## 전제 조건

- `YOUTUBE_API_KEY`, `GEMINI_API_KEY`, `DATABASE_URL` 환경변수 설정
- 테이블이 이미 생성되어 있어야 함 (없으면 `--ensure-tables` 먼저 실행)

## 1단계: 테이블 생성 확인 (최초 1회)

```bash
python data/youtube_narrative_sync.py --ensure-tables
```

## 2단계: 과거 소급 수집

```bash
# 2026년 1월 1일부터 전일까지 전체 소급
python data/youtube_narrative_sync.py --backfill --from 2026-01-01

# 특정 기간만
python data/youtube_narrative_sync.py --backfill --from 2025-07-01 --to 2025-12-31
```

**소요 시간**: 날짜당 ~10초 (YouTube API 호출 + Gemini 호출). 1년치 백필은 수십 분~수 시간 소요. YouTube API 일일 쿼터(10,000 units) 확인 필요.

진행 상황은 표준 로그로 출력됩니다:

```
INFO  [yt-sync] 영상 목록: 8개 (2026-05-28 ~ 2026-05-28)
INFO  [yt-sync] 처리: 삼성전자 → buy (HBM3E 납품...)
INFO  [yt-sync] 저장: 12건 (video_id: abc123)
```

이미 수집된 `(video_id, stock_name_raw, source_quote)` 조합은 UNIQUE 제약으로 자동 무시됩니다. 백필을 중단했다가 재시작해도 중복 저장 없이 이어서 진행할 수 있습니다.

## 3단계: Forward Return 채우기

백테스트 분석에는 언급 이후 실제 수익률이 필요합니다. 종가 데이터가 DB에 쌓인 후 실행합니다.

```bash
python data/youtube_narrative_sync.py --fill-returns
```

`youtube_mention_forward_returns`에 1d/5d/20d 수익률이 채워집니다. `filled_at`이 없는 레코드만 처리하므로 반복 실행해도 안전합니다.

## 4단계: 백테스트 결과 조회

```sql
-- ticker별 attention_score와 이후 수익률 대조
SELECT
    s.ticker,
    s.window_end,
    s.attention_score,
    AVG(r.ret_5d) as avg_5d_ret
FROM youtube_attention_scores s
JOIN youtube_mention_raw m ON m.ticker = s.ticker
    AND m.video_date = s.window_end
JOIN youtube_mention_forward_returns r ON r.mention_id = m.id
WHERE s.attention_score > 2.0
GROUP BY s.ticker, s.window_end, s.attention_score
ORDER BY s.window_end DESC;
```

## 주의 사항

- `--backfill`은 과거 날짜를 재처리하지 않습니다. 이미 수집된 날짜는 UNIQUE 제약으로 스킵됩니다.
- YouTube Data API는 일일 10,000 units 쿼터가 있습니다. 검색 1회 = 100 units. 하루 100회 검색 가능.
- 삼프로TV 영상 자막이 없는 경우(`_MIN_TRANSCRIPT_LEN = 200자 미만`) 해당 영상은 스킵됩니다.
- Gemini가 가끔 배열 대신 객체를 반환합니다. 이 경우 해당 언급은 빈 배열로 처리됩니다.

## 관련 문서

- [YouTube 내러티브 수집 레퍼런스](reference-youtube-narrative.md)
