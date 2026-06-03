# YouTube 내러티브 소급 수집 방법

## 목적

과거 날짜의 삼프로TV 영상을 소급 수집하여 백테스트용 `youtube_mention_raw` 데이터를 채웁니다. 블라인드 백테스트 원칙: 방법론 확정(git tag) → 과거 데이터 소급 수집 → 가격 대조 순서를 지켜야 합니다.

## 전제 조건

- `YOUTUBE_API_KEY`, `GEMINI_API_KEY`, `DATABASE_URL` 환경변수 설정
- 테이블이 이미 생성되어 있어야 함 (없으면 `--ensure-tables` 먼저 실행)
- yfinance 설치: `pip install yfinance` (forward return 채우기에 필요)

## 1단계: 테이블 생성 확인 (최초 1회)

```bash
python data/youtube_narrative_sync.py --ensure-tables
```

## 2단계: 과거 소급 수집

> **권장 방법**: `youtube_backfill_monthly.py` 스크립트를 사용한다.
> 단계 분리(sync → fill-returns → scores)와 월별 오류 격리가 내장되어 있어,
> 네트워크 hang이나 API 차단 시에도 안전하게 재실행할 수 있다.

```bash
# 1~5월 전체 자동 실행 (권장)
python scripts/youtube_backfill_monthly.py

# 특정 월만
python scripts/youtube_backfill_monthly.py --from 2026-02 --to 2026-03

# 단계 분리 실행 (문제 발생 시)
python scripts/youtube_backfill_monthly.py --step sync
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores
```

스크립트 상세 옵션은 [reference-youtube-backfill-monthly.md](reference-youtube-backfill-monthly.md)를 참고한다.

### 직접 CLI 사용 (단일 기간)

```bash
# 특정 날짜 범위를 한 번에 실행 (단계 분리 불필요할 때)
python data/youtube_narrative_sync.py --backfill --from 2026-01-01 --to 2026-01-31
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

백테스트 분석에는 언급 이후 실제 수익률이 필요합니다.

```bash
python data/youtube_narrative_sync.py --fill-returns
```

`youtube_mention_forward_returns`에 1d/5d/20d 수익률이 채워집니다. `filled_at`이 없는 레코드만 처리하므로 반복 실행해도 안전합니다.

**가격 소스**: yfinance 배치 다운로드. `ticker_names` 테이블에서 6자리 KRX 코드 → `.KS`/`.KQ` yfinance 심볼로 자동 변환합니다. 미채움 레코드 전체의 날짜 범위를 한 번의 API 호출로 처리합니다.

**주말 기준가 처리**: `video_date`가 토·일이면 직전 금요일 종가를 기준가로 사용합니다 (`_prev_business_day_or_self`). 주말 날짜로 가격 조회 시 빈 결과가 반환되는 yfinance 동작을 보정합니다.

> **순서 의존성**: 2월 backfill 전에 1월 backfill이 완료되어 있어야 합니다. 2월 attention_score rolling window가 1월 데이터를 참조하기 때문입니다. 항상 순차 실행하세요 (1월 → 2월 → 3월).

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

## IP 차단 및 쿠키 우회

YouTube는 자막 요청이 과도하면 IP를 차단합니다 (`IpBlocked` 예외). 차단 시 해당 영상은 스킵되고 mention_raw 저장 건수가 0이 됩니다.

### 쿠키 파일로 우회

1. Chrome/Firefox에서 YouTube에 로그인합니다.
2. "Get cookies.txt LOCALLY" 등 브라우저 확장으로 `youtube.com_cookies.txt`를 Netscape 형식으로 내보냅니다.
3. 파일을 `docs/youtube.com_cookies.txt`에 저장합니다. (`.gitignore`에 등록되어 있어 커밋되지 않습니다.)

파일이 존재하면 `_make_yt_api()`가 자동으로 쿠키를 로드해 `requests.Session`에 적용합니다. 파일이 없으면 쿠키 없이 동작합니다.

> **주의**: 쿠키는 로그인 세션 인증에 사용되며, IP 차단 자체를 해제하지는 않습니다. IP 차단 중에는 VPN·핫스팟으로 IP를 변경하거나 차단이 해제될 때까지 대기하세요. 차단은 보통 수 시간~1일 내에 해제됩니다.

### 증상 확인

IP 차단 발생 시 로그:
```
WARNING  [yt-sync] 자막 없음 <video_id>: YouTube is blocking requests from your IP.
```

### 백필 실패 후 재실행

UNIQUE 제약 덕분에 중복 저장 없이 이어서 실행할 수 있습니다:

```bash
# 자막 수집 재실행 (이미 저장된 건 스킵)
python data/youtube_narrative_sync.py --backfill --from 2026-02-01 --to 2026-02-28

# forward return 채우기 (미채움 레코드만 처리)
python data/youtube_narrative_sync.py --fill-returns
```

## 주의 사항

- `--backfill`은 이미 수집된 `(video_id, stock_name_raw, source_quote)` 조합을 자동 스킵합니다. 중단 후 재시작해도 중복 없이 이어서 진행됩니다.
- YouTube Data API는 일일 10,000 units 쿼터가 있습니다. 검색 1회 = 100 units. 하루 100회 검색 가능.
- 삼프로TV 영상 자막이 없는 경우(`_MIN_TRANSCRIPT_LEN = 200자 미만`) 해당 영상은 스킵됩니다.
- Gemini가 가끔 배열 대신 객체를 반환합니다. 이 경우 해당 언급은 빈 배열로 처리됩니다.
- forward return 채우기는 최대 500건씩 처리합니다. 미채움 레코드가 많으면 반복 실행하세요.

## 관련 문서

- [YouTube 내러티브 수집 레퍼런스](reference-youtube-narrative.md)
