# YouTube 내러티브 소급 수집 방법

## 목적

과거 날짜의 삼프로TV 영상을 소급 수집하여 백테스트용 `youtube_mention_raw` 데이터를 채웁니다. 블라인드 백테스트 원칙: 방법론 확정(git tag) → 과거 데이터 소급 수집 → 가격 대조 순서를 지켜야 합니다.

## 전제 조건

- `YOUTUBE_API_KEY`, `DATABASE_URL` 환경변수 설정 (LLM 추출은 Gemini가 아닌 Ollama 사용 — `OLLAMA_BASE`/`OLLAMA_MODEL`, 기본값 `http://localhost:11434` / `qwen3.5:9b`)
- 테이블이 이미 생성되어 있어야 함 (없으면 `--ensure-tables` 먼저 실행)
- yfinance 설치: `pip install yfinance` (forward return 채우기에 필요)

## 1단계: 테이블 생성 확인 (최초 1회)

```bash
python data/youtube_narrative_sync.py --ensure-tables
```

## 2단계: 과거 소급 수집

> **권장 방법 — 분산 백필**: `youtube_backfill_monthly.py`의 `enqueue`/`process` 단계를 사용한다.
> 영상 목록을 큐(`youtube_backfill_queue`)에 미리 적재하고, 일일 운영 잡과 동일한 소량(8개/회)으로
> 외부 스케줄러가 반복 호출해 자막을 수집한다. burst 방식(한 번에 수백 건 연속 요청)은
> YouTube의 안티스크래핑 임계값을 넘겨 IP 차단을 유발하므로 더 이상 권장하지 않는다
> (2026-06-03 시도 — 810/810 전부 IP 차단, 저장 0건. 자세한 내용은 [백필 계획](plan-youtube-backfill.md) 참고).

```bash
# 1) 큐 적재 — 검색 API만 사용 (자막 요청 없음, 차단 위험 없음). 1회만 실행
python scripts/youtube_backfill_monthly.py --step enqueue --from 2026-01 --to 2026-05

# 2) Windows 작업 스케줄러에 배치 처리 등록 (하루 3회: 11/14/17시)
schtasks /Create /TN "YTBackfillBatch" ^
  /TR "<repo>\venv\Scripts\python.exe <repo>\scripts\youtube_backfill_monthly.py --step process --batch-size 8" ^
  /SC DAILY /ST 11:00 /RI 180 /DU 0006:00

# 큐 진행 상황 확인
#   SELECT status, COUNT(*) FROM youtube_backfill_queue GROUP BY status;

# 3) 큐가 모두 done/no_transcript/error가 되면 작업 삭제
schtasks /Delete /TN "YTBackfillBatch" /F

# 4) 마무리 — forward return 채우기 + attention_score 재집계
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores --from 2026-01 --to 2026-05
```

스크립트 상세 옵션은 [reference-youtube-backfill-monthly.md](reference-youtube-backfill-monthly.md)를 참고한다.

### [구버전, 비권장] burst 방식

```bash
# 단계 분리 실행 — IP 차단 위험 (자세한 내용은 백필 계획의 "burst 백필 시도" 참고)
python scripts/youtube_backfill_monthly.py --step sync
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores

# 특정 날짜 범위를 한 번에 실행
python data/youtube_narrative_sync.py --backfill --from 2026-01-01 --to 2026-01-31
```

**소요 시간**: 날짜당 ~10초 (YouTube API 호출 + Ollama 로컬 LLM 추출). burst 방식은 1년치 백필이 수십 분
이내에 끝나지만 IP 차단 위험이 크다. 분산 백필은 8개/회 × 하루 3회 = 24개/일로, 810개 기준
약 34일이 소요된다. YouTube API 일일 쿼터(10,000 units) 확인 필요.

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

YouTube는 자막 요청이 과도하면 IP를 차단합니다 (`IpBlocked`/`RequestBlocked` 예외). 차단 시 해당 영상은 스킵되고 mention_raw 저장 건수가 0이 됩니다.

> **분산 백필(`enqueue`/`process`)을 사용하면 이 섹션은 대부분 무관합니다.** `process`
> 단계는 `RequestBlocked` 감지 시 해당 영상을 `pending`으로 유지하고 배치를 즉시 중단해
> 다음 스케줄 실행에서 자동 재시도합니다 — 데이터 손실이나 수동 개입이 없습니다.
> 아래 내용은 [구버전] burst 방식(`--step sync`)을 직접 사용할 때 참고하세요.
>
> 또한 **쿠키는 IP 레벨 차단에는 영향을 주지 않습니다** — `youtube_transcript_api`
> 라이브러리 자체에 "Cookie auth has been temporarily disabled, as it is not working
> properly with YouTube's most recent changes"라는 코멘트가 있어, 쿠키 인증 경로가
> 비활성화된 상태입니다. 차단 해결책은 요청 빈도를 낮추는 것뿐입니다(분산 백필이 채택한 방식).

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
- Ollama가 가끔 배열 대신 객체를 반환합니다. 이 경우 해당 언급은 빈 배열로 처리됩니다.
- forward return 채우기는 최대 500건씩 처리합니다. 미채움 레코드가 많으면 반복 실행하세요.

## 관련 문서

- [YouTube 내러티브 수집 레퍼런스](reference-youtube-narrative.md)
