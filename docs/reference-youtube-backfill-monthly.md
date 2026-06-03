# youtube_backfill_monthly — 월별 순차 백필 실행기

삼프로TV 내러티브 파이프라인의 과거 데이터 소급 수집 CLI 스크립트.
`youtube_narrative_sync.py`의 세 핵심 함수를 단계 분리해 월별로 순차 실행한다.

## 단계 구성

| 단계 | 내부 함수 | 대상 테이블 | 재실행 안전성 |
|------|----------|-----------|------------|
| `sync` | `run_sync()` | `youtube_mention_raw` | UNIQUE 제약 — 중복 스킵 |
| `fill-returns` | `fill_forward_returns()` | `youtube_mention_forward_returns` | ON CONFLICT COALESCE — 부분 채움 보존 |
| `scores` | `compute_attention_scores()` | `youtube_attention_scores` | ON CONFLICT DO UPDATE — 덮어쓰기 무해 |

## CLI

```
python scripts/youtube_backfill_monthly.py [--from YYYY-MM] [--to YYYY-MM] [--step STEP]
```

### 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--from YYYY-MM` | `2026-01` | 시작 월 |
| `--to YYYY-MM` | `2026-05` | 종료 월 |
| `--step` | `all` | `sync` / `fill-returns` / `scores` / `all` |

`all`은 sync → fill-returns → scores 순서로 세 단계를 모두 실행한다.

### 환경변수

| 변수 | 필수 단계 | 설명 |
|------|---------|------|
| `YOUTUBE_API_KEY` | sync | YouTube Data API v3 키 |
| `GEMINI_API_KEY` | sync | Google Gemini Flash API 키 |
| `DATABASE_URL` | 전체 | PostgreSQL DSN. 없으면 `core.db.get_dsn()` fallback |

## 예시

```bash
# 1~5월 전체 자동 실행
python scripts/youtube_backfill_monthly.py

# 2월부터 이어서 실행 (1월 완료 후)
python scripts/youtube_backfill_monthly.py --from 2026-02

# 1월 sync만 (LLM 추출 포함)
python scripts/youtube_backfill_monthly.py --from 2026-01 --to 2026-01 --step sync

# forward return만 채우기 (전체 기간, 단계 분리 실행 시)
python scripts/youtube_backfill_monthly.py --step fill-returns

# attention_score 재집계 (특정 기간)
python scripts/youtube_backfill_monthly.py --step scores --from 2026-01 --to 2026-03
```

## 순서 의존성

`sync`는 반드시 오래된 달 순서로 실행해야 한다.
`compute_attention_scores()`의 5영업일 rolling window가 이전 달 데이터를 참조하기 때문이다.

```
2026-01 sync → 2026-02 sync → ... → fill-returns → scores
```

fill-returns와 scores는 전체 완료 후 한 번씩 실행하면 된다.

## 오류 처리

**월별 실패 스킵**: 특정 월 sync가 예외로 실패하면 해당 월을 `failed` 목록에 기록하고 다음 달로 계속 진행한다. 로그 끝에 `[sync] 실패 월: 2026-02` 형태로 출력된다.

**fill-returns 잔여분**: 레코드가 500건 초과면 스크립트가 자동으로 1회 추가 실행한다. 그래도 미채움이 남으면 `--step fill-returns`를 재실행한다.

**IP 차단**: `YouTube is blocking requests from your IP` 경고가 출력되면 자막 수집이 차단된 상태다. 해당 영상은 스킵되어 `mention_raw` 저장 건수가 줄어든다. 쿠키 파일 설정 또는 IP 변경 후 `--step sync`를 재실행하면 UNIQUE 제약으로 이미 수집된 건은 중복 저장 없이 스킵된다.

## 관련

- [소급 수집 방법 (how-to)](howto-youtube-backfill.md)
- [백필 계획](plan-youtube-backfill.md)
- [youtube_narrative_sync 레퍼런스](reference-youtube-narrative.md)
