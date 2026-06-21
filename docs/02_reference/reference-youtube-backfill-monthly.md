# youtube_backfill_monthly — 월별 순차 백필 실행기

삼프로TV 내러티브 파이프라인의 과거 데이터 소급 수집 CLI 스크립트.
`youtube_narrative_sync.py`의 세 핵심 함수를 단계 분리해 월별로 순차 실행한다.

## 단계 구성

| 단계 | 내부 함수 | 대상 테이블 | 재실행 안전성 |
|------|----------|-----------|------------|
| `enqueue` | `enqueue_backfill_videos()` | `youtube_backfill_queue` | `video_id` PK — 이미 큐에 있는 영상 스킵 |
| `process` | `process_backfill_queue()` | `youtube_mention_raw` + `youtube_backfill_queue` | IP 차단 시 `pending` 유지 후 배치 중단 — 자동 재시도 |
| `sync` | `run_sync()` | `youtube_mention_raw` | [구버전 — burst 방식, IP 차단 위험] UNIQUE 제약 — 중복 스킵 |
| `fill-returns` | `fill_forward_returns()` | `youtube_mention_forward_returns` | ON CONFLICT COALESCE — 부분 채움 보존 |
| `scores` | `compute_attention_scores()` | `youtube_attention_scores` | ON CONFLICT DO UPDATE — 덮어쓰기 무해 |

> **권장 흐름**: `enqueue`(1회) → `process`(외부 스케줄러로 반복 호출) → `fill-returns` → `scores`.
> 구 `--step sync`/`all`(burst 방식)은 IP 차단 위험이 있어 비권장 — 자세한 내용은
> [백필 계획](plan-youtube-backfill.md)의 "분산 백필 설계" 참고.

## CLI

```
python scripts/youtube_backfill_monthly.py [--from YYYY-MM] [--to YYYY-MM] [--step STEP] [--batch-size N]
```

### 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--from YYYY-MM` | `2026-01` | 시작 월 |
| `--to YYYY-MM` | `2026-05` | 종료 월 |
| `--step` | `all` | `enqueue` / `process` / `sync` / `fill-returns` / `scores` / `all` |
| `--batch-size` | `8` | `process` 단계에서 1회 호출당 처리할 영상 수 (일일 운영 잡과 동일 수준) |

`all`은 sync → fill-returns → scores 순서로 세 단계를 모두 실행한다 ([구버전] burst 방식, 비권장).

### 환경변수

| 변수 | 필수 단계 | 설명 |
|------|---------|------|
| `YOUTUBE_API_KEY` | sync | YouTube Data API v3 키 |
| `OLLAMA_BASE` / `OLLAMA_MODEL` | sync, process | Ollama 로컬 LLM 서버/모델 (기본 `http://localhost:11434` / `qwen3.5:9b`) |
| `DATABASE_URL` | 전체 | PostgreSQL DSN. 없으면 `core.db.get_dsn()` fallback |

## 예시

```bash
# 1) 큐 적재 (1회만 — 검색 API만 사용, 차단 위험 없음. 월 단위로 순회한다)
python scripts/youtube_backfill_monthly.py --step enqueue --from 2026-01 --to 2026-05

# 2) 큐에서 1배치(기본 8개) 처리 — 외부 스케줄러(Windows 작업 스케줄러 등)로 반복 호출
python scripts/youtube_backfill_monthly.py --step process
python scripts/youtube_backfill_monthly.py --step process --batch-size 12

# 큐 소진 후 마무리
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores --from 2026-01 --to 2026-05

# [구버전, 비권장] burst 방식 — IP 차단 위험
python scripts/youtube_backfill_monthly.py --step sync --from 2026-01 --to 2026-01
```

## 순서 의존성

`enqueue`/`sync`는 반드시 오래된 달 순서로 큐에 쌓이고 처리되어야 한다.
`compute_attention_scores()`의 5영업일 rolling window가 이전 달 데이터를 참조하기 때문이다.
(`enqueue`는 영상 목록을 `video_date` 오름차순으로 적재하고, `process`도 `video_date` 오름차순으로
꺼내므로 순서는 자동 보장된다.)

```
enqueue (1회) → process 반복 (큐 소진까지) → fill-returns → scores
```

fill-returns와 scores는 큐 소진 후 한 번씩 실행하면 된다.

## 오류 처리

**enqueue 날짜 범위 주의**: YouTube 검색 API는 넓은 날짜 범위를 한 번에 조회하면 결과가
누락될 수 있다(예: 1~5월 전체를 한 번에 조회하면 3개만 반환되지만, 2월만 단독 조회하면
100개가 반환되는 현상 확인). `step_enqueue`는 `step_sync`와 동일하게 내부적으로 월 단위로
나눠서 호출하므로 항상 `--from`/`--to`로 여러 달을 지정해도 안전하다.

**월별 실패 스킵**: 특정 월 sync가 예외로 실패하면 해당 월을 `failed` 목록에 기록하고 다음 달로 계속 진행한다. 로그 끝에 `[sync] 실패 월: 2026-02` 형태로 출력된다.

**fill-returns 잔여분**: 레코드가 500건 초과면 스크립트가 자동으로 1회 추가 실행한다. 그래도 미채움이 남으면 `--step fill-returns`를 재실행한다.

**IP 차단(`process`)**: `RequestBlocked` 감지 시 해당 영상을 `pending`으로 유지하고 배치를
즉시 중단한다 — 다음 스케줄 실행에서 자동 재시도되므로 수동 개입이 필요 없다. 로그에는
`[process] 완료: N개 처리, M건 저장 — IP 차단 감지로 조기 종료, 다음 실행에서 재시도`가 출력된다.

**IP 차단(구버전 `sync`)**: `YouTube is blocking requests from your IP` 경고가 출력되면 자막 수집이 차단된 상태다. 해당 영상은 스킵되어 `mention_raw` 저장 건수가 줄어든다. 쿠키 파일 설정 또는 IP 변경 후 `--step sync`를 재실행하면 UNIQUE 제약으로 이미 수집된 건은 중복 저장 없이 스킵된다.

## 관련

- [소급 수집 방법 (how-to)](howto-youtube-backfill.md)
- [백필 계획](plan-youtube-backfill.md)
- [youtube_narrative_sync 레퍼런스](reference-youtube-narrative.md)
