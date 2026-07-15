# 스케줄러 레퍼런스 및 운영 방법

`run_scheduler.py` — 뉴스 수집·요약 분리 구조와 KRX/DART/YouTube 잡을 모두 관리하는 중앙 스케줄러.

## 구조 개요

```
run_scheduler.py
    │
    ├─ [뉴스 수집 잡]   interval분마다 RSS 피드 수집 → Queue 적재
    ├─ [요약 워커]       Queue 대기 → LLM 요약 → DB 저장 → 신호 감지 → Telegram
    ├─ [APScheduler]    CronTrigger 기반 정시 잡들
    │     ├─ 분류기, 스크리너, 워치리스트
    │     ├─ YouTube 내러티브 수집/집계
    │     ├─ KRX 리스팅/수급/시간외
    │     └─ DART 공시/XBRL/추출기
    └─ [Telegram 봇]    bot_polling_loop (별도 태스크)
```

뉴스 수집 잡이 요약을 기다리지 않으므로 요약이 지연돼도 잡 스킵이 발생하지 않는다.

## CLI

```bash
python run_scheduler.py               # 기본 7분 간격
python run_scheduler.py --interval 1  # 1분 간격 (빠른 테스트)
python run_scheduler.py --no-summary  # 요약 없이 수집만
```

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--interval N` | 7 | 뉴스 수집 잡 실행 간격 (분) |
| `--no-summary` | off | LLM 요약 비활성. 기사 수집만 진행 |

종료: `Ctrl+C`

---

## 등록된 잡 전체 목록

### 뉴스 수집 (항상 활성)

| 잡 ID | 트리거 | 내용 |
|-------|--------|------|
| `news_collect` | `interval minutes=N` | Reuters/Yahoo/Bloomberg/CNBC/연합/한경/매경 RSS 수집 → Queue |

시작 후 3초 뒤 첫 실행. `coalesce=True` (밀린 잡 합치기).

### 분석 잡 (DB 연결 필수)

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `krx_daily_refresh` | 매일 20:00 (요일 제한 없음) | KRX 종목 리스트 갱신 (krx_listings) |
| `weekly_chart_screener` | 일요일 20:30 | 주봉 Ichimoku 스크리닝 (chart_signals) |
| `daily_stage_classifier` | 평일 16:30 | 일봉 3단계 Stage 분류기 |
| `daily_watchlist_brief` | 평일 17:00 | 워치리스트 거래대금 일보 → Telegram |

### YouTube 잡 (`YOUTUBE_API_KEY` 필수, LLM 추출은 Ollama — `OLLAMA_BASE`/`OLLAMA_MODEL`)

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `youtube_narrative_sync` | 평일 09:05 | 삼프로TV 전일 업로드 수집 → mention 추출 |
| `youtube_attention_score` | 평일 09:35 | 5영업일 rolling attention_score 계산 |
| `youtube_forward_return` | 평일 15:40 | mention forward return 채우기 |

09:35는 의도된 30분 간격: sync 잡이 15개 영상 처리에 60~120초 소요.

### DART 잡 (DART_API_KEY 필수)

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `daily_dart_disclosures` | 평일 09:00 | 전일 Top 20 공시 이벤트 수집 |
| `monthly_dart_xbrl` | 매월 2일 02:00 | Top 20 XBRL 재무수치 갱신 |
| `dart_extractions_spring` | 5월 21일 03:00 | 사업보고서 + 1분기 보고서 Ollama 내러티브 추출 |
| `dart_extractions_autumn` | 9월 2일 03:00 | 반기보고서 Ollama 내러티브 추출 |
| `dart_extractions_winter` | 11월 21일 03:00 | 3분기보고서 Ollama 내러티브 추출 |

`DART_API_KEY` 미설정 시 잡은 경고 로그 후 자동 스킵.

### KRX 수급·시간외 잡

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `daily_aftermarket_sync` | 평일 16:05 | 키움 시간외 단일가 스냅샷 (aftermarket_snap) |
| `daily_flow_sync` | 평일 18:00 | KRX 외국인·기관·개인 순매수 증분 (daily_flow, `--backend krx-direct`) |
| `daily_ohlcv_warm` | 평일 20:00 | KRX OpenAPI 전일 전 종목 OHLCV → daily_ohlcv (KRX_OPENAPI_KEY 필수) |
| `sector_stats` | 평일 20:30 | 섹터별 일별 수급·수익률 집계 (daily_flow_sync + daily_ohlcv_warm 완료 후) |
| `daily_market_snap` | 평일 16:10 | 당일 거래금액 Top 100 스냅샷 |

`daily_ohlcv_warm`/`sector_stats`는 원래 18:30/19:00이었으나, 2026-07-11 Tor 지터(요청 간격 랜덤화) 도입으로 `daily_flow_sync` 실행시간이 최대 ~90분(세션 만료 자동복구 대기까지 겹치면 ~120분)으로 늘어나 20:00/20:30으로 뒤로 밀림(`26284a8`).

### 모의투자 잡

| 잡 ID | 실행 시각 (KST) | 필요 환경변수 | 내용 |
|-------|-----------------|--------------|------|
| `compose_paper_entry` | 일요일 21:15 | DB만 필요 | FUNNEL-1/AND-1/SCORE-1 주간 신호 → pending 적재 |
| `paper_open_entry` | 평일 09:05 | `KIWOOM_MOCK_APPKEY` | T+1 진입 주문 실행 |
| `paper_exit_checker` | 평일 15:20 | `KIWOOM_MOCK_APPKEY` | 익절/손절 조건 확인 |
| `paper_eod_sampler` | 평일 16:40 | `KIWOOM_MOCK_APPKEY` | 일별 포지션 스냅샷 저장 |

`compose_paper_entry`는 Kiwoom 계정 없이 DB만으로 동작한다. `paper_open_entry`가 다음 영업일 09:05에 실제 매수주문을 실행한다.

---

## 잡스토어

APScheduler 잡스토어는 PostgreSQL SQLAlchemy 기반. DB 연결 실패 시 MemoryJobStore로 폴백 (재시작 시 잡 등록 초기화됨).

`DATABASE_URL` 환경변수의 `postgres://` prefix는 `postgresql+psycopg2://`로 자동 정규화.

---

## 뉴스 게이팅

뉴스 신호가 Telegram으로 전달되려면 다음 조건 중 하나를 충족해야 합니다:

1. 해당 종목이 주봉 스크리너(`_screener_tickers`) 통과 종목
2. 해당 종목이 최근 7일 내 Stage 1/2/3 분류 종목(`_active_stage_tickers`)

게이팅 캐시가 비어 있으면 (초기 실행 시) 모든 신호 통과.

---

## 운영 방법

### 재시작

```bash
# Windows: 재시작 스크립트
.\restart_scheduler.bat
# 또는 직접
python run_scheduler.py
```

### 로그 확인

```bash
# 실시간 (Windows)
Get-Content logs\news_crawler.log -Wait -Tail 50

# 최근 에러만
Select-String "ERROR|WARNING" logs\news_crawler.log | Select-Object -Last 50
```

로그 파일: `logs/news_crawler.log`. 자정마다 롤오버, 14일 보관.

### 대시보드에서 잡 수동 트리거

대시보드 → Scheduler 탭에서 `stage`(추세 단계), `screener`(강세 후보), `paper_sample`(모의투자 샘플링), `youtube`(유튜브 수집), `flow`(KRX 수급) 5개 잡을 버튼으로 즉시 실행할 수 있습니다. `dart_screened`는 백엔드 `_VALID_JOBS`에는 있지만 UI 버튼은 없어 API 직접 호출로만 트리거 가능합니다. `scheduler_triggers` 테이블에 `pending` 행을 INSERT하면 30초 내에 `_trigger_watcher_job`이 실행합니다.

### 새 잡 추가

`run_scheduler.py`의 `main()` 함수에 `scheduler.add_job(...)` 추가:

```python
scheduler.add_job(
    my_new_job,
    CronTrigger(day_of_week="mon-fri", hour=10, minute=0, timezone="Asia/Seoul"),
    id="my_new_job",
    max_instances=1,
    misfire_grace_time=3600,
    replace_existing=True,
)
```

`max_instances=1`은 필수 (중복 실행 방지). `replace_existing=True`는 재시작 시 기존 잡스토어의 잡 교체.

### 환경변수별 잡 활성화 여부

| 환경변수 | 없을 때 비활성화되는 잡 |
|----------|------------------------|
| `DART_API_KEY` | daily_dart_disclosures, monthly_dart_xbrl, dart_extractions_spring/autumn/winter |
| `YOUTUBE_API_KEY` | youtube_narrative_sync, youtube_attention_score, youtube_forward_return |
| `KIWOOM_MOCK_APPKEY` | paper_open_entry, paper_exit_checker, paper_eod_sampler |
| `KRX_OPENAPI_KEY` | daily_ohlcv_warm (경고 후 0 반환, 스케줄러 크래시 없음) |
| (DB 연결만 필요) | compose_paper_entry — Kiwoom 계정 없이도 동작 |

---

## 관련 문서

- [DART 파이프라인 레퍼런스](reference-dart-pipeline.md)
- [YouTube 내러티브 수집 레퍼런스](reference-youtube-narrative.md)
- [KRX 파이프라인 레퍼런스](reference-krx-pipeline.md)
- [키움 연동 레퍼런스](reference-kiwoom.md)
