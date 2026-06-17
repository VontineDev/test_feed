# Changelog

All notable changes to this project will be documented in this file.

## [0.10.1.8] - 2026-06-17

### Removed
- **`scripts/` 임시 디버그·일회성 체크 스크립트 67개 삭제**: 언더스코어 prefix(`_debug_*`, `_check_*`, `_analyze_*`, `_fix_*`, `_verify_*` 등) 54개(git-tracked) + 종목별 일회성 체크 스크립트 13개(untracked) 삭제. 파이프라인 핵심 스크립트 20개만 보존.

### Added
- **`scripts/dart_reextract_quarterly.py`** — 분기·반기보고서 선별 force 재추출 유틸리티: 사업보고서를 건드리지 않고 분기/반기 DART 디렉터리만 XBRL+LLM으로 재추출. `logs/dart_quarterly_reextract_YYYYMMDD.log`에 진행 로그 기록, `dart_extractions` 테이블에 `ON CONFLICT DO UPDATE`로 upsert.

## [0.10.1.7] - 2026-06-16

### Added
- **텔레그램 — 파이프라인 수동 트리거 명령어 5종** (`telegram/telegram_bot.py`): 스케줄러 잡을 대기하지 않고 각 파이프라인을 즉시 실행할 수 있는 명령어 추가. 모두 `asyncio.create_task`로 백그라운드 실행되어 실행 중에도 다른 명령어가 응답 가능. 각 파이프라인은 독립 락으로 중복 실행 방지.
  - `/run_flow` — KRX 수급 수집 즉시 실행 (`daily_flow_sync_job`, 40~60분)
  - `/run_stage` — 스테이지 분류 즉시 실행 (`daily_stage_job`, 10~20분)
  - `/run_screener` — 주봉 스크리너 즉시 실행 (전 종목 스캔 + DB 저장, 10~20분)
  - `/run_youtube` — 유튜브 수집 + 어텐션 점수 집계 즉시 실행 (5~10분)
  - `/run_all` — 4개 파이프라인 동시 기동, 이미 실행 중인 항목은 건너뜀

### Removed
- **`/scan` 명령어 제거** (`telegram/telegram_bot.py`): `/run_screener`와 동일 기능이므로 중복 제거. `_handle_scan` 함수·라우팅·help 텍스트·`setMyCommands` 등록 모두 삭제.

## [0.10.1.6] - 2026-06-16

### Added
- **종목분석 탭 — 파이프라인 수집 상태 배너** (`dashboard/backend/main.py`, `dashboard/frontend/src/components/Report.tsx`): 종목분석 탭 헤더 아래에 수급·스테이지·스크리너·유튜브 4개 파이프라인의 최신 수집일과 상태(초록/노랑/빨강)를 한눈에 표시하는 `PipelineStatusBar` 컴포넌트 추가.
  - **`GET /api/report/pipeline-status`** 신설: `daily_flow`(MAX trade_date + 티커수), `stage_classifications`(MAX classified_date), `chart_signals`(MAX screened_at), `youtube_attention_scores`(MAX window_end)를 단일 쿼리로 조회해 각 파이프라인의 날짜·상태(ok/warn/error)를 반환.
  - 상태 임계값: 일간 파이프라인(수급·유튜브) — gap ≤1일=ok, ≤3일=warn, >3일=error. 주간 파이프라인(스테이지·스크리너) — gap ≤7일=ok, ≤14일=warn, >14일=error.
  - 각 항목에 마우스오버 시 상세 툴팁(날짜·티커 수) 표시. 새로고침 버튼과 연동해 탭 리로드 시 상태도 함께 갱신.

## [0.10.1.5] - 2026-06-16

### Fixed
- **`daily_flow_sync_job` subprocess 절대경로 누락 — 약 1개월 수급 수집 중단 복구** (`jobs/infra_jobs.py`): `asyncio.create_subprocess_exec`에 `"krx_flow_sync.py"` 파일명만 넘겨 Python이 현재 작업 디렉토리(프로젝트 루트)에서 스크립트를 찾지 못해 exit code 2로 종료하던 버그 수정. 절대경로(`os.path.join(_root, "data", "krx_flow_sync.py")`)와 `PYTHONPATH=<프로젝트 루트>` 환경변수를 함께 주입해 `import core.db` 경로도 해결. `daily_flow` 테이블의 2026-05-15 ~ 2026-06-14 공백(19 영업일, 15,307행)을 수동 백필로 복구 완료.
- **KRX_SESSION 쿠키 주입 후 warmup이 덮어쓰는 버그 수정** (`data/krx_flow_sync.py`): `_make_krx_direct()`가 `warmup()` → `inject_session()` 순으로 실행하면, warmup 시 서버가 새 익명 JSESSIONID를 발급해 `requests.Session` 쿠키 저장소에 `data.krx.co.kr` 범위로 저장하고, 이어서 주입한 인증 쿠키(`.krx.co.kr` 범위)보다 구체적인 도메인 쿠키가 우선 전송돼 인증이 실패하던 문제 수정. KRX_SESSION이 설정된 경우 warmup을 건너뛰고, `inject_session()`에서 `.krx.co.kr`과 `data.krx.co.kr` 두 도메인에 모두 JSESSIONID를 등록해 도메인 우선순위 충돌을 차단.

## [0.10.1.4] - 2026-06-11

### Added
- **유튜브 내러티브 탭 — 대시보드 Method 2 구현** (`dashboard/backend/main.py`, `dashboard/frontend/src/components/Narrative.tsx`, `dashboard/frontend/src/tabs.ts`): 유튜브 관심도 데이터와 스테이지 분류·스크리너 결과를 결합한 전용 탭 "내러티브(📡)" 추가.
  - **`GET /api/youtube/screener`** 엔드포인트 신설: `youtube_attention_scores`(최신 window_end) ↔ `stage_classifications`(ticker별 최신, `DISTINCT ON`) ↔ `chart_signals`(최신 week_of) ↔ `ticker_names` ↔ `youtube_mention_raw` 5-way LEFT JOIN. 종목명은 `COALESCE(ticker_names.name_ko, chart_signals.name, mention_raw.stock_name_raw, ticker)` 순으로 채움. 응답에 `total` / `stage2_plus` / `in_screener` / `narrative_q` / `triple_combo` 요약 카운트 포함.
  - **`Narrative.tsx`** 컴포넌트: 요약 칩 5개(전체·S2+·스크리너·내러티브Q·트리플콤보), 4-way 필터 버튼(전체/트리플콤보/S2+/스크리너), 테이블(티커·종목명·관심도·내러티브Q뱃지·스테이지뱃지·스크리너뱃지·섹터·현재가). 트리플콤보 = S2+ AND 스크리너 통과 AND 관심도 Q2-4.
  - `tabs.ts`에 `'narrative'` 탭 키 추가 및 lazy import.
  - **JOIN 버그 수정**: 이전 구현에서 `stage_classifications JOIN`이 0건을 반환하던 원인 파악 — `classified_date = MAX(classified_date)` 필터가 당일 갱신된 2개 ticker만 포함해 나머지 종목의 stage가 전부 NULL로 나왔음. `DISTINCT ON (ticker) ORDER BY ticker, classified_date DESC`로 교체해 ticker별 최신 stage를 올바르게 가져오도록 수정. 프론트엔드 빌드: `Narrative-BLbjyxkp.js` (6.19 kB, gzip 2.40 kB) 신규 생성.

## [0.10.1.3] - 2026-06-09

### Added
- **YouTube 내러티브 백필 — 분산 실행 재설계** (`data/youtube_narrative_sync.py`, `scripts/youtube_backfill_monthly.py`): 2026-06-03 burst 백필(월별 순차, 영상당 2초 간격)이 1~5월 810개 영상 전부 "IP blocked by YouTube" 오류로 실패한 것(`logs/backfill_monthly.log`)을 분석한 결과, IP 자체가 영구 차단된 게 아니라 **burst 요청량이 YouTube 안티스크래핑 임계값을 넘겨 일시 차단을 유발**한다는 것을 확인 — 같은 IP로 도는 일일 운영 잡(영상 6개/일)은 정상 동작 중이었음. 쿠키 인증(`docs/youtube.com_cookies.txt`)은 IP 레벨 차단에 영향이 없음도 라이브러리 소스에서 확인(`youtube_transcript_api`의 cookie auth 비활성화 코멘트).
  - `youtube_backfill_queue` 테이블 추가 — `video_id` PK, `status`(pending/done/no_transcript/blocked/error)로 진행률 추적
  - `enqueue_backfill_videos()` — 검색 API만으로 영상 목록을 큐에 적재(자막 요청 없음, 차단 위험 없음). 월 단위로 호출해야 함(넓은 날짜 범위를 한 번에 조회하면 YouTube 검색 API가 결과를 누락하는 현상 확인 — 5개월 전체 조회 시 3개, 1개월 단독 조회 시 100개 반환)
  - `_fetch_transcript_classified()` / `process_backfill_queue()` — 큐에서 `--batch-size`(기본 8)개씩 자막 수집 + LLM 추출, `RequestBlocked` 감지 시 해당 영상을 `pending`으로 유지하고 배치를 즉시 중단해 다음 실행에서 자동 재시도
  - `youtube_backfill_monthly.py`에 `enqueue`/`process`/`--batch-size` 단계 추가, 구 `--step sync`(burst 방식)는 비권장으로 표시
  - Windows 작업 스케줄러 `YTBackfillBatch` 등록 — 매일 11/14/17시(`/RI 180 /DU 0006:00`)에 `process --batch-size 8` 실행, 일일 운영 잡과 동일한 요청 수준으로 IP 차단 임계값 회피. 큐 적재 완료(972건, 2026-01-01~05-31), 1차 실행 2026-06-09 11:00 예정
  - 문서: `docs/plan-youtube-backfill.md`(분산 백필 설계·진행 상태), `docs/howto-youtube-backfill.md`, `docs/reference-youtube-backfill-monthly.md`, `docs/reference-youtube-narrative.md` 갱신

## [0.10.1.2] - 2026-06-08

### Fixed
- **텔레그램 메시지 "티커만 표시" 버그 — 3중 원인 해결**: 랠리 소멸 경고·거래대금 워치리스트 등 `_tc.get_name()`(`core/ticker_cache.py`)을 사용하는 모든 텔레그램 메시지가 한글 종목명 없이 코드만 표시되던 문제의 근본 원인을 추적해 모두 수정.
  1. `KRX_OPENAPI_KEY`가 약 2주간 401 Unauthorized를 반환해 `krx_listings` 테이블이 비어있었음(`[ticker_cache] 0개 이름 항목 로드`) → 사용자가 키를 재발급하면서 해결 확인(실거래 호출 200 OK, 2,770행 응답).
  2. **`sync_krx_listings()` 휴장일 대응** (`data/krx_sync.py`): `bas_dd = 어제`로 하드코딩되어 주말·공휴일에는 KRX Open API가 빈 `OutBlock_1`을 반환해 동기화가 실패하던 문제 수정. 데이터가 나올 때까지 최근 영업일을 최대 10일 거꾸로 탐색하는 재시도 루프로 교체(최장 추석/설 연휴 커버). 검증: 일요일(2026-06-07) 기동 시 자동으로 직전 영업일(금요일 2026-06-05)까지 거슬러 올라가 2,770행 upsert 완료, `pytest tests/test_krx_sync.py` 31건 통과.
  3. **`_run_once_watchlist()` ticker_cache 미로드** (`run_scheduler.py`): `--once watchlist` CLI 경로가 `_run_once_stage()`와 달리 `ticker_cache.load()`를 호출하지 않아, `krx_listings`가 채워진 뒤에도 수동 실행 시 여전히 코드만 전송되던 문제 수정. `_run_once_stage()`와 동일하게 DB 풀 생성 직후 `ticker_cache.load(_db_pool)` 호출 추가(60초 타임아웃, 실패 시 정적 코드로 진행하는 graceful degradation 유지). 검증: `python run_scheduler.py --once watchlist` 재실행 후 로그에 `[ticker_cache] 6736개 이름 항목, 2770개 단축코드 로드 완료` 출력 확인, 실제 텔레그램 메시지에 `삼성전자(005930)` 형식으로 한글 종목명 정상 표시.

## [0.10.1.1] - 2026-06-08

### Fixed
- **TradingDashboard 중복 자동 시작 경로 제거**: 레지스트리 `HKCU\...\Run`의 `TradingDashboard`(→ `start_dashboard_hidden.vbs`) 항목과 Task Scheduler `TradingDashboard` 태스크(AtStartup/AtLogOn)가 동시에 등록되어, 로그온 시 두 경로가 각각 uvicorn을 기동해 포트 8000 충돌을 일으킬 수 있던 문제 수정. `register_tasks.ps1`의 HKCU Run 정리 코드가 실제로는 실행되지 않아 항목이 남아있었음. HKCU Run 항목을 수동 제거하고, 더 이상 참조되지 않는 `scripts/start_dashboard_hidden.vbs` 파일도 삭제 — Task Scheduler(AtLogOn) 단독 관리로 통일, `start_dashboard.ps1`이 수동 시작/재시작의 단일 진입점이라는 원래 의도(0.9.9.x "시작 스크립트 단일화")를 복원. 검증: 기존 uvicorn 종료 후 `schtasks /Run /TN TradingDashboard`로 AtLogOn 경로를 재현 — 단일 master/worker 프로세스만 기동되고 포트 8000 LISTEN 인스턴스 1개로 충돌 없음을 확인.

## [0.10.1.0] - 2026-06-05

### Added
- **DART 재무 현황 패널** (`StageHistoryPopup.tsx`): 종목 상세 팝업 하단에 `DartFinancials` 컴포넌트 추가. `/api/dart/summary/{ticker}` 조회 결과로 최신 보고서 기준 연결 매출·영업이익·이익률 테이블, YoY 등락률, 사업부문별 매출 구성을 표시.
- **`GET /api/dart/summary/{ticker}`** (`dashboard/backend/main.py`): DART 최신 추출 결과에서 corp_name → stock_code 조인 후 `revenue_json`·`segments_json` 반환. 기간·보고서 유형 뱃지 포함.
- **`extract_all_for_company()`** (`data/dart_extractor.py`): 단일 기업만 3-Pass 추출하는 함수. `dart_screened_sync` 잡에서 호출.
- **`dart_screened_sync_job()`** (`jobs/infra_jobs.py`): 최근 N일 Stage/스크리너 종목 대상 자동 DART 분석 잡. 스케줄러 트리거 허용 목록(`_VALID_JOBS`)에 `dart_screened` 추가.

### Fixed
- **`_is_holiday()` 타이밍 버그** (`dashboard/backend/main.py`): 서버 새벽 시작 시 네이버 Finance API에 오늘 거래 데이터가 없어 오늘 날짜를 휴장일로 잘못 캐싱하던 문제 수정. 과거 날짜만 "데이터 부재 = 휴장"으로 판정하고, 오늘/미래 날짜는 데이터가 있을 때만 영업일로 확정. 장 개시 전 서버 기동 시에도 `_is_market_open()` 이 09:00 이후 정상적으로 `True` 반환.
- **지수 데이터 로딩 실패 수정** (`dashboard/backend/main.py`): KRX OpenAPI 지수 엔드포인트(401/404) 실패 시 yfinance `period='10d', interval='1d'`로 KOSPI·KOSDAQ close + prev_close를 가져오는 fallback 추가. 오늘 부분 데이터를 제외하고 직전 영업일 종가를 prev_close로 사용해 등락률 정확 계산. 기존 KRX 성공 시 yfinance 실시간(1m)으로 업데이트하는 로직은 유지.

## [0.10.0.0] - 2026-06-01

### Added
- **YouTube 내러티브 스크리닝**: 삼프로TV 전문가 멘션을 정량 신호로 전환할 수 있습니다. 자막 수집(YouTube Data API v3) → Gemini 2.5 Flash 종목 언급 추출 → `youtube_mention_raw` 저장
- 5영업일 롤링 `attention_score` 집계 (`youtube_attention_scores`)
- 언급 종목 +1d/+5d/+20d forward return 자동 채우기 (`youtube_mention_forward_returns`)
- 블라인드 백테스트 스크립트 (`scripts/youtube_backtest.py`): Spearman IC / t-stat / 종목 히트율 산출
- 스케줄러 3개 잡 등록: 09:05 수집, 09:35 집계, 15:40 수익률 (KST 기준)
- `data/youtube_ticker_aliases.json`: 100개 한국어 약칭 → KRX 코드 수동 매핑 테이블
- `--backfill`, `--ensure-tables`, `--fill-returns`, `--compute-scores` CLI 플래그
- 순수 로직 단위 테스트: `normalize_ticker`, `_prev/_next_business_day`, `_get_env`, `_load_aliases`, `_parse_json3` (26 cases)

### Fixed
- `youtube_backtest.py`: `ret_col` f-string SQL 인젝션 패턴 → dict 조회 방식으로 교체
- `extract_mentions()`: LLM이 dict 반환 시 AttributeError 크래시 → isinstance 가드 추가
- `extract_mentions()`: `direction`/`horizon` 비정상 값 경고 로그 후 기본값으로 대체
- `normalize_ticker()`: 빈 문자열 입력 시 첫 번째 종목 반환 버그 → None 반환으로 수정
- `kiwoom_aftermarket_sync.py`: `trde_prica` 누락 필드 시 행 스킵 버그 수정 (명시적 0만 스킵)
- `--backfill` 백테스트용 날짜별 `attention_score` 집계 누락 수정 (IC 항상 NULL 방지)
- `source_quote` 500자 truncation으로 PostgreSQL B-tree 인덱스 크기 초과 방지
- `attention_score` 컬럼 `NUMERIC(6,4)` → `NUMERIC(10,4)` 오버플로우 방지
- `youtube_attention_score_job` 스케줄 09:10 → 09:35 KST (sync 완료 대기 여유 확보)

## [0.9.9.6] - 2026-05-28

### Added

- **3단계 역할 기반 대시보드** (`dashboard/backend/main.py`, `dashboard/frontend/`): `SPECIAL_USER`/`SPECIAL_PASSWORD` 환경변수 추가로 admin·special·user 3단계 인증 체계 구성. `_BasicAuthMiddleware`가 3개 자격증명을 순서대로 비교. `GET /api/auth/me` 엔드포인트 신설 — 현재 요청의 role을 JSON으로 반환.
- **내 자산 포트폴리오 탭** (`dashboard/backend/main.py`, `dashboard/frontend/src/components/Portfolio.tsx`): admin·special 역할 전용 `GET /api/portfolio` 엔드포인트. 키움 REST API `kt00018`(계좌평가잔고내역) + `kt00001`(예수금상세현황) 를 호출하여 총자산·종목별 보유현황 반환. stale-while-revalidate 5분 캐시. React `Portfolio.tsx` 컴포넌트 — 추정 예탁자산 히어로 카드, 6-열 통계 그리드, 보유 종목 테이블(손익 한국 컬러 규칙).
- **탭 역할 게이팅** (`dashboard/frontend/src/tabs.ts`, `dashboard/frontend/src/hooks/useRole.ts`): `TabConfig.roles` 필드로 탭별 접근 가능 역할 선언. `getVisibleTabs(role)` 함수로 역할에 따라 탭 목록 필터링. `useRole()` 훅이 `/api/auth/me`를 모듈 레벨에서 한 번만 호출하여 역할을 캐싱. `MobileNav`가 정적 `TAB_CONFIG` 대신 `tabs` prop을 받도록 변경.

## [0.9.9.5] - 2026-05-28

### Added

- **OpenDART 전자공시 통합** (`data/dart_sync.py`, `core/db.py`, `jobs/infra_jobs.py`, `run_scheduler.py`): 금융감독원 DART 전자공시 시스템 데이터를 파이프라인에 추가. DB 테이블 4개 신설(`dart_companies`, `dart_disclosures`, `dart_xbrl`, `dart_segments`). Top 20 기업 공시 이벤트 일별 수집, XBRL 재무수치 월별 갱신, 사업보고서 세그먼트 Ollama 파싱 연간 실행. `DART_API_KEY` 환경변수로 인증.
- **스케줄러 DART 잡 3종** (`run_scheduler.py`): ① `daily_dart_disclosures` — 평일 09:00 KST Top 20 기업 전일 공시 수집; ② `monthly_dart_xbrl` — 매월 1일 02:00 KST 전년도 XBRL 재무수치 갱신; ③ `annual_dart_segments` — 매년 5월 1일 03:00 KST 사업보고서 II-2/II-4 Ollama 파싱.
- **공시 갭 감지 + 자동 백필** (`jobs/infra_jobs.py`): `daily_dart_disclosure_job`이 `MAX(rcept_dt)` 기준으로 수집 갭(스케줄러 재시작 등)을 자동 감지. 마지막 수집일 다음날부터 전일까지 자동 백필, 최대 90일 제한.
- **DART 보고서 로컬 다운로더** (`data/dart_download.py`): 사업보고서·반기보고서·분기보고서 원문 ZIP을 `reports/dart/{기업명}/{rcept_no}_{보고서명}/` 구조로 저장하는 독립 실행 스크립트. EUC-KR 파일명 자동 변환, 이미 다운로드된 보고서 자동 스킵, `--dry-run`/`--zip` 옵션 지원.
- **사업보고서 세그먼트 TOC/본문 구분 로직** (`data/dart_sync.py`): `_extract_section_text()`에서 TOC 항목을 본문으로 오인하던 버그 수정. 정지 키워드 탐색 오프셋 200→30 축소 + 행 시작 여부(`\n` 직전) 검사로 TOC 헤딩과 본문 내 인용 구분. 섹션 최대 추출 길이 3,500→6,000자 확장.

## [0.9.9.4] - 2026-05-27

### Fixed

- **매크로 분석 Kiwoom 티커 포맷 변환 누락** (`dashboard/backend/main.py`): Kiwoom ka10032 API가 반환하는 `XXXXXX_AL`(KOSPI) / `XXXXXX_AQ`(KOSDAQ) 포맷 티커를 yfinance에 그대로 넘겨 `다운로드 실패` 오류가 매 10분 갱신마다 반복되던 버그. `_kiwoom_to_yfinance()` 헬퍼 추가 — `_AL` → `.KS`, `_AQ` → `.KQ` 변환. 변환 불가 티커는 aftermarket_snap 폴백.
- **서버 재시작 직후 매크로가 어제 종목으로 채워지는 스타트업 레이스** (`dashboard/backend/main.py`): `_warmup_caches()`가 heatmap·macro를 `asyncio.gather()`로 동시 실행하여 macro가 빈 heatmap 캐시를 읽고 aftermarket_snap(전날 종목)으로 폴백하던 문제. warmup을 2단계로 분리 — ① heatmap 단독 완료 후 ② market_index + macro 병렬 실행. 재시작 즉시부터 오늘 TOP 20 종목 기준으로 분석.

### Changed

- **매크로 종목 선정: 거래대금 상위 20종목 + 거래대금 순 정렬** (`dashboard/backend/main.py`): `_run_macro_analysis()`의 히트맵 슬라이스를 `[:20]`에서 전체 풀(최대 50개)로 확대. yfinance 관측 부족으로 스킵된 자리를 하위 순위 종목으로 채워 유효 종목 항상 20개 확보. 결과 정렬을 `macro_score` 내림차순에서 거래대금(heatmap rank) 순서로 변경.
- **EWY 팩터 표기명 한국어화** (`dashboard/frontend/src/components/Macro.utils.ts`): `FACTOR_LABELS.export` 값을 `'iShares MSCI Korea ETF'` → `'아이셰어즈 대한민국 ETF(EWY)'`로 변경. 팩터 카드 제목·배너 등 전체 반영.
- **히트맵 색상 한국 시장 컨벤션 적용** (`dashboard/frontend/src/tokens.ts`): `heat.hot`(상승 셀)을 초록(`#4ade80`~`#15803d`) → 빨강(`#fca5a5`~`#b91c1c`), `heat.cold`(하락 셀)을 빨강 → 파랑(`#93c5fd`~`#1e40af`)으로 교체. `semantic.up/down`(텍스트)은 이미 한국식이었으므로 변경 없음.
- **히트맵 셀 레이블 2행** (`dashboard/frontend/src/components/Heatmap.tsx`): Nivo 내장 `label` 문자열 렌더러를 폐기하고 커스텀 SVG 레이어(`layers`)로 교체. 셀 높이 ≥ 28px: 종목명(1행) + 등락률 한국색(2행) 표시. 셀 높이 18~27px: 종목명 단행. 셀 너비 기준 자동 truncate (`…` 처리).

## [0.9.9.3] - 2026-05-27

### Changed

- **FactorCard 레이아웃 개선** (`dashboard/frontend/src/components/Macro.tsx`): 현재가와 1일 등락률을 같은 행에 나란히 표시(flexbox `alignItems: baseline`). 이전에는 현재가가 별도 행에 작게 표시되어 가독성이 낮았음. 추세 표시 기간을 기존 5d·20d에서 1일·3일·10일 3단계로 변경하여 단기 모멘텀 파악 용이.
- **팩터 스냅샷 3d·10d 기간 추가** (`analysis/macro_tracker.py`, `dashboard/frontend/src/components/Macro.utils.ts`): `get_macro_snapshot()` 반환 dict에 `change_3d`, `change_10d` 필드 추가. `FactorSnap` 인터페이스와 테스트 목(mock)도 동기화.
- **EWY 팩터 해석 변경** (`analysis/macro_tracker.py`, `dashboard/frontend/src/components/Macro.utils.ts`, `docs/ARCHITECTURE.md`): `export` 팩터를 "수출 모멘텀 대리변수"에서 "미장 한국 투자심리 대리변수(iShares MSCI Korea ETF)"로 재정의. EWY는 미국 시장에 상장된 한국 주식 ETF로, 외국인 수급 유입/이탈 방향을 반영. 라벨(`FACTOR_LABELS`, `FACTOR_SHORT_LABELS`), 1줄 해석(`FACTOR_CARD_INTERPRET`), 섹터 영향(`FACTOR_IMPACT_UP`) 전면 갱신.
- **종목별 매크로 분석 주식 목록 동적화** (`dashboard/backend/main.py`): `_run_macro_analysis()`가 정적 `DEFAULT_TICKERS` 대신 3-tier fallback으로 종목 목록 결정. ① 오늘 실시간 히트맵 캐시 TOP 20 → ② `aftermarket_snap` DB 전날 거래대금 TOP 20(신규 `_fetch_prev_top20_sync()` 함수) → ③ `DEFAULT_TICKERS`. 캐시 비어있는 장 초반·주말에도 전날 거래대금 기준 종목으로 분석.

## [0.9.9.2] - 2026-05-26

### Added

- **피드백 위젯** (`dashboard/frontend/src/components/Feedback.tsx`, `dashboard/backend/main.py`): 헤더 오른쪽 끝 "피드백" 버튼 클릭 시 모달이 열리며, 텍스트 입력 + 선택적 스크린샷(html2canvas)을 Telegram으로 전송. `POST /api/feedback` 엔드포인트 신설 — 스크린샷 있으면 `sendPhoto`, 없으면 `sendMessage`. html2canvas는 동적 import로 lazy 로드되어 번들 크기 영향 없음. 발신자 역할(admin/user)이 캡션에 포함되어 누가 보냈는지 식별 가능.
- **캐시 워밍업** (`dashboard/backend/main.py`): `_warmup_caches()` — FastAPI lifespan 시작 시 heatmap·market_index·macro 3개 캐시를 백그라운드 태스크로 병렬 사전 로딩. 재시작 직후 cold start 응답 지연 해소 (RISK-07).
- **이력 조회 범위 제한** (`dashboard/backend/main.py`): `_HISTORY_MAX_DAYS=365` 상수 추가. `/api/history/stage`, `/api/history/screener`, `/api/history/ticker/{ticker}` 3개 엔드포인트에 날짜 범위 초과 시 HTTP 422 반환. 단일 사용자의 대범위 쿼리로 DB 연결을 독점하는 경로 차단 (RISK-08).
- **`/health` 모니터링 엔드포인트** (`dashboard/backend/main.py`): `GET /health` 신설 — 서버 업타임, asyncpg 풀 상태(size·free·min/max), 캐시별 TTL 잔여 시간, SSE 연결 수(`signals`·`scheduler`)를 JSON으로 반환. Caddy `reverse_proxy` 헬스체크 및 운영 모니터링 용도 (RISK-09).

### Performance

- **stale-while-revalidate 캐시 패턴** (`dashboard/backend/main.py`): `_bg_refresh()` 헬퍼 도입. TTL 만료 후 stale 데이터가 있으면 즉시 반환하고 백그라운드에서 갱신. 10명 동시 접속 시 캐시 만료 직후 모든 요청이 큐 대기하던 Thundering Herd 현상 해소. `_HEATMAP_LOCK` 누락 추가, `_fetch_market_index_data()` 순수 함수로 추출해 락 내부에서 fetch 완료 보장 (RISK-04).
- **외부 API 전용 스레드 풀** (`dashboard/backend/main.py`): `_EXT_EXECUTOR = ThreadPoolExecutor(max_workers=4)` — yfinance·Kiwoom·KRX 동기 호출을 기본 executor에서 분리. `_ext_thread(fn, *args, timeout)` 래퍼로 모든 외부 API 호출에 명시적 타임아웃 적용 (Kiwoom 15s, yfinance 20s, macro 90s). yfinance/Kiwoom 장애 시 executor 스레드 고갈로 이벤트 루프가 멈추던 문제 해소 (RISK-05).

### Fixed

- **장 미개장 시 거래대금 상위 탭 빈 항목 표시** (`data/kiwoom_aftermarket_sync.py`, `dashboard/frontend/src/components/Top.tsx`): 09:00 개장 전 키움 ka10032 API가 종목명은 채워주지만 현재가·거래대금을 0으로 반환할 때, 0원/0%/0억인 의미 없는 행들이 표에 나타나던 문제 수정. `fetch_top_volume`에서 거래대금 0 행을 건너뛰도록 수정하고, `Top.tsx` 렌더링에도 `amount > 0` 필터를 추가해 이중으로 방어.

## [0.9.9.1] - 2026-05-26

### Added

- **역할 기반 인증** (`dashboard/backend/main.py`): `_BasicAuthMiddleware`를 역할 기반으로 확장. `ADMIN_USER`/`ADMIN_PASSWORD` → `role=admin` (쓰기 권한), `DASHBOARD_USER`/`DASHBOARD_PASSWORD` → `role=user` (읽기 전용). `ADMIN_USER` 미설정 시 `DASHBOARD_USER`도 admin 취급하여 하위 호환성 유지. `POST /api/scheduler/trigger`에 admin 가드 추가 — 일반 사용자가 잡 중복 트리거로 DB 부하를 유발하는 경로 차단.

### Fixed

- **Caddy basicauth 이중 인증 충돌 제거** (`C:\caddy\Caddyfile`): Caddy `basic_auth` 블록과 FastAPI `_BasicAuthMiddleware`가 동시에 활성화되어 `ADMIN_USER`(realAdmin 등) 계정이 Caddy 레이어에서 401 루프에 빠지던 문제 수정. Caddyfile에서 `basic_auth` 블록을 제거하고 인증을 FastAPI 단일 레이어로 통합.
- **스케줄러 트리거 403 피드백 누락** (`dashboard/frontend/src/components/Scheduler.tsx`): user 역할 계정에서 스케줄러 트리거 버튼을 누르면 서버가 403을 반환했으나 프론트엔드가 응답을 조용히 무시하여 아무 반응이 없었던 문제 수정. 이제 "관리자 전용 기능입니다. 관리자 계정으로 로그인해 주세요." 알림을 표시합니다.

### Changed

- **uvicorn 크래시 즉시 재시작** (`scripts/start_dashboard_service.bat`): uvicorn 종료 후 5초 대기 후 즉시 재시작하는 내부 restart 루프 추가. 기존 Task Scheduler 1분 대기 방식보다 서비스 복구가 빠름. 재시작 타임스탬프를 `dashboard.log`에 기록.
- **Task Scheduler 등록 시 HKCU Run 키 자동 정리** (`scripts/register_tasks.ps1`): `Register-DashboardTask` 실행 시 레지스트리 `HKCU\...\Run`의 `TradingDashboard` 항목을 자동 제거. Task Scheduler AtLogOn으로 단일 관리.
- **asyncpg 연결 풀 확장** (`dashboard/backend/database.py`): `max_size` 6 → 20, `command_timeout=30` 추가. 10명 동시 접속 시 `pool.acquire()` 큐 누적 방지. 스케줄러 풀(8) 합산 28개 — Supabase 60 연결 한도 이내.
- **스케줄러 스트림 폴링 최적화** (`dashboard/backend/main.py`): `/api/scheduler/stream` 폴링 인터벌 3s → 10s. 10명 기준 200쿼리/분 → 60쿼리/분 (70% 감소). `idx_sched_stream` 인덱스 추가로 SeqScan 방지.

## [0.9.9.0] - 2026-05-24

### Added

- **MarketSummaryBanner 컴포넌트** (`dashboard/frontend/src/components/MarketSummaryBanner.tsx`): 히트맵 상단에 KOSPI/KOSDAQ 지수 + 시장 심리 한마디를 표시하는 초보자 친화적 배너. 장마감/장없는날 상태 구분, 5분 자동 갱신, 모바일 팁 숨김(`market-banner-tip`).
- **`/api/market_index` 엔드포인트** (`dashboard/backend/main.py`): KRX OpenAPI + yfinance 혼합으로 KOSPI/KOSDAQ 지수 조회. `asyncio.Lock` 기반 5분 TTL 캐시, `_market_sentiment()` 순수 함수로 강세/상승/보합/하락/급락 5단계 판정.
- **KOSDAQ 지수 KRX 조회** (`data/krx_openapi.py`): `get_kosdaq_index()` / `get_kosdaq_index_ohlcv()` 신규, KOSPI `prev_close`(`BASPRC_IDX`) 필드 추가.
- **텔레그램 `/status` 시장 현황** (`telegram/telegram_bot.py`): KRX 클라이언트 직접 호출로 KOSPI/KOSDAQ 등락률을 대시보드 HTTP 의존 없이 표시.

### Changed

- **종목분석 상세 보기 우측 패널 방식으로 전환** (`dashboard/frontend/src/components/Report.tsx`, `StageHistoryPopup.tsx`): 모달 오버레이 팝업 방식에서 모의투자 탭과 동일한 Master–Detail 분할 패널 방식으로 변경. 종목 클릭 시 목록이 55%로 수축하고 우측에 45% 상세 패널 슬라이드인. 같은 종목 재클릭·날짜 범위 변경 시 패널 닫힘. `StageHistoryPopup`에 `mode='panel'` prop 추가로 오버레이 없이 부모 영역 채우기 지원.
- **종목분석 팝업 너비 확대** (`StageHistoryPopup.tsx`): 모달 모드 최대 너비 480px → 680px.
- **오늘 필터 종목 상세 연결** (`Report.tsx`): 추세 단계·강세 후보 발굴 두 섹션 모두 종목 클릭 이벤트와 `StageHistoryPopup` 연결. 이전에는 "오늘" 날짜 범위에서 클릭해도 팝업이 열리지 않던 문제 수정.
- **종목분석 모바일 세로 스택 대응** (`dashboard/frontend/src/index.css`): 768px 이하에서 좌우 분할 패널이 세로 스택으로 전환되는 `report-split-*` 미디어쿼리 추가. 모의투자 `paper-split-*` 패턴과 동일.
- **시작 스크립트 단일화** (`scripts/`): `restart_dashboard.bat`·`start_dashboard_hidden.vbs` 제거. `start_dashboard.ps1`이 수동 시작/재시작의 단일 진입점. Task Scheduler 자동 시작은 `start_dashboard_service.bat` 래퍼가 담당.
- **대시보드 탭·섹션 이름 직관화** (`dashboard/frontend/src/tabs.ts`, `Report.tsx`): `레포트` 탭 → `종목 분석`, `Stage 분류` 섹션 → `추세 단계`, `차트 스크리닝` 섹션 → `강세 후보 발굴`. 처음 접하는 사용자도 탭 이름만으로 기능을 파악할 수 있도록 한국어 직관명으로 변경. `InfoTip` 컴포넌트(ⓘ)로 호버 시 동작 설명 팝업 표시.
- **모의투자 성과분석 섹션 제거** (`PaperPortfolio.tsx`): 포지션 탭과 내용이 겹치던 성과분석(누적 P&L 커브·미실현 포지션 리더보드·모델 통계 테이블) 섹션 제거. CSV 다운로드 버튼은 포지션 섹션 헤더 우측으로 이전. `PaperAnalytics.tsx` 컴포넌트 및 관련 테스트 삭제. 번들 크기 350KB → 20KB(recharts 의존성 제거 효과).

### Fixed

- **종목분석 이력 필터 우측 패널 미작동** (`dashboard/frontend/src/components/HistoryStageView.tsx`, `HistoryScreenerView.tsx`): `-3일/-1주/-2주/-1달` 필터에서 종목 클릭 시 우측 패널이 열리지 않던 버그. 두 컴포넌트가 내부 `popup` state로 구식 모달 방식으로 동작하고 있었음. `selectedTicker`/`onSelect` prop으로 부모(`Report`) 패널 상태와 통합하여 모든 날짜 필터에서 동작 통일.
- **`time` 모듈 이름 충돌** (`dashboard/backend/main.py`): `from datetime import time`이 `import time` 모듈을 shadow하여 캐시 TTL 계산(`time.time()`)이 `TypeError`로 실패하던 버그. `import time as _time_module`로 해결.
- **TOP/Macro/Heatmap `time.strftime` 오류** (`dashboard/backend/main.py`): `_fetch_top_kiwoom`(L800)·`_run_macro_analysis`(L1393)에서 `time.strftime()`이 `datetime.time` 클래스의 언바운드 메서드로 해석되어 세 탭 모두 "API 오류" 표시. `_time_module.strftime()`으로 수정.

## [0.9.8.1] - 2026-05-24

### Security

- **대시보드 인증 우회 취약점 수정** (`dashboard/backend/main.py`): Nginx 리버스 프록시 뒤에서 모든 요청의 `client.host`가 `127.0.0.1`로 보여 localhost 면제 로직이 인증을 사실상 전면 무력화하던 취약점 제거. `DASHBOARD_USER`/`DASHBOARD_PASSWORD` 미설정 시 인증 비활성화(기존 동작 유지), 설정 시 반드시 `Authorization` 헤더 검증.

### Refactored

- **스케줄러 잡 패키지 분리** (`jobs/`): `run_scheduler.py` 1,710줄 → 892줄. Stage 분류·주봉 스크리너·KRX 갱신·워치리스트·모의투자 잡 5종을 `jobs/` 패키지(6개 모듈)로 추출. `run_scheduler.py`는 얇은 래퍼로 외부 import 호환성 유지. 596개 테스트 전량 통과.

## [0.9.8.0] - 2026-05-24

### Added
- **디자인 토큰 시스템** (`dashboard/frontend/src/tokens.ts`): 프론트엔드 전체 색상·간격·타이포그래피·그림자를 단일 SSOT(Single Source of Truth)로 통합. `bg`, `bd`, `tx`, `accent`, `stage`, `semantic`, `heat`, `chart`, `space`, `radius`, `type`, `shadow` 12개 네임스페이스 + 헬퍼 함수 4개(`pctTextColor`, `heatCellColor`, `stageColor`, `scoreColor`) 제공.
- **`tokens.bg.active`** (`tokens.ts`): 선택 칩·활성 행 배경색 `#1e3a5f` 공통 토큰 추가. Report, PaperPortfolio, Positions, DateRangeBar 등 6개 이상 컴포넌트에서 참조.

### Changed
- **디자인 토큰 마이그레이션** (15개 컴포넌트 파일): 모든 인라인 hex 색상 문자열을 `tokens.*` 참조로 교체. 변경 파일: `App.tsx`, `Heatmap.tsx`, `Top.tsx`, `Macro.tsx`, `PaperAnalytics.tsx`, `PaperPortfolio.tsx`, `Positions.tsx`, `SignalFeed.tsx`, `Scheduler.tsx`, `Report.tsx`, `DateRangeBar.tsx`, `HistoryStageView.tsx`, `HistoryScreenerView.tsx`, `StageHistoryPopup.tsx`, `TickerHistory.tsx`.
- **한국 색상 관례 명시적 분리**: 등락률 컬러(`pctTextColor`)는 한국 관례(빨강=상승, 파랑=하락)를 `tokens.semantic.up/down`으로 통일. PaperAnalytics·Positions·PaperPortfolio·TickerHistory의 손익 컬러(`pctColor`)는 서양 관례(초록=수익, 빨강=손실) 유지 — 의도적 분리, 혼용 없음.

### Fixed
- **Stage 1 색상 버그** (`Report.tsx`, `HistoryStageView.tsx`, `StageHistoryPopup.tsx`, `HistoryScreenerView.tsx`): Stage 1 색상이 `#60a5fa`(sky-400)로 잘못 하드코딩되어 있던 문제 수정. 올바른 값 `tokens.stage[1]` = `#3b82f6`(blue-500)으로 교체. Stage 2(`#a78bfa` 보라)·Stage 3(`#f59e0b` 주황)은 정상이었으므로 변경 없음.

## [0.9.7.0] - 2026-05-23

### Added
- **모의투자 성과분석** (`dashboard/backend/main.py`, `dashboard/frontend/src/components/PaperAnalytics.tsx`): 모델별 누적 P&L 커브 + 미실현 손익 연장선 차트. 4개 모델(Stage/KOSDAQ/Cross/Ichimoku) 동시 비교 가능.
- **`GET /api/paper/curve`** (`dashboard/backend/main.py`): 모델별 누적 시계열(window function), 집계 통계(n_trades/n_wins/win_rate/avg_win/avg_loss/total_realized), ticker_name_map, open 포지션 미실현 수익률을 단일 응답으로 반환.
- **`GET /api/paper/export`** (`dashboard/backend/main.py`): paper_positions 전체를 utf-8-sig BOM CSV로 다운로드. Excel 한글 호환.
- **`PaperAnalytics` 컴포넌트** (`dashboard/frontend/src/components/PaperAnalytics.tsx`): 모델 통계 테이블 + Recharts 커브 + 미실현 포지션 리더보드 + CSV 다운로드 버튼. `PaperPortfolio` 하단에 임베드.
- **`pivotSeries` 순수 함수** (`PaperAnalytics.tsx`): 모델별 날짜 시계열을 Recharts 호환 피벗 배열로 변환. 날짜 공백 구간은 null fill(connectNulls 대응).

### Tests
- **백엔드 단위 테스트** (`tests/test_paper_analytics.py`): pytest-asyncio 기반 5개 테스트. curve happy path(closed+open), closed 없음, open 없음(yfinance 미호출), export happy path(BOM+헤더+데이터), export 빈 테이블.
- **프론트엔드 단위 테스트** (`dashboard/frontend/src/components/PaperAnalytics.test.ts`): Vitest 기반 5개 테스트. pivotSeries 빈 입력, 단일 모델, 날짜 분리 null fill, 동일 날짜 복수 모델, 정렬 보장.

### Fixed
- **Supabase RLS 정책 누락** (`core/db.py`, `data/kiwoom_paper_trader.py`, `data/kiwoom_aftermarket_sync.py`): `ENABLE ROW LEVEL SECURITY`만 설정하고 `CREATE POLICY`를 생성하지 않아 Supabase Security Advisor `rls_enabled_no_policy` 경고 14건 발생. 모든 대상 테이블에 `CREATE POLICY backend_all FOR ALL USING (true) WITH CHECK (true)` 멱등 DO 블록 추가. 기존 DB 즉시 적용용 `sql/rls_policies.sql` 추가 — Supabase SQL 에디터 또는 pgAdmin에서 실행.

### Changed
- **스크립트 통합** (`scripts/`): `register_task.ps1` + `register_aftermarket_task.ps1` → `register_tasks.ps1` 단일 스크립트로 통합(`-Task all|crawler|aftermarket|dashboard` 파라미터 지원). `start_dashboard_hidden.vbs`·`start_dashboard_service.bat` 추가 — 창 없는 백그라운드 대시보드 실행. `run_aftermarket_sync.bat` 경로 수정(`kiwoom_aftermarket_sync.py`→`data/kiwoom_aftermarket_sync.py`). `start_crawler.bat` `.env` 파싱 수정. Docker 파일(`Dockerfile`·`docker-compose.yml`) 및 미사용 스크립트 3개 제거.

## [0.9.6.1] - 2026-05-23

### Changed
- **매크로 현재가 정확도 개선** (`dashboard/backend/main.py`): yfinance 조회 파라미터를 `period="2d" interval="1d"` → `period="1d" interval="1m"`으로 변경. 일봉 기준 전날 종가가 반환되던 문제 해결 — 이제 장중 최신 1분봉 마지막 값을 사용.

### Fixed
- **현재가 멀티 티커 컬럼 오류** (`dashboard/backend/main.py`): `close_df[t]` 키 부재 시 `KeyError` 발생하던 버그 수정. 티커가 반환 컬럼에 없으면 `continue`로 건너뛰도록 처리.

### Refactored
- **Macro 순수 유틸 분리** (`dashboard/frontend/src/components/Macro.utils.ts`): `Macro.tsx`에 인라인으로 있던 타입(`FactorSnap`, `StockResult`), 상수(`FACTOR_KEYS`, `FACTOR_LABELS`, `FACTOR_UNITS`, `FACTOR_CARD_INTERPRET` 등), 순수 함수(`getFactorState`, `getSensitivity`, `generateVerdict`, `generateBanner`)를 별도 모듈로 추출. 테스트 가능한 단위로 분리.
- **FactorCard 컴포넌트 분리** (`Macro.tsx`): 팩터 카드 렌더링 로직을 `FactorCard` 독립 컴포넌트로 분리. ScoreBar `width` 100→80px 조정.

### Tests
- **Macro 유틸 단위 테스트** (`dashboard/frontend/src/components/Macro.utils.test.ts`): Vitest 기반 174줄. `getFactorState`(경계값·극단값), `getSensitivity`(R² 구간), `generateVerdict`(시나리오별 판정문), `generateBanner`(배너 생성·null 조건) 4개 함수 커버.

## [0.9.6.0] - 2026-05-22

### Added
- **매크로 탭** (`analysis/macro_tracker.py`, `dashboard/frontend/src/components/Macro.tsx`): 대시보드에 매크로 탭 추가. OLS 팩터 모델로 6개 매크로 팩터(USD/KRW, 기준금리, 코스피 52주 고저비, VIX, 구리/금 비율, 10Y-2Y 스프레드) 추적. `--scenario` CLI 옵션으로 시나리오별 팩터 시뮬레이션 가능.

### Changed
- **프로젝트 패키지 구조 재편** (`core/`, `data/`, `analysis/`, `telegram/`, `reports/`): 루트에 흩어져 있던 Python 소스 파일 23개를 기능별 패키지로 분류. `core/`(db, ticker_cache, ohlcv_cache, article_fetcher), `data/`(market_data, krx_*, kiwoom_*), `analysis/`(signal_detector, chart_screener, stage_classifier, backtest_engine, macro_tracker, volume_pattern, screener_filters), `telegram/`(telegram_bot, telegram_notify, telegram_trade), `reports/`(summarizer, generate_html_report). 모든 import 경로 및 테스트 파일 업데이트 포함. 591개 테스트 통과.
- **scripts/ 폴더 정리**: 루트에 있던 배치파일(`restart_dashboard.bat`, `start_crawler.bat`, `run_aftermarket_sync.bat`, `duckdns_update.bat`)을 `scripts/`로 이동. `start_dashboard.bat` 제거(`restart_dashboard.bat`과 중복).

### Fixed
- **히트맵 세로 텍스트** (`Heatmap.tsx`): `orientLabel=false` 적용 — 좁은 셀에서 종목명이 90도 회전되어 표시되던 문제 해결.
- **히트맵 모바일 글씨 미표시** (`Heatmap.tsx`): `labelSkipSize={36}→{0}` + label 함수에서 width 기준 직접 스킵 처리. nivo 기본 `labelSkipSize`가 width·height 둘 다 체크해서 세로로 긴 좁은 셀(모바일에서 흔함)의 라벨이 전부 숨겨지던 문제 해결.
- **히트맵 라벨 overflow** (`Heatmap.tsx`): 셀 너비(÷11px) 기반 동적 글자 수 계산으로 작은 셀에서 텍스트가 인접 셀로 삐져나오는 문제 해결.

## [0.9.5.0] - 2026-05-21

### Changed
- **히트맵 재설계** (`Heatmap.tsx`, `main.py`): 오늘 시장에서 돈이 어디 가는지 파악할 수 있도록 데이터 소스 교체. Stage 분류 종목(10~50개) 대신 Kiwoom 당일 거래대금 상위 50종목을 항상 표시. 셀 크기=당일 실제 거래대금, 등락률=Kiwoom 일중 change_pct. Stage 분류된 종목은 컬러 테두리(S1 파랑·S2 보라·S3 주황), 미분류 종목은 회색 테두리로 표시. Stage 분류 잡 미실행 시에도 히트맵이 항상 채워짐.

## [0.9.4.0] - 2026-05-20

### Added
- **스테이지/스크리너 이력 트래킹** (`dashboard/`): Report 탭에 날짜 범위 선택 바 추가. 오늘/-3일/-1주/-2주/-1달 퀵 버튼으로 기간을 설정하면 해당 기간에 등장한 종목을 등장횟수 순으로 집계 표시. 종목 클릭 시 Stage 이력(날짜/스테이지/진입고가/피크아웃) + 스크리너 이력(주차/강화/갭점프) 팝업.
- **이력 API 3개** (`dashboard/backend/main.py`): `/api/history/stage`(기간별 Stage 집계, UNION ALL per-stage LIMIT 50), `/api/history/screener`(기간별 스크리너 집계, date→week_of 자동 변환), `/api/history/ticker/{ticker}`(종목별 Stage+스크리너 이력).
- **모의투자 모델 필터** (`PaperPortfolio.tsx`, `Positions.tsx`): 모델 카드(Stage/KOSDAQ/Cross/Ichimoku) 클릭으로 포지션 테이블 필터링.

## [0.9.3.0] - 2026-05-20

### Added
- **Fuzzy 티커 해석** (`ticker_cache.py`): `resolve_fuzzy(name, threshold=0.82)` 메서드 추가. `difflib.SequenceMatcher`로 LLM이 추출한 종목명을 KRX DB 명칭에 근사 매칭. "셀트리온헬스케어" vs "셀트리온헬스케어(주)" 자동 연결. 임계값 0.82: "현대차" vs "현대차증권" (ratio ≈ 0.75) false positive 차단. `_parse_signal_json()`에서 정확 매칭 → fuzzy 매칭 → `_resolution_misses` 카운터 순서로 해석.
- **HIGH CONFIDENCE 통합** (`run_scheduler.py`, `telegram_notify.py`): 해당 주 Ichimoku 스크리너 통과 종목과 뉴스 신호가 교차할 때 `signal.confidence = "HIGH"`로 상향. 텔레그램 알림에 🔥 *HIGH CONFIDENCE - 스크리너 교차 종목* 배지 표시. `TradeSignal`에 `confidence: str = "NORMAL"` 필드 추가(dataclass 마지막 필드).
- **`/watchlist` 봇 명령어** (`telegram_bot.py`, `run_scheduler.py`): 거래대금 워치리스트 온디맨드 조회. `_build_watchlist_entries(pool)` 헬퍼를 스케줄러 잡과 봇 핸들러가 공유. `/help` 및 `_register_commands()` 등록 완료.
- **Vol ratio 전일 대비 델타** (`run_scheduler.py`, `telegram_notify.py`): 워치리스트 일보에 `+5%▲` / `-8%▼` 형식의 전일 대비 거래대금 비율 변화 표시. `watchlist_vol_log` lookback=2 로드 후 `delta = today_ratio − yesterday_ratio` 계산.
- **D+10 마지막 추적일 배지** (`run_scheduler.py`, `telegram_notify.py`): `days_since >= 10`인 종목에 `[마지막 추적일]` 표시. 다음 일보에서 자동 퇴장.
- **Enhanced Ichimoku 실제 적용** (`chart_screener.py`): `calc_ichimoku()`에 `tenkan_sen`(전환선, 9주)·`kijun_sen`(기준선, 26주) 컬럼 추가. `screen_ticker()`에서 H(전환선 > 기준선)·I(둘 다 우상향) 조건 판정 → `is_enhanced` 실제 설정. 기존에는 항상 `False`였으나 이제 실제 Enhanced 종목에 배지 부여.
- **조건 G NaN 보정 토글** (`chart_screener.py`): `SCREENER_G_NAN_STRICT=1` 환경변수로 120주선 데이터 부족 종목의 통과 여부를 제어. 기본(미설정): NaN → 통과. Strict 모드: NaN → 실패. DB `null_pct > 20%` 확인 후 활성화 권장.
- **일봉 분류기 티커 캡** (`run_scheduler.py`): `DAILY_CLASSIFIER_TICKERS=150` 환경변수(기본값)로 최대 처리 종목 수 제한. Ichimoku 통과 종목은 캡 초과 여부와 관계없이 우선 포함.
- **뉴스 게이팅 이중 레이어** (`run_scheduler.py`, `db.py`): 기존 `_screener_tickers`(주봉 Ichimoku)에 더해 `_active_stage_tickers`(최근 7일 이내 Stage 1/2/3 활성 종목) 레이어 추가. `get_active_stage_tickers(pool, days=7)` DB 함수 추가. 스케줄러 시작 시 초기 로드, `_daily_stage_job()` 완료 후 자동 갱신. 스크리너 교차 → HIGH CONFIDENCE, Stage 7일 활성 → NORMAL 전달, 둘 다 해당 없음 → 억제.
- **거래대금 기반 스테이지 분류** (`stage_classifier.py`, `backtest_engine.py`): Stage 1/2/3 거래량 조건을 거래대금(`Volume × Close`, 원화)으로 전면 교체. 시가총액 소형주 과잉 선정 방지. `_calc_txamt(price_df)` 헬퍼 추가. Stage 1 조건 2: `txamt_today >= 2.0 × avg_txamt20`. Stage 2 조건 3: `txamt_ratio = txamt_today / s1_txamt` 범위 `[0.25, 0.65]`. Stage 3 조건 4: `txamt_today >= 1.5 × avg_txamt30`.
- **`s1_txamt` 컬럼** (`db.py`): `stage_classifications` 및 `watchlist_vol_log` 테이블에 `BIGINT` 컬럼 추가. 기존 DB 행은 `s1_volume × s1_high` 추정값으로 자동 폴백. `get_stage1_history()`, `save_stage_classifications()`, `get_stage1_watchlist()`, `upsert_watchlist_vol_log()` 모두 갱신.
- **신규 문서 6종** (`docs/`): Diataxis 프레임워크 기반. `howto-screener.md` (스크리너 설정·Calibration), `howto-stage-classifier.md` (분류기 설정), `howto-watchlist.md` (워치리스트 온디맨드 조회), `explanation-signal-pipeline.md` (신호 파이프라인·게이팅 설계 이유), `reference-env-vars.md` (환경변수 전체 목록), `reference-telegram-commands.md` (명령어 전체 목록).

### Changed
- **워치리스트 `_fetch_vol()` → `_fetch_txamt()`** (`run_scheduler.py`): 일별 거래량 대신 `Volume × Close` 거래대금으로 vol_ratio 계산. `s1_txamt_map` 추가, 기존 DB 행 폴백(`s1_volume × s1_high`).
- **Stage 2 txamt 임계값** (`stage_classifier.py`, `backtest_engine.py`): 거래량 비율 `[0.30, 0.60]` → 거래대금 비율 `[0.25, 0.65]`. Stage 2 가격은 Stage 1 고점 대비 -5%~-20% 수준이므로 하한 0.05p 하향 조정.
- **게이팅 버그 수정** (`run_scheduler.py`): `signal.ticker_symbols[:3]`(list 슬라이스 오류) → `list(signal.ticker_symbols.keys())[:3]`. `set(signal.ticker_symbols) & _screener_tickers`(dict key 교차) → `set(signal.ticker_symbols.values()) & _screener_tickers`(yfinance 심볼 기준 교차).
- **`send_watchlist_brief()` `target_chat_id` 파라미터** (`telegram_notify.py`): `/watchlist` 봇 명령어에서 개인 DM으로 전송하기 위해 추가. 채널 브로드캐스트를 막고 요청자의 chat_id로만 전달.

### Tests
- `test_resolve_fuzzy.py` (13개): `resolve_fuzzy` 정확 임계값·false positive 방지·대소문자·공백·symbol 해석 통합.
- `test_high_confidence.py` (7개): HIGH CONFIDENCE 배지 표시·NORMAL 미표시·게이팅 로직.
- `test_watchlist_features.py` (17개): vol_ratio_delta 포맷, retiring 배지, `target_chat_id`, `/watchlist` 핸들러.
- `test_p3_remaining.py` (24개): 조건 G NaN 토글, Enhanced Ichimoku, 티커 캡, 이중 게이팅.

## [0.9.2.0] - 2026-05-19

### Added
- **모바일 하단 탭 내비게이션** (`MobileNav.tsx`, `index.css`): 768px 이하에서 iOS 앱처럼 전체화면 탭 전환. 히트맵·레포트·Top·모의투자·시그널 5탭 — 각 탭 전환 시 해당 컴포넌트만 마운트되어 API 호출 최소화.
- **모의투자 전용 탭** (`PaperPortfolio.tsx`): 모의투자 포지션(22건 이상 추적 중)과 스케줄러 컨트롤을 하나의 탭으로 통합 — PC와 모바일 양쪽에서 접근 가능.
- **대시보드 외부 접근** (`main.py`): DuckDNS + 포트포워딩으로 외부 접속 지원. HTTP Basic Auth 미들웨어 추가 (localhost는 인증 면제).

### Changed
- **탭 구조 통일** (`tabs.ts`): `TAB_CONFIG` 단일 배열로 PC·모바일 탭 이름·순서·기본값 동기화 — 불일치 구조적 차단. 기본 탭 `heatmap`으로 통일.
- **iOS 스크롤 수정** (`index.css`): 조상 요소 `overflow: hidden`이 iOS Safari 터치 스크롤을 차단하던 문제 해결. `#root`·`.app-root` 모바일에서 `overflow: visible`로 변경.
- **키움 모의투자 주문** (`kiwoom_paper_trader.py`): `kt10000`/`kt10001` 요청 body에 `acnt_no` 필드 추가 — 재신청 계좌에서 매수 오류(`RC4091`) 해소.
- **모의투자 탭 통합** (`Report.tsx`, `PaperPortfolio.tsx`): 레포트 탭의 모의투자 섹션(모델별 요약·오픈 포지션·청산 이력)을 모의투자 전용 탭으로 이전 — 모든 모의투자 정보가 한 탭에 집중. 레포트 탭은 Stage 분류·차트 스크리닝만 유지.
- **차트 스크리닝 칩 필터** (`Report.tsx`): 통과·강화·갭점프 칩을 클릭 가능한 필터로 전환 — 칩 클릭 시 해당 유형 종목만 테이블에 표시.
- **모바일 히트맵 블랙아웃 수정** (`Heatmap.tsx`, `index.css`): 모바일에서 `height: auto !important` 오버라이드가 `ResponsiveTreeMap` 컨테이너 높이를 0으로 만들어 블랙아웃이 발생하던 문제 해결. `.heatmap-root` 예외 규칙으로 `calc(100svh − 헤더 − 탭바)` 명시적 높이 부여.

## [0.9.1.0] - 2026-05-18

### Added
- **대시보드 Top 탭** (`dashboard/`): 왼쪽 패널에 "Top" 탭 추가 — 당일 거래대금 상위 20개 종목을 실시간 표시.
  - Kiwoom REST API ka10032(`fetch_top_volume()`)로 KOSPI+KOSDAQ 전체 조회 (문서 p.102-103 검증)
  - 5분 캐시(asyncio.Lock 동시 호출 방지) + 수동 새로고침(↻) 버튼
  - 401 수신 시 토큰 무효화 후 1회 재시도 — 서버측 토큰 조기 폐기 대응
  - 거래대금 자동 단위 변환 (조/억/원), 등락률 색상 코딩, tabular-nums
- **대시보드 레포트 탭** (`dashboard/`): Stage 분류·차트 스크리너·모의투자 포지션 3개 패널.
  - 각 패널 섹션 접기/펼치기, 갱신 시간 표시
- **대시보드 스케줄러 SSE 스트림** (`/api/scheduler/stream`): 스케줄러 상태 실시간 표시 (1초 폴링).
- **히트맵 등락률 실시간 표시** (`Heatmap.tsx`): yfinance 2일 종가 비교로 당일 등락률 계산, 5분 자동갱신.
  - nivo TreeMap 블랙아웃 버그 수정 (`leavesOnly={true}`, null 가드)
- **종목명 한글화** (`ticker_names` 캐시 테이블): stage 분류 종목 전체에 대해 pykrx 한글명 자동 upsert.
  - 서버 기동 시 백그라운드 시드, 7일 TTL, COALESCE 우선순위 체계

### Changed
- **디자인 개선** (`dashboard/frontend/`):
  - 폰트: `Segoe UI, system-ui` → Pretendard Variable (한국어 최적화)
  - 모바일 레이아웃: 768px 미만에서 좌우 패널 → 상하 스택으로 전환
  - 터치 타겟: 탭 버튼 35px → 44px, ↻ 버튼 23px → 36px
  - focus-visible 링 복원 (#3b82f6), color-scheme: dark
  - 헤더 이모지 → SVG 바 차트 아이콘
  - 빈 히트맵 상태: 아이콘 + 주 메시지 + 힌트 문구
- **`/api/top` 코드 품질** (`main.py`): 엔드포인트 주석 정정(5→9개), 중복 상수 제거, 오류 응답 sanitize

### Removed
- **뉴스 교차분석 기능** (`cross_analyze`, `backtest.py`, 관련 DB 테이블): 별도 backtest_engine으로 대체됨.
  - `cross_analysis_results`, `cross_analysis_prices`, `price_outcomes` 테이블 스키마 제거
  - `market_data.py`에서 `PriceContext`, `CrossAnalysis`, `cross_analyze`, `_fetch_yfinance` 제거

## [0.8.0.0] - 2026-05-12

### Added
- **Portfolio Tracker** (`telegram_trade.py`): 실제 거래 기록 및 P&L 추적 시스템.
  - `/buy <ticker> <price> <qty> [YYYYMMDD]` — 진입 기록 (stage_classifications 자동 조회)
  - `/sell <ticker> <price>` — FIFO 청산 (entry_date 기준 최선입선출)
  - `/port` — 미청산 포지션 현황 + yfinance 미실현 P&L (asyncio.to_thread)
  - `/pnl [week|month|all]` — 실현 P&L 요약 + Stage별 승률
- **`trade_log` 테이블** (`db.py`): PostgreSQL GENERATED ALWAYS AS STORED 컬럼 활용.
  - `entry_delay_days`: signal_date → entry_date 거래일 수 자동 계산
  - `pnl`: (exit_price - entry_price) × qty 자동 계산
  - `pnl_pct`: 수익률(%) 소수점 3자리 자동 계산
- **DB 헬퍼 5종** (`db.py`): `save_trade`, `close_position`, `get_open_positions`, `get_pnl_summary`, DDL init
- **Kiwoom REST API 시간외 동기화** (`kiwoom_aftermarket_sync.py`): KRX BLD 대신 Kiwoom ka10098 bulk API 활용.
  - HTTP 헤더 페이지네이션 (`cont-yn / next-key`)
  - KOSPI(001) + KOSDAQ(101) 별도 호출 → `.KS/.KQ` 심볼 결정
  - `acc_trde_prica` 단위: 백만원(×1,000,000)
  - CLI: `--today`, `--incremental`, `--probe`, `--mock`, `--force`
- **Windows Task Scheduler 등록** (`scripts/register_aftermarket_task.ps1`): 평일 16:05 `KiwoomAftermarketSync` 작업 자동 실행

### Changed
- **`telegram_bot.py`**: `/buy`, `/sell`, `/port`, `/pnl` 명령 dispatch 추가 (lazy import from telegram_trade)

### Tests
- `tests/test_trade_journal.py` 18개: /buy·/sell·/port·/pnl 핸들러 단위 테스트 + DB 레이어 검증
- `tests/test_trade_integration.py` 3개: PostgreSQL GENERATED COLUMN 통합 테스트 (DB 없으면 자동 skip)

## [0.7.3.0] - 2026-05-08

### Added
- **Layer 6: 거래대금 워치리스트 일보** (`run_scheduler.py`): Stage 1 진입 종목을 최대 14캘린더일 추적하는 일별 Telegram 요약 메시지. 평일 17:00 KST(08:00 UTC) APScheduler CronJob. `python run_scheduler.py --once watchlist`로 즉시 실행 가능.
- **`get_stage1_watchlist()`** (`db.py`): 최근 N일 이내 Stage 1로 분류된 종목을 종목당 최신 1건만 반환하는 DB 헬퍼.
- **`send_watchlist_brief()`** (`telegram_notify.py`): 거래대금 비율(✅⚠️❌), 외국인/기관 스트릭(🔵🔴❓), Ichimoku 상태(☁️), 확신도 순 정렬 포함. plain text 전송(MarkdownV2 이스케이프 없음).
- **Stage 2 전환 알림**: 워치리스트 종목이 Stage 1→2로 전환될 때 별도 Telegram 알림("🟢 Stage 2 전환 확인") 발송.
- **랠리 소멸 경고**: vol_ratio < 0.6 3거래일 연속 시 별도 Telegram 경고("❌ 랠리 소멸 경고") 발송.
- **`watchlist_vol_log` 테이블** (`db.py`): 일별 vol_ratio 이력 저장. 랠리 소멸 3거래일 판정 및 향후 vol delta 추적 기반.
- **`upsert_watchlist_vol_log()` / `get_watchlist_vol_log()`** (`db.py`): vol_ratio 이력 upsert·조회 헬퍼.
- **`--once watchlist` CLI** (`run_scheduler.py`): 스케줄러 없이 워치리스트 일보 1회 즉시 실행 후 종료.

### Changed
- **N+1 → 5개 벌크 쿼리** (`run_scheduler.py`): 기존 종목당 3 DB roundtrip → 1 connection acquire 내 5개 벌크 쿼리로 전환.
- **yfinance 병렬 fetch** (`run_scheduler.py`): `ThreadPoolExecutor(max_workers=4)`로 종목별 거래량 병렬 수집.
- **확신도 순 정렬**: vol_ratio × 스트릭 composite 점수 기준 내림차순 정렬 후 Telegram 전송.
- **스트릭 None 표시 개선** (`telegram_notify.py`): daily_flow 데이터 없는 종목을 🔴(+0일) 대신 ❓ N/A로 표시.
- **`_post_message()` `parse_mode` 파라미터화** (`telegram_notify.py`): 기본값 `"MarkdownV2"` 유지, `parse_mode=None` 시 plain text 전송.
- **`sys.stdout/stderr.reconfigure(encoding="utf-8")`** (`run_scheduler.py`): Windows cp949 환경 한글 로그 출력 오류 해결.

### Tests
- `test_watchlist_brief.py` 신규 22개: `send_watchlist_brief` 포맷(vol_ratio 전 분기, 경계값, 스트릭 None, Ichimoku 상태), `get_stage1_watchlist` DB 헬퍼(정상·빈 결과·DB 오류).

### Maintenance
- **프로젝트 구조 개편**: `tests/` (pytest 27개 파일 + `conftest.py`), `docs/` (`ARCHITECTURE.md`, `USER_MANUAL.md`, `TODOS.md`), `scripts/` (배포·운영 보조) 폴더로 분리. 루트는 실행 가능한 Python 모듈만 유지. `pytest.ini` 추가 (`testpaths = tests`).
- **미사용 파일 제거**: `chart_backtest.py` · `generate_backtest_html.py` (로컬 파일 전용, Telegram 출력 없음), `run_backtest.py` (`/backtest2` 명령으로 대체), `compare_tx_amt.py` (`krx_flow_sync.py` 도입 후 역할 없음), `scripts/check_feeds.py` (개발 초기 일회성), `scripts/generate_report.py` (`/scan` 명령으로 대체). `run_scheduler.py`에서 `chart_backtest` lazy-import 블록 35줄 제거.

## [0.7.2.0] - 2026-05-08

### Added
- **KRX 수급 세션 자동 갱신** (`krx_flow_sync.py`): 연속 빈 응답 5건 감지 → Samsung 프로브로 세션 만료 확인 → `.env`의 `KRX_SESSION` 갱신 감지(30초 폴링) → 프로세스 재시작 없이 자동 재개. `_handle_possible_expiry()` 추가.
- **ISIN 자동 계산** (`krx_flow_sync.py`): `_isin_from_krx_code()` — ISO 6166 Luhn 체크디짓으로 6자리 KRX 코드에서 12자리 ISIN 자동 생성 (KOSPI `KR7`, KOSDAQ `KR8`).
- **브라우저 세션 쿠키 우회** (`krx_flow_sync.py`): `KRX_SESSION`(JSESSIONID), `KRX_VISITOR`(`__smVisitorID`) 환경변수 지원. `inject_session()`이 `domain=.krx.co.kr` + `mdc.client_session=true` 자동 주입. data.krx.co.kr 브라우저 전용 로그인 정책 우회.

### Changed
- **`fetch_raw` ISIN 필수화** (`krx_flow_sync.py`): MDCSTAT02302 엔드포인트는 `isuSrtCd`(6자리) 단독 시 `output:[]` 반환. `isuCd`(ISIN) 병행 전송 필수.
- **`fetch_records` 응답 포맷 전환** (`krx_flow_sync.py`): 투자자유형별 행 구조(`INVST_TP_NM`/`NETBID_TRDVOL`) → 날짜별 1행 구조(`TRDVAL1`=외국인합계, `TRDVAL2`=기관합계). 날짜 키 `TRD_DD` (`YYYY/MM/DD`).
- **날짜 범위 청킹** (`krx_flow_sync.py`): MDCSTAT02302는 장기 범위(약 4개월 초과) 시 HTTP 400 반환. `_CHUNK_DAYS=90` 단위 분할 요청 자동 적용.
- **Windows UTF-8 출력** (`krx_flow_sync.py`, `run_backtest.py`): `sys.stdout/stderr.reconfigure(encoding="utf-8")` 추가. cp949 콘솔 환경 한글·특수문자 인코딩 오류 해결.
- **Karpathy 리팩터** (`krx_flow_sync.py`): `_filter_tickers()`, `_already_loaded()` 헬퍼 추출(중복 8줄×2 제거), `from collections import defaultdict`·`from dataclasses import dataclass` 모듈 수준 이동, 인라인 `_to_int` → `_parse_int` 교체.

### Fixed
- **MDD 부호 오류** (`backtest_engine.py`): `_compute_mdd()`가 양수 낙폭 비율을 반환하여 리포트에 `+65.42%`로 표시. `-max_dd` 반환으로 수정 (`-65.42%`).
- **`asyncpg` DATE 타입 오류** (`krx_flow_sync.py`): `_save_batch()`에서 `rec.trade_date.isoformat()`(str) 전달 시 `DataError: str object has no attribute toordinal`. `rec.trade_date`(date 객체) 직접 전달로 수정. bare `except`에 의해 DEBUG 레벨로 묵살되어 0건 저장 오류로 나타남.

### Technical
- KOSPI 788종목 2025-01-01~2026-05-03 수급 데이터 248,999건 적재 완료.
- Cross 모드 백테스트(KOSPI 200종목): 146 신호, 승률28d 53.5%, 승률91d 65.8%, 평균91d +28.21%, MDD -65.42%.

### Tests
- `test_backtest_engine.py` MDD 테스트 5개 부호 수정 (`>= 0` → `<= 0`, approx 값 음수 전환).

## [0.7.1.0] - 2026-05-05

### Added
- **KRX OpenAPI 클라이언트** (`krx_openapi.py`): `data-dbg.krx.co.kr` 공식 REST API 경유로 KOSPI/KOSDAQ 종목 마스터, 일별 OHLCV, KOSPI 지수 시세 수집. Bearer 토큰 인증(`KRX_OPENAPI_KEY`).
- **OHLCV DB 캐시 레이어** (`ohlcv_cache.py`): yfinance 반복 다운로드를 줄이기 위한 psycopg2 캐시. `batch_fetch_cached()`로 캐시 히트 시 DB 직접 로드, 미스 시 yfinance 병렬 수집 후 저장. `load_flow_data()`로 수급 데이터 사전 로드.
- **수급 데이터 파이프라인** (`krx_flow_sync.py`): data.krx.co.kr에서 외국인·기관 순매수 이력을 `daily_flow` 테이블에 적재. krx-direct / pykrx / CSV 백엔드 지원. KRX 계정(KRX_ID/KRX_PW) 필요.
- **백테스트 조건 5 수급 연결** (`backtest_engine.py`): `BacktestConfig.dsn` 설정 시 `daily_flow`에서 수급 데이터 사전 로드 후 `_replay_stage()` 조건 5(외국인·기관 순매수 > 0)에 적용. DSN 없으면 수급 조건 자동 생략(하위 호환).
- **`run_backtest.py` DSN 플래그**: `--dsn` CLI 옵션 추가. 미설정 시 `DATABASE_URL` 환경변수 자동 로드.
- **샤프비율 7d·91d 추가** (`backtest_engine.py`): 기존 28d 연환산 샤프비율에 더해 7일·91일 보유 기준 샤프비율(`sharpe_7d`, `sharpe_91d`)도 함께 산출.
- **`_replay_ichimoku` 단위 테스트** (`test_backtest_engine.py`): 데이터 부족·횡보·기간 외·구름 돌파·연속 신호 방지 5개 케이스 추가. 총 65개.

### Changed
- **`krx_sync.py` KRX OpenAPI 전환**: 기존 data.krx.co.kr HTTP 스크래핑을 KRX 공식 REST API로 대체. 스크래핑 없이 `ISU_CD`, `ISU_NM` 등 정식 필드 사용.
- **`chart_screener.py` 종목 조회 1순위**: `KRX_OPENAPI_KEY` 설정 시 KRX OpenAPI가 FinanceDataReader보다 먼저 사용됨.
- **`MODE_KOR` 상수 분리** (`backtest_engine.py`): 모드별 한국어 표시 문자열을 모듈 수준 상수로 추출. 리포트 메서드와 텔레그램 봇에서 재사용.

### Fixed
- **`.env.example` 키 이름**: `KRX_API_KEY` → `KRX_OPENAPI_KEY` (코드와 불일치 수정).
- **`backtest_engine.py:723` 빈 dict 판정**: `if flow_lookup:` → `if flow_lookup is not None:` (수급 데이터 0건일 때 note 문구 오표시 수정).

### Technical
- `backtest_engine.py`, `run_backtest.py`, `test_backtest_engine.py` 코드베이스 실제 반영 (v0.7.0.0은 설계 문서 선반영 버전).
- `/backtest2` 텔레그램 명령어 구현 완료 (`telegram_bot.py`).

## [0.7.0.0] - 2026-04-27

### Added
- **통합 백테스트 엔진** (`backtest_engine.py`): 이치모쿠(주봉) / 3단계 Stage 1(일봉) / 교차(두 신호 동일 ISO 주) 3개 모드. 기간·시장·티커 수·거래비용 전부 파라미터로 지정. 출력 지표: 승률(7d/28d/91d), 평균·중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD(equity curve). 티커당 1개 OHLCV fetch → 이치모쿠는 주봉으로 리샘플, Stage는 일봉 직접 사용.
- **백테스트 CLI** (`run_backtest.py`): `python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01 [--market KOSPI|KOSDAQ|ALL] [--max N] [--tx-cost F] [--rf F]` 로 커맨드라인 실행. 결과는 텍스트 표로 출력.
- **`/backtest2` 텔레그램 명령어** (`telegram_bot.py`): `/backtest2 ichimoku 2025-01-01 2026-01-01`, `stage`, `cross` 모드 지원. 백테스트 완료 시 결과 자동 전송. 중복 실행 방지 Lock 내장. 10년 초과 기간 및 음수 거래비용 입력 차단.
- **KRX 거래비용 반영**: 기본값 0.210% 왕복 (매수 수수료 0.014% + 매도 수수료 0.014% + 증권거래세 0.180% + 농어촌특별세 0.002%). `--tx-cost` 플래그로 사용자 정의 가능.
- **Cross 신호 중복 제거** (`backtest_engine.py`): 동일 종목·동일 주에 Stage 1이 복수 발동해도 가장 이른 신호 1건만 유지, 지표 과대계상 방지.

### Technical
- 데이터 수집: 백테스트 시작 760일 전부터 yfinance 일봉 병렬 수집(ThreadPoolExecutor, 기본 8워커). MA120w(주봉 120주, min_periods=100) 충분한 룩백 확보.
- Ichimoku 재현: W-FRI 주봉 리샘플 → `calc_ichimoku(visual=False)` → 7조건 walk-forward. 미래 데이터 참조 없음.
- Stage 1 재현: 일봉 4/5 조건(상승률·거래량·MA20/60·52주 고점 괴리). 수급 조건(외국인·기관)은 과거 데이터 미제공으로 생략, 결과 리포트에 경고 표시.
- MDD는 신호 날짜 순 누적 equity curve 기준 (equal-weight, 순차 포지션 가정).
- 샤프비율: 보유 기간 28일 기준 연환산 (periods_per_year = 252/28 ≈ 9, rf_annual = 3% 기본).

### Tests
- `test_backtest_engine.py`: 60개 — 유틸(week_label·price_lookup·nearest_price), Sharpe(7개), MDD(5개), GroupMetrics(5개), fill_returns(5개), cross_filter(6개 + 중복 제거 1개), BacktestConfig 검증(7개), _replay_stage(5개 — 조건별·시장별·기간 외), 리포트 생성(5개).

## [0.6.0.0] - 2026-04-26

### Added
- **Daily 3-stage stock classifier** (`stage_classifier.py`): `classify_stage()` evaluates all KOSPI + KOSDAQ tickers (~2,770) every weekday at 16:30 KST. Returns Stage 1 (랠리 초입 — daily surge + volume spike + net-buy), Stage 2 (중간 조정·재매집 — pullback within 14 days of a Stage 1 signal), or Stage 3 (과열 재가속 — breakout above consolidation high, RSI ≥ 70, both foreign and institutional net-buy). Priority: Stage 3 > 2 > 1.
- **Peakout signal** (`stage_classifier.py`): `check_peakout()` flags Stage 3 stocks where foreign + institutional streaks both hit ≤ −2, or where an upper-wick candle (high − close > 50% of range) coincides with a volume spike.
- **`stage_classifications` DB table** (`db.py`): stores ticker, date, stage (1/2/3), s1_entry_date, s1_high, s1_volume, peakout_flag. Primary key: (ticker, classified_date). Two indexes for date-desc and ticker+date-desc queries.
- **DB helpers** (`db.py`): `get_stage1_history()` batch-loads Stage 1 records from `stage_classifications` (not `chart_signals`) for Stage 2 lookback; `save_stage_classifications()` upserts; `get_prev_streak()` returns the previous day's foreign/institutional streak for daily increment computation.
- **`_daily_stage_job()`** (`run_scheduler.py`): new APScheduler job at UTC 07:30 (= 16:30 KST). Steps: fetch daily flow + compute streaks via `get_prev_streak()`, load 60-day OHLCV via yfinance, batch-load flow_df and Stage 1 history from DB (asyncpg thread-safety — no DB calls inside ThreadPoolExecutor), classify all tickers in parallel, upsert results, send comparison Telegram message.
- **Ichimoku + Stage comparison Telegram message** (`telegram_notify.py`): `send_screener_comparison()` sends an overlap-first summary after each daily classification — tickers passing both Ichimoku (weekly) and Stage (daily) appear first as highest-confidence candidates, followed by Ichimoku-only and Stage-only sections. Peakout warnings appended when triggered.
- **USER_MANUAL.md Section 8** — 일봉 3단계 분류기: documents Stage 1/2/3 conditions as user-readable tables, comparison Telegram message format, peakout signal, and SCREENER_WORKERS latency note.

### Technical
- All DB queries batch-loaded before entering `ThreadPoolExecutor` — no asyncpg calls from worker threads (learnings: asyncpg-threadpool-no-db).
- Stage 1 history source: `stage_classifications WHERE stage=1` (full 2770-ticker coverage, independent of Ichimoku gate).
- `s1_volume` column added to `stage_classifications` for Stage 2 volume contraction check.

### Fixed (pre-landing review)
- **`daily_stage_classifier` weekend cron** (`run_scheduler.py`): job was firing every day including Saturday and Sunday, calling Naver's API 2770 times for all-null results and sending an empty Telegram message. Now correctly restricted to `day_of_week="mon-fri"`.
- **`get_stage1_history` silent failure** (`db.py`): on DB timeout or pool exhaustion, the function was swallowing the exception and returning `{}`, causing Stage 2 results to silently disappear for the entire day. Now propagates the exception so `_daily_stage_job` can abort and avoid saving corrupted data.
- **`check_peakout` volume average off-by-one** (`stage_classifier.py`): `iloc[-20:-1]` gives 19 days, not 20. Fixed to `iloc[-21:-1]` to match `_check_stage1`'s 20-day average.
- **`_check_stage3` breakout guard** (`stage_classifier.py`): breakout condition was silently skipped for stocks with fewer than 11 High data points (newly listed stocks). Added explicit `if len(highs) < 11: return False` guard.

### Tests
- `test_stage_classifier.py`: 29 tests covering Stage 1 (12 — including zero-avg-vol and zero-52w-high guards), Stage 2 (7), Stage 3 (3), priority ordering (1), `check_peakout` (6). All 384 tests pass (29 new + 355 existing).
- Priority ordering test redesigned — previous fixture never satisfied Stage 1 or Stage 3 conditions (0.81% daily change < 5% threshold), making `assert result in (None,1,2,3)` a tautology. New fixture triggers both Stage 1 and Stage 3 simultaneously and asserts `result == 3`.

## [0.5.0.0] - 2026-04-25

### Added
- **Screener-first architecture** (`run_scheduler.py`): news signals are now gated by the weekly screener. If a ticker hasn't passed Stage 1 screening this week, its news signals are suppressed. `_screener_tickers: set[str]` is a module-level cache warmed from DB at startup and refreshed after every Sunday screener job.
- **Stage 2 filter presets** (`screener_filters.py`): four named filters — `저평가`, `성장`, `배당`, `가격건전성` — that apply quantitative conditions (PER, PBR, EPS, dividend yield, volume spike) to `ScreenResult` lists. Used by the HTML report and future Telegram segmentation.
- **Backtest engine** (`chart_backtest.py`): `BacktestSignal` dataclass tracks 1w/4w/13w returns and KOSPI excess returns per screener signal. `incremental_update()` fills pending return fields using yfinance weekly OHLCV (no look-ahead via `_check_at_row`). `build_summary()` produces monthly + annual metrics (win rate, avg return, median, excess return). Runs automatically after each Sunday screener job.
- **Backtest HTML report** (`generate_backtest_html.py`): renders the backtest summary as a dark-mode HTML file written to `BACKTEST_DIR/chart_backtest_latest.html`. Shows annual and monthly breakdowns for 전체/정배열/일반 groups with sparkline trend.
- **Foreign/institutional flow columns** in HTML screener report (`generate_html_report.py`): `_flow_badge()` renders green/red badges for `foreign_net_buy` and `inst_net_buy` fields. Data shows `—` until Sprint 2 wires the Naver flow API.
- **`daily_flow` DB table** (`db.py`): stores daily foreign/institutional net-buy data per ticker (`ticker`, `trade_date`, `foreign_net`, `inst_net`, `foreign_streak`, `inst_streak`). `save_daily_flow()` upserts with ON CONFLICT. Sprint 2 will wire the scheduler.
- **`high_w` / `volume_w` in `chart_signals`** (`db.py`, `chart_screener.py`): weekly high and volume captured from yfinance OHLCV and persisted alongside each screener result. Required for Stage 2 spike-volume and S1-high conditions.
- **`fetch_daily_flow()`** (`market_data.py`): queries Naver Finance mobile `investorTrendDays` endpoint for foreign/institutional net-buy data. Synchronous (for `run_in_executor`). Handles null values from the API gracefully.
- **`target_chat_id` param** in `send_weekly_screener()` (`telegram_notify.py`): when set, sends only to that chat ID and skips channel broadcast. `/screener` bot command now passes the invoker's `chat_id` — prevents channel broadcasts triggered by individual users.
- **`SCREENER_WORKERS=8`** documented in `.env.example` for the daily 16:30 KST classifier job latency budget.

### Changed
- **`calc_rsi()` renamed from `_calc_rsi()`** (`market_data.py`): now a public function for reuse in `screener_filters.py` and future stage classifier modules.
- **`save_chart_signals()` extended to 14 positional params** (`db.py`): `$13=high_w`, `$14=volume_w` added with ON CONFLICT upsert.

### Tests
- `test_chart_backtest.py`: 31 tests — no look-ahead slice guard, return calculations, excess return, nearest-price lookup, `_group_metrics` (win rate, avg, median, None exclusion), `compute_metrics` (전체/정배열/일반), `build_summary` (monthly grouping, annual totals), JSON round-trip, `_week_label`.
- `test_screener_filters.py`: Stage 2 filter preset validation.
- `test_news_gating.py`: 5 tests — non-screener signal suppressed, screener signal allowed, empty cache disables gating, partial overlap allows, non-actionable still blocked.
- `test_screener_telegram_regression_1.py`: ISSUE-001/002 regression (ticker code span, backtick escaping).
- `test_screener_telegram_regression_2.py`: ISSUE-003 regression (`target_chat_id` channel broadcast fix).
- `test_chart_screener.py`: 2 new tests for `save_chart_signals` `$13`/`$14` params and backward compatibility.

## [0.4.2.0] - 2026-04-19

### Added
- **Sparklines in HTML screener report** (`generate_html_report.py`): each stock row now shows a 12-week close-price trend as an inline SVG sparkline. Lets you distinguish a fresh breakout from a months-old stale one at a glance, without opening a chart app. No JS or external dependencies — pure SVG `<polyline>`.
- **Sector-grouped view** (`generate_html_report.py`): a second `업종별` section now appears below the 정배열/일반 tables, grouping results by KIND 업종명. Shows at a glance whether a breakout week is broad-based or concentrated in one sector. Uses the `ScreenResult.sector` field already populated — zero new API calls.
- **Ticker resolution diagnostics** (`market_data.py`): a module-level `Counter` now tracks every ticker name that fully fails resolution (all 5 steps). `get_resolution_miss_report(top_n=10)` returns the top misses as a human-readable string. Called at the end of each `collect_job()` cycle — resolution gaps now surface as WARNING log lines in production.

### Changed
- **Step 5 resolution log level** (`market_data.py`): full ticker-miss logging upgraded from `DEBUG` to `WARNING` so it surfaces in production logs without requiring verbose mode.
- **`test_feeds.py` renamed to `scripts/check_feeds.py`**: the RSS connectivity checker is not a pytest test file. Moved to `scripts/` to stop pytest from attempting collection (0 tests found, misleading).

## [0.4.1.0] - 2026-04-19

### Added
- **HTML screener report** (`generate_html_report.py`): every Sunday screener run now writes a self-contained HTML file to `reports/screener/screener_YYYYMMDD_HHMM.html`. You get a clean two-section table (★ 정배열 / 일반), sorted by close price, with 120주선 displayed as `—` when data is insufficient (< 100 bars) — matching the footnote that explains the NaN-pass rule. Sectors default to `기타` when empty. HTML is generated as a pure function (`generate_html(results)`) so it's easy to test and easy to run standalone from the CLI.
- **Design bundle support** (`generate_html_report.py`): the generator loads `web/screener_design/colors_and_type.css` and inlines fonts as base64 when the bundle is present. Falls back to system fonts with a WARNING log if the bundle is missing — no hard dependency.
- **Print styles** (`generate_html_report.py`): the generated HTML includes `@media print` styles (white background, collapsed table borders) so you can print or save as PDF directly from the browser.
- **Scheduler integration** (`run_scheduler.py`): HTML generation runs automatically after every Sunday screener job. Failure is isolated — a broken HTML write does not affect the existing Telegram delivery.

### Tests
- `test_generate_html_report.py`: 10 tests — empty state, ★ 정배열 section rendering, 일반 section absent when all stocks are 정배열, `ma_120w=None` → `—` (not "None"), HTML escaping on stock names, no external stylesheet links, `sector=""` → "기타", close-price sort order, `lang="ko"` attribute, footer footnote.

## [0.4.0.0] - 2026-04-18

### Added
- **Article type classification** (`signal_detector.py`, `db.py`, `telegram_notify.py`): `TradeSignal` now carries an `article_type` field (EARNINGS / MACRO / TECHNICAL / GUIDANCE / SECTOR / OTHER). `SIGNAL_PROMPT` instructs the LLM to classify each article. Type is persisted in `trade_signals.article_type` (DB column added idempotently). Signals include a `TYPE_BADGE` emoji in Telegram messages (🔴 earnings, 🌐 macro, 📊 technical, etc.).
- **Per-article-type hit rate breakdown** (`backtest.py`, `generate_report.py`): backtest report now includes a breakdown of signal accuracy by article type. Identifies which categories produce the highest-conviction signals.
- **Weekly chart screener** (`chart_screener.py`): scans all KOSPI/KOSDAQ stocks (~2,770) every Sunday at 20:30 KST using Ichimoku + 20/60-week MA conditions (6-condition filter). Results stored in `chart_signals` DB table and delivered to Telegram DM + channel simultaneously.
- **`/screener` Telegram command** (`telegram_bot.py`): on-demand access to the latest weekly screening results from DB — no re-scan required.
- **Screener v2: 120-week MA filter** (`chart_screener.py`): `close > 120wMA` required (condition G). Stocks with < 100 weeks of data pass automatically. `ScreenResult` gains `ma_120w` field.
- **Screener v2: KIND sector grouping** (`chart_screener.py`, `telegram_notify.py`): each screened ticker carries its KRX sector from KIND. Sunday messages show top 5 sectors with top 3 stocks per sector.
- **DB migrations** (`db.py`): `chart_signals.sector` (VARCHAR 80), `chart_signals.ma_120w` (FLOAT), `trade_signals.article_type` (VARCHAR 40) — all added idempotently via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- **Signal Cross-Analysis v2: MACD + Bollinger Bands + MA trend layer** (`market_data.py`): `cross_analyze()` now factors in MACD histogram direction, MACD cross (bullish/bearish), Bollinger %B position, and price vs MA20/MA50 alignment. Zero extra API calls — all indicators computed from the existing 1-year daily Close series.
- **Richer Telegram signal messages** (`telegram_notify.py`): each price line now shows price change, RSI, MACD direction (▲▲/▼▼ for crosses), Bollinger %B, and MA20/MA50 position.
- **Fundamental layer: PER/PBR/EPS enrichment** (`market_data.py`): `cross_analyze()` factors in valuation fundamentals sourced from Naver Finance mobile API (no credentials required). `PriceContext` gains `per`, `pbr`, `eps` fields. Fundamental score adjusts signal score by −3 to +2. Startup pre-warms all Korean tickers via 5-worker thread pool.
- **Fundamental display in Telegram** (`telegram_notify.py`): price lines show PER/PBR tokens when noteworthy (`PER:12↓`, `PER:80↑`, `적자`, `PBR:0.6↓`). Neutral companies show no tokens.

### Fixed
- **WATCH signal noise** (`signal_detector.py`): `is_actionable` now requires strength ≥ 3 for WATCH signals (was ≥ 2), reducing false positives from ambiguous news.
- **`_fetch_type_breakdown` SQL** (`backtest.py`): WATCH signals excluded from article type breakdown query — WATCH is a hold signal, not a trading action, and inflated breakdown counts.
- **Ollama model name mismatch** (`summarizer.py`): default `OLLAMA_MODEL` changed from `Qwen3.5-9B:latest` to `qwen3.5:9b` — previous value caused 404 on all `/api/chat` calls.
- **`/help` MarkdownV2 parse error** (`telegram_bot.py`): `<` and `>` in `/volume <종목명|티커>` escaped to `\<` and `\>`.

### Tests (242 total)
- `test_article_type.py`: 19 tests — 17 unit tests for type classification logic, 2 backtest integration tests.
- `test_chart_screener.py`: 31 tests — KIND sector fetch, condition G (120wMA), screener pipeline.
- `test_fundamental.py`: 41 tests — `_parse_naver_value`, `_fundamental_score`, `_fetch_fundamental`, `cross_analyze` average delta, Telegram display.
- `test_macro_signal.py`: 10 new tests — MACD/BB/MA scoring (Group 8), yfinance TA computation (Group 9).
- `test_signal_prompt.py`: 7 tests — WATCH ≥ 3 threshold.
- `test_screener_telegram_regression_1.py`: 6 regression tests — KIND failure warning, sector-grouped format.
- `test_screener_cmd.py`: 4 tests — `/screener` command handler.
- `test_backtest.py`: updated with article type breakdown tests.

## [0.3.0.0] - 2026-04-14

### Added
- **KRX full listing DB** (`krx_sync.py`): fetches all KOSPI/KOSDAQ securities (2,500+ tickers) from KRX public API and upserts into `krx_listings` PostgreSQL table. Table is created automatically on first run. EUC-KR response encoding handled explicitly. KONEX and unsupported market types excluded.
- **Ticker cache singleton** (`ticker_cache.py`): in-memory `TickerCache` loaded at startup and refreshed daily at 20:00 KST. Resolves Korean stock names (including full-length names that exceed KRX's 10-character display limit) and 6-digit short codes to yfinance symbols. Safe to call before `load()` — returns `None` and falls through to static maps.
- **Automatic daily sync** (`run_scheduler.py`): APScheduler job at 20:00 KST (`Asia/Seoul`) syncs `krx_listings` and reloads the cache after market close. Startup sequence runs initial sync before the first article job fires, with `try/finally` so cache reloads from existing DB rows even if the KRX API is down.
- **Regression tests** (`test_krx_sync.py`, `test_ticker_cache_integration.py`): 38 tests covering field parsing, KONEX exclusion, ISU_ABBRV vs ISU_NM name resolution, atomic cache reload, EUC-KR decoding, empty-response error handling, clock skew guard, all-KONEX silent-wipe guard, and real `get_price_context()` integration.
- **Index on `krx_listings.updated_at`** (`db.py`): ensures the daily delisted-stock DELETE is a fast index scan, not a full table scan.

### Fixed
- **Clock skew race condition** (`krx_sync.py`): `sync_start_ts` is now fetched via `SELECT NOW()` from inside the transaction instead of Python's clock. If Python's clock was even 1 ms ahead of Postgres, the DELETE at the end of the transaction would silently delete all just-upserted rows.
- **Silent full-table wipe on all-KONEX API response** (`krx_sync.py`): if all rows in the API response were KONEX or unsupported markets, `executemany()` was a no-op and the subsequent DELETE wiped the entire table. Now raises `ValueError` before the transaction opens.

### Changed
- `market_data.py` `get_price_context()`: `ticker_cache.resolve()` queried before static `YFINANCE_MAP` fallback. Articles mentioning tickers not in the hardcoded map now receive full price context, RSI, and volume signals.
- `volume_pattern.py` `resolve_ticker()`: `ticker_cache.resolve()` queried before `KR_KOSDAQ`/`KR_KOSPI` static dicts. Return type unchanged — still returns `(yfinance_ticker, display_name, market_code)` tuple.
## [0.2.9.0] - 2026-04-18

### Added

- **Fundamental layer: PER/PBR/EPS enrichment** (`market_data.py`): `cross_analyze()` now factors in valuation fundamentals. Data sourced from Naver Finance mobile API (no credentials required; replaces pykrx which requires KRX login under Python 3.14). `PriceContext` gains `per`, `pbr`, `eps` fields. Fundamental score adjusts the signal score by −3 to +2: loss-making tickers (EPS < 0) penalized −2, high PER (>50) or high PBR (>5) penalized −1 each, cheap tickers (PER < 15 or PBR < 1) rewarded +1 each. Multi-ticker score uses average delta (not sum) to prevent one bad ticker from dominating.
- **Fundamental display in Telegram** (`telegram_notify.py`): price lines now show PER/PBR tokens when noteworthy — `PER:12↓` for cheap, `PER:80↑` for expensive, `적자` for loss-making, `PBR:0.6↓` or `PBR:7.0↑` at extremes. Neutral companies show no fundamental tokens, keeping the price line clean for normal cases.
- **Startup pre-warm** (`run_scheduler.py`): all Korean tickers in `YFINANCE_MAP` are pre-warmed via `prewarm_fundamentals()` at startup using a 5-worker thread pool. Avoids 150ms cold-miss latency on first signal of the day.
- **Daily cache** (`market_data.py`): fundamental results cached by date key (`_fund_cache`), reset on process restart. Cache hits cost ~0ms; misses cost ~150ms (Naver API). Thread-safe via `dict.setdefault()`.

### For contributors

- `test_fundamental.py` (41 tests): `_parse_naver_value` (10 edge cases including Korean unit suffixes), `_to_krx_code` (4 cases), `_fundamental_score` (11 cases including boundary values per==15/50, pbr==1/5, eps==0), `_fetch_fundamental` (6 cases with mocked httpx: cache hit, HTTPX_OK=False, success, loss-maker, PER>200 cap, API exception), `cross_analyze` average delta (3 cases), Telegram per_str/pbr_str display (7 cases).

## [0.2.8.0] - 2026-04-17

### Added

- **Signal Cross-Analysis v2: MACD + Bollinger Bands + MA trend layer** (`market_data.py`): `cross_analyze()` now factors in five new technical indicators per ticker — MACD histogram direction, MACD cross (bullish/bearish), Bollinger %B position, and price vs MA20/MA50 alignment. Zero extra API calls: all indicators computed from the same 1-year daily Close series already fetched for RSI. `PriceContext` gains five new Optional fields (`macd_hist`, `macd_cross`, `bb_pct`, `above_ma20`, `above_ma50`) with `None` defaults for full backward compatibility.
- **Richer Telegram signal messages** (`telegram_notify.py`): each price line in a signal alert now shows up to five data points — price change, RSI, MACD direction (▲/▼/▲▲/▼▼ for crosses), Bollinger %B, and MA20/MA50 position (↑↓). All indicators are skipped gracefully if data is unavailable (illiquid tickers, short history).
- **WATCH noise reduction** (`signal_detector.py`): `is_actionable` now requires strength ≥ 3 for WATCH signals (previously ≥ 2), reducing false positives from ambiguous news.

### For contributors

- `TestCrossAnalyzeMACDBBMA` (Group 8, `test_macro_signal.py`): 8 unit tests covering BUY + bullish_cross, BUY + overbought BB (regression guard), BUY + below both MAs, BUY + oversold BB, SELL + bearish_cross, SELL + above both MAs, all-None fields (no scoring change), Telegram price line format.
- `TestFetchYfinanceTAComputation` (Group 9, `test_macro_signal.py`): 2 integration tests — sufficient data (60 bars) produces valid float indicators; insufficient data (10 bars) returns `bb_pct=None` from NaN guard.
- `TestIsActionableThreshold` (`test_signal_prompt.py`): 7 tests for the WATCH ≥ 3 threshold.

## [0.2.7.0] - 2026-04-17

### Added

- **Screener v2: 120-week MA filter (condition G)** (`chart_screener.py`): the weekly breakout screen now requires close price above the 120-week moving average. Stocks with less than 100 weeks of data pass automatically so recently-listed tickers aren't excluded. `ScreenResult` gains a `ma_120w` field showing the computed average (or `None` when data is insufficient). OHLCV fetch extended to 3 years (`period="3y"`) to supply enough bars.
- **Screener v2: KIND sector data** (`chart_screener.py`): each screened ticker now carries its exchange sector (업종) from KIND (한국거래소 기업공시시스템). Fetched at scan time, gracefully skipped on network failure — the screener still runs, just without sector labels.
- **Screener v2: sector-grouped Telegram output** (`telegram_notify.py`): Sunday screener messages now show the top 5 sectors by candidate count, with the top 3 stocks per sector. Previously a flat list of 20 stocks. Sector names longer than 20 characters are truncated. Shows a warning when KIND data was unavailable.
- **DB migration — `sector` and `ma_120w`** (`db.py`): `chart_signals` table gains two columns. `init_db()` adds them idempotently on startup so existing deployments upgrade automatically.

### For contributors

- `TestFetchKindSectorMap` (`test_chart_screener.py`): 3 tests — happy-path HTML parse, KIND down (exception), empty HTML (no `<tr>`).
- `TestConditionG` (`test_chart_screener.py`): 3 tests — G passes (close > 120wMA), G fails (close < 120wMA), NaN-pass (< 100 bars).
- `TestKINDFailureWarning` (`test_screener_telegram_regression_1.py`): 2 tests — warning shown when all sectors empty, absent when sectors populated.
- `TestSectorGroupedFormat` (`test_screener_telegram_regression_1.py`): 4 tests — sector order, 기타 fallback, 20-char truncation, top-3 limit.

## [0.2.6.0] - 2026-04-16

### Added

- **Weekly chart screener** (`chart_screener.py`): scans all KOSPI/KOSDAQ stocks (~2,770) every Sunday at 20:30 KST using Ichimoku + 20/60-week MA conditions (6-condition filter). Results sent to Telegram DM and channel simultaneously. `has_gapjum` flag (20wMA > 60wMA) marks highest-conviction candidates.
- **`/screener` Telegram command** (`telegram_bot.py`): on-demand access to the latest weekly screening results — loads from DB, no re-scan needed. Sends to both DM and channel.
- **`chart_signals` DB table** (`db.py`): stores screener output per ticker per ISO week. `save_chart_signals()` upserts results; `load_chart_signals_latest()` fetches the most recent week's full result set.
- **DM + channel simultaneous screener delivery** (`telegram_notify.py`): `send_weekly_screener()` now always sends to personal DM, and also to the channel when `TELEGRAM_CHANNEL_ID` is set. Previously it was either/or.

### Fixed

- **Ollama model name mismatch** (`summarizer.py`): default `OLLAMA_MODEL` was `Qwen3.5-9B:latest` (uppercase, hyphen) but Ollama's actual model tag is `qwen3.5:9b`. All `/api/chat` calls returned 404. Changed default to `qwen3.5:9b`.
- **`/help` MarkdownV2 parse error** (`telegram_bot.py`): `<` and `>` in `/volume <종목명|티커>` were unescaped, causing Telegram to reject the message with "Character '>' is reserved". Escaped to `\<` and `\>`.

## [0.2.5.0] - 2026-04-16

### Added

- **Article type classification** (`signal_detector.py`): every LLM signal inference call now extracts `article_type` as part of the same JSON response — zero additional API calls. Eight types supported: `earnings`, `ma`, `management`, `analyst`, `regulatory`, `product`, `macro`, `other`.
- **Telegram type badges** (`telegram_notify.py`): actionable signal alerts now show a type emoji badge (📊 earnings, 🤝 M&A, 🔍 analyst, ⚖️ regulatory, 🚀 product, 🌐 macro, 👤 management). Badge is suppressed for `other` to avoid noise.
- **Backtest type breakdown** (`backtest.py`): `/backtest` Telegram report now shows per-article-type hit rates at the 1d checkpoint. Types with fewer than 5 signals are suppressed until enough data accumulates.
- **DB migration** (`db.py`): `ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS article_type VARCHAR(20) DEFAULT 'other'` — idempotent, safe on existing deployments.

### Fixed

- **`send_signal()` esc() missing backtick** (`telegram_notify.py`): local `esc()` in `send_signal` was missing `` ` `` from the escape list, inconsistent with `send_weekly_screener`. Fixes potential MarkdownV2 rendering breakage for tickers or reasons containing backticks.
- **WATCH signals inflated article type hit rates** (`backtest.py`): `_fetch_type_breakdown` included WATCH signals in `COUNT(*)` but WATCH has no directional hit definition. This deflated hit rates for all article types. Filtered to `BUY/SELL` only, matching the existing `_fetch_ticker_breakdown` pattern.
- **`start_crawler.bat` hardcoded user path** (`start_crawler.bat`): paths were hardcoded to `C:\Users\Jin\test_feed` — won't work on any other machine. Replaced with `%~dp0` (relative to the .bat file location).

## [0.2.4.0] - 2026-04-11

### Fixed

- **Ollama empty JSON responses** (`summarizer.py`): `repeat_penalty: 1.3` penalised `{` `"` `:` tokens that already appeared in the *generated output*, making the model unable to produce multi-key JSON. Reset to `1.0` (disabled). Affects both summarisation and signal detection calls.
- **Summariser always empty after thinking block** (`summarizer.py`): `max_tokens=300` was exhausted by Qwen3's ~290-token `<think>` block, leaving no room for the Korean summary. Raised to `600`. Summary failures drop from ~50/day to near zero.
- **Signal detector echoed macro context instead of JSON** (`signal_detector.py`): `SIGNAL_PROMPT` ended with `{macro_section}` — the last text the model saw was "Note: High USD/KRW..." so it continued/echoed that instead of outputting JSON. Added explicit output instruction after the macro section. Fixes `JSON 없음 | raw: Macro context:` parse failures.
- **Silent Ollama model-not-found errors** (`summarizer.py`): Some Ollama versions return HTTP 200 with `{"error": "model not found"}` instead of 4xx. Added `data.get("error")` check before the empty-content path so the error is logged clearly instead of masquerading as two empty-response retries.
- **Misleading empty-response error when Ollama returns thinking content** (`summarizer.py`): When Ollama separates `message.thinking` from `message.content`, an empty content field now logs `think=Nchar` so operators can tune `max_tokens` or verify `think:false` is working.

### Changed

- Removed explicit `frequency_penalty: 0` / `presence_penalty: 0` from `_call_openai_compat` payload — these are defaults and caused `400 Bad Request` on strict OpenAI-compat backends.

### Tests

- Regression tests for all five fixed failure modes (ISSUE-003 through ISSUE-007) in `test_summarizer_regression_1.py` and new `test_signal_prompt.py`.

## [0.2.3.0] - 2026-04-10

### Security

- **[HIGH] Removed hardcoded default PostgreSQL password** (`db.py`): `DB_PASSWORD` now has no fallback — `get_dsn()` raises `RuntimeError` at startup if neither `DB_PASSWORD` nor `DATABASE_URL` is set. Prevents silent use of well-known credentials on misconfigured deployments.
- **[HIGH] Fixed Telegram auth bypass** (`telegram_bot.py`): `_get_allowed_ids()` now raises `RuntimeError` when both `ALLOWED_CHAT_IDS` and `TELEGRAM_CHAT_ID` are unset, instead of returning `set()` which caused the `if allowed and ...` guard to short-circuit and allow all users.
- **[MEDIUM] Removed default PostgreSQL password from `docker-compose.yml`**: `POSTGRES_PASSWORD: ${DB_PASSWORD:-news1234}` → `${DB_PASSWORD}` (no fallback). Container startup now fails explicitly if `DB_PASSWORD` is not in `.env`.
- **[MEDIUM] Bound PostgreSQL port to loopback** (`docker-compose.yml`): `"5432:5432"` → `"127.0.0.1:5432:5432"` — prevents remote access to the DB port on servers without an application-layer firewall.
- **[MEDIUM] Dockerfile runs as non-root**: Added `appuser` system account and `USER appuser` directive — container process no longer runs as root.

### Added

- `.env.example`: documents all required environment variables with placeholder values to prevent misconfiguration.

## [0.2.2.0] - 2026-04-10

### Added
- Backtest Telegram report now shows a random baseline ("랜덤 기준선: X% (BUY) / Y% (SELL)") so CONFIRM hit rates have context — e.g., CONFIRM=62% against a baseline of 54% shows 8pp of real alpha.
- Scheduler startup logs the current KOREA_BASE_RATE value and warns when the `.env` file hasn't been updated in 90+ days, preventing stale macro context from reaching the LLM silently.
- APScheduler jobs now persist to Postgres via SQLAlchemyJobStore — a crash before Sunday 20:00 no longer silently drops the weekly backtest report. Falls back to MemoryJobStore if Postgres is unreachable at startup.
- Backtest baseline and market_baseline key included in JSON and CSV report exports.

### Changed
- Backfill (`backfill_historical`) now caches 365-day yfinance history per (symbol, ISO week), reducing HTTP calls from 1 per signal×ticker to 1 per unique symbol×week. A 500-signal backfill of 3 tickers goes from ~1500 calls to ~(unique symbol × week combos).
- `requirements.txt`: added `sqlalchemy>=2.0.0`, `psycopg2-binary>=2.9.0`; bumped `APScheduler>=3.10.4`.

### Fixed
- `asyncio.get_event_loop()` → `asyncio.get_running_loop()` in cross_analyze call, removing Python 3.12+ DeprecationWarning.
- Cache no longer stores `None` for failed yfinance fetches — transient failures no longer poison all signals in the same ISO week.
- SQLAlchemyJobStore DSN normalization handles both `postgresql://` and `postgres://` (Heroku/Render) via regex, not fragile string replace.

## [0.2.1.0] - 2026-04-06

### Fixed
- Telegram routing (ISSUE-005): all articles — Korean and foreign — now only send when signal is actionable. Previously Korean articles were forwarded unconditionally regardless of signal result.
- LM Studio health check (ISSUE-001): `/v1/models` replaced with a lightweight inference probe, so an unresponsive LM Studio no longer appears alive and blocks Ollama fallback.
- Qwen3 thinking suppression (ISSUE-002): `_call_ollama_native` now prepends `/no_think\n\n` when `enable_thinking=False`, preventing runaway reasoning tokens from Qwen3 models.

### Added
- `requirements.txt` (ISSUE-003): all Python dependencies with minimum versions for reproducible installs.
- Regression test suite: `test_summarizer_regression_1.py` (covers ISSUE-001/002 health-check and thinking-token fixes), `test_telegram_routing.py` (covers ISSUE-005 signal-gated routing for Korean and foreign categories, 4 scenarios).

### Removed
- Dead `tg_send` import alias from `run_scheduler.py` (ISSUE-006) — `send_article` was unused after routing fix.

## [0.2.0.0] - 2026-04-06

### Added
- Korean-language news feeds: 연합뉴스 (economy, market), 한국경제 (economy, finance), 매일경제 (경제, 증권) — 6 new RSS feeds, all category="korea"
- Site-specific HTML parsers for Korean sources: `_parse_yonhap()`, `_parse_hankyung()`, `_parse_mk()` in `article_fetcher.py`
- Korean articles now get full body extraction (not just RSS summary), enabling richer LLM summaries and better signal detection

### Changed
- `summarizer.py` SUMMARY_PROMPT: "English news" → "financial news" so Korean-source articles are processed correctly by Qwen3
- `yna.co.kr/rss/stock.xml` (404) replaced with `/rss/market.xml` (연합뉴스 마켓+ 최신기사)

## [0.1.0.0] - 2026-04-04

### Added
- Cross-analysis backtesting system: track verdict accuracy (CONFIRM/CAUTION/FILTER/NEUTRAL) at 1h/4h/1d/3d checkpoints using yfinance price data
- `/backtest` Telegram bot command: on-demand formatted report with per-verdict hit rates and per-ticker accuracy breakdown
- Weekly automated backtest report every Sunday 20:00 KST via APScheduler CronTrigger
- `backtest_report_telegram()`: MarkdownV2-safe formatter with 48h data freshness warning
- `test_backtest.py`: 12 pytest unit tests covering `_esc()` escaping and `calculate_metrics()` hit-rate logic (BUY/SELL/FILTER/WATCH branches, empty DB, NaN filtering)
- Volume pattern analysis tool (`volume_pattern.py`) for KR/US stocks
- Daily OHLCV export, DB cache check, and batch analysis scripts (`batch_run.py`)
- Telegram channel broadcasting support

### Changed
- `cross_analyze_historical()` now delegates to `cross_analyze()` directly — eliminates DRY drift where backfill used different WATCH threshold (2% vs 0.5%) and missing conflict logic
- All `time.sleep(0.3)` in async functions replaced with `await asyncio.sleep(0.3)`
- WATCH direction `hit_rate` is now `None` (N/A) instead of incorrect unconditional 100%
- `fetch_pending_outcomes` default limit raised from 100 to 500
- Daily log rotation with 14-day retention

### Fixed
- Summarizer repeated output and unclosed thinking blocks
- Signal detector robustness with retry and thinking token cleanup
- NaN handling in backtest metrics

### Removed
- `batchrun.py` — superseded by `batch_run.py`
